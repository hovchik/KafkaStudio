using System.Collections.Concurrent;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Persistence;

namespace KafkaStudio.Search;

/// <summary>A named, ordered sequence of topics a message key is expected to travel through
/// (e.g. orders → payments → shipments).</summary>
public sealed record KeyChain
{
    public required string Name { get; init; }
    public List<string> Topics { get; init; } = new();
}

/// <summary>Persists named <see cref="KeyChain"/>s (<c>key-chains.json</c>).</summary>
public static class KeyChainStore
{
    private const string FileName = "key-chains.json";

    public static IReadOnlyList<KeyChain> Load() =>
        JsonFileStore.Load<List<KeyChain>>(FileName, new List<KeyChain>())
            .Where(c => !string.IsNullOrWhiteSpace(c.Name))
            .Select(c => c with { Topics = (c.Topics ?? new()).Where(t => !string.IsNullOrWhiteSpace(t)).ToList() })
            .GroupBy(c => c.Name, StringComparer.OrdinalIgnoreCase).Select(g => g.First())
            .ToList();

    /// <summary>Returns an error message on failure, or null on success.</summary>
    public static string? Save(IEnumerable<KeyChain> chains) => JsonFileStore.TrySave(FileName, chains.ToList());
}

public enum ChainStepStatus
{
    /// <summary>The key was found in this topic.</summary>
    Found,
    /// <summary>The topic was read but holds no message with the key.</summary>
    Missing,
    /// <summary>The topic could not be read.</summary>
    Unreadable
}

/// <summary>The outcome for one topic of the chain.</summary>
public sealed record ChainStepResult
{
    public required int Index { get; init; }
    public required string Topic { get; init; }
    public required ChainStepStatus Status { get; init; }

    /// <summary>All messages with the key in this topic, oldest first (capped at <see cref="KeyChainAnalyzer.MaxMatchesPerTopic"/>).</summary>
    public IReadOnlyList<KafkaMessage> Matches { get; init; } = Array.Empty<KafkaMessage>();

    public string? Error { get; init; }

    /// <summary>Time since the previous topic of the chain that had the key (null for the first found step or when not found).</summary>
    public TimeSpan? SincePrevious { get; init; }

    /// <summary>Time since the first topic of the chain that had the key.</summary>
    public TimeSpan? SinceStart { get; init; }

    /// <summary>The key reached this topic earlier than the previous one - the pipeline order isn't what the chain says.</summary>
    public bool OutOfOrder => SincePrevious is { } d && d < TimeSpan.Zero;

    public KafkaMessage? First => Matches.Count > 0 ? Matches[0] : null;
    public int Count => Matches.Count;
    public bool Found => Status == ChainStepStatus.Found;
    public bool IsMissing => Status == ChainStepStatus.Missing;
    public bool IsUnreadable => Status == ChainStepStatus.Unreadable;

    public string StatusText => Status switch
    {
        ChainStepStatus.Found => OutOfOrder ? "found · out of order" : "found",
        ChainStepStatus.Missing => "missing",
        _ => "unreadable"
    };

    public string TimeText => First is { } m ? m.Timestamp.ToLocalTime().ToString("MM-dd HH:mm:ss.fff") : "";
    public string SincePreviousText => SincePrevious is { } d ? (OutOfOrder ? "" : "+") + TraceHop.FormatSpan(d) + (OutOfOrder ? " ⚠" : "") : "";
    public string SinceStartText => SinceStart is { } d && d != TimeSpan.Zero ? "+" + TraceHop.FormatSpan(d) : "";
    public string PositionText => First is { } m ? $"#{m.Partition}@{m.Offset}" : "";
    public string CountText => Matches.Count > 1 ? $"×{Matches.Count}" : "";
}

public sealed record ChainResult
{
    public required string ChainName { get; init; }
    public required string Key { get; init; }
    public required IReadOnlyList<ChainStepResult> Steps { get; init; }
    public required ScanSummary Scan { get; init; }

    public int FoundCount => Steps.Count(s => s.Found);
    public bool Complete => Steps.Count > 0 && FoundCount == Steps.Count;

    /// <summary>First topic of the chain without the key, if the key got anywhere at all.</summary>
    public ChainStepResult? FirstGap => FoundCount == 0 ? null : Steps.FirstOrDefault(s => !s.Found);

    /// <summary>First found topic to last found topic.</summary>
    public TimeSpan? EndToEnd => Steps.LastOrDefault(s => s.Found)?.SinceStart;

    public string Summary
    {
        get
        {
            if (Steps.Count == 0) return "The chain has no topics.";
            if (FoundCount == 0) return $"'{Key}' was not found in any of the {Steps.Count} topic(s) of '{ChainName}'.{Scan.FailedSuffix}";
            var text = Complete
                ? $"'{Key}' went through all {Steps.Count} topic(s) of '{ChainName}'"
                : $"'{Key}' is in {FoundCount} of {Steps.Count} topic(s) of '{ChainName}' - missing from {string.Join(", ", Steps.Where(s => !s.Found).Select(s => s.Topic))}";
            if (EndToEnd is { } span && FoundCount > 1) text += $"; first to last took {TraceHop.FormatSpan(span)}";
            if (Steps.Any(s => s.OutOfOrder)) text += "; some topics saw it out of order";
            return text + "." + Scan.FailedSuffix;
        }
    }
}

/// <summary>
/// Checks, topic by topic along a <see cref="KeyChain"/>, whether a message with a given key exists and when it
/// arrived. Strictly read-only: it only consumes (with throwaway consumer groups, see <see cref="TopicScanner"/>).
/// </summary>
public static class KeyChainAnalyzer
{
    public const int MaxMatchesPerTopic = 200;

    public static async Task<ChainResult> RunAsync(
        IKafkaGateway gateway,
        KeyChain chain,
        string key,
        SearchRange range,
        CancellationToken cancellationToken = default,
        ScanProgress? progress = null,
        DateTimeOffset? now = null)
    {
        var topics = chain.Topics.Distinct(StringComparer.Ordinal).ToList();
        var found = new ConcurrentDictionary<string, ConcurrentBag<KafkaMessage>>(StringComparer.Ordinal);
        foreach (var t in topics) found[t] = new ConcurrentBag<KafkaMessage>();

        var scan = await TopicScanner.ScanAsync(gateway, topics, range, (topic, message) =>
        {
            if (!string.Equals(message.Key, key, StringComparison.Ordinal)) return ScanDecision.Continue;
            var bag = found[topic];
            bag.Add(message);
            return bag.Count >= MaxMatchesPerTopic ? ScanDecision.StopTopic : ScanDecision.Continue;
        }, cancellationToken, progress, now: now).ConfigureAwait(false);

        return Build(chain.Name, key, topics, found.ToDictionary(p => p.Key, p => (IReadOnlyCollection<KafkaMessage>)p.Value), scan);
    }

    /// <summary>Turns the per-topic matches into the ordered step results with their time deltas.</summary>
    public static ChainResult Build(string chainName, string key, IReadOnlyList<string> topics,
        IReadOnlyDictionary<string, IReadOnlyCollection<KafkaMessage>> matches, ScanSummary scan)
    {
        var steps = new List<ChainStepResult>(topics.Count);
        KafkaMessage? start = null, previous = null;
        for (var i = 0; i < topics.Count; i++)
        {
            var topic = topics[i];
            var list = (matches.TryGetValue(topic, out var m) ? m : Array.Empty<KafkaMessage>())
                .OrderBy(x => x.Timestamp).ThenBy(x => x.Partition).ThenBy(x => x.Offset).ToList();
            if (list.Count == 0)
            {
                var failed = scan.FailedTopics.TryGetValue(topic, out var error);
                steps.Add(new ChainStepResult
                {
                    Index = i, Topic = topic,
                    Status = failed ? ChainStepStatus.Unreadable : ChainStepStatus.Missing,
                    Error = failed ? error : null
                });
                continue;
            }

            var first = list[0];
            steps.Add(new ChainStepResult
            {
                Index = i, Topic = topic, Status = ChainStepStatus.Found, Matches = list,
                SincePrevious = previous is null ? null : first.Timestamp - previous.Timestamp,
                SinceStart = start is null ? TimeSpan.Zero : first.Timestamp - start.Timestamp
            });
            start ??= first;
            previous = first;
        }
        return new ChainResult { ChainName = chainName, Key = key, Steps = steps, Scan = scan };
    }
}

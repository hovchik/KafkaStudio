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

    /// <summary>"orders → payments → shipments" for chain lists.</summary>
    [System.Text.Json.Serialization.JsonIgnore]
    public string PathText => Topics.Count == 0 ? "(no topics)" : string.Join(" → ", Topics);

    [System.Text.Json.Serialization.JsonIgnore]
    public string CountText => Topics.Count == 1 ? "1 topic" : $"{Topics.Count} topics";
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

    /// <summary>1-based position in the chain, as shown in the results table.</summary>
    public int Number => Index + 1;
    public required string Topic { get; init; }
    public required ChainStepStatus Status { get; init; }

    /// <summary>All messages with the key in this topic, oldest first (capped at <see cref="KeyChainAnalyzer.MaxMatchesPerTopic"/>).</summary>
    public IReadOnlyList<KafkaMessage> Matches { get; init; } = Array.Empty<KafkaMessage>();

    public string? Error { get; init; }

    /// <summary>Time since the previous topic of the chain that had the key (null for the first found step or when not found).</summary>
    public TimeSpan? SincePrevious { get; init; }

    /// <summary>Time since the first topic of the chain that had the key.</summary>
    public TimeSpan? SinceStart { get; init; }

    /// <summary>No message in this topic is at or after the message that satisfied the earlier steps - the pipeline
    /// order isn't what the chain says.</summary>
    public bool OutOfOrder { get; init; }

    /// <summary>The message that represents this step: the earliest match at or after the previous step's message
    /// (so an early duplicate or retry in an upstream topic doesn't break the order), or the earliest match when none is.</summary>
    public KafkaMessage? Chosen { get; init; }

    public KafkaMessage? First => Chosen ?? (Matches.Count > 0 ? Matches[0] : null);
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

public enum SequenceVerdict
{
    /// <summary>The text was found in no topic at all.</summary>
    NotFound,
    /// <summary>Found in every topic, each no earlier than the one before it in the chain.</summary>
    Ok,
    /// <summary>A topic is missing, unreadable, or saw the text before an earlier topic of the chain did.</summary>
    Broken
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

    /// <summary>The first step that breaks the chain's order: a topic without the text, or one that saw it before the previous one.</summary>
    public ChainStepResult? BrokenStep => FoundCount == 0 ? null : Steps.FirstOrDefault(s => !s.Found || s.OutOfOrder);

    public SequenceVerdict Verdict => Steps.Count == 0 || FoundCount == 0 ? SequenceVerdict.NotFound
        : BrokenStep is null ? SequenceVerdict.Ok : SequenceVerdict.Broken;

    /// <summary>The one-line answer: "Sequence OK" / "Sequence broken at step 3 (shipments): ..." / "Not found".</summary>
    public string VerdictText
    {
        get
        {
            if (Steps.Count == 0) return "The chain has no topics.";
            if (Verdict == SequenceVerdict.NotFound) return $"Not found - '{Key}' is in none of the {Steps.Count} topic(s) of '{ChainName}'.";
            if (BrokenStep is not { } step)
            {
                var text = $"Sequence OK - '{Key}' reached all {Steps.Count} topic(s) of '{ChainName}' in order";
                return Steps.Count > 1 && EndToEnd is { } span ? text + $", first to last in {TraceHop.FormatSpan(span)}" : text;
            }
            var before = step.Index > 0 ? $" after step {step.Index} ({Steps[step.Index - 1].Topic})" : "";
            var why = step.Status switch
            {
                ChainStepStatus.Missing => $"not found in {step.Topic}",
                ChainStepStatus.Unreadable => $"{step.Topic} could not be read",
                _ => $"seen in {step.Topic} {TraceHop.FormatSpan(step.SincePrevious ?? TimeSpan.Zero).TrimStart('-')} before it reached {Steps.Take(step.Index).Last(s => s.Found).Topic}"
            };
            return $"Sequence broken at step {step.Index + 1} ({step.Topic}) - {why}{(step.Status == ChainStepStatus.Found ? "" : before)}";
        }
    }

    /// <summary>What <see cref="Summary"/> says beyond <see cref="VerdictText"/> (null when nothing): for a screen that
    /// already shows the verdict on its own line.</summary>
    public string? Details
    {
        get
        {
            var rest = Summary.StartsWith(VerdictText, StringComparison.Ordinal)
                ? Summary[VerdictText.Length..] : Summary;
            rest = rest.TrimStart('.', ';', ' ').Trim();
            return rest.Length == 0 ? null : rest;
        }
    }

    public string Summary
    {
        get
        {
            if (Steps.Count == 0 || FoundCount == 0) return VerdictText + Scan.FailedSuffix;
            var text = VerdictText;
            if (Verdict == SequenceVerdict.Broken)
            {
                text += $". Found in {FoundCount} of {Steps.Count} topic(s)";
                var missing = Steps.Where(s => !s.Found).Select(s => s.Topic).ToList();
                if (missing.Count > 0) text += $"; missing from {string.Join(", ", missing)}";
                if (EndToEnd is { } span && FoundCount > 1) text += $"; first to last took {TraceHop.FormatSpan(span)}";
                if (Steps.Any(s => s.OutOfOrder)) text += "; some topics saw it out of order";
            }
            return text + "." + Scan.FailedSuffix;
        }
    }
}

/// <summary>
/// Checks, topic by topic along a <see cref="KeyChain"/>, whether a message containing some text (a key, an id, any word) exists and when it
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
        DateTimeOffset? now = null,
        bool caseSensitive = false)
    {
        var topics = chain.Topics.Distinct(StringComparer.Ordinal).ToList();
        var found = new ConcurrentDictionary<string, ConcurrentBag<KafkaMessage>>(StringComparer.Ordinal);
        var comparison = caseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;
        foreach (var t in topics) found[t] = new ConcurrentBag<KafkaMessage>();

        var scan = await TopicScanner.ScanAsync(gateway, topics, range, (topic, message) =>
        {
            if (!Matches(message, key, comparison)) return ScanDecision.Continue;
            var bag = found[topic];
            bag.Add(message);
            return bag.Count >= MaxMatchesPerTopic ? ScanDecision.StopTopic : ScanDecision.Continue;
        }, cancellationToken, progress, now: now).ConfigureAwait(false);

        return Build(chain.Name, key, topics, found.ToDictionary(p => p.Key, p => (IReadOnlyCollection<KafkaMessage>)p.Value), scan);
    }

    /// <summary>True when <paramref name="text"/> appears anywhere in the message: its key, any header
    /// (name or value) or its value.</summary>
    public static bool Matches(KafkaMessage message, string text, StringComparison comparison = StringComparison.OrdinalIgnoreCase)
    {
        if (message.Key?.Contains(text, comparison) == true) return true;
        if (message.Value?.Contains(text, comparison) == true) return true;
        foreach (var header in message.Headers)
        {
            if (header.Key.Contains(text, comparison) || header.Value?.Contains(text, comparison) == true) return true;
        }
        return false;
    }

    /// <summary>Turns the per-topic matches into the ordered step results with their time deltas.</summary>
    public static ChainResult Build(string chainName, string key, IReadOnlyList<string> topics,
        IReadOnlyDictionary<string, IReadOnlyCollection<KafkaMessage>> matches, ScanSummary scan)
    {
        var steps = new List<ChainStepResult>(topics.Count);
        KafkaMessage? start = null, previous = null, watermark = null;
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

            // Earliest match at or after the last message that kept the sequence intact; if there is none this
            // topic saw the text too early and the sequence is broken here.
            var inOrder = watermark is null ? list[0] : list.FirstOrDefault(x => x.Timestamp >= watermark.Timestamp);
            var chosen = inOrder ?? list[0];
            steps.Add(new ChainStepResult
            {
                Index = i, Topic = topic, Status = ChainStepStatus.Found, Matches = list, Chosen = chosen,
                OutOfOrder = inOrder is null,
                SincePrevious = previous is null ? null : chosen.Timestamp - previous.Timestamp,
                SinceStart = start is null ? TimeSpan.Zero : chosen.Timestamp - start.Timestamp
            });
            start ??= chosen;
            previous = chosen;
            if (inOrder is not null) watermark = chosen;
        }
        return new ChainResult { ChainName = chainName, Key = key, Steps = steps, Scan = scan };
    }
}

using System.Collections.Concurrent;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Search;

public sealed record SearchRequest
{
    public required IReadOnlyCollection<string> Topics { get; init; }
    public required MessageQuery Query { get; init; }
    public SearchRange Range { get; init; } = SearchRange.All;

    /// <summary>"Does it exist?" mode: stop reading a topic at its first match.</summary>
    public bool FirstHitPerTopic { get; init; }

    public int MaxHits { get; init; } = 5_000;
}

/// <summary>Per-topic outcome of a search - also the answer to "does it exist on this topic?".</summary>
public sealed record TopicSearchOutcome(string Topic, int Hits, KafkaMessage? FirstHit, bool Failed, string? Error);

public sealed record SearchResult
{
    public required IReadOnlyList<KafkaMessage> Hits { get; init; }
    public required IReadOnlyList<TopicSearchOutcome> Topics { get; init; }
    public required ScanSummary Scan { get; init; }
    public required bool Truncated { get; init; }

    public int TopicsWithHits => Topics.Count(t => t.Hits > 0);

    /// <summary>One-line answer: "Found on 3 of 48 topics (…)" / "Not found in 48 topics (scanned 1,204 messages)".</summary>
    public string Summary(bool existenceCheck)
    {
        var scanned = $"scanned {Scan.MessagesScanned:N0} message(s)";
        if (Hits.Count == 0)
        {
            var incomplete = Scan.Cancelled ? " - search was cancelled, so this is incomplete" : "";
            return $"Not found in {Scan.TopicsTotal} topic(s) ({scanned}){incomplete}.{Scan.FailedSuffix}";
        }
        if (existenceCheck)
        {
            var where = string.Join(", ", Topics.Where(t => t.FirstHit is not null).OrderBy(t => t.FirstHit!.Timestamp).Take(5)
                .Select(t => $"{t.Topic} (#{t.FirstHit!.Partition}@{t.FirstHit.Offset}, {t.FirstHit.Timestamp:yyyy-MM-dd HH:mm:ss})"));
            var more = TopicsWithHits > 5 ? $" and {TopicsWithHits - 5} more" : "";
            return $"Found on {TopicsWithHits} of {Scan.TopicsTotal} topic(s): {where}{more} - {scanned}.{Scan.FailedSuffix}";
        }
        var truncated = Truncated ? $" - stopped at {Hits.Count:N0} matches, narrow the search" : "";
        var cancelled = Scan.Cancelled ? " (cancelled)" : "";
        return $"Found {Hits.Count:N0} message(s) on {TopicsWithHits} of {Scan.TopicsTotal} topic(s){truncated}{cancelled} - {scanned}.{Scan.FailedSuffix}";
    }
}

/// <summary>Runs a <see cref="MessageQuery"/> across topics (the engine behind "Search" and "Exists?").</summary>
public static class MessageSearch
{
    /// <param name="onHit">Optional streaming callback (called from background threads) for live results.</param>
    public static async Task<SearchResult> RunAsync(
        IKafkaGateway gateway,
        SearchRequest request,
        CancellationToken cancellationToken = default,
        ScanProgress? progress = null,
        Action<KafkaMessage>? onHit = null,
        DateTimeOffset? now = null)
    {
        var hits = new ConcurrentQueue<KafkaMessage>();
        var perTopic = new ConcurrentDictionary<string, (int Count, KafkaMessage? First)>(StringComparer.Ordinal);
        var total = 0;
        var truncated = 0;

        var scan = await TopicScanner.ScanAsync(gateway, request.Topics, request.Range, (topic, message) =>
        {
            if (!request.Query.Matches(message)) return ScanDecision.Continue;

            if (Interlocked.Increment(ref total) > request.MaxHits)
            {
                Interlocked.Exchange(ref truncated, 1);
                return ScanDecision.StopAll;
            }

            hits.Enqueue(message);
            perTopic.AddOrUpdate(topic, _ => (1, message), (_, existing) => (existing.Count + 1, existing.First ?? message));
            onHit?.Invoke(message);
            return request.FirstHitPerTopic ? ScanDecision.StopTopic : ScanDecision.Continue;
        }, cancellationToken, progress, now: now).ConfigureAwait(false);

        var outcomes = request.Topics.Distinct(StringComparer.Ordinal).Select(topic =>
        {
            var failed = scan.FailedTopics.TryGetValue(topic, out var error);
            return perTopic.TryGetValue(topic, out var found)
                ? new TopicSearchOutcome(topic, found.Count, found.First, failed, error)
                : new TopicSearchOutcome(topic, 0, null, failed, error);
        }).OrderByDescending(o => o.Hits).ThenBy(o => o.Topic, StringComparer.OrdinalIgnoreCase).ToList();

        return new SearchResult
        {
            Hits = hits.ToList(),
            Topics = outcomes,
            Scan = scan,
            Truncated = truncated == 1
        };
    }
}

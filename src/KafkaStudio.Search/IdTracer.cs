using System.Collections.Concurrent;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Search;

/// <summary>One message on an id's journey, with the time since the previous hop and since the first one.</summary>
public sealed record TraceHop(int Index, KafkaMessage Message, TimeSpan SincePrevious, TimeSpan SinceFirst)
{
    public string SincePreviousText => Index == 0 ? "start" : "+" + FormatSpan(SincePrevious);
    public string SinceFirstText => Index == 0 ? "" : "+" + FormatSpan(SinceFirst);

    public static string FormatSpan(TimeSpan span)
    {
        var negative = span < TimeSpan.Zero;
        span = span.Duration();
        var text = span.TotalSeconds < 1 ? $"{span.TotalMilliseconds:0}ms"
            : span.TotalMinutes < 1 ? $"{span.TotalSeconds:0.###}s"
            : span.TotalHours < 1 ? $"{(int)span.TotalMinutes}m {span.Seconds}s"
            : span.TotalDays < 1 ? $"{(int)span.TotalHours}h {span.Minutes}m"
            : $"{(int)span.TotalDays}d {span.Hours}h";
        return negative ? "-" + text : text;
    }
}

public sealed record TraceResult
{
    public required string Id { get; init; }
    public required IReadOnlyList<TraceHop> Hops { get; init; }
    public required ScanSummary Scan { get; init; }
    public required bool Truncated { get; init; }

    public TimeSpan TotalSpan => Hops.Count == 0 ? TimeSpan.Zero : Hops[^1].SinceFirst;

    public string Summary => Hops.Count == 0
        ? $"'{Id}' was not found in {Scan.TopicsTotal} topic(s) (scanned {Scan.MessagesScanned:N0} message(s)).{Scan.FailedSuffix}"
        : $"'{Id}' appears in {Hops.Count:N0} message(s) on {Hops.Select(h => h.Message.Topic).Distinct().Count()} topic(s), " +
          $"spanning {TraceHop.FormatSpan(TotalSpan)}{(Truncated ? " (first matches only - truncated)" : "")}.{Scan.FailedSuffix}";

    /// <summary>Topic → topic transitions in time order ("orders → payments → shipments").</summary>
    public string Path => string.Join(" → ", Hops.Select(h => h.Message.Topic)
        .Aggregate(new List<string>(), (acc, t) => { if (acc.Count == 0 || acc[^1] != t) acc.Add(t); return acc; }));
}

/// <summary>
/// Follows one entity through a pipeline: every message carrying an id (in its key, a header, or
/// anywhere in its value - or in a specific field) across a set of topics, as a timeline.
/// </summary>
public static class IdTracer
{
    public const int MaxHops = 5_000;

    public static async Task<TraceResult> RunAsync(
        IKafkaGateway gateway,
        IReadOnlyCollection<string> topics,
        string id,
        FieldSelector field,
        SearchRange range,
        bool caseSensitive = true,
        CancellationToken cancellationToken = default,
        ScanProgress? progress = null,
        DateTimeOffset? now = null)
    {
        var matcher = new IdMatcher(new[] { id }, field, caseSensitive);
        var found = new ConcurrentBag<KafkaMessage>();
        var count = 0;
        var truncated = 0;

        var scan = await TopicScanner.ScanAsync(gateway, topics, range, (_, message) =>
        {
            if (matcher.Match(message).Count == 0) return ScanDecision.Continue;
            if (Interlocked.Increment(ref count) > MaxHops)
            {
                Interlocked.Exchange(ref truncated, 1);
                return ScanDecision.StopAll;
            }
            found.Add(message);
            return ScanDecision.Continue;
        }, cancellationToken, progress, now: now).ConfigureAwait(false);

        return new TraceResult { Id = id.Trim(), Hops = BuildTimeline(found), Scan = scan, Truncated = truncated == 1 };
    }

    public static IReadOnlyList<TraceHop> BuildTimeline(IEnumerable<KafkaMessage> messages)
    {
        var ordered = messages
            .OrderBy(m => m.Timestamp)
            .ThenBy(m => m.Topic, StringComparer.Ordinal)
            .ThenBy(m => m.Partition)
            .ThenBy(m => m.Offset)
            .ToList();
        var hops = new List<TraceHop>(ordered.Count);
        for (var i = 0; i < ordered.Count; i++)
        {
            var sincePrevious = i == 0 ? TimeSpan.Zero : ordered[i].Timestamp - ordered[i - 1].Timestamp;
            hops.Add(new TraceHop(i, ordered[i], sincePrevious, ordered[i].Timestamp - ordered[0].Timestamp));
        }
        return hops;
    }
}

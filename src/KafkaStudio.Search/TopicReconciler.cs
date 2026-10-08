using System.Collections.Concurrent;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Search;

public enum ReconcileStatus { OnlyInA, OnlyInB, Matched, Mismatched }

/// <summary>One join value (e.g. an order id) and how it looks on each side.</summary>
public sealed record ReconcileRow
{
    public required string JoinValue { get; init; }
    public required ReconcileStatus Status { get; init; }
    public KafkaMessage? A { get; init; }
    public KafkaMessage? B { get; init; }
    public int CountA { get; init; }
    public int CountB { get; init; }

    /// <summary>"$.amount: 10 ≠ 12; $.currency: EUR ≠ USD" for mismatched rows.</summary>
    public string Differences { get; init; } = "";

    /// <summary>Time from the first A message to the first B message (null unless both exist).</summary>
    public TimeSpan? Latency => A is not null && B is not null ? B.Timestamp - A.Timestamp : null;

    public string LatencyText => Latency is { } l ? TraceHop.FormatSpan(l) : "";

    public string StatusText => Status switch
    {
        ReconcileStatus.OnlyInA => "only in A",
        ReconcileStatus.OnlyInB => "only in B",
        ReconcileStatus.Matched => "matched",
        _ => "different"
    };
}

public sealed record ReconcileRequest
{
    public required string TopicA { get; init; }
    public required FieldSelector JoinA { get; init; }
    public required string TopicB { get; init; }
    public required FieldSelector JoinB { get; init; }

    /// <summary>Fields compared between matched pairs (same selector read on both sides). Empty = only
    /// check presence.</summary>
    public IReadOnlyList<FieldSelector> CompareFields { get; init; } = Array.Empty<FieldSelector>();

    public SearchRange Range { get; init; } = SearchRange.All;
    public bool CaseSensitive { get; init; } = true;
}

public sealed record ReconcileResult
{
    public required IReadOnlyList<ReconcileRow> Rows { get; init; }
    public required ScanSummary Scan { get; init; }

    /// <summary>Messages whose join field was absent, per side.</summary>
    public required int UnjoinableA { get; init; }
    public required int UnjoinableB { get; init; }

    public int OnlyInA => Rows.Count(r => r.Status == ReconcileStatus.OnlyInA);
    public int OnlyInB => Rows.Count(r => r.Status == ReconcileStatus.OnlyInB);
    public int Matched => Rows.Count(r => r.Status == ReconcileStatus.Matched);
    public int Mismatched => Rows.Count(r => r.Status == ReconcileStatus.Mismatched);

    public TimeSpan? AverageLatency
    {
        get
        {
            var latencies = Rows.Where(r => r.Latency is not null).Select(r => r.Latency!.Value.TotalMilliseconds).ToList();
            return latencies.Count == 0 ? null : TimeSpan.FromMilliseconds(latencies.Average());
        }
    }

    public TimeSpan? MaxLatency
    {
        get
        {
            var latencies = Rows.Where(r => r.Latency is not null).Select(r => r.Latency!.Value).ToList();
            return latencies.Count == 0 ? null : latencies.Max();
        }
    }

    public string Summary
    {
        get
        {
            var latency = AverageLatency is { } avg
                ? $" · A→B latency avg {TraceHop.FormatSpan(avg)}, max {TraceHop.FormatSpan(MaxLatency!.Value)}"
                : "";
            var unjoinable = UnjoinableA + UnjoinableB > 0 ? $" · {UnjoinableA + UnjoinableB:N0} message(s) without the join field" : "";
            return $"{Matched:N0} matched, {Mismatched:N0} different, {OnlyInA:N0} only in A, {OnlyInB:N0} only in B{latency}{unjoinable}" +
                   $" - scanned {Scan.MessagesScanned:N0} message(s){(Scan.Cancelled ? " (cancelled - incomplete)" : "")}.{Scan.FailedSuffix}";
        }
    }

    public string ToCsv() => Csv.Write(
        new[] { "join_value", "status", "count_a", "count_b", "a_position", "b_position", "a_timestamp", "b_timestamp", "latency_ms", "differences" },
        Rows.Select(r => new[]
        {
            r.JoinValue, r.StatusText, r.CountA.ToString(), r.CountB.ToString(),
            r.A is null ? "" : $"{r.A.Partition}@{r.A.Offset}", r.B is null ? "" : $"{r.B.Partition}@{r.B.Offset}",
            r.A?.Timestamp.ToString("O") ?? "", r.B?.Timestamp.ToString("O") ?? "",
            r.Latency is { } l ? ((long)l.TotalMilliseconds).ToString() : "", r.Differences
        }));
}

/// <summary>
/// Joins two topics on a field and reports what exists on only one side, what matches, and (with
/// compare fields) which pairs differ - "was every order.created followed by a payment?", answered for
/// historical data rather than only live traffic.
/// </summary>
public static class TopicReconciler
{
    private sealed class Side
    {
        public readonly ConcurrentDictionary<string, (int Count, KafkaMessage First)> ByJoin;
        public int Unjoinable;

        public Side(bool caseSensitive) =>
            ByJoin = new ConcurrentDictionary<string, (int, KafkaMessage)>(caseSensitive ? StringComparer.Ordinal : StringComparer.OrdinalIgnoreCase);
    }

    public static async Task<ReconcileResult> RunAsync(
        IKafkaGateway gateway,
        ReconcileRequest request,
        CancellationToken cancellationToken = default,
        ScanProgress? progress = null,
        DateTimeOffset? now = null)
    {
        // Reconciling a topic with itself would match every row with itself - never what was meant.
        if (request.TopicA == request.TopicB) throw new ArgumentException("the two topics must differ", nameof(request));

        var a = new Side(request.CaseSensitive);
        var b = new Side(request.CaseSensitive);
        var topics = new[] { request.TopicA, request.TopicB };

        var scan = await TopicScanner.ScanAsync(gateway, topics, request.Range, (topic, message) =>
        {
            if (topic == request.TopicA) Add(a, request.JoinA, message);
            if (topic == request.TopicB) Add(b, request.JoinB, message);
            return ScanDecision.Continue;
        }, cancellationToken, progress, now: now).ConfigureAwait(false);

        var comparer = request.CaseSensitive ? StringComparer.Ordinal : StringComparer.OrdinalIgnoreCase;
        var comparison = request.CaseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;
        var rows = new List<ReconcileRow>();
        foreach (var (join, left) in a.ByJoin)
        {
            if (b.ByJoin.TryGetValue(join, out var right))
            {
                var differences = Compare(left.First, right.First, request.CompareFields, comparison);
                rows.Add(new ReconcileRow
                {
                    JoinValue = join,
                    Status = differences.Length == 0 ? ReconcileStatus.Matched : ReconcileStatus.Mismatched,
                    A = left.First, B = right.First, CountA = left.Count, CountB = right.Count,
                    Differences = differences
                });
            }
            else
            {
                rows.Add(new ReconcileRow { JoinValue = join, Status = ReconcileStatus.OnlyInA, A = left.First, CountA = left.Count });
            }
        }
        foreach (var (join, right) in b.ByJoin)
        {
            if (!a.ByJoin.ContainsKey(join))
            {
                rows.Add(new ReconcileRow { JoinValue = join, Status = ReconcileStatus.OnlyInB, B = right.First, CountB = right.Count });
            }
        }

        // Problems first: only-in-A (never arrived), then only-in-B, then different, then matched.
        var ordered = rows
            .OrderBy(r => r.Status switch
            {
                ReconcileStatus.OnlyInA => 0,
                ReconcileStatus.OnlyInB => 1,
                ReconcileStatus.Mismatched => 2,
                _ => 3
            })
            .ThenBy(r => r.JoinValue, comparer)
            .ToList();

        return new ReconcileResult { Rows = ordered, Scan = scan, UnjoinableA = a.Unjoinable, UnjoinableB = b.Unjoinable };
    }

    private static void Add(Side side, FieldSelector join, KafkaMessage message)
    {
        var value = join.Read(message)?.Trim();
        if (string.IsNullOrEmpty(value))
        {
            Interlocked.Increment(ref side.Unjoinable);
            return;
        }
        side.ByJoin.AddOrUpdate(value, _ => (1, message), (_, existing) =>
            (existing.Count + 1, message.Timestamp < existing.First.Timestamp ? message : existing.First));
    }

    private static string Compare(KafkaMessage a, KafkaMessage b, IReadOnlyList<FieldSelector> fields, StringComparison comparison)
    {
        var differences = new List<string>();
        foreach (var field in fields)
        {
            // Trimmed and under the request's case rule, like the join values.
            var left = field.Read(a)?.Trim();
            var right = field.Read(b)?.Trim();
            if (!string.Equals(left, right, comparison))
            {
                differences.Add($"{field.Label}: {Short(left)} ≠ {Short(right)}");
            }
        }
        return string.Join("; ", differences);
    }

    private static string Short(string? value) =>
        value is null ? "(missing)" : value.Length > 60 ? value[..60] + "…" : value;
}

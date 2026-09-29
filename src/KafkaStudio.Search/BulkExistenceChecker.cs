using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Search;

/// <summary>Where (and how often) one id from a bulk check was found.</summary>
public sealed class IdPresence
{
    private readonly object _gate = new();
    private readonly SortedSet<string> _topics = new(StringComparer.Ordinal);

    public required string Id { get; init; }
    public int Count { get; private set; }
    public KafkaMessage? First { get; private set; }
    public KafkaMessage? Last { get; private set; }

    public bool Found => Count > 0;
    public bool IsDuplicated => Count > 1;
    public IReadOnlyCollection<string> Topics { get { lock (_gate) return _topics.ToList(); } }
    public string TopicsText => string.Join(", ", Topics);
    public string Status => Count switch { 0 => "missing", 1 => "found", _ => $"found ×{Count}" };

    internal void Add(KafkaMessage message)
    {
        lock (_gate)
        {
            Count++;
            _topics.Add(message.Topic);
            if (First is null || message.Timestamp < First.Timestamp) First = message;
            if (Last is null || message.Timestamp > Last.Timestamp) Last = message;
        }
    }
}

public sealed record BulkExistenceResult
{
    public required IReadOnlyList<IdPresence> Ids { get; init; }
    public required ScanSummary Scan { get; init; }

    public int FoundCount => Ids.Count(i => i.Found);
    public int MissingCount => Ids.Count(i => !i.Found);
    public int DuplicatedCount => Ids.Count(i => i.IsDuplicated);

    public string Summary =>
        $"{FoundCount:N0} of {Ids.Count:N0} id(s) found, {MissingCount:N0} missing, {DuplicatedCount:N0} found more than once - " +
        $"scanned {Scan.MessagesScanned:N0} message(s) on {Scan.TopicsTotal} topic(s){(Scan.Cancelled ? " (cancelled - incomplete)" : "")}.{Scan.FailedSuffix}";

    /// <summary>Id, status, count, topics, first/last position and time - for a CSV export.</summary>
    public string ToCsv() => Csv.Write(
        new[] { "id", "status", "count", "topics", "first_topic", "first_partition", "first_offset", "first_timestamp", "last_timestamp" },
        Ids.Select(i => new[]
        {
            i.Id, i.Found ? "found" : "missing", i.Count.ToString(), i.TopicsText,
            i.First?.Topic ?? "", i.First?.Partition.ToString() ?? "", i.First?.Offset.ToString() ?? "",
            i.First?.Timestamp.ToString("O") ?? "", i.Last?.Timestamp.ToString("O") ?? ""
        }));
}

/// <summary>
/// "Did all of these ids make it?" - checks a list of ids against one or more topics in a single pass
/// per topic, reporting for each id whether it was found, how many times, and where.
/// </summary>
public static class BulkExistenceChecker
{
    public static async Task<BulkExistenceResult> RunAsync(
        IKafkaGateway gateway,
        IReadOnlyCollection<string> topics,
        IReadOnlyList<string> ids,
        FieldSelector field,
        SearchRange range,
        bool caseSensitive = true,
        CancellationToken cancellationToken = default,
        ScanProgress? progress = null,
        DateTimeOffset? now = null)
    {
        var matcher = new IdMatcher(ids, field, caseSensitive);
        var rows = ids.Select(i => i.Trim()).Where(i => i.Length > 0).Distinct(StringComparer.Ordinal)
            .Select(i => new IdPresence { Id = i }).ToList();
        var byId = new Dictionary<string, IdPresence>(caseSensitive ? StringComparer.Ordinal : StringComparer.OrdinalIgnoreCase);
        foreach (var row in rows) byId.TryAdd(row.Id, row);

        var scan = await TopicScanner.ScanAsync(gateway, topics, range, (_, message) =>
        {
            foreach (var id in matcher.Match(message))
            {
                if (byId.TryGetValue(id, out var row)) row.Add(message);
            }
            return ScanDecision.Continue;
        }, cancellationToken, progress, now: now).ConfigureAwait(false);

        // Missing ids first - they're what a "did everything arrive?" check is looking for.
        var ordered = rows.OrderBy(r => r.Found).ThenByDescending(r => r.Count).ToList();
        return new BulkExistenceResult { Ids = ordered, Scan = scan };
    }
}

using System.Security.Cryptography;
using System.Text;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Search;

public enum DuplicateGroupBy
{
    /// <summary>Same message key.</summary>
    Key,
    /// <summary>Byte-identical value.</summary>
    Value,
    /// <summary>Same key and byte-identical value (a true re-publish).</summary>
    KeyAndValue,
    /// <summary>Same value once volatile fields (timestamps, generated ids) are ignored - producer retries.</summary>
    ValueIgnoringVolatile,
    /// <summary>Same values of chosen fields (e.g. $.orderId, $.eventType).</summary>
    Fields
}

/// <summary>Messages that share a grouping value.</summary>
public sealed record DuplicateGroup
{
    public required string GroupKey { get; init; }
    public required int Count { get; init; }

    /// <summary>The first <see cref="DuplicateDetector.MaxMessagesPerGroup"/> messages, oldest first.</summary>
    public required IReadOnlyList<KafkaMessage> Messages { get; init; }

    public DateTimeOffset FirstSeen => Messages[0].Timestamp;
    public DateTimeOffset LastSeen { get; init; }
    public TimeSpan Span => LastSeen - FirstSeen;
    public string SpanText => TraceHop.FormatSpan(Span);
    public string PositionsText => string.Join(", ", Messages.Take(6).Select(m => $"#{m.Partition}@{m.Offset}")) + (Count > 6 ? ", …" : "");
}

/// <summary>
/// Streams messages in and reports groups of duplicates (count &gt; 1). Values are hashed, so memory
/// grows with the number of distinct groups rather than the size of the messages.
/// </summary>
public sealed class DuplicateDetector
{
    public const int MaxMessagesPerGroup = 50;

    private sealed class Group
    {
        public required string Display;
        public int Count;
        public readonly List<KafkaMessage> Messages = new();
        public DateTimeOffset LastSeen = DateTimeOffset.MinValue;
    }

    private readonly DuplicateGroupBy _groupBy;
    private readonly IReadOnlyList<FieldSelector> _fields;
    private readonly Dictionary<string, Group> _groups = new(StringComparer.Ordinal);
    private readonly object _gate = new();

    public int MessagesSeen { get; private set; }

    /// <summary>Messages that had nothing to group by (no key / missing fields).</summary>
    public int Ungroupable { get; private set; }

    public DuplicateDetector(DuplicateGroupBy groupBy, IReadOnlyList<FieldSelector>? fields = null)
    {
        _groupBy = groupBy;
        _fields = fields ?? Array.Empty<FieldSelector>();
        if (groupBy == DuplicateGroupBy.Fields && _fields.Count == 0)
        {
            throw new ArgumentException("choose at least one field to group by", nameof(fields));
        }
    }

    public void Add(KafkaMessage message)
    {
        var (key, display) = GroupKeyFor(message);
        lock (_gate)
        {
            MessagesSeen++;
            if (key is null)
            {
                Ungroupable++;
                return;
            }
            if (!_groups.TryGetValue(key, out var group))
            {
                group = new Group { Display = display! };
                _groups[key] = group;
            }
            group.Count++;
            if (group.Messages.Count < MaxMessagesPerGroup) group.Messages.Add(message);
            if (message.Timestamp > group.LastSeen) group.LastSeen = message.Timestamp;
        }
    }

    public void AddRange(IEnumerable<KafkaMessage> messages)
    {
        foreach (var message in messages) Add(message);
    }

    /// <summary>Groups with more than one message, largest first.</summary>
    public IReadOnlyList<DuplicateGroup> GetDuplicates()
    {
        lock (_gate)
        {
            return _groups.Values
                .Where(g => g.Count > 1)
                .OrderByDescending(g => g.Count)
                .ThenBy(g => g.Display, StringComparer.Ordinal)
                .Select(g => new DuplicateGroup
                {
                    GroupKey = g.Display,
                    Count = g.Count,
                    Messages = g.Messages.OrderBy(m => m.Timestamp).ThenBy(m => m.Partition).ThenBy(m => m.Offset).ToList(),
                    LastSeen = g.LastSeen
                })
                .ToList();
        }
    }

    /// <summary>Total messages that are extra copies (sum of count - 1 over groups).</summary>
    public int ExtraCopies
    {
        get { lock (_gate) return _groups.Values.Where(g => g.Count > 1).Sum(g => g.Count - 1); }
    }

    private (string? Key, string? Display) GroupKeyFor(KafkaMessage message)
    {
        switch (_groupBy)
        {
            case DuplicateGroupBy.Key:
                return message.Key is null ? (null, null) : ("k:" + message.Key, message.Key);
            case DuplicateGroupBy.Value:
            {
                var hash = HashValue(message);
                return hash is null ? (null, null) : ("v:" + hash, Preview(message));
            }
            case DuplicateGroupBy.KeyAndValue:
            {
                var hash = HashValue(message);
                return hash is null ? (null, null) : ($"kv:{message.Key}\u0000{hash}", $"{message.Key ?? "(no key)"} · {Preview(message)}");
            }
            case DuplicateGroupBy.ValueIgnoringVolatile:
            {
                var canonical = CanonicalIgnoringVolatile(message);
                return canonical is null ? (null, null) : ("c:" + Hash(canonical), Preview(message));
            }
            default:
            {
                var parts = new List<string>(_fields.Count);
                foreach (var field in _fields)
                {
                    var value = field.Read(message);
                    if (value is null) return (null, null);
                    parts.Add(value);
                }
                var display = string.Join(" · ", _fields.Select((f, i) => $"{f.Label}={parts[i]}"));
                return ("f:" + string.Join("\u0000", parts), display);
            }
        }
    }

    /// <summary>Flattened JSON leaves (sorted, volatile ones dropped) plus the key - so two messages that
    /// differ only in timestamps/ids/field order group together. Non-JSON values are used as-is.</summary>
    public static string? CanonicalIgnoringVolatile(KafkaMessage message)
    {
        if (message.IsTombstone) return null;
        if (!JsonFlattener.TryFlatten(message.Value, out var leaves))
        {
            return message.Value ?? Convert.ToBase64String(message.RawValue!);
        }
        var sb = new StringBuilder();
        sb.Append(message.Key).Append('\u0001');
        foreach (var leaf in leaves.Where(l => !VolatileFields.IsVolatile(l.Path, l.Value)).OrderBy(l => l.Path, StringComparer.Ordinal))
        {
            sb.Append(leaf.Path).Append('=').Append(leaf.Value).Append('\u0001');
        }
        return sb.ToString();
    }

    private static string? HashValue(KafkaMessage message)
    {
        if (message.IsTombstone) return null;
        var bytes = message.RawValue ?? Encoding.UTF8.GetBytes(message.Value!);
        return Convert.ToHexString(SHA256.HashData(bytes));
    }

    private static string Hash(string text) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text)));

    private static string Preview(KafkaMessage message)
    {
        var preview = message.ValuePreview;
        return preview.Length > 120 ? preview[..120] + "…" : preview;
    }
}

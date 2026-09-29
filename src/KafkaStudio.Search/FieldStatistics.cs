using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Search;

/// <summary>How often one value of a field occurs.</summary>
public sealed record ValueCount(string? Value, int Count, DateTimeOffset FirstSeen, DateTimeOffset LastSeen)
{
    public string DisplayValue => Value ?? "(null)";
}

/// <summary>Statistics for one field across the analysed messages.</summary>
public sealed record FieldStats
{
    /// <summary>Shape path (<c>$.items[*].sku</c>), <c>key</c>, or <c>header "name"</c>.</summary>
    public required string Path { get; init; }
    public required FieldSelector? Selector { get; init; }
    public required int Present { get; init; }
    public required int Missing { get; init; }
    public required int Nulls { get; init; }
    public required int Distinct { get; init; }

    /// <summary>True when distinct-value tracking hit its cap (the real distinct count is higher).</summary>
    public required bool DistinctCapped { get; init; }
    public required string Types { get; init; }
    public required IReadOnlyList<ValueCount> TopValues { get; init; }
    public required DateTimeOffset? FirstSeen { get; init; }
    public required DateTimeOffset? LastSeen { get; init; }

    public string DistinctText => DistinctCapped ? $"{Distinct:N0}+" : Distinct.ToString("N0");
    public double PresentRatio { get; init; }
    public string PresentText => $"{PresentRatio:P0}";

    /// <summary>A field present in only some messages - often a schema drift worth a look.</summary>
    public bool IsSometimesMissing => Missing > 0 && Present > 0;
}

/// <summary>
/// Streams messages in and summarizes every field: how often it's present/missing/null, its distinct
/// values with counts, types seen, and first/last time seen. Arrays are folded (<c>$.items[*].sku</c>).
/// </summary>
public sealed class FieldStatisticsCollector
{
    public const int MaxDistinctPerField = 10_000;
    public const int MaxFields = 2_000;

    private sealed class Acc
    {
        public int Present;
        public int Nulls;
        public bool Capped;
        public readonly HashSet<string> Types = new(StringComparer.Ordinal);
        public readonly Dictionary<string, (int Count, DateTimeOffset First, DateTimeOffset Last)> Values = new(StringComparer.Ordinal);
        public int NullValueCount;
        public DateTimeOffset NullFirst, NullLast;
        public DateTimeOffset? First, Last;
    }

    private readonly Dictionary<string, Acc> _fields = new(StringComparer.Ordinal);
    private readonly object _gate = new();

    public int MessageCount { get; private set; }
    public int JsonMessageCount { get; private set; }

    public void Add(KafkaMessage message)
    {
        lock (_gate)
        {
            MessageCount++;
            var seenThisMessage = new HashSet<string>(StringComparer.Ordinal);

            if (message.Key is not null) Record("key", message.Key, "string", message.Timestamp, seenThisMessage);
            foreach (var (name, value) in message.Headers)
            {
                Record($"header \"{name}\"", value, "string", message.Timestamp, seenThisMessage);
            }

            if (JsonFlattener.TryFlatten(message.Value, out var leaves))
            {
                JsonMessageCount++;
                foreach (var leaf in leaves)
                {
                    Record(leaf.ShapePath, leaf.Value, leaf.Type.ToString().ToLowerInvariant(), message.Timestamp, seenThisMessage);
                }
            }
            else if (message.Value is not null)
            {
                Record("value", message.Value.Length > 200 ? message.Value[..200] + "…" : message.Value, "text", message.Timestamp, seenThisMessage);
            }
            else if (message.IsBinary)
            {
                Record("value", $"<binary, {message.RawValue!.Length} bytes>", "binary", message.Timestamp, seenThisMessage);
            }
        }
    }

    public void AddRange(IEnumerable<KafkaMessage> messages)
    {
        foreach (var message in messages) Add(message);
    }

    private void Record(string path, string? value, string type, DateTimeOffset at, HashSet<string> seenThisMessage)
    {
        if (!_fields.TryGetValue(path, out var acc))
        {
            if (_fields.Count >= MaxFields) return;
            acc = new Acc();
            _fields[path] = acc;
        }

        // A field counts as "present" once per message even when an array holds it several times.
        if (seenThisMessage.Add(path)) acc.Present++;
        acc.Types.Add(type);
        acc.First = acc.First is null || at < acc.First ? at : acc.First;
        acc.Last = acc.Last is null || at > acc.Last ? at : acc.Last;

        if (value is null)
        {
            acc.Nulls++;
            acc.NullFirst = acc.NullValueCount == 0 || at < acc.NullFirst ? at : acc.NullFirst;
            acc.NullLast = acc.NullValueCount == 0 || at > acc.NullLast ? at : acc.NullLast;
            acc.NullValueCount++;
            return;
        }

        if (acc.Values.TryGetValue(value, out var existing))
        {
            acc.Values[value] = (existing.Count + 1, at < existing.First ? at : existing.First, at > existing.Last ? at : existing.Last);
        }
        else if (acc.Values.Count < MaxDistinctPerField)
        {
            acc.Values[value] = (1, at, at);
        }
        else
        {
            acc.Capped = true;
        }
    }

    /// <summary>Fields ordered: key first, then headers, then value fields by path.</summary>
    public IReadOnlyList<FieldStats> GetStatistics(int topValues = 20)
    {
        lock (_gate)
        {
            return _fields
                .OrderBy(f => f.Key == "key" ? 0 : f.Key.StartsWith("header ", StringComparison.Ordinal) ? 1 : 2)
                .ThenBy(f => f.Key, StringComparer.Ordinal)
                .Select(f =>
                {
                    var acc = f.Value;
                    var top = acc.Values
                        .Select(v => new ValueCount(v.Key, v.Value.Count, v.Value.First, v.Value.Last))
                        .Concat(acc.NullValueCount > 0 ? new[] { new ValueCount(null, acc.NullValueCount, acc.NullFirst, acc.NullLast) } : Array.Empty<ValueCount>())
                        .OrderByDescending(v => v.Count)
                        .ThenBy(v => v.Value, StringComparer.Ordinal)
                        .Take(topValues)
                        .ToList();
                    return new FieldStats
                    {
                        Path = f.Key,
                        Selector = SelectorFor(f.Key),
                        Present = acc.Present,
                        Missing = MessageCount - acc.Present,
                        Nulls = acc.Nulls,
                        Distinct = acc.Values.Count + (acc.NullValueCount > 0 ? 1 : 0),
                        DistinctCapped = acc.Capped,
                        Types = string.Join("|", acc.Types.OrderBy(t => t, StringComparer.Ordinal)),
                        TopValues = top,
                        FirstSeen = acc.First,
                        LastSeen = acc.Last,
                        PresentRatio = MessageCount == 0 ? 0 : (double)acc.Present / MessageCount
                    };
                })
                .ToList();
        }
    }

    /// <summary>A selector that reads the field back (null for array-folded paths, which have no single value).</summary>
    private static FieldSelector? SelectorFor(string path)
    {
        if (path.Contains("[*]", StringComparison.Ordinal)) return null;
        return FieldSelector.TryParse(path, out var selector, out _) ? selector : null;
    }
}

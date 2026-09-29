using System.Globalization;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.Search;

public enum FieldKind { Key, Value, Json, Header, Topic, Partition, Offset, Any }

/// <summary>
/// Names one piece of a message - its key, value, a JSON field inside the value, a header, or its
/// position - so every search feature (queries, bulk checks, reconciliation, duplicates, traces) can
/// point at "the order id" the same way. Text forms accepted by <see cref="TryParse"/>:
/// <c>key</c>, <c>value</c>, <c>$.order.id</c>, <c>json "$.order.id"</c>, <c>header "trace-id"</c>,
/// <c>header:trace-id</c>, <c>topic</c>, <c>partition</c>, <c>offset</c>, <c>any</c>.
/// </summary>
public sealed record FieldSelector(FieldKind Kind, string? Name = null)
{
    public static readonly FieldSelector Key = new(FieldKind.Key);
    public static readonly FieldSelector Value = new(FieldKind.Value);
    public static readonly FieldSelector Any = new(FieldKind.Any);

    public static FieldSelector Json(string path) => new(FieldKind.Json, path);
    public static FieldSelector Header(string name) => new(FieldKind.Header, name);

    /// <summary>Reads the field as text; null when it is absent (no key, missing JSON field, no such header...).
    /// <see cref="FieldKind.Any"/> has no single value and reads as null - use <see cref="ReadAll"/>.</summary>
    public string? Read(KafkaMessage message) => Kind switch
    {
        FieldKind.Key => message.Key,
        FieldKind.Value => message.Value,
        FieldKind.Json => message.Value is null || Name is null ? null : JsonPathEvaluator.Evaluate(message.Value, Name),
        FieldKind.Header => Name is not null && message.Headers.TryGetValue(Name, out var h) ? h : null,
        FieldKind.Topic => message.Topic,
        FieldKind.Partition => message.Partition.ToString(CultureInfo.InvariantCulture),
        FieldKind.Offset => message.Offset.ToString(CultureInfo.InvariantCulture),
        _ => null
    };

    /// <summary>Every candidate text for the field: for <see cref="FieldKind.Any"/> the key, the value and
    /// every header value; otherwise just <see cref="Read"/> (when present).</summary>
    public IEnumerable<string> ReadAll(KafkaMessage message)
    {
        if (Kind != FieldKind.Any)
        {
            if (Read(message) is { } single) yield return single;
            yield break;
        }
        if (message.Key is not null) yield return message.Key;
        if (message.Value is not null) yield return message.Value;
        foreach (var header in message.Headers.Values) yield return header;
    }

    public override string ToString() => Kind switch
    {
        FieldKind.Json => $"json \"{Name}\"",
        FieldKind.Header => $"header \"{Name}\"",
        _ => Kind.ToString().ToLowerInvariant()
    };

    /// <summary>Short label for tables and column headers (<c>$.order.id</c>, <c>header trace-id</c>).</summary>
    public string Label => Kind switch
    {
        FieldKind.Json => Name ?? "$",
        FieldKind.Header => $"header {Name}",
        _ => Kind.ToString().ToLowerInvariant()
    };

    public static FieldSelector Parse(string text) =>
        TryParse(text, out var field, out var error) ? field : throw new FormatException(error);

    public static bool TryParse(string? text, out FieldSelector field, out string? error)
    {
        field = Key;
        error = null;
        var t = (text ?? "").Trim();
        if (t.Length == 0)
        {
            error = "no field given - use key, value, $.path, or header \"name\"";
            return false;
        }

        var lower = t.ToLowerInvariant();
        switch (lower)
        {
            case "key": field = Key; return true;
            case "value": field = Value; return true;
            case "topic": field = new FieldSelector(FieldKind.Topic); return true;
            case "partition": field = new FieldSelector(FieldKind.Partition); return true;
            case "offset": field = new FieldSelector(FieldKind.Offset); return true;
            case "any": case "anywhere": field = Any; return true;
        }

        if (t.StartsWith('$')) return TryJson(t, out field, out error);
        if (lower.StartsWith("json", StringComparison.Ordinal) && t.Length > 4 && (char.IsWhiteSpace(t[4]) || t[4] == ':'))
        {
            return TryJson(Unquote(t[5..].Trim()), out field, out error);
        }
        if (lower.StartsWith("header", StringComparison.Ordinal) && t.Length > 6 && (char.IsWhiteSpace(t[6]) || t[6] == ':'))
        {
            var name = Unquote(t[7..].Trim());
            if (name.Length == 0)
            {
                error = "header needs a name, e.g. header \"trace-id\"";
                return false;
            }
            field = Header(name);
            return true;
        }

        error = $"unknown field '{t}' - use key, value, $.path, json \"$.path\", header \"name\", topic, partition or offset";
        return false;
    }

    /// <summary>Parses a comma separated list of fields (<c>$.a, $.b, key</c>).</summary>
    public static bool TryParseList(string? text, out IReadOnlyList<FieldSelector> fields, out string? error)
    {
        var list = new List<FieldSelector>();
        fields = list;
        error = null;
        foreach (var part in SplitList(text ?? ""))
        {
            if (!TryParse(part, out var field, out error)) return false;
            list.Add(field);
        }
        return true;
    }

    private static IEnumerable<string> SplitList(string text)
    {
        // Split on commas that are not inside quotes or brackets ($['a,b'] stays one path).
        var depth = 0;
        char? quote = null;
        var start = 0;
        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];
            if (quote is not null)
            {
                if (c == quote) quote = null;
                continue;
            }
            if (c is '"' or '\'') quote = c;
            else if (c == '[') depth++;
            else if (c == ']') depth--;
            else if (c == ',' && depth == 0)
            {
                if (text[start..i].Trim() is { Length: > 0 } part) yield return part;
                start = i + 1;
            }
        }
        if (text[start..].Trim() is { Length: > 0 } last) yield return last;
    }

    private static bool TryJson(string path, out FieldSelector field, out string? error)
    {
        field = Key;
        error = JsonPathEvaluator.Validate(path);
        if (error is not null) return false;
        field = Json(path.StartsWith('$') ? path : "$." + path.TrimStart('.'));
        return true;
    }

    private static string Unquote(string s) =>
        s.Length >= 2 && (s[0] == '"' || s[0] == '\'') && s[^1] == s[0] ? s[1..^1] : s;
}

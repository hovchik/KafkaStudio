using System.Text.Json;

namespace KafkaStudio.Scripting.Runtime;

/// <summary>
/// Minimal JSONPath-flavoured accessor: supports "$.a.b", "$.arr[0]", and "$.arr[0].c" - the subset
/// that covers the vast majority of "check a field on the message body" assertions. Not a full
/// JSONPath implementation (no wildcards, filters, or recursive descent) by design: KafScript
/// conditions are meant to be simple, predictable, and fast to evaluate against a live message stream.
/// </summary>
public static class JsonPathEvaluator
{
    /// <summary>Returns the value at <paramref name="path"/> as a string, or null if it doesn't exist
    /// or <paramref name="json"/> isn't valid JSON.</summary>
    public static string? Evaluate(string json, string path)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var current = doc.RootElement;

            foreach (var segment in ParseSegments(path))
            {
                if (segment.IsIndex)
                {
                    if (current.ValueKind != JsonValueKind.Array || segment.Index >= current.GetArrayLength())
                    {
                        return null;
                    }
                    current = current[segment.Index];
                }
                else
                {
                    if (current.ValueKind != JsonValueKind.Object ||
                        !current.TryGetProperty(segment.Name!, out var next))
                    {
                        return null;
                    }
                    current = next;
                }
            }

            return ElementToString(current);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private readonly record struct Segment(string? Name, int Index, bool IsIndex);

    private static IEnumerable<Segment> ParseSegments(string path)
    {
        var p = path.Trim();
        if (p.StartsWith('$')) p = p[1..];
        if (p.StartsWith('.')) p = p[1..];

        if (p.Length == 0) yield break;

        foreach (var rawPart in p.Split('.'))
        {
            var part = rawPart;
            var bracket = part.IndexOf('[');
            if (bracket < 0)
            {
                yield return new Segment(part, 0, false);
                continue;
            }

            var name = part[..bracket];
            if (name.Length > 0) yield return new Segment(name, 0, false);

            var rest = part[bracket..];
            while (rest.Length > 0 && rest[0] == '[')
            {
                var close = rest.IndexOf(']');
                if (close < 0) throw new FormatException($"malformed json path '{path}': missing ']'");
                var indexText = rest[1..close];
                if (!int.TryParse(indexText, out var index))
                {
                    throw new FormatException($"malformed json path '{path}': '{indexText}' is not an array index");
                }
                yield return new Segment(null, index, true);
                rest = rest[(close + 1)..];
            }
        }
    }

    private static string? ElementToString(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.String => element.GetString(),
        JsonValueKind.Number => element.GetRawText(),
        JsonValueKind.True => "true",
        JsonValueKind.False => "false",
        JsonValueKind.Null => null,
        JsonValueKind.Undefined => null,
        _ => element.GetRawText() // object / array -> return the raw JSON fragment
    };
}

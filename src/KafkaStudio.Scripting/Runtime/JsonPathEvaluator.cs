using System.Globalization;
using System.Text;
using System.Text.Json;

namespace KafkaStudio.Scripting.Runtime;

/// <summary>
/// Minimal JSONPath-flavoured accessor. Supports:
/// <list type="bullet">
/// <item><c>$.a.b</c> - object properties;</item>
/// <item><c>$.arr[0]</c>, <c>$.arr[-1]</c> - array elements (negative indexes count from the end);</item>
/// <item><c>$['odd.key']</c> / <c>$["odd key"]</c> - properties whose names contain dots, spaces or brackets.</item>
/// </list>
/// Not a full JSONPath implementation (no wildcards, filters, or recursive descent) by design: KafScript
/// conditions are meant to be simple, predictable, and fast to evaluate against a live message stream.
/// </summary>
public static class JsonPathEvaluator
{
    private readonly record struct Segment(string? Name, int Index, bool IsIndex);

    /// <summary>Returns the value at <paramref name="path"/> as a string, or null if it doesn't exist,
    /// the path is malformed, or <paramref name="json"/> isn't valid JSON. Never throws.</summary>
    public static string? Evaluate(string json, string path)
    {
        if (!TryParseSegments(path, out var segments, out _)) return null;

        try
        {
            using var doc = JsonDocument.Parse(json);
            var current = doc.RootElement;

            foreach (var segment in segments)
            {
                if (segment.IsIndex)
                {
                    if (current.ValueKind != JsonValueKind.Array) return null;
                    var length = current.GetArrayLength();
                    var index = segment.Index < 0 ? length + segment.Index : segment.Index;
                    if (index < 0 || index >= length) return null;
                    current = current[index];
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

    /// <summary>Returns a human readable problem description, or null when the path is well-formed.</summary>
    public static string? Validate(string path) =>
        TryParseSegments(path, out _, out var error) ? null : error;

    private static bool TryParseSegments(string path, out List<Segment> segments, out string? error)
    {
        segments = new List<Segment>();
        error = null;

        var p = (path ?? string.Empty).Trim();
        if (p.StartsWith('$')) p = p[1..];

        var i = 0;
        while (i < p.Length)
        {
            var c = p[i];
            if (c == '.')
            {
                i++;
                var start = i;
                while (i < p.Length && p[i] != '.' && p[i] != '[') i++;
                if (i == start)
                {
                    error = $"malformed json path '{path}': empty property name";
                    return false;
                }
                segments.Add(new Segment(p[start..i], 0, false));
            }
            else if (c == '[')
            {
                var close = FindClosingBracket(p, i);
                if (close < 0)
                {
                    error = $"malformed json path '{path}': missing ']'";
                    return false;
                }
                var inner = p[(i + 1)..close].Trim();
                if (inner.Length >= 2 && (inner[0] == '\'' || inner[0] == '"') && inner[^1] == inner[0])
                {
                    segments.Add(new Segment(Unescape(inner[1..^1]), 0, false));
                }
                else if (int.TryParse(inner, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var index))
                {
                    segments.Add(new Segment(null, index, true));
                }
                else
                {
                    error = $"malformed json path '{path}': '{inner}' is not an array index or quoted name";
                    return false;
                }
                i = close + 1;
            }
            else if (segments.Count == 0 && i == 0)
            {
                // Tolerate a missing leading "$." / "." - "a.b" means "$.a.b".
                p = "." + p;
            }
            else
            {
                error = $"malformed json path '{path}': unexpected '{c}'";
                return false;
            }
        }

        return true;
    }

    private static int FindClosingBracket(string p, int open)
    {
        var i = open + 1;
        while (i < p.Length && p[i] == ' ') i++;
        if (i < p.Length && (p[i] == '\'' || p[i] == '"'))
        {
            var quote = p[i];
            i++;
            while (i < p.Length && p[i] != quote)
            {
                if (p[i] == '\\') i++;
                i++;
            }
            i++;
        }
        return p.IndexOf(']', Math.Min(i, p.Length));
    }

    private static string Unescape(string s)
    {
        if (!s.Contains('\\')) return s;
        var sb = new StringBuilder(s.Length);
        for (var i = 0; i < s.Length; i++)
        {
            if (s[i] == '\\' && i + 1 < s.Length) i++;
            sb.Append(s[i]);
        }
        return sb.ToString();
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

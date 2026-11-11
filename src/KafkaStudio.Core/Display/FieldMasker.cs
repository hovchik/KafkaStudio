using System.Text.Json;
using System.Text.Json.Nodes;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Core.Display;

/// <summary>
/// Hides sensitive fields when messages are shown on screen. Rules are JSON paths such as
/// <c>$.card.number</c>, <c>$.items[*].email</c> or a bare <c>password</c> (any field with that name,
/// at any depth); header names are matched case-insensitively. Display only: the original message,
/// and anything produced from it by a script or rethrow rule, is never changed.
/// </summary>
public sealed class FieldMasker
{
    public const string Mask = "••••••";

    private readonly List<string[]> _paths = [];
    private readonly HashSet<string> _anyDepthNames = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _headers = new(StringComparer.OrdinalIgnoreCase);

    public FieldMasker(IEnumerable<string> rules)
    {
        foreach (var raw in rules)
        {
            var rule = raw.Trim();
            if (rule.Length == 0) continue;
            if (rule.StartsWith("header:", StringComparison.OrdinalIgnoreCase))
            {
                _headers.Add(rule["header:".Length..].Trim());
            }
            else if (rule.StartsWith('$'))
            {
                _paths.Add(SplitPath(rule));
            }
            else
            {
                _anyDepthNames.Add(rule);
            }
        }
    }

    public bool IsEmpty => _paths.Count == 0 && _anyDepthNames.Count == 0 && _headers.Count == 0;

    public KafkaMessage Apply(KafkaMessage message)
    {
        if (IsEmpty) return message;
        var result = message;

        if (_headers.Count > 0 && message.Headers.Count > 0)
        {
            result = result with
            {
                Headers = message.Headers.ToDictionary(h => h.Key, h => _headers.Contains(h.Key) ? Mask : h.Value)
            };
        }

        if (message.Value is { } text && (_paths.Count > 0 || _anyDepthNames.Count > 0))
        {
            var masked = MaskJson(text);
            if (masked is not null) result = result with { Value = masked };
        }
        return result;
    }

    /// <summary>Returns the masked JSON text, or null when the value isn't a JSON object/array.</summary>
    public string? MaskJson(string text)
    {
        var trimmed = text.AsSpan().TrimStart();
        if (trimmed.Length == 0 || (trimmed[0] != '{' && trimmed[0] != '[')) return null;
        JsonNode? root;
        try { root = JsonNode.Parse(text); }
        catch (JsonException) { return null; }
        if (root is null) return null;

        if (_anyDepthNames.Count > 0) MaskNames(root);
        foreach (var path in _paths) MaskPath(root, path, 0);
        return root.ToJsonString(new JsonSerializerOptions
        {
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        });
    }

    private void MaskNames(JsonNode node)
    {
        switch (node)
        {
            case JsonObject obj:
                foreach (var key in obj.Select(p => p.Key).ToList())
                {
                    if (_anyDepthNames.Contains(key) && obj[key] is not null) obj[key] = Mask;
                    else if (obj[key] is { } child) MaskNames(child);
                }
                break;
            case JsonArray arr:
                foreach (var item in arr)
                {
                    if (item is not null) MaskNames(item);
                }
                break;
        }
    }

    private static void MaskPath(JsonNode node, string[] path, int index)
    {
        if (index >= path.Length) return;
        var segment = path[index];
        var last = index == path.Length - 1;
        switch (node)
        {
            case JsonObject obj:
                if (segment == "*")
                {
                    foreach (var key in obj.Select(p => p.Key).ToList())
                    {
                        if (last) obj[key] = Mask;
                        else if (obj[key] is { } c) MaskPath(c, path, index + 1);
                    }
                }
                else if (obj.ContainsKey(segment) && obj[segment] is not null)
                {
                    if (last) obj[segment] = Mask;
                    else MaskPath(obj[segment]!, path, index + 1);
                }
                break;
            case JsonArray arr:
                for (var i = 0; i < arr.Count; i++)
                {
                    if (segment != "*" && (!int.TryParse(segment, out var wanted) || wanted != i)) continue;
                    if (last) arr[i] = Mask;
                    else if (arr[i] is { } c) MaskPath(c, path, index + 1);
                }
                break;
        }
    }

    /// <summary>"$.a.b[0].c" -> ["a","b","0","c"]; "[*]" becomes "*".</summary>
    internal static string[] SplitPath(string rule)
    {
        var text = rule.StartsWith("$.") ? rule[2..] : rule.TrimStart('$');
        var parts = new List<string>();
        foreach (var piece in text.Split('.', StringSplitOptions.RemoveEmptyEntries))
        {
            var bracket = piece.IndexOf('[');
            if (bracket < 0) { parts.Add(piece); continue; }
            if (bracket > 0) parts.Add(piece[..bracket]);
            foreach (var part in piece[bracket..].Split(['[', ']'], StringSplitOptions.RemoveEmptyEntries))
            {
                parts.Add(part);
            }
        }
        return parts.ToArray();
    }
}

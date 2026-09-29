using System.Text.Json;
using System.Text.RegularExpressions;

namespace KafkaStudio.Search;

public enum JsonLeafType { String, Number, Boolean, Null, EmptyObject, EmptyArray }

/// <summary>One scalar inside a JSON document: its exact path (<c>$.items[0].sku</c>), its value as text
/// (null for JSON null) and its type.</summary>
public sealed record JsonLeaf(string Path, string? Value, JsonLeafType Type)
{
    /// <summary>Path with array indexes replaced by <c>[*]</c> (<c>$.items[*].sku</c>) - the same "field"
    /// across messages whose arrays have different lengths.</summary>
    public string ShapePath => JsonFlattener.ToShapePath(Path);
}

/// <summary>
/// Turns a JSON value into a flat list of leaves. The basis for field pickers, field statistics,
/// structural diffs, "same shape" matching and similarity scoring.
/// </summary>
public static class JsonFlattener
{
    /// <summary>Documents larger than this are not flattened (treated as plain text) to keep scans fast.</summary>
    public const int MaxDocumentLength = 1024 * 1024;

    /// <summary>At most this many leaves are produced per document.</summary>
    public const int MaxLeaves = 5_000;

    private static readonly Regex ArrayIndex = new(@"\[-?\d+\]", RegexOptions.CultureInvariant);
    private static readonly Regex SimpleName = new(@"^[A-Za-z_$][A-Za-z0-9_$-]*$", RegexOptions.CultureInvariant);

    public static string ToShapePath(string path) => ArrayIndex.Replace(path, "[*]");

    /// <summary>True when <paramref name="text"/> looks like a JSON object or array (cheap pre-check).</summary>
    public static bool LooksLikeJson(string? text)
    {
        if (string.IsNullOrEmpty(text) || text.Length > MaxDocumentLength) return false;
        foreach (var c in text)
        {
            if (char.IsWhiteSpace(c)) continue;
            return c is '{' or '[';
        }
        return false;
    }

    /// <summary>Flattens <paramref name="json"/>; returns false (and no leaves) when it isn't a JSON
    /// object/array.</summary>
    public static bool TryFlatten(string? json, out IReadOnlyList<JsonLeaf> leaves)
    {
        leaves = Array.Empty<JsonLeaf>();
        if (!LooksLikeJson(json)) return false;
        try
        {
            using var doc = JsonDocument.Parse(json!);
            var list = new List<JsonLeaf>();
            Walk(doc.RootElement, "$", list);
            leaves = list;
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    public static IReadOnlyList<JsonLeaf> Flatten(string? json) => TryFlatten(json, out var leaves) ? leaves : Array.Empty<JsonLeaf>();

    /// <summary>The distinct <see cref="JsonLeaf.ShapePath"/>s of a document - its "shape".</summary>
    public static IReadOnlySet<string> Shape(IEnumerable<JsonLeaf> leaves) =>
        leaves.Select(l => l.ShapePath).ToHashSet(StringComparer.Ordinal);

    /// <summary>Appends a property name to a path, bracket-quoting names that aren't plain identifiers.</summary>
    public static string Child(string parent, string name) =>
        SimpleName.IsMatch(name) ? $"{parent}.{name}" : $"{parent}['{name.Replace("\\", "\\\\").Replace("'", "\\'")}']";

    private static void Walk(JsonElement element, string path, List<JsonLeaf> leaves)
    {
        if (leaves.Count >= MaxLeaves) return;
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var any = false;
                foreach (var property in element.EnumerateObject())
                {
                    any = true;
                    Walk(property.Value, Child(path, property.Name), leaves);
                }
                if (!any) leaves.Add(new JsonLeaf(path, "{}", JsonLeafType.EmptyObject));
                break;
            case JsonValueKind.Array:
                var index = 0;
                foreach (var item in element.EnumerateArray())
                {
                    Walk(item, $"{path}[{index++}]", leaves);
                }
                if (index == 0) leaves.Add(new JsonLeaf(path, "[]", JsonLeafType.EmptyArray));
                break;
            case JsonValueKind.String:
                leaves.Add(new JsonLeaf(path, element.GetString(), JsonLeafType.String));
                break;
            case JsonValueKind.Number:
                leaves.Add(new JsonLeaf(path, element.GetRawText(), JsonLeafType.Number));
                break;
            case JsonValueKind.True:
            case JsonValueKind.False:
                leaves.Add(new JsonLeaf(path, element.ValueKind == JsonValueKind.True ? "true" : "false", JsonLeafType.Boolean));
                break;
            default:
                leaves.Add(new JsonLeaf(path, null, JsonLeafType.Null));
                break;
        }
    }
}

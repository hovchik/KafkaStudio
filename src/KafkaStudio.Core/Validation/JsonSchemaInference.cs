using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace KafkaStudio.Core.Validation;

/// <summary>
/// Builds a starting-point JSON Schema from sample message values - the quickest way to write a contract
/// for a topic that has none: infer from real traffic, then tighten by hand. Rules: a field is
/// <c>required</c> when every sampled object has it; a field seen with several types gets a type list;
/// strings that are all UUIDs / ISO date-times get a <c>format</c>; integers stay <c>integer</c> unless a
/// fraction was seen. Values that aren't JSON are skipped (and counted).
/// </summary>
public static partial class JsonSchemaInference
{
    public sealed record Result(string SchemaText, int SamplesUsed, int SamplesSkipped);

    private sealed class Node
    {
        public readonly HashSet<string> Types = new(StringComparer.Ordinal);
        public readonly Dictionary<string, Node> Properties = new(StringComparer.Ordinal);
        public readonly Dictionary<string, int> PresentCount = new(StringComparer.Ordinal);
        public readonly List<string> PropertyOrder = new();
        public int ObjectCount;
        public Node? Items;
        public int StringCount, UuidCount, DateTimeCount;
    }

    public static Result Infer(IEnumerable<string?> samples, int maxSamples = 10_000)
    {
        var root = new Node();
        int used = 0, skipped = 0;
        foreach (var sample in samples)
        {
            if (used >= maxSamples) break;
            if (string.IsNullOrWhiteSpace(sample))
            {
                skipped++;
                continue;
            }
            try
            {
                using var doc = JsonDocument.Parse(sample);
                Merge(root, doc.RootElement);
                used++;
            }
            catch (JsonException)
            {
                skipped++;
            }
        }

        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
        {
            writer.WriteStartObject();
            writer.WriteString("$schema", "https://json-schema.org/draft/2020-12/schema");
            if (used == 0)
            {
                writer.WriteString("description", "no JSON samples were available to infer from");
            }
            else
            {
                WriteBody(writer, root);
            }
            writer.WriteEndObject();
        }
        return new Result(Encoding.UTF8.GetString(stream.ToArray()), used, skipped);
    }

    private static void Merge(Node node, JsonElement value)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.Object:
                node.Types.Add("object");
                node.ObjectCount++;
                foreach (var property in value.EnumerateObject())
                {
                    if (!node.Properties.TryGetValue(property.Name, out var child))
                    {
                        child = new Node();
                        node.Properties[property.Name] = child;
                        node.PropertyOrder.Add(property.Name);
                    }
                    node.PresentCount[property.Name] = node.PresentCount.GetValueOrDefault(property.Name) + 1;
                    Merge(child, property.Value);
                }
                break;
            case JsonValueKind.Array:
                node.Types.Add("array");
                node.Items ??= new Node();
                foreach (var item in value.EnumerateArray()) Merge(node.Items, item);
                break;
            case JsonValueKind.String:
                node.Types.Add("string");
                node.StringCount++;
                var text = value.GetString()!;
                if (Guid.TryParseExact(text, "D", out _)) node.UuidCount++;
                if (IsoDateTime().IsMatch(text) && DateTimeOffset.TryParse(text, out _)) node.DateTimeCount++;
                break;
            case JsonValueKind.Number:
                node.Types.Add(value.TryGetInt64(out _) ? "integer" : "number");
                break;
            case JsonValueKind.True:
            case JsonValueKind.False:
                node.Types.Add("boolean");
                break;
            case JsonValueKind.Null:
                node.Types.Add("null");
                break;
        }
    }

    private static void WriteBody(Utf8JsonWriter writer, Node node)
    {
        var types = node.Types.ToList();
        if (types.Contains("number")) types.Remove("integer"); // 1 and 1.5 seen: it's a number
        types.Sort(StringComparer.Ordinal);
        if (types.Count == 1)
        {
            writer.WriteString("type", types[0]);
        }
        else if (types.Count > 1)
        {
            writer.WriteStartArray("type");
            foreach (var t in types) writer.WriteStringValue(t);
            writer.WriteEndArray();
        }

        if (node.StringCount > 0 && node.UuidCount == node.StringCount) writer.WriteString("format", "uuid");
        else if (node.StringCount > 0 && node.DateTimeCount == node.StringCount) writer.WriteString("format", "date-time");

        if (node.Properties.Count > 0)
        {
            writer.WriteStartObject("properties");
            foreach (var name in node.PropertyOrder)
            {
                writer.WriteStartObject(name);
                WriteBody(writer, node.Properties[name]);
                writer.WriteEndObject();
            }
            writer.WriteEndObject();

            var required = node.PropertyOrder.Where(p => node.PresentCount[p] == node.ObjectCount).ToList();
            if (required.Count > 0)
            {
                writer.WriteStartArray("required");
                foreach (var r in required) writer.WriteStringValue(r);
                writer.WriteEndArray();
            }
        }

        if (node.Items is { Types.Count: > 0 } items)
        {
            writer.WriteStartObject("items");
            WriteBody(writer, items);
            writer.WriteEndObject();
        }
    }

    [GeneratedRegex(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}")]
    private static partial Regex IsoDateTime();
}

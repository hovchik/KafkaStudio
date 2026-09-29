using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.App.ViewModels.Shared;

/// <summary>Serializes messages to a readable, re-importable JSON array (used by "Export" and "Copy as JSON").</summary>
public static class MessageExport
{
    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = true,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    public static JsonObject ToJsonObject(KafkaMessage m)
    {
        var obj = new JsonObject
        {
            ["topic"] = m.Topic,
            ["partition"] = m.Partition,
            ["offset"] = m.Offset,
            ["timestamp"] = m.Timestamp.ToString("O"),
            ["key"] = m.Key
        };

        var headers = new JsonObject();
        foreach (var (name, value) in m.Headers) headers[name] = value;
        obj["headers"] = headers;

        if (m.IsTombstone)
        {
            obj["value"] = null;
        }
        else if (m.Value is null)
        {
            obj["valueBase64"] = Convert.ToBase64String(m.RawValue!);
        }
        else
        {
            // Embed JSON payloads as JSON (not as an escaped string) so the export stays readable.
            JsonNode? parsed = null;
            try { parsed = JsonNode.Parse(m.Value); }
            catch (JsonException) { /* not JSON */ }
            obj["value"] = parsed ?? JsonValue.Create(m.Value);
        }

        return obj;
    }

    public static string ToJson(IEnumerable<KafkaMessage> messages)
    {
        var array = new JsonArray();
        foreach (var m in messages) array.Add(ToJsonObject(m));
        return array.ToJsonString(Options);
    }

    public static string ToJson(KafkaMessage message) => ToJsonObject(message).ToJsonString(Options);
}

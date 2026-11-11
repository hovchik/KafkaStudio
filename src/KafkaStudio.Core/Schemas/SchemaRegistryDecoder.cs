using System.Buffers.Binary;
using System.Text.Json;
using System.Text.Json.Nodes;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Core.Schemas;

/// <summary>
/// Turns Confluent wire-format values (magic byte 0, 4-byte schema id, payload) into JSON text using
/// the schema from the registry. Values in other formats, and values whose schema can't be fetched or
/// decoded, are returned unchanged (with a header noting why, so the failure isn't silent).
/// </summary>
public sealed class SchemaRegistryDecoder
{
    public const string SchemaHeader = "x-kafkastudio-schema";
    public const string ErrorHeader = "x-kafkastudio-decode-error";

    private static readonly JsonSerializerOptions Compact = new()
    {
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    private readonly SchemaRegistryClient _client;
    private readonly Dictionary<int, AvroDecoder> _avro = new();

    public SchemaRegistryDecoder(SchemaRegistryClient client) => _client = client;

    public static bool LooksLikeWireFormat(byte[]? bytes) => bytes is { Length: >= 5 } && bytes[0] == 0;

    public async ValueTask<KafkaMessage> DecodeAsync(KafkaMessage message, CancellationToken ct = default)
    {
        var bytes = message.RawValue;
        if (!LooksLikeWireFormat(bytes)) return message;

        var id = BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(1, 4));
        try
        {
            var schema = await _client.GetByIdAsync(id, ct).ConfigureAwait(false);
            var payload = bytes.AsSpan(5);
            string json;
            switch (schema.SchemaType.ToUpperInvariant())
            {
                case "AVRO":
                {
                    AvroDecoder decoder;
                    lock (_avro)
                    {
                        if (!_avro.TryGetValue(id, out decoder!)) _avro[id] = decoder = new AvroDecoder(schema.Schema);
                    }
                    json = (decoder.Decode(payload) ?? JsonValue.Create("null")).ToJsonString(Compact);
                    break;
                }
                case "PROTOBUF":
                {
                    // After the id comes the message-index list (usually a single 0 byte), then the message.
                    var pos = 0;
                    var count = ReadZigZag(payload, ref pos);
                    for (var i = 0; i < count; i++) ReadZigZag(payload, ref pos);
                    json = ProtobufWireDecoder.Decode(payload[pos..]).ToJsonString(Compact);
                    break;
                }
                default: // JSON Schema: the payload is already JSON text
                    json = System.Text.Encoding.UTF8.GetString(payload);
                    break;
            }

            var headers = new Dictionary<string, string>(message.Headers) { [SchemaHeader] = $"{schema.SchemaType} #{id}" };
            return message with { Value = json, Headers = headers };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var headers = new Dictionary<string, string>(message.Headers) { [ErrorHeader] = $"schema #{id}: {ex.Message}" };
            return message with { Headers = headers };
        }
    }

    private static long ReadZigZag(ReadOnlySpan<byte> data, ref int pos)
    {
        long value = 0;
        var shift = 0;
        while (true)
        {
            if (pos >= data.Length) throw new FormatException("Protobuf message index ended early");
            var b = data[pos++];
            value |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        return (long)((ulong)value >> 1) ^ -(value & 1);
    }
}

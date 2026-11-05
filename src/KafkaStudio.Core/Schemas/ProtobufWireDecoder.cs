using System.Text;
using System.Text.Json.Nodes;

namespace KafkaStudio.Core.Schemas;

/// <summary>
/// Schema-less Protobuf wire decoding: shows each field by its number ("1", "2", ...). Without the
/// .proto descriptor names and exact types are unknown, so varints are shown as numbers, length-delimited
/// fields as a nested message, UTF-8 text or base64 (whichever fits first), and fixed-width fields as numbers.
/// </summary>
public static class ProtobufWireDecoder
{
    public static JsonObject Decode(ReadOnlySpan<byte> data, int depth = 0)
    {
        var result = new JsonObject();
        var pos = 0;
        while (pos < data.Length)
        {
            var tag = Varint(data, ref pos);
            var field = (int)(tag >> 3);
            var wire = (int)(tag & 7);
            JsonNode? value;
            switch (wire)
            {
                case 0: value = JsonValue.Create(Varint(data, ref pos)); break;
                case 1:
                    Need(data, pos, 8);
                    value = JsonValue.Create(BitConverter.ToUInt64(data.Slice(pos, 8))); pos += 8; break;
                case 2:
                {
                    var len = (int)Varint(data, ref pos);
                    Need(data, pos, len);
                    value = LengthDelimited(data.Slice(pos, len), depth);
                    pos += len;
                    break;
                }
                case 5:
                    Need(data, pos, 4);
                    value = JsonValue.Create(BitConverter.ToUInt32(data.Slice(pos, 4))); pos += 4; break;
                default: throw new FormatException($"unsupported Protobuf wire type {wire}");
            }
            var key = field.ToString();
            if (result[key] is { } existing)
            {
                if (existing is JsonArray arr) arr.Add(value);
                else
                {
                    result.Remove(key);
                    result[key] = new JsonArray(existing, value);
                }
            }
            else
            {
                result[key] = value;
            }
        }
        return result;
    }

    private static JsonNode? LengthDelimited(ReadOnlySpan<byte> bytes, int depth)
    {
        if (bytes.Length == 0) return JsonValue.Create("");
        if (depth < 6)
        {
            try
            {
                var nested = Decode(bytes, depth + 1);
                if (nested.Count > 0) return nested;
            }
            catch (FormatException) { /* not a message */ }
        }
        var text = KafkaStudio.Core.Messaging.KafkaMessage.DecodeText(bytes.ToArray());
        if (text is not null && text.All(c => !char.IsControl(c) || c is '\n' or '\t' or '\r')) return JsonValue.Create(text);
        return JsonValue.Create(Convert.ToBase64String(bytes));
    }

    private static void Need(ReadOnlySpan<byte> data, int pos, int count)
    {
        if (count < 0 || pos + count > data.Length) throw new FormatException("Protobuf data ended early");
    }

    private static ulong Varint(ReadOnlySpan<byte> data, ref int pos)
    {
        ulong value = 0;
        var shift = 0;
        while (true)
        {
            if (pos >= data.Length) throw new FormatException("Protobuf data ended early");
            var b = data[pos++];
            value |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return value;
            shift += 7;
            if (shift > 63) throw new FormatException("bad Protobuf varint");
        }
    }
}

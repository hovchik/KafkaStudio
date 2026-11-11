using System.Text;
using System.Text.Json.Nodes;

namespace KafkaStudio.Core.Schemas;

/// <summary>
/// Decodes Avro binary data to JSON using a writer schema (the schema text from the registry).
/// Logical types are shown as their underlying value (a timestamp-millis is a number). Covers
/// primitives, records, enums, arrays, maps, unions, fixed and named-type references.
/// </summary>
public sealed class AvroDecoder
{
    private readonly Dictionary<string, JsonNode> _named = new(StringComparer.Ordinal);
    private readonly JsonNode _root;

    public AvroDecoder(string schemaJson)
    {
        _root = JsonNode.Parse(schemaJson) ?? throw new FormatException("empty Avro schema");
        Collect(_root, null);
    }

    public JsonNode? Decode(ReadOnlySpan<byte> data)
    {
        var reader = new Reader(data.ToArray());
        var result = Read(_root, reader, null);
        return result;
    }

    private void Collect(JsonNode? node, string? ns)
    {
        switch (node)
        {
            case JsonArray arr:
                foreach (var item in arr) Collect(item, ns);
                break;
            case JsonObject obj:
                var type = obj["type"];
                var typeName = type is JsonValue v && v.TryGetValue<string>(out var t) ? t : null;
                if (typeName is "record" or "error" or "enum" or "fixed")
                {
                    var name = obj["name"]?.GetValue<string>();
                    ns = obj["namespace"]?.GetValue<string>() ?? ns;
                    if (name is not null)
                    {
                        var full = name.Contains('.') || ns is null ? name : ns + "." + name;
                        _named[full] = obj;
                        _named[name] = obj;
                    }
                    if (obj["fields"] is JsonArray fields)
                    {
                        foreach (var f in fields) Collect(f?["type"], ns);
                    }
                }
                else if (typeName is "array") Collect(obj["items"], ns);
                else if (typeName is "map") Collect(obj["values"], ns);
                else if (type is not null) Collect(type, ns);
                break;
        }
    }

    private JsonNode? Read(JsonNode? schema, Reader r, string? ns)
    {
        switch (schema)
        {
            case JsonValue sv when sv.TryGetValue<string>(out var name):
                return ReadNamed(name, r, ns);
            case JsonArray union:
            {
                var index = (int)r.Long();
                if (index < 0 || index >= union.Count) throw new FormatException($"bad Avro union branch {index}");
                return Read(union[index], r, ns);
            }
            case JsonObject obj:
            {
                var type = obj["type"];
                if (type is JsonValue tv && tv.TryGetValue<string>(out var typeName))
                {
                    switch (typeName)
                    {
                        case "record":
                        case "error":
                        {
                            ns = obj["namespace"]?.GetValue<string>() ?? ns;
                            var result = new JsonObject();
                            foreach (var field in obj["fields"]!.AsArray())
                            {
                                result[field!["name"]!.GetValue<string>()] = Read(field["type"], r, ns);
                            }
                            return result;
                        }
                        case "enum":
                        {
                            var symbols = obj["symbols"]!.AsArray();
                            var i = (int)r.Long();
                            return i >= 0 && i < symbols.Count ? JsonValue.Create(symbols[i]!.GetValue<string>()) : null;
                        }
                        case "array":
                        {
                            var result = new JsonArray();
                            while (true)
                            {
                                var count = r.Long();
                                if (count == 0) break;
                                if (count < 0) { count = -count; r.Long(); }
                                for (var i = 0; i < count; i++) result.Add(Read(obj["items"], r, ns));
                            }
                            return result;
                        }
                        case "map":
                        {
                            var result = new JsonObject();
                            while (true)
                            {
                                var count = r.Long();
                                if (count == 0) break;
                                if (count < 0) { count = -count; r.Long(); }
                                for (var i = 0; i < count; i++)
                                {
                                    var key = r.String();
                                    result[key] = Read(obj["values"], r, ns);
                                }
                            }
                            return result;
                        }
                        case "fixed":
                            return JsonValue.Create(Convert.ToBase64String(r.Bytes(obj["size"]!.GetValue<int>())));
                        default:
                            return ReadNamed(typeName, r, ns);
                    }
                }
                return Read(type, r, ns);
            }
            default:
                throw new FormatException("unsupported Avro schema node");
        }
    }

    private JsonNode? ReadNamed(string name, Reader r, string? ns)
    {
        switch (name)
        {
            case "null": return null;
            case "boolean": return JsonValue.Create(r.Byte() != 0);
            case "int": return JsonValue.Create((int)r.Long());
            case "long": return JsonValue.Create(r.Long());
            case "float": return JsonValue.Create(BitConverter.ToSingle(r.Bytes(4)));
            case "double": return JsonValue.Create(BitConverter.ToDouble(r.Bytes(8)));
            case "string": return JsonValue.Create(r.String());
            case "bytes": return JsonValue.Create(Convert.ToBase64String(r.Bytes((int)r.Long())));
        }
        if (_named.TryGetValue(name, out var target) || (ns is not null && _named.TryGetValue(ns + "." + name, out target)))
        {
            return Read(target, r, ns);
        }
        throw new FormatException($"unknown Avro type '{name}'");
    }

    private sealed class Reader(byte[] data)
    {
        private int _pos;

        public byte Byte()
        {
            if (_pos >= data.Length) throw new FormatException("Avro data ended early");
            return data[_pos++];
        }

        public byte[] Bytes(int count)
        {
            if (count < 0 || _pos + count > data.Length) throw new FormatException("Avro data ended early");
            var result = data.AsSpan(_pos, count).ToArray();
            _pos += count;
            return result;
        }

        public long Long()
        {
            long value = 0;
            var shift = 0;
            while (true)
            {
                var b = Byte();
                value |= (long)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
                if (shift > 63) throw new FormatException("bad Avro varint");
            }
            return (long)((ulong)value >> 1) ^ -(value & 1);
        }

        public string String() => Encoding.UTF8.GetString(Bytes((int)Long()));
    }
}

using System.Text;
using System.Text.Json;

namespace KafkaStudio.Core.Messaging;

/// <summary>
/// A single Kafka record, normalized to a broker-agnostic shape used throughout KafkaStudio.
/// </summary>
public sealed record KafkaMessage
{
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
    private static readonly JsonSerializerOptions IndentedJson = new()
    {
        WriteIndented = true,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    /// <summary>Above this size the UI shows values as-is instead of re-formatting them.</summary>
    public const int MaxPrettyPrintLength = 256 * 1024;

    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public string? Key { get; init; }

    /// <summary>
    /// Value decoded as UTF-8 text, or null when the value is a tombstone (no value at all) or is not
    /// valid UTF-8 (binary payload - see <see cref="RawValue"/>).
    /// </summary>
    public string? Value { get; init; }

    /// <summary>Exact value bytes as stored in Kafka (null for a tombstone).</summary>
    public byte[]? RawValue { get; init; }

    public IReadOnlyDictionary<string, string> Headers { get; init; } =
        new Dictionary<string, string>();

    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>Set by the gateway when the message was read as part of a specific consumer group's subscription.</summary>
    public string? ConsumerGroup { get; init; }

    /// <summary>True when the record has no value at all (a compaction "delete" marker).</summary>
    public bool IsTombstone => Value is null && RawValue is null;

    /// <summary>True when the value exists but isn't valid UTF-8 text.</summary>
    public bool IsBinary => Value is null && RawValue is not null;

    public int ValueSize => RawValue?.Length ?? (Value is null ? 0 : Encoding.UTF8.GetByteCount(Value));

    /// <summary>Decodes value bytes: text when they are valid UTF-8, otherwise null (binary).</summary>
    public static string? DecodeText(byte[]? bytes)
    {
        if (bytes is null) return null;
        try
        {
            return StrictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }

    /// <summary>
    /// <see cref="Value"/> pretty-printed as indented JSON when it parses as JSON; otherwise the raw value
    /// unchanged. Binary values are rendered as a hex dump and tombstones as a marker. Used by the UI to
    /// display message bodies in a more readable, "beautified" form while still remaining plain,
    /// selectable/copyable text.
    /// </summary>
    public string PrettyValue
    {
        get
        {
            if (IsTombstone) return "<null - tombstone>";
            if (Value is null) return HexDump(RawValue!);
            if (Value.Length > MaxPrettyPrintLength) return Value;

            var trimmed = Value.AsSpan().TrimStart();
            if (trimmed.Length == 0 || (trimmed[0] != '{' && trimmed[0] != '[')) return Value;

            try
            {
                using var document = JsonDocument.Parse(Value);
                return JsonSerializer.Serialize(document.RootElement, IndentedJson);
            }
            catch (JsonException)
            {
                return Value;
            }
        }
    }

    /// <summary>One-line, length-capped rendering of the value for compact list rows.</summary>
    public string ValuePreview
    {
        get
        {
            if (IsTombstone) return "<null - tombstone>";
            if (Value is null) return $"<binary, {RawValue!.Length} bytes>";
            var span = Value.AsSpan(0, Math.Min(Value.Length, 400));
            var sb = new StringBuilder(span.Length);
            var lastWasSpace = false;
            foreach (var c in span)
            {
                var isSpace = char.IsWhiteSpace(c);
                if (isSpace && lastWasSpace) continue;
                sb.Append(isSpace ? ' ' : c);
                lastWasSpace = isSpace;
            }
            if (Value.Length > span.Length) sb.Append('…');
            return sb.ToString().Trim();
        }
    }

    /// <summary>"name: value" per header, one per line (empty when there are none).</summary>
    public string HeadersText => string.Join(Environment.NewLine, Headers.Select(h => $"{h.Key}: {h.Value}"));

    public string ToDisplayString(int maxLength = 200)
    {
        var v = Value ?? (IsTombstone ? "<null>" : "<binary>");
        if (v.Length > maxLength)
        {
            v = string.Concat(v.AsSpan(0, maxLength), "…");
        }
        return $"[{Topic}#{Partition}@{Offset}] key={Key ?? "<null>"} value={v}";
    }

    private static string HexDump(byte[] bytes, int maxBytes = 4096)
    {
        var sb = new StringBuilder();
        sb.Append($"<binary, {bytes.Length} bytes>");
        var count = Math.Min(bytes.Length, maxBytes);
        for (var row = 0; row < count; row += 16)
        {
            sb.AppendLine();
            sb.Append(row.ToString("x6")).Append("  ");
            var ascii = new StringBuilder(16);
            for (var i = 0; i < 16; i++)
            {
                if (row + i < count)
                {
                    var b = bytes[row + i];
                    sb.Append(b.ToString("x2")).Append(' ');
                    ascii.Append(b is >= 0x20 and < 0x7F ? (char)b : '.');
                }
                else
                {
                    sb.Append("   ");
                }
            }
            sb.Append(' ').Append(ascii);
        }
        if (bytes.Length > count) sb.AppendLine().Append('…');
        return sb.ToString();
    }
}

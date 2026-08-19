namespace KafkaStudio.Core.Messaging;

/// <summary>
/// A single Kafka record, normalized to a broker-agnostic shape used throughout KafkaStudio.
/// </summary>
public sealed record KafkaMessage
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public string? Key { get; init; }

    /// <summary>Raw value bytes decoded as UTF-8 text. Binary/Avro payloads should use <see cref="RawValue"/>.</summary>
    public string? Value { get; init; }

    public byte[]? RawValue { get; init; }

    public IReadOnlyDictionary<string, string> Headers { get; init; } =
        new Dictionary<string, string>();

    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>Set by the gateway when the message was read as part of a specific consumer group's subscription.</summary>
    public string? ConsumerGroup { get; init; }

    /// <summary>
    /// <see cref="Value"/> pretty-printed as indented JSON when it parses as JSON; otherwise the raw value
    /// unchanged. Used by the UI to display message bodies in a more readable, "beautified" form while
    /// still remaining plain, selectable/copyable text.
    /// </summary>
    public string PrettyValue
    {
        get
        {
            if (Value is null) return "<binary>";

            try
            {
                using var document = System.Text.Json.JsonDocument.Parse(Value);
                return System.Text.Json.JsonSerializer.Serialize(document.RootElement,
                    new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
            }
            catch (System.Text.Json.JsonException)
            {
                return Value;
            }
        }
    }

    public string ToDisplayString(int maxLength = 200)
    {
        var v = Value ?? "<binary>";
        if (v.Length > maxLength)
        {
            v = string.Concat(v.AsSpan(0, maxLength), "…");
        }
        return $"[{Topic}#{Partition}@{Offset}] key={Key ?? "<null>"} value={v}";
    }
}


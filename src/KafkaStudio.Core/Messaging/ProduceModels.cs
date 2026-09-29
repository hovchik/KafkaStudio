namespace KafkaStudio.Core.Messaging;

public sealed record ProduceRequest
{
    public required string Topic { get; init; }
    public string? Key { get; init; }

    /// <summary>Text value, sent as UTF-8. Null (with no <see cref="RawValue"/>) produces a tombstone.</summary>
    public required string? Value { get; init; }

    /// <summary>
    /// Exact value bytes. Takes precedence over <see cref="Value"/> when set, so relaying a binary
    /// (Avro/Protobuf/compressed) message byte-for-byte doesn't corrupt it through a text round trip.
    /// </summary>
    public byte[]? RawValue { get; init; }

    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    /// <summary>Leave null to let the partitioner (murmur2 hash of the key, or round-robin) decide.</summary>
    public int? Partition { get; init; }

    /// <summary>The bytes that will actually be sent, or null for a tombstone.</summary>
    public byte[]? GetValueBytes() =>
        RawValue ?? (Value is null ? null : System.Text.Encoding.UTF8.GetBytes(Value));
}

public sealed record ProduceReceipt
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
}

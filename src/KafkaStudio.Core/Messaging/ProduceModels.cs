namespace KafkaStudio.Core.Messaging;

public sealed record ProduceRequest
{
    public required string Topic { get; init; }
    public string? Key { get; init; }
    public required string Value { get; init; }
    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    /// <summary>Leave null to let the partitioner (murmur2 hash of the key, or round-robin) decide.</summary>
    public int? Partition { get; init; }
}

public sealed record ProduceReceipt
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
}

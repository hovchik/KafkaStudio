namespace KafkaStudio.Core.Messaging;

public sealed record PartitionInfo
{
    public required int Id { get; init; }
    public required int LeaderBrokerId { get; init; }
    public required long EarliestOffset { get; init; }
    public required long LatestOffset { get; init; }

    public long MessageCount => LatestOffset - EarliestOffset;
}

public sealed record TopicMetadata
{
    public required string Name { get; init; }
    public required int ReplicationFactor { get; init; }
    public required IReadOnlyList<PartitionInfo> Partitions { get; init; }

    public long TotalMessageCount => Partitions.Sum(p => p.MessageCount);
}

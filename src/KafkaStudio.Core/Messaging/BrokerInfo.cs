namespace KafkaStudio.Core.Messaging;

public sealed record BrokerInfo
{
    public required int Id { get; init; }
    public required string Host { get; init; }
    public required int Port { get; init; }
    public bool IsController { get; init; }
    public string? Rack { get; init; }

    public string Address => $"{Host}:{Port}";
}

public sealed record ClusterInfo
{
    public string? ClusterId { get; init; }
    public int? ControllerId { get; init; }
    public required IReadOnlyList<BrokerInfo> Brokers { get; init; }
}

public sealed record BrokerConfigEntry
{
    public required string Name { get; init; }
    public string? Value { get; init; }
    public bool IsDefault { get; init; }
    public bool IsSensitive { get; init; }
    public string? Source { get; init; }
}

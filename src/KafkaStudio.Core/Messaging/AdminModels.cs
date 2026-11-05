namespace KafkaStudio.Core.Messaging;

/// <summary>One configuration entry of a topic.</summary>
public sealed record TopicConfigEntry
{
    public required string Name { get; init; }
    public string? Value { get; init; }

    /// <summary>True when the value is the broker default (no topic-level override).</summary>
    public bool IsDefault { get; init; }
    public bool IsReadOnly { get; init; }
    public bool IsSensitive { get; init; }
    public string? Source { get; init; }
}

/// <summary>A single access control entry on the cluster.</summary>
public sealed record AclEntry
{
    public required string ResourceType { get; init; }
    public required string ResourceName { get; init; }
    public string PatternType { get; init; } = "Literal";
    public required string Principal { get; init; }
    public string Host { get; init; } = "*";
    public required string Operation { get; init; }
    public required string Permission { get; init; }
}

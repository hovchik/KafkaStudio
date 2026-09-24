namespace KafkaStudio.Core.Topics;

/// <summary>
/// A named set of topics captured from a cross-topic search's results, so a future search can be
/// scoped to just those topics instead of every topic on the connection.
/// </summary>
public sealed record SavedTopicSet
{
    public required string Name { get; init; }
    public required IReadOnlyList<string> Topics { get; init; }
}

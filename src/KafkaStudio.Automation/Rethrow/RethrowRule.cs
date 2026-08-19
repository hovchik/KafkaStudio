using KafkaStudio.Scripting.Ast;

namespace KafkaStudio.Automation.Rethrow;

/// <summary>
/// A standing "relay" rule: continuously watch <see cref="SourceTopic"/> and, for every message that
/// matches <see cref="Filters"/>, republish it to <see cref="DestinationTopic"/>. This is the
/// first-class, no-script way to express the "rethrow messages from one topic to another" workflow;
/// the same behaviour is also expressible as a one-shot KafScript scenario ("watch" + "message
/// arrives" + "rethrow last message"), which is what you'd use inside a repeatable check instead.
/// </summary>
public sealed record RethrowRule
{
    public required string Name { get; init; }
    public required string SourceConnection { get; init; }
    public required string SourceTopic { get; init; }
    public required string DestinationConnection { get; init; }
    public required string DestinationTopic { get; init; }

    public IReadOnlyList<Condition> Filters { get; init; } = Array.Empty<Condition>();

    public bool KeepSourceKey { get; init; } = true;
    public string? FixedKey { get; init; }

    public IReadOnlyDictionary<string, string> ExtraHeaders { get; init; } = new Dictionary<string, string>();
}

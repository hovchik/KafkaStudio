namespace KafkaStudio.Scripting.Ast;

public sealed record Step(StepKeyword Keyword, ScriptAction Action, int Line);

/// <summary>
/// A Scenario or Task. <see cref="Tags"/> are the <c>@tags</c> written on the lines above its header
/// (without the <c>@</c>, in source order). A block expanded from a <c>Scenario Outline</c> carries the
/// outline's name in <see cref="OutlineName"/> and its 1-based example row in <see cref="ExampleIndex"/>.
/// Steps from a <c>Background:</c> are already prepended to <see cref="Steps"/>.
/// </summary>
public sealed record ScriptBlock(
    BlockKind Kind,
    string Name,
    ScheduleSpec? Schedule,
    IReadOnlyList<Step> Steps,
    int Line)
{
    public IReadOnlyList<string> Tags { get; init; } = Array.Empty<string>();

    public string? OutlineName { get; init; }

    public int? ExampleIndex { get; init; }

    public bool HasTag(string tag) =>
        Tags.Any(t => string.Equals(t, tag.TrimStart('@'), StringComparison.OrdinalIgnoreCase));
}

/// <summary>A parsed file. <see cref="Background"/> holds the <c>Background:</c> steps (already merged
/// into every block), kept separately so tools can show them. <see cref="FeatureName"/> is the optional
/// <c>Feature:</c> line's text (tags written above it are inherited by every block).</summary>
public sealed record ScriptDocument(IReadOnlyList<ScriptBlock> Blocks)
{
    public IReadOnlyList<Step> Background { get; init; } = Array.Empty<Step>();

    public string? FeatureName { get; init; }
}

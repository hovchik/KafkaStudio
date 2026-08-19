namespace KafkaStudio.Scripting.Ast;

public sealed record Step(StepKeyword Keyword, ScriptAction Action, int Line);

public sealed record ScriptBlock(
    BlockKind Kind,
    string Name,
    ScheduleSpec? Schedule,
    IReadOnlyList<Step> Steps,
    int Line);

public sealed record ScriptDocument(IReadOnlyList<ScriptBlock> Blocks);

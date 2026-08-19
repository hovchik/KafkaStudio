using KafkaStudio.Scripting.Ast;

namespace KafkaStudio.Scripting.Runtime;

public enum StepStatus { Passed, Failed, Skipped }

public sealed record StepResult(Step Step, StepStatus Status, string Message, TimeSpan Duration);

public sealed record ScriptRunResult(
    ScriptBlock Block,
    bool Success,
    IReadOnlyList<StepResult> Steps,
    TimeSpan Duration)
{
    public string Summary => Success
        ? $"{Block.Name}: passed ({Steps.Count} step(s), {Duration.TotalMilliseconds:F0} ms)"
        : $"{Block.Name}: FAILED - {Steps.LastOrDefault(s => s.Status == StepStatus.Failed)?.Message ?? "unknown error"}";
}

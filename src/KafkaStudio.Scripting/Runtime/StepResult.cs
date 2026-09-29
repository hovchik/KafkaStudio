using KafkaStudio.Scripting.Ast;

namespace KafkaStudio.Scripting.Runtime;

public enum StepStatus { Passed, Failed, Skipped, Cancelled }

public sealed record StepResult(Step Step, StepStatus Status, string Message, TimeSpan Duration);

public sealed record ScriptRunResult(
    ScriptBlock Block,
    bool Success,
    IReadOnlyList<StepResult> Steps,
    TimeSpan Duration,
    bool Cancelled = false)
{
    public string? FailureMessage =>
        Steps.LastOrDefault(s => s.Status is StepStatus.Failed or StepStatus.Cancelled)?.Message;

    public string Summary => Success
        ? $"{Block.Name}: passed ({Steps.Count} step(s), {Duration.TotalMilliseconds:F0} ms)"
        : Cancelled
            ? $"{Block.Name}: cancelled"
            : $"{Block.Name}: FAILED - {FailureMessage ?? "unknown error"}";
}

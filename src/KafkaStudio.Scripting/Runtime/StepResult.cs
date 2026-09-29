using KafkaStudio.Scripting.Ast;

namespace KafkaStudio.Scripting.Runtime;

public enum StepStatus { Passed, Failed, Skipped, Cancelled }

/// <summary>
/// One step's outcome. For a Failed step, <see cref="IsError"/> tells a broken/misconfigured script or
/// an infrastructure problem (unknown connection, broker unreachable, invalid schema file) apart from a
/// check that ran fine and wasn't satisfied (a <see cref="StepAssertionException"/>) - test reports
/// list the former as errors and the latter as failures.
/// </summary>
public sealed record StepResult(Step Step, StepStatus Status, string Message, TimeSpan Duration)
{
    public bool IsError { get; init; }
}

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

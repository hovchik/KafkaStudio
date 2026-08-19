namespace KafkaStudio.Scripting.Runtime;

/// <summary>
/// Raised when a step's condition is well-formed but not satisfied (e.g. an "expect message... within
/// 30 seconds" that timed out, or a failed "assert"). Distinguished from the base
/// <see cref="KafScriptException"/> so callers (the check engine, the UI) can tell "this check failed"
/// apart from "this script is broken / misconfigured".
/// </summary>
public sealed class StepAssertionException : KafScriptException
{
    public StepAssertionException(string message, int? line = null) : base(message, line)
    {
    }
}

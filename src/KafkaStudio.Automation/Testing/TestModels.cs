using KafkaStudio.Scripting.Ast;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.Automation.Testing;

/// <summary>
/// One runnable test: a Scenario (or an expanded Scenario Outline row) from a .kafscript file. A file that
/// doesn't parse is still represented - as a single case with <see cref="LoadError"/> set - so a broken
/// test file shows up as an error in the report instead of silently shrinking the suite.
/// </summary>
public sealed record TestCase(string FilePath, ScriptBlock? Block, string? FeatureName = null, string? LoadError = null)
{
    /// <summary>Identity within a run: file, line and name (the line keeps two same-named scenarios,
    /// or two identical Examples rows, apart).</summary>
    public string Id => $"{FilePath}:{Line}:{Name}";

    public string Name => Block?.Name ?? Path.GetFileName(FilePath);

    public string FileName => string.IsNullOrEmpty(FilePath) ? "(editor)" : Path.GetFileName(FilePath);

    /// <summary>The suite name used in reports: the Feature line, or the file name without extension.</summary>
    public string SuiteName => FeatureName ?? (string.IsNullOrEmpty(FilePath) ? "Script Editor" : Path.GetFileNameWithoutExtension(FilePath));

    public IReadOnlyList<string> Tags => Block?.Tags ?? Array.Empty<string>();

    public int Line => Block?.Line ?? 0;
}

public enum TestOutcome { Passed, Failed, Error, Skipped, Cancelled }

/// <summary>The result of running one <see cref="TestCase"/> (its last attempt, when retried).</summary>
public sealed record TestCaseResult
{
    public required TestCase Case { get; init; }
    public required TestOutcome Outcome { get; init; }
    public required TimeSpan Duration { get; init; }
    public DateTimeOffset StartedAt { get; init; }

    /// <summary>How many times it ran (1 unless retried).</summary>
    public int Attempts { get; init; } = 1;

    /// <summary>Step results of the last attempt.</summary>
    public IReadOnlyList<StepResult> Steps { get; init; } = Array.Empty<StepResult>();

    /// <summary>Why it failed / errored / was skipped.</summary>
    public string? Message { get; init; }

    /// <summary>Source line of the failing step, when there is one.</summary>
    public int? FailedLine { get; init; }

    /// <summary>Failure messages of earlier attempts (a test that passes on a retry is flaky).</summary>
    public IReadOnlyList<string> EarlierFailures { get; init; } = Array.Empty<string>();

    public bool IsFlaky => Outcome == TestOutcome.Passed && Attempts > 1;

    public StepResult? FailedStep => Steps.LastOrDefault(s => s.Status is StepStatus.Failed or StepStatus.Cancelled);
}

/// <summary>Everything a report needs about one suite run.</summary>
public sealed record TestRunReport
{
    public string Name { get; init; } = "KafScript tests";
    public required DateTimeOffset StartedAt { get; init; }
    public required TimeSpan Duration { get; init; }
    public required IReadOnlyList<TestCaseResult> Results { get; init; }

    /// <summary>The tag filter the run used, if any ("@smoke and not @wip").</summary>
    public string? TagFilter { get; init; }

    /// <summary>Free-form environment facts shown in reports (connections, machine, variables...).</summary>
    public IReadOnlyDictionary<string, string> Environment { get; init; } = new Dictionary<string, string>();

    public bool WasCancelled { get; init; }

    public int Total => Results.Count;
    public int Passed => Results.Count(r => r.Outcome == TestOutcome.Passed);
    public int Failed => Results.Count(r => r.Outcome == TestOutcome.Failed);
    public int Errors => Results.Count(r => r.Outcome == TestOutcome.Error);
    public int Skipped => Results.Count(r => r.Outcome is TestOutcome.Skipped or TestOutcome.Cancelled);
    public int Flaky => Results.Count(r => r.IsFlaky);

    /// <summary>True when nothing failed or errored (skips don't count against a run).</summary>
    public bool Success => Failed == 0 && Errors == 0 && !WasCancelled;

    /// <summary>Passed / executed, 0..100 (skipped tests are left out).</summary>
    public double PassRate
    {
        get
        {
            var executed = Passed + Failed + Errors;
            return executed == 0 ? 0 : 100.0 * Passed / executed;
        }
    }

    public string Summary =>
        $"{Passed} passed, {Failed} failed, {Errors} error(s), {Skipped} skipped" +
        (Flaky > 0 ? $", {Flaky} flaky" : "") +
        $" · {Total} test(s) in {FormatDuration(Duration)}";

    public static string FormatDuration(TimeSpan d) =>
        d.TotalMinutes >= 1 ? $"{(int)d.TotalMinutes}m {d.Seconds}s"
        : d.TotalSeconds >= 1 ? $"{d.TotalSeconds:0.0}s"
        : $"{d.TotalMilliseconds:0}ms";
}

/// <summary>How a suite run selects and runs tests.</summary>
public sealed record TestRunOptions
{
    public TagExpression Tags { get; init; } = TagExpression.Any;

    /// <summary>Only tests whose name contains this (case-insensitive).</summary>
    public string? NameFilter { get; init; }

    /// <summary>Re-run a failed/errored test up to this many extra times (0 = no retries).</summary>
    public int Retries { get; init; }

    /// <summary>Stop at the first failure; the remaining tests are reported as skipped.</summary>
    public bool FailFast { get; init; }

    /// <summary>A test still running after this long is stopped and reported as an error.</summary>
    public TimeSpan? Timeout { get; init; }

    /// <summary>Variables every test starts with (environment data such as a base URL or test customer id).</summary>
    public IReadOnlyDictionary<string, string> Variables { get; init; } = new Dictionary<string, string>();

    /// <summary>Also run Task blocks once (by default only Scenarios are tests).</summary>
    public bool IncludeTasks { get; init; }
}

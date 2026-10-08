using System.Diagnostics;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.Automation.Testing;

/// <summary>
/// Runs a list of <see cref="TestCase"/>s one after another (tests share Kafka topics, so running them in
/// parallel would make them interfere) and produces a <see cref="TestRunReport"/>. Adds what a QA run
/// needs on top of <see cref="ScriptRunner"/>: retries of failed tests (flaky detection), fail-fast, a
/// per-test timeout, shared starting variables, and failures vs. errors kept apart for reports.
/// Used by both the app's Test Runner screen and the headless <c>kafkastudio-test</c> CLI.
/// </summary>
public sealed class TestSuiteRunner
{
    private readonly IReadOnlyDictionary<string, IKafkaGateway> _connections;
    private readonly IKafkaGateway? _defaultGateway;

    public TestSuiteRunner(IReadOnlyDictionary<string, IKafkaGateway> connections, IKafkaGateway? defaultGateway = null)
    {
        _connections = connections;
        _defaultGateway = defaultGateway;
    }

    /// <summary>Raised (on the runner's thread) just before a test (or one of its retries) starts.</summary>
    public event Action<TestCase, int>? CaseStarted;

    /// <summary>Raised (on the runner's thread) for every step of every attempt, as it completes.</summary>
    public event Action<TestCase, StepResult>? StepCompleted;

    /// <summary>Raised (on the runner's thread) when a test's final result is known.</summary>
    public event Action<TestCaseResult>? CaseCompleted;

    public async Task<TestRunReport> RunAsync(
        IReadOnlyList<TestCase> cases,
        TestRunOptions options,
        CancellationToken cancellationToken = default,
        string name = "KafScript tests",
        IReadOnlyDictionary<string, string>? environment = null)
    {
        var startedAt = DateTimeOffset.Now;
        var total = Stopwatch.StartNew();
        var results = new List<TestCaseResult>();
        var stopReason = (string?)null;

        void Complete(TestCaseResult result)
        {
            results.Add(result);
            CaseCompleted?.Invoke(result);
        }

        foreach (var testCase in cases)
        {
            if (stopReason is not null || cancellationToken.IsCancellationRequested)
            {
                Complete(new TestCaseResult
                {
                    Case = testCase,
                    Outcome = TestOutcome.Skipped,
                    Duration = TimeSpan.Zero,
                    StartedAt = DateTimeOffset.Now,
                    Message = cancellationToken.IsCancellationRequested ? "run was stopped" : stopReason
                });
                continue;
            }

            var result = await RunCaseAsync(testCase, options, cancellationToken).ConfigureAwait(false);
            Complete(result);
            if (options.FailFast && result.Outcome is TestOutcome.Failed or TestOutcome.Error)
            {
                stopReason = $"not run: fail-fast stopped the run after '{testCase.Name}' failed";
            }
        }

        return new TestRunReport
        {
            Name = name,
            StartedAt = startedAt,
            Duration = total.Elapsed,
            Results = results,
            TagFilter = string.IsNullOrEmpty(options.Tags.Text) ? null : options.Tags.Text,
            Environment = environment ?? new Dictionary<string, string>(),
            WasCancelled = cancellationToken.IsCancellationRequested
        };
    }

    private async Task<TestCaseResult> RunCaseAsync(TestCase testCase, TestRunOptions options, CancellationToken cancellationToken)
    {
        var startedAt = DateTimeOffset.Now;
        if (testCase.Block is null)
        {
            CaseStarted?.Invoke(testCase, 1);
            return new TestCaseResult
            {
                Case = testCase,
                Outcome = TestOutcome.Error,
                Duration = TimeSpan.Zero,
                StartedAt = startedAt,
                Message = testCase.LoadError ?? "no scenario to run"
            };
        }

        var timer = Stopwatch.StartNew();
        var earlier = new List<string>();
        var maxAttempts = 1 + Math.Max(0, options.Retries);
        for (var attempt = 1; ; attempt++)
        {
            CaseStarted?.Invoke(testCase, attempt);
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (options.Timeout is { } timeout) timeoutCts.CancelAfter(timeout);

            var runner = new ScriptRunner(_connections, _defaultGateway)
            {
                InitialVariables = options.Variables,
                BaseDirectory = string.IsNullOrEmpty(testCase.FilePath) ? null : Path.GetDirectoryName(testCase.FilePath)
            };
            runner.StepCompleted += step => StepCompleted?.Invoke(testCase, step);

            ScriptRunResult run;
            string? crash = null;
            try
            {
                run = await runner.RunAsync(testCase.Block, timeoutCts.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // ScriptRunner turns script/Kafka errors into failed steps; this is a last-resort guard.
                run = new ScriptRunResult(testCase.Block, false, Array.Empty<StepResult>(), timer.Elapsed);
                crash = $"unexpected error: {ex.Message}";
            }

            var timedOut = run.Cancelled && !cancellationToken.IsCancellationRequested;
            var (outcome, message) = Classify(run, timedOut, options.Timeout);
            if (crash is not null) (outcome, message) = (TestOutcome.Error, crash);
            var failed = run.Steps.LastOrDefault(s => s.Status is StepStatus.Failed or StepStatus.Cancelled);

            var retry = outcome is TestOutcome.Failed or TestOutcome.Error && attempt < maxAttempts && !cancellationToken.IsCancellationRequested;
            if (retry)
            {
                earlier.Add($"attempt {attempt}: {message}");
                continue;
            }

            return new TestCaseResult
            {
                Case = testCase,
                Outcome = outcome,
                Duration = timer.Elapsed,
                StartedAt = startedAt,
                Attempts = attempt,
                Steps = run.Steps,
                Message = message,
                FailedLine = outcome == TestOutcome.Passed ? null : failed?.Step.Line,
                EarlierFailures = earlier
            };
        }
    }

    private static (TestOutcome, string?) Classify(ScriptRunResult run, bool timedOut, TimeSpan? timeout)
    {
        if (run.Success) return (TestOutcome.Passed, null);
        if (timedOut) return (TestOutcome.Error, $"timed out after {TestRunReport.FormatDuration(timeout ?? TimeSpan.Zero)}");
        if (run.Cancelled) return (TestOutcome.Cancelled, "run was stopped");
        var failed = run.Steps.LastOrDefault(s => s.Status == StepStatus.Failed);
        if (failed is null) return (TestOutcome.Error, run.FailureMessage ?? "unknown error");
        return (failed.IsError ? TestOutcome.Error : TestOutcome.Failed, failed.Message);
    }
}

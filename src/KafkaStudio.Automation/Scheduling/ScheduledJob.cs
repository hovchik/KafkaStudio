using KafkaStudio.Core.Abstractions;
using KafkaStudio.Scripting.Ast;

namespace KafkaStudio.Automation.Scheduling;

/// <summary>A KafScript Scenario/Task block registered with the <see cref="AutomationScheduler"/>,
/// bound to the connections it should run against.</summary>
public sealed class ScheduledJob
{
    private int _isRunning;

    public required string Id { get; init; }
    public required ScriptBlock Block { get; init; }
    public required IReadOnlyDictionary<string, IKafkaGateway> Connections { get; init; }

    /// <summary>Disabled jobs keep their schedule but are skipped by the timer (Run now still works).</summary>
    public bool Enabled { get; set; } = true;

    public DateTimeOffset? LastRunAt { get; internal set; }
    public DateTimeOffset? NextRunAt { get; internal set; }
    public int RunCount { get; internal set; }

    /// <summary>True while a run of this job is in progress; a job never runs concurrently with itself.</summary>
    public bool IsRunning => Volatile.Read(ref _isRunning) == 1;

    internal bool TryBeginRun() => Interlocked.CompareExchange(ref _isRunning, 1, 0) == 0;

    internal void EndRun() => Volatile.Write(ref _isRunning, 0);
}

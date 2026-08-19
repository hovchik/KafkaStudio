using System.Collections.Concurrent;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Scripting.Ast;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.Automation.Scheduling;

/// <summary>
/// Drives every "Task" (and, when run on a schedule, "Scenario") block: computes each job's next due
/// time from its <see cref="ScheduleSpec"/> ("run once" / "every &lt;duration&gt;" / "at &lt;hh:mm&gt;")
/// and fires it via <see cref="ScriptRunner"/> when due. Deliberately hand-rolled on
/// <see cref="PeriodicTimer"/> instead of a hosting framework's BackgroundService, so it has zero
/// dependencies beyond the BCL and Core/Scripting.
/// </summary>
public sealed class AutomationScheduler : IAsyncDisposable
{
    private static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(1);

    private readonly ConcurrentDictionary<string, ScheduledJob> _jobs = new();
    private readonly IClock _clock;
    private CancellationTokenSource? _cts;
    private Task? _loopTask;

    public event Action<ScheduledJob, ScriptRunResult>? RunCompleted;
    public event Action<ScheduledJob, Exception>? RunFailed;

    public AutomationScheduler(IClock? clock = null)
    {
        _clock = clock ?? SystemClock.Instance;
    }

    public IReadOnlyCollection<ScheduledJob> Jobs => (IReadOnlyCollection<ScheduledJob>)_jobs.Values;

    public ScheduledJob Register(string id, ScriptBlock block, IReadOnlyDictionary<string, IKafkaGateway> connections)
    {
        var job = new ScheduledJob { Id = id, Block = block, Connections = connections };
        ComputeNextRun(job);
        _jobs[id] = job;
        return job;
    }

    public void Unregister(string id) => _jobs.TryRemove(id, out _);

    /// <summary>Runs a job immediately, outside of its normal schedule (e.g. a UI "Run now" button).</summary>
    public Task RunNowAsync(string id, CancellationToken cancellationToken = default) =>
        _jobs.TryGetValue(id, out var job)
            ? RunJobAsync(job, cancellationToken)
            : throw new KeyNotFoundException($"no scheduled job with id '{id}'");

    public void Start()
    {
        if (_loopTask is not null) return;
        _cts = new CancellationTokenSource();
        _loopTask = Task.Run(() => LoopAsync(_cts.Token));
    }

    private async Task LoopAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TickInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                var now = _clock.UtcNow;
                foreach (var job in _jobs.Values)
                {
                    if (job.Enabled && job.NextRunAt is { } next && next <= now)
                    {
                        _ = RunJobAsync(job, cancellationToken); // don't let one slow job stall the tick
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // normal shutdown
        }
    }

    private async Task RunJobAsync(ScheduledJob job, CancellationToken cancellationToken)
    {
        job.LastRunAt = _clock.UtcNow;
        job.RunCount++;
        ComputeNextRun(job); // scheduled before running, so a slow job doesn't get queued again on the next tick

        try
        {
            var runner = new ScriptRunner(job.Connections);
            var result = await runner.RunAsync(job.Block, cancellationToken).ConfigureAwait(false);
            RunCompleted?.Invoke(job, result);
        }
        catch (Exception ex)
        {
            RunFailed?.Invoke(job, ex);
        }
    }

    private void ComputeNextRun(ScheduledJob job)
    {
        var schedule = job.Block.Schedule;
        if (schedule is null)
        {
            job.NextRunAt = null;
            return;
        }

        var now = _clock.UtcNow;
        job.NextRunAt = schedule.Kind switch
        {
            ScheduleKind.RunOnce => job.LastRunAt is null ? now : null,
            ScheduleKind.Every => now + schedule.Every!.ToTimeSpan(),
            ScheduleKind.At => NextDailyOccurrence(now, schedule.At!.Value),
            _ => null
        };
    }

    private static DateTimeOffset NextDailyOccurrence(DateTimeOffset now, TimeOnly at)
    {
        var candidate = new DateTimeOffset(now.Year, now.Month, now.Day, at.Hour, at.Minute, 0, now.Offset);
        if (candidate <= now) candidate = candidate.AddDays(1);
        return candidate;
    }

    public async ValueTask DisposeAsync()
    {
        _cts?.Cancel();
        if (_loopTask is not null)
        {
            try { await _loopTask.ConfigureAwait(false); }
            catch { /* shutdown */ }
        }
        _cts?.Dispose();
    }
}

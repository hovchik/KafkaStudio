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
///
/// Guarantees: a job never overlaps with itself (a slow "every 10 seconds" job that takes 30s skips
/// the ticks it's busy for instead of piling up concurrent runs), and "at HH:MM" means local wall-clock
/// time in <see cref="TimeZone"/> (the machine's time zone by default), not UTC.
/// </summary>
public sealed class AutomationScheduler : IAsyncDisposable
{
    private static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(1);

    private readonly ConcurrentDictionary<string, ScheduledJob> _jobs = new();
    private readonly ConcurrentDictionary<Task, byte> _inFlight = new();
    private readonly IClock _clock;
    // Shutdown token for every run, manual ones included - it exists from construction so a "Run now"
    // can be stopped by DisposeAsync even when the schedule loop was never started.
    private readonly CancellationTokenSource _cts = new();
    private Task? _loopTask;

    public event Action<ScheduledJob>? RunStarted;
    public event Action<ScheduledJob, ScriptRunResult>? RunCompleted;
    public event Action<ScheduledJob, Exception>? RunFailed;

    public AutomationScheduler(IClock? clock = null, TimeZoneInfo? timeZone = null)
    {
        _clock = clock ?? SystemClock.Instance;
        TimeZone = timeZone ?? TimeZoneInfo.Local;
    }

    /// <summary>Time zone "at HH:MM" schedules are interpreted in.</summary>
    public TimeZoneInfo TimeZone { get; }

    public IReadOnlyCollection<ScheduledJob> Jobs => (IReadOnlyCollection<ScheduledJob>)_jobs.Values;

    public ScheduledJob Register(string id, ScriptBlock block, IReadOnlyDictionary<string, IKafkaGateway> connections)
    {
        var job = new ScheduledJob { Id = id, Block = block, Connections = connections };
        ComputeNextRun(job);
        _jobs[id] = job;
        return job;
    }

    public void Unregister(string id) => _jobs.TryRemove(id, out _);

    /// <summary>Enables/disables a job. Re-enabling recomputes its next run from now, so a job that was
    /// disabled for a while doesn't immediately fire for the slot it missed.</summary>
    public void SetEnabled(string id, bool enabled)
    {
        if (!_jobs.TryGetValue(id, out var job)) return;
        job.Enabled = enabled;
        if (enabled && job.Block.Schedule?.Kind != ScheduleKind.RunOnce) ComputeNextRun(job);
    }

    /// <summary>Runs a job immediately, outside of its normal schedule (e.g. a UI "Run now" button).
    /// Throws <see cref="InvalidOperationException"/> if that job is already running.</summary>
    public Task RunNowAsync(string id, CancellationToken cancellationToken = default)
    {
        if (!_jobs.TryGetValue(id, out var job))
        {
            throw new KeyNotFoundException($"no scheduled job with id '{id}'");
        }
        if (!job.TryBeginRun())
        {
            throw new InvalidOperationException($"'{job.Block.Name}' is already running");
        }

        // Also stop a manual run when the scheduler shuts down.
        var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
        return Track(RunAndDisposeAsync(job, linked));
    }

    private async Task RunAndDisposeAsync(ScheduledJob job, CancellationTokenSource linked)
    {
        using (linked)
        {
            await RunJobAsync(job, linked.Token, manual: true).ConfigureAwait(false);
        }
    }

    public void Start()
    {
        if (_loopTask is not null) return;
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
                    if (job.Enabled && job.NextRunAt is { } next && next <= now && job.TryBeginRun())
                    {
                        _ = Track(RunJobAsync(job, cancellationToken, manual: false)); // don't let one slow job stall the tick
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // normal shutdown
        }
    }

    private Task Track(Task task)
    {
        _inFlight[task] = 0;
        _ = task.ContinueWith(t => _inFlight.TryRemove(t, out _), TaskScheduler.Default);
        return task;
    }

    /// <summary>Precondition: <see cref="ScheduledJob.TryBeginRun"/> succeeded.</summary>
    private async Task RunJobAsync(ScheduledJob job, CancellationToken cancellationToken, bool manual)
    {
        try
        {
            job.LastRunAt = _clock.UtcNow;
            job.RunCount++;
            // A manual run doesn't consume a "run once" job's scheduled run, nor shift an "every" cadence.
            if (!manual) ComputeNextRun(job);
            SafeInvoke(() => RunStarted?.Invoke(job));

            var runner = new ScriptRunner(job.Connections);
            var result = await runner.RunAsync(job.Block, cancellationToken).ConfigureAwait(false);
            SafeInvoke(() => RunCompleted?.Invoke(job, result));
        }
        catch (Exception ex)
        {
            SafeInvoke(() => RunFailed?.Invoke(job, ex));
        }
        finally
        {
            job.EndRun();
        }
    }

    // A throwing UI handler must not kill the scheduler or leave a job marked as running.
    private static void SafeInvoke(Action action)
    {
        try { action(); }
        catch { /* subscriber bug - ignore */ }
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
            ScheduleKind.At => NextDailyOccurrence(now, schedule.At!.Value, TimeZone),
            _ => null
        };
    }

    /// <summary>Next instant (after <paramref name="now"/>) whose wall-clock time in
    /// <paramref name="zone"/> is <paramref name="at"/>. Handles DST: a time skipped by a
    /// spring-forward transition runs at the first valid instant after it; an ambiguous fall-back time
    /// runs once, at its first occurrence.</summary>
    public static DateTimeOffset NextDailyOccurrence(DateTimeOffset now, TimeOnly at, TimeZoneInfo zone)
    {
        var localNow = TimeZoneInfo.ConvertTime(now, zone);
        for (var dayOffset = 0; dayOffset <= 2; dayOffset++)
        {
            var date = DateOnly.FromDateTime(localNow.DateTime).AddDays(dayOffset);
            var local = date.ToDateTime(at, DateTimeKind.Unspecified);

            while (zone.IsInvalidTime(local)) local = local.AddMinutes(1);

            var offset = zone.IsAmbiguousTime(local)
                ? zone.GetAmbiguousTimeOffsets(local).Max()
                : zone.GetUtcOffset(local);
            var candidate = new DateTimeOffset(local, offset);
            if (candidate > now) return candidate.ToUniversalTime();
        }

        return now.AddDays(1); // unreachable in practice
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        if (_loopTask is not null)
        {
            try { await _loopTask.ConfigureAwait(false); }
            catch { /* shutdown */ }
        }

        // Let in-flight runs observe cancellation and close their Kafka subscriptions before the
        // gateways they use get disposed.
        try { await Task.WhenAll(_inFlight.Keys).WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false); }
        catch { /* shutdown */ }

        _cts.Dispose();
    }
}

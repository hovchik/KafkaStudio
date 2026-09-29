using System.Collections.Concurrent;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Automation.Rethrow;

/// <summary>Starts/stops named <see cref="RethrowRule"/> background relays and tracks which are live -
/// the piece the UI's "Rethrow rules" screen binds to.</summary>
public sealed class RethrowManager : IAsyncDisposable
{
    private sealed record RunningRule(CancellationTokenSource Cts, Task Task);

    private readonly RethrowEngine _engine = new();
    private readonly ConcurrentDictionary<string, RunningRule> _running = new();

    public event Action<RethrowRule, KafkaMessage, ProduceReceipt>? MessageRelayed
    {
        add => _engine.MessageRelayed += value;
        remove => _engine.MessageRelayed -= value;
    }

    public event Action<RethrowRule, KafkaMessage>? MessageSkipped
    {
        add => _engine.MessageSkipped += value;
        remove => _engine.MessageSkipped -= value;
    }

    public event Action<RethrowRule, Exception>? RelayFailed
    {
        add => _engine.RelayFailed += value;
        remove => _engine.RelayFailed -= value;
    }

    /// <summary>
    /// Raised when a rule stops on its own (not via <see cref="StopAsync"/>) - e.g. its stream ended or
    /// it failed permanently. The exception is null for a clean end. Without this a dead relay would
    /// keep showing as "running" forever.
    /// </summary>
    public event Action<RethrowRule, Exception?>? RuleStopped;

    public IReadOnlyCollection<string> RunningRuleNames => (IReadOnlyCollection<string>)_running.Keys;

    public bool IsRunning(string ruleName) => _running.ContainsKey(ruleName);

    /// <summary>Starts relaying. Throws <see cref="ArgumentException"/> for an invalid rule and
    /// <see cref="InvalidOperationException"/> if a rule with that name is already running.</summary>
    public void Start(RethrowRule rule, IReadOnlyDictionary<string, IKafkaGateway> connections)
    {
        RethrowEngine.Validate(rule, connections);

        var cts = new CancellationTokenSource();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var task = Task.Run(async () =>
        {
            await gate.Task.ConfigureAwait(false); // don't start before we're registered
            await _engine.RunAsync(rule, connections, cts.Token).ConfigureAwait(false);
        });
        var running = new RunningRule(cts, task);

        if (!_running.TryAdd(rule.Name, running))
        {
            cts.Cancel();
            gate.TrySetResult();
            cts.Dispose();
            throw new InvalidOperationException($"rethrow rule '{rule.Name}' is already running");
        }

        _ = task.ContinueWith(t =>
        {
            // Only report if the rule wasn't stopped deliberately (StopAsync removes it first).
            if (((ICollection<KeyValuePair<string, RunningRule>>)_running).Remove(new KeyValuePair<string, RunningRule>(rule.Name, running)))
            {
                running.Cts.Dispose();
                RuleStopped?.Invoke(rule, t.Exception?.GetBaseException());
            }
        }, TaskScheduler.Default);

        gate.TrySetResult();
    }

    public async Task StopAsync(string ruleName)
    {
        if (_running.TryRemove(ruleName, out var running))
        {
            running.Cts.Cancel();
            try { await running.Task.ConfigureAwait(false); }
            catch { /* expected on cancellation */ }
            running.Cts.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var name in _running.Keys.ToList())
        {
            await StopAsync(name).ConfigureAwait(false);
        }
    }
}

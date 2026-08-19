using System.Collections.Concurrent;
using KafkaStudio.Core.Abstractions;

namespace KafkaStudio.Automation.Rethrow;

/// <summary>Starts/stops named <see cref="RethrowRule"/> background relays and tracks which are live -
/// the piece the UI's "Rethrow rules" screen binds to.</summary>
public sealed class RethrowManager : IAsyncDisposable
{
    private sealed record RunningRule(CancellationTokenSource Cts, Task Task);

    private readonly RethrowEngine _engine = new();
    private readonly ConcurrentDictionary<string, RunningRule> _running = new();

    public event Action<RethrowRule, Core.Messaging.KafkaMessage, Core.Messaging.ProduceReceipt>? MessageRelayed
    {
        add => _engine.MessageRelayed += value;
        remove => _engine.MessageRelayed -= value;
    }

    public event Action<RethrowRule, Exception>? RelayFailed
    {
        add => _engine.RelayFailed += value;
        remove => _engine.RelayFailed -= value;
    }

    public IReadOnlyCollection<string> RunningRuleNames => (IReadOnlyCollection<string>)_running.Keys;

    public bool IsRunning(string ruleName) => _running.ContainsKey(ruleName);

    public void Start(RethrowRule rule, IReadOnlyDictionary<string, IKafkaGateway> connections)
    {
        if (_running.ContainsKey(rule.Name))
        {
            throw new InvalidOperationException($"rethrow rule '{rule.Name}' is already running");
        }

        var cts = new CancellationTokenSource();
        var task = Task.Run(() => _engine.RunAsync(rule, connections, cts.Token));
        _running[rule.Name] = new RunningRule(cts, task);
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

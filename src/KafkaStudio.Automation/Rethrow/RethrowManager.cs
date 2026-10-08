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
        if (_running.ContainsKey(rule.Name))
        {
            throw new InvalidOperationException($"rethrow rule '{rule.Name}' is already running");
        }
        RejectCycle(rule);

        var cts = new CancellationTokenSource();
        var token = cts.Token; // read before the dispose in the duplicate-name path below can race it
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var task = Task.Run(async () =>
        {
            await gate.Task.ConfigureAwait(false); // don't start before we're registered
            if (token.IsCancellationRequested) return;
            await _engine.RunAsync(rule, connections, token).ConfigureAwait(false);
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
                _rules.TryRemove(rule.Name, out _);
                running.Cts.Dispose();
                RuleStopped?.Invoke(rule, t.Exception?.GetBaseException());
            }
        }, TaskScheduler.Default);

        gate.TrySetResult();
    }

    private readonly ConcurrentDictionary<string, RethrowRule> _rules = new();

    /// <summary>
    /// A rule's own source == destination is caught by <see cref="RethrowEngine.Validate"/>; this catches the
    /// indirect version - A→B while B→A (or A→B, B→C, C→A) is already running - which would otherwise
    /// relay every message round and round forever, flooding every topic on the loop.
    /// </summary>
    private void RejectCycle(RethrowRule rule)
    {
        var edges = _running.Keys
            .Select(name => _rules.TryGetValue(name, out var r) ? r : null)
            .Where(r => r is not null)
            .Select(r => r!)
            .ToList();
        var start = (rule.SourceConnection, rule.SourceTopic);
        var seen = new HashSet<(string, string)> { (rule.DestinationConnection, rule.DestinationTopic) };
        var frontier = new Queue<(string, string)>(seen);
        while (frontier.Count > 0)
        {
            var node = frontier.Dequeue();
            if (node == start)
            {
                throw new ArgumentException(
                    $"'{rule.Name}' would complete a loop: messages relayed to '{rule.DestinationTopic}' already come back to '{rule.SourceTopic}' through a running rule");
            }
            foreach (var edge in edges)
            {
                if ((edge.SourceConnection, edge.SourceTopic) == node && seen.Add((edge.DestinationConnection, edge.DestinationTopic)))
                {
                    frontier.Enqueue((edge.DestinationConnection, edge.DestinationTopic));
                }
            }
        }
        _rules[rule.Name] = rule;
    }

    public async Task StopAsync(string ruleName)
    {
        if (_running.TryRemove(ruleName, out var running))
        {
            _rules.TryRemove(ruleName, out _);
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

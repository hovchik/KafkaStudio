using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Scripting.Runtime;

/// <summary>Mutable state threaded through a single scenario/task run: variables, active connection,
/// the most recently seen message, any bulk-scanned messages, and live topic watches.</summary>
public sealed class ScenarioContext
{
    public Dictionary<string, string> Variables { get; } = new();

    public IKafkaGateway? Gateway { get; set; }

    public KafkaMessage? LastMessage { get; set; }

    public List<KafkaMessage> ScannedMessages { get; } = new();

    public string? LastWatchedTopic { get; set; }

    // Ordinal: Kafka topic names are case-sensitive ("Orders" and "orders" are different topics).
    internal Dictionary<string, WatchHandle> Watches { get; } = new(StringComparer.Ordinal);

    /// <summary>Superseded watches whose disposal hasn't been awaited yet (see ScriptRunner.ExecuteWatch).</summary>
    internal List<Task> PendingDisposals { get; } = new();

    public async ValueTask DisposeWatchesAsync()
    {
        foreach (var handle in Watches.Values)
        {
            await handle.DisposeAsync().ConfigureAwait(false);
        }
        Watches.Clear();

        try { await Task.WhenAll(PendingDisposals).ConfigureAwait(false); }
        catch { /* best effort teardown */ }
        PendingDisposals.Clear();
    }
}

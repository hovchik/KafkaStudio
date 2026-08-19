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

    internal Dictionary<string, WatchHandle> Watches { get; } = new(StringComparer.OrdinalIgnoreCase);

    public async ValueTask DisposeWatchesAsync()
    {
        foreach (var handle in Watches.Values)
        {
            await handle.DisposeAsync().ConfigureAwait(false);
        }
        Watches.Clear();
    }
}

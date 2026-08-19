using KafkaStudio.Core.Connections;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Core.Abstractions;

/// <summary>
/// The single seam between all of KafkaStudio's logic (DSL interpreter, check engine, task runner, UI)
/// and an actual Kafka cluster. Everything above this interface - the scripting language, the
/// rethrow/scan-acknowledge/cross-topic-timing checks, the task scheduler - is written purely against
/// this abstraction, which is what lets it be built and unit tested without a running broker (see
/// <see cref="KafkaStudio.Core.Testing.InMemoryKafkaGateway"/>). The real implementation
/// (KafkaStudio.Kafka's ConfluentKafkaGateway) is a thin adapter over Confluent.Kafka / librdkafka.
/// </summary>
public interface IKafkaGateway : IAsyncDisposable
{
    ConnectionProfile Profile { get; }

    Task ConnectAsync(CancellationToken cancellationToken = default);

    Task<IReadOnlyList<string>> ListTopicsAsync(CancellationToken cancellationToken = default);

    Task<TopicMetadata> DescribeTopicAsync(string topic, CancellationToken cancellationToken = default);

    Task CreateTopicAsync(string topic, int partitions, short replicationFactor,
        CancellationToken cancellationToken = default);

    Task<ProduceReceipt> ProduceAsync(ProduceRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes and streams messages until the caller stops enumerating or cancels. Used directly by
    /// "watch/expect" steps, and as the building block the rethrow service and scan+acknowledge step
    /// are implemented on top of.
    /// </summary>
    IAsyncEnumerable<KafkaMessage> ConsumeAsync(ConsumeOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>Commits the offset for a message that was consumed with AutoAcknowledge = false.</summary>
    Task AcknowledgeAsync(KafkaMessage message, CancellationToken cancellationToken = default);
}

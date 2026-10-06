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

    /// <summary>
    /// Deletes every record in <paramref name="partition"/> of <paramref name="topic"/> whose offset is
    /// lower than <paramref name="beforeOffset"/> (Kafka's DeleteRecords). Kafka cannot remove a single
    /// record from the middle of a log - only advance the log start offset. Returns the new log start offset.
    /// </summary>
    Task<long> DeleteRecordsBeforeAsync(string topic, int partition, long beforeOffset,
        CancellationToken cancellationToken = default);

    /// <summary>Commits the offset for a message that was consumed with AutoAcknowledge = false.</summary>
    Task AcknowledgeAsync(KafkaMessage message, CancellationToken cancellationToken = default);

    /// <summary>Lists the cluster's consumer groups with their state and member count.</summary>
    Task<IReadOnlyList<ConsumerGroupSummary>> ListConsumerGroupsAsync(CancellationToken cancellationToken = default);

    /// <summary>Members, committed offsets, partition end offsets and lag for one consumer group.</summary>
    Task<ConsumerGroupDetail> DescribeConsumerGroupAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Works out (without changing anything) which offset each affected partition would move to, so the
    /// caller can show it for confirmation. Throws if nothing matches the request.
    /// </summary>
    Task<IReadOnlyList<OffsetChange>> PlanOffsetResetAsync(OffsetResetRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the given offsets for the group. Kafka only allows this while the group has no live
    /// members, so this throws <see cref="InvalidOperationException"/> for an active group.
    /// </summary>
    Task ApplyOffsetResetAsync(string groupId, IReadOnlyList<OffsetChange> changes,
        CancellationToken cancellationToken = default);
}

using System.Runtime.CompilerServices;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Connections;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Core.Display;

/// <summary>
/// A read-side view of a gateway for the screens that show messages (Topics, Consume, Find Data).
/// Everything is forwarded unchanged except <see cref="ConsumeAsync"/>, whose messages pass through the
/// decode / format / mask pipeline first. Scripts, rethrow rules and tasks use the plain gateway, so a
/// mask or a display format can never change what the app produces. Dispose does not dispose the inner gateway.
/// </summary>
public sealed class DisplayKafkaGateway : IKafkaGateway
{
    private readonly IKafkaGateway _inner;
    private readonly Func<KafkaMessage, CancellationToken, ValueTask<KafkaMessage>> _transform;

    public DisplayKafkaGateway(IKafkaGateway inner, Func<KafkaMessage, CancellationToken, ValueTask<KafkaMessage>> transform)
    {
        _inner = inner;
        _transform = transform;
    }

    public ConnectionProfile Profile => _inner.Profile;
    public Task ConnectAsync(CancellationToken ct = default) => _inner.ConnectAsync(ct);
    public Task<IReadOnlyList<string>> ListTopicsAsync(CancellationToken ct = default) => _inner.ListTopicsAsync(ct);
    public Task<TopicMetadata> DescribeTopicAsync(string topic, CancellationToken ct = default) => _inner.DescribeTopicAsync(topic, ct);
    public Task<ClusterInfo> DescribeClusterAsync(CancellationToken ct = default) => _inner.DescribeClusterAsync(ct);
    public Task<IReadOnlyList<BrokerConfigEntry>> GetBrokerConfigAsync(int brokerId, CancellationToken ct = default) => _inner.GetBrokerConfigAsync(brokerId, ct);
    public Task CreateTopicAsync(string topic, int partitions, short replicationFactor, CancellationToken ct = default) => _inner.CreateTopicAsync(topic, partitions, replicationFactor, ct);
    public Task<ProduceReceipt> ProduceAsync(ProduceRequest request, CancellationToken ct = default) => _inner.ProduceAsync(request, ct);
    public Task<bool> IsTopicCompactedAsync(string topic, CancellationToken ct = default) => _inner.IsTopicCompactedAsync(topic, ct);
    public Task AcknowledgeAsync(KafkaMessage message, CancellationToken ct = default) => _inner.AcknowledgeAsync(message, ct);
    public Task<IReadOnlyList<ConsumerGroupSummary>> ListConsumerGroupsAsync(CancellationToken ct = default) => _inner.ListConsumerGroupsAsync(ct);
    public Task<ConsumerGroupDetail> DescribeConsumerGroupAsync(string groupId, CancellationToken ct = default) => _inner.DescribeConsumerGroupAsync(groupId, ct);
    public Task<IReadOnlyList<OffsetChange>> PlanOffsetResetAsync(OffsetResetRequest request, CancellationToken ct = default) => _inner.PlanOffsetResetAsync(request, ct);
    public Task ApplyOffsetResetAsync(string groupId, IReadOnlyList<OffsetChange> changes, CancellationToken ct = default) => _inner.ApplyOffsetResetAsync(groupId, changes, ct);
    public Task<IReadOnlyList<TopicConfigEntry>> GetTopicConfigAsync(string topic, CancellationToken ct = default) => _inner.GetTopicConfigAsync(topic, ct);
    public Task AlterTopicConfigAsync(string topic, IReadOnlyDictionary<string, string?> changes, CancellationToken ct = default) => _inner.AlterTopicConfigAsync(topic, changes, ct);
    public Task DeleteTopicAsync(string topic, CancellationToken ct = default) => _inner.DeleteTopicAsync(topic, ct);
    public Task CreatePartitionsAsync(string topic, int totalPartitions, CancellationToken ct = default) => _inner.CreatePartitionsAsync(topic, totalPartitions, ct);
    public Task DeleteConsumerGroupAsync(string groupId, CancellationToken ct = default) => _inner.DeleteConsumerGroupAsync(groupId, ct);
    public Task<IReadOnlyList<AclEntry>> ListAclsAsync(CancellationToken ct = default) => _inner.ListAclsAsync(ct);

    public async IAsyncEnumerable<KafkaMessage> ConsumeAsync(ConsumeOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var message in _inner.ConsumeAsync(options, cancellationToken).ConfigureAwait(false))
        {
            yield return await _transform(message, cancellationToken).ConfigureAwait(false);
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

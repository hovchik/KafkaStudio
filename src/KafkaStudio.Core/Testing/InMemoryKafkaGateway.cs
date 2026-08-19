using System.Runtime.CompilerServices;
using System.Text;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Connections;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Core.Testing;

/// <summary>
/// <see cref="IKafkaGateway"/> backed by an <see cref="InMemoryKafkaBroker"/> instead of a real
/// cluster. This is the workhorse for unit tests (deterministic, no network, no timing flakiness
/// beyond what a test explicitly asks for) and doubles as KafkaStudio's "offline / demo mode" so the
/// UI, DSL runner and check engine can all be exercised without Kafka installed.
/// </summary>
public sealed class InMemoryKafkaGateway : IKafkaGateway
{
    private readonly InMemoryKafkaBroker _broker;
    private readonly IClock _clock;

    public ConnectionProfile Profile { get; }

    public InMemoryKafkaGateway(ConnectionProfile profile, InMemoryKafkaBroker broker, IClock? clock = null)
    {
        Profile = profile;
        _broker = broker;
        _clock = clock ?? SystemClock.Instance;
    }

    public Task ConnectAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task<IReadOnlyList<string>> ListTopicsAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(_broker.ListTopics());

    public Task<TopicMetadata> DescribeTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        var (earliest, latest) = _broker.GetOffsets(topic);
        var metadata = new TopicMetadata
        {
            Name = topic,
            ReplicationFactor = 1,
            Partitions = new[]
            {
                new PartitionInfo { Id = 0, LeaderBrokerId = 0, EarliestOffset = earliest, LatestOffset = latest }
            }
        };
        return Task.FromResult(metadata);
    }

    public Task CreateTopicAsync(string topic, int partitions, short replicationFactor,
        CancellationToken cancellationToken = default)
    {
        _broker.EnsureTopic(topic);
        return Task.CompletedTask;
    }

    public Task<ProduceReceipt> ProduceAsync(ProduceRequest request, CancellationToken cancellationToken = default)
    {
        var message = _broker.Append(
            request.Topic,
            request.Key,
            request.Value,
            rawValue: request.Value is null ? null : Encoding.UTF8.GetBytes(request.Value),
            request.Headers,
            _clock.UtcNow);

        return Task.FromResult(new ProduceReceipt
        {
            Topic = message.Topic,
            Partition = message.Partition,
            Offset = message.Offset,
            Timestamp = message.Timestamp
        });
    }

    public async IAsyncEnumerable<KafkaMessage> ConsumeAsync(ConsumeOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var (history, live, unsubscribe) = _broker.Subscribe(options.Topic);
        try
        {
            long startOffset = options.StartPosition switch
            {
                ConsumeStartPosition.Earliest => 0,
                ConsumeStartPosition.Latest => history.Count,
                ConsumeStartPosition.Committed => _broker.GetCommittedOffset(options.Topic, options.ConsumerGroup) + 1,
                ConsumeStartPosition.FromTimestamp => FindFirstIndexAtOrAfter(history, options.FromTimestamp),
                _ => history.Count
            };

            var emitted = 0L;
            long nextExpectedOffset = startOffset;

            for (var i = (int)Math.Max(0, startOffset); i < history.Count; i++)
            {
                if (options.MaxMessages is { } cap && emitted >= cap) yield break;
                cancellationToken.ThrowIfCancellationRequested();

                var msg = history[i] with { ConsumerGroup = options.ConsumerGroup };
                yield return msg;
                emitted++;
                nextExpectedOffset = msg.Offset + 1;

                if (options.AutoAcknowledge)
                {
                    _broker.Commit(options.Topic, options.ConsumerGroup, msg.Offset);
                }
            }

            if (options.MaxMessages is { } cap2 && emitted >= cap2) yield break;

            // Bounded "scan and display" style reads (StopAtPartitionEnd) should stop once history has
            // been drained, mirroring how the real gateway stops at partition EOF, rather than blocking
            // on the live stream forever like an unbounded "watch" subscription does.
            if (options.StopAtPartitionEnd) yield break;

            await foreach (var msg in live.ReadAllAsync(cancellationToken))
            {
                if (msg.Offset < nextExpectedOffset) continue; // already emitted from history
                nextExpectedOffset = msg.Offset + 1;

                var tagged = msg with { ConsumerGroup = options.ConsumerGroup };
                yield return tagged;
                emitted++;

                if (options.AutoAcknowledge)
                {
                    _broker.Commit(options.Topic, options.ConsumerGroup, tagged.Offset);
                }

                if (options.MaxMessages is { } cap3 && emitted >= cap3) yield break;
            }
        }
        finally
        {
            unsubscribe();
        }
    }

    public Task AcknowledgeAsync(KafkaMessage message, CancellationToken cancellationToken = default)
    {
        if (message.ConsumerGroup is null)
        {
            throw new InvalidOperationException(
                $"Cannot acknowledge message at {message.Topic}#{message.Partition}@{message.Offset}: " +
                "it was not associated with a consumer group (was it produced rather than consumed?).");
        }

        _broker.Commit(message.Topic, message.ConsumerGroup, message.Offset);
        return Task.CompletedTask;
    }

    private static long FindFirstIndexAtOrAfter(IReadOnlyList<KafkaMessage> history, DateTimeOffset? timestamp)
    {
        if (timestamp is null) return 0;
        for (var i = 0; i < history.Count; i++)
        {
            if (history[i].Timestamp >= timestamp.Value) return i;
        }
        return history.Count;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

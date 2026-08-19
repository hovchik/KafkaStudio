using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using Confluent.Kafka;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Connections;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Kafka;

/// <summary>
/// The real <see cref="IKafkaGateway"/>: a thin async adapter over Confluent.Kafka / librdkafka.
/// Confluent.Kafka's consumer API is synchronous and blocking by design (that's how librdkafka's
/// polling model works), so every consume subscription here runs its own dedicated background thread
/// that polls in a loop and hands messages to callers through a <see cref="Channel{T}"/> - the same
/// bridge-to-async pattern <see cref="Core.Testing.InMemoryKafkaGateway"/> uses, so callers (the
/// KafScript interpreter, the rethrow engine, the UI) don't need to know or care which one they're
/// talking to.
/// </summary>
public sealed class ConfluentKafkaGateway : IKafkaGateway
{
    private static readonly TimeSpan MetadataTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan AssignmentWaitTimeout = TimeSpan.FromSeconds(5);

    public ConnectionProfile Profile { get; }

    private IProducer<string?, string?>? _producer;
    private IAdminClient? _admin;

    // Tracks live consumers by the (unique, per-subscription) consumer group id so AcknowledgeAsync
    // can route an explicit commit back to the exact consumer instance that read the message -
    // Confluent.Kafka's IConsumer is not safe to use concurrently, hence the per-consumer lock.
    private readonly ConcurrentDictionary<string, (IConsumer<string?, string?> Consumer, object Lock)> _activeConsumers = new();

    public ConfluentKafkaGateway(ConnectionProfile profile)
    {
        Profile = profile;
    }

    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        _producer = new ProducerBuilder<string?, string?>(ConfigMapper.ToProducerConfig(Profile))
            .SetKeySerializer(Serializers.Utf8!)
            .SetValueSerializer(Serializers.Utf8!)
            .Build();

        _admin = new AdminClientBuilder(ConfigMapper.ToAdminConfig(Profile)).Build();

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<string>> ListTopicsAsync(CancellationToken cancellationToken = default)
    {
        var metadata = RequireAdmin().GetMetadata(MetadataTimeout);
        IReadOnlyList<string> topics = metadata.Topics
            .Select(t => t.Topic)
            .Where(name => !name.StartsWith("__", StringComparison.Ordinal)) // hide internal topics (__consumer_offsets etc.)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToList();
        return Task.FromResult(topics);
    }

    public Task<Core.Messaging.TopicMetadata> DescribeTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        var metadata = RequireAdmin().GetMetadata(topic, MetadataTimeout);
        var topicMeta = metadata.Topics.FirstOrDefault(t => t.Topic == topic)
            ?? throw new KeyNotFoundException($"topic '{topic}' not found");

        using var probe = new ConsumerBuilder<string?, string?>(
                ConfigMapper.ToConsumerConfig(Profile, $"kafka-studio-describe-{Guid.NewGuid():N}", AutoOffsetReset.Earliest))
            .SetKeyDeserializer(Deserializers.Utf8!)
            .SetValueDeserializer(Deserializers.Utf8!)
            .Build();

        var partitions = topicMeta.Partitions.Select(p =>
        {
            var watermarks = probe.QueryWatermarkOffsets(new TopicPartition(topic, new Partition(p.PartitionId)), MetadataTimeout);
            return new PartitionInfo
            {
                Id = p.PartitionId,
                LeaderBrokerId = p.Leader,
                EarliestOffset = watermarks.Low.Value,
                LatestOffset = watermarks.High.Value
            };
        }).ToList();

        return Task.FromResult(new Core.Messaging.TopicMetadata
        {
            Name = topic,
            ReplicationFactor = topicMeta.Partitions.Count == 0 ? 0 : topicMeta.Partitions[0].Replicas.Length,
            Partitions = partitions
        });
    }

    public async Task CreateTopicAsync(string topic, int partitions, short replicationFactor,
        CancellationToken cancellationToken = default)
    {
        await RequireAdmin().CreateTopicsAsync(new[]
        {
            new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = replicationFactor }
        }).ConfigureAwait(false);
    }

    public async Task<ProduceReceipt> ProduceAsync(ProduceRequest request, CancellationToken cancellationToken = default)
    {
        var producer = RequireProducer();

        var message = new Message<string?, string?> { Key = request.Key, Value = request.Value };
        if (request.Headers is { Count: > 0 })
        {
            var headers = new Headers();
            foreach (var (key, value) in request.Headers)
            {
                headers.Add(key, Encoding.UTF8.GetBytes(value));
            }
            message.Headers = headers;
        }

        var result = await producer.ProduceAsync(request.Topic, message, cancellationToken).ConfigureAwait(false);

        return new ProduceReceipt
        {
            Topic = result.Topic,
            Partition = result.Partition.Value,
            Offset = result.Offset.Value,
            Timestamp = DateTimeOffset.UtcNow
        };
    }

    public async IAsyncEnumerable<KafkaMessage> ConsumeAsync(ConsumeOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var autoOffsetReset = options.StartPosition == ConsumeStartPosition.Earliest
            ? AutoOffsetReset.Earliest
            : AutoOffsetReset.Latest; // Latest also covers Committed/FromTimestamp as the *fallback* when no offset exists yet

        var consumer = new ConsumerBuilder<string?, string?>(
                ConfigMapper.ToConsumerConfig(Profile, options.ConsumerGroup, autoOffsetReset))
            .SetKeyDeserializer(Deserializers.Utf8!)
            .SetValueDeserializer(Deserializers.Utf8!)
            .Build();

        var consumerLock = new object();
        _activeConsumers[options.ConsumerGroup] = (consumer, consumerLock);

        var channel = Channel.CreateUnbounded<KafkaMessage>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });

        var pump = Task.Run(() => PumpLoop(consumer, consumerLock, options, channel, cancellationToken), cancellationToken);

        try
        {
            var emitted = 0;
            await foreach (var message in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return message;
                emitted++;
                if (options.MaxMessages is { } cap && emitted >= cap) break;
            }
        }
        finally
        {
            _activeConsumers.TryRemove(options.ConsumerGroup, out _);
            try { await pump.ConfigureAwait(false); } catch { /* pump already logs/handles its own errors */ }
            lock (consumerLock)
            {
                consumer.Close();
                consumer.Dispose();
            }
        }
    }

    private void PumpLoop(
        IConsumer<string?, string?> consumer,
        object consumerLock,
        ConsumeOptions options,
        Channel<KafkaMessage> channel,
        CancellationToken cancellationToken)
    {
        try
        {
            consumer.Subscribe(options.Topic);

            if (options.StartPosition == ConsumeStartPosition.FromTimestamp && options.FromTimestamp is { } from)
            {
                SeekToTimestamp(consumer, consumerLock, options.Topic, from, cancellationToken);
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<string?, string?>? result;
                lock (consumerLock)
                {
                    result = consumer.Consume(200); // short poll so we keep checking for cancellation
                }

                if (result?.Message is null) continue;

                if (options.AutoAcknowledge)
                {
                    lock (consumerLock)
                    {
                        consumer.Commit(new[] { new TopicPartitionOffset(result.Topic, result.Partition, new Offset(result.Offset.Value + 1)) });
                    }
                }

                var message = new KafkaMessage
                {
                    Topic = result.Topic,
                    Partition = result.Partition.Value,
                    Offset = result.Offset.Value,
                    Key = result.Message.Key,
                    Value = result.Message.Value,
                    Headers = result.Message.Headers?.ToDictionary(h => h.Key, h => h.Value is null ? string.Empty : Encoding.UTF8.GetString(h.Value))
                              ?? new Dictionary<string, string>(),
                    Timestamp = new DateTimeOffset(result.Message.Timestamp.UtcDateTime),
                    ConsumerGroup = options.ConsumerGroup
                };

                if (!channel.Writer.TryWrite(message)) break;
            }
        }
        catch (OperationCanceledException)
        {
            // expected on shutdown
        }
        catch (Exception ex)
        {
            channel.Writer.TryComplete(ex);
            return;
        }
        finally
        {
            channel.Writer.TryComplete();
        }
    }

    private static void SeekToTimestamp(IConsumer<string?, string?> consumer, object consumerLock, string topic,
        DateTimeOffset timestamp, CancellationToken cancellationToken)
    {
        // Partition assignment happens asynchronously after Subscribe(); poll briefly until it shows up
        // so we know which partitions to compute timestamp offsets for.
        var deadline = DateTime.UtcNow + AssignmentWaitTimeout;
        List<TopicPartition> assignment;
        while (true)
        {
            lock (consumerLock)
            {
                assignment = consumer.Assignment.Where(tp => tp.Topic == topic).ToList();
            }
            if (assignment.Count > 0 || DateTime.UtcNow > deadline || cancellationToken.IsCancellationRequested) break;
            lock (consumerLock) { consumer.Consume(100); } // pumping Consume is what drives assignment callbacks
        }

        if (assignment.Count == 0) return; // fall back to the AutoOffsetReset default rather than blocking forever

        var ts = new Timestamp(timestamp.UtcDateTime, TimestampType.CreateTime);
        var request = assignment.Select(tp => new TopicPartitionTimestamp(tp, ts));

        lock (consumerLock)
        {
            var offsets = consumer.OffsetsForTimes(request, MetadataTimeout);
            consumer.Assign(offsets);
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

        if (!_activeConsumers.TryGetValue(message.ConsumerGroup, out var entry))
        {
            throw new InvalidOperationException(
                $"Cannot acknowledge message: the subscription for consumer group '{message.ConsumerGroup}' is no longer active.");
        }

        lock (entry.Lock)
        {
            entry.Consumer.Commit(new[]
            {
                new TopicPartitionOffset(message.Topic, new Partition(message.Partition), new Offset(message.Offset + 1))
            });
        }

        return Task.CompletedTask;
    }

    private IProducer<string?, string?> RequireProducer() =>
        _producer ?? throw new InvalidOperationException("not connected - call ConnectAsync first");

    private IAdminClient RequireAdmin() =>
        _admin ?? throw new InvalidOperationException("not connected - call ConnectAsync first");

    public async ValueTask DisposeAsync()
    {
        foreach (var (consumer, consumerLock) in _activeConsumers.Values)
        {
            lock (consumerLock)
            {
                try { consumer.Close(); } catch { /* best effort */ }
                consumer.Dispose();
            }
        }
        _activeConsumers.Clear();

        _producer?.Flush(TimeSpan.FromSeconds(5));
        _producer?.Dispose();
        _admin?.Dispose();

        await Task.CompletedTask;
    }
}

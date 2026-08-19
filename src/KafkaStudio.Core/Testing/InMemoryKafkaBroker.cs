using System.Collections.Concurrent;
using System.Threading.Channels;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Core.Testing;

/// <summary>
/// Shared, thread-safe, in-process simulation of a Kafka cluster: an append-only log per topic,
/// broadcast to live subscribers, plus per-consumer-group committed offsets. Deliberately models a
/// single partition per topic (partition 0) - that's enough to exercise every DSL step and check the
/// same way a real single-partition topic would, without reimplementing Kafka's partition assignment
/// protocol. Multiple <see cref="InMemoryKafkaGateway"/> instances can share one broker, which is how
/// tests simulate independent producer/consumer connections talking to "the same cluster".
/// </summary>
public sealed class InMemoryKafkaBroker
{
    private sealed class TopicLog
    {
        public readonly List<KafkaMessage> Messages = new();
        public readonly object Gate = new();
        public readonly List<Channel<KafkaMessage>> Subscribers = new();
        public readonly ConcurrentDictionary<string, long> CommittedOffsets = new();
    }

    private readonly ConcurrentDictionary<string, TopicLog> _topics = new();

    private TopicLog GetOrCreate(string topic) =>
        _topics.GetOrAdd(topic, static _ => new TopicLog());

    public IReadOnlyList<string> ListTopics() => _topics.Keys.OrderBy(t => t, StringComparer.Ordinal).ToArray();

    public void EnsureTopic(string topic) => GetOrCreate(topic);

    public (long earliest, long latest) GetOffsets(string topic)
    {
        var log = GetOrCreate(topic);
        lock (log.Gate)
        {
            return (0L, log.Messages.Count);
        }
    }

    public KafkaMessage Append(string topic, string? key, string? value, byte[]? rawValue,
        IReadOnlyDictionary<string, string>? headers, DateTimeOffset timestamp)
    {
        var log = GetOrCreate(topic);
        KafkaMessage message;
        List<Channel<KafkaMessage>> subscribersSnapshot;
        lock (log.Gate)
        {
            var offset = log.Messages.Count;
            message = new KafkaMessage
            {
                Topic = topic,
                Partition = 0,
                Offset = offset,
                Key = key,
                Value = value,
                RawValue = rawValue,
                Headers = headers ?? new Dictionary<string, string>(),
                Timestamp = timestamp
            };
            log.Messages.Add(message);
            subscribersSnapshot = log.Subscribers.ToList();
        }

        foreach (var sub in subscribersSnapshot)
        {
            // Bounded-but-generous channels; TryWrite is fine because we never complete/close the
            // channel until the subscriber unregisters, and consumers are expected to keep draining.
            sub.Writer.TryWrite(message);
        }

        return message;
    }

    /// <summary>
    /// Registers a live subscriber and returns a snapshot of historical messages captured atomically
    /// with the subscription, so callers can replay history then switch to the channel with no gap and
    /// no duplication.
    /// </summary>
    internal (IReadOnlyList<KafkaMessage> history, ChannelReader<KafkaMessage> live, Action unsubscribe) Subscribe(string topic)
    {
        var log = GetOrCreate(topic);
        var channel = Channel.CreateUnbounded<KafkaMessage>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

        List<KafkaMessage> history;
        lock (log.Gate)
        {
            log.Subscribers.Add(channel);
            history = log.Messages.ToList();
        }

        void Unsubscribe()
        {
            lock (log.Gate)
            {
                log.Subscribers.Remove(channel);
            }
            channel.Writer.TryComplete();
        }

        return (history, channel.Reader, Unsubscribe);
    }

    public long GetCommittedOffset(string topic, string consumerGroup)
    {
        var log = GetOrCreate(topic);
        return log.CommittedOffsets.TryGetValue(consumerGroup, out var offset) ? offset : -1;
    }

    public void Commit(string topic, string consumerGroup, long offset)
    {
        var log = GetOrCreate(topic);
        log.CommittedOffsets.AddOrUpdate(consumerGroup, offset,
            (_, existing) => Math.Max(existing, offset));
    }
}

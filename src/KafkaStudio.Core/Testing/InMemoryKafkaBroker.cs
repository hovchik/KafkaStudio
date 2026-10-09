using System.Collections.Concurrent;
using System.Threading.Channels;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Core.Testing;

/// <summary>
/// Shared, thread-safe, in-process simulation of a Kafka cluster: per topic a set of append-only
/// partition logs (with the same key-hash partitioner the real client uses), broadcast to live
/// subscribers, plus per-consumer-group committed offsets per partition and live group membership.
/// Multiple <see cref="InMemoryKafkaGateway"/> instances can share one broker, which is how tests
/// simulate independent producer/consumer connections talking to "the same cluster".
/// </summary>
public sealed class InMemoryKafkaBroker
{
    private sealed class PartitionLog
    {
        public readonly List<KafkaMessage> Messages = new();
        public long NextOffset;
    }

    private sealed class TopicLog
    {
        public TopicLog(int partitions, short replicationFactor)
        {
            Partitions = Enumerable.Range(0, partitions).Select(_ => new PartitionLog()).ToArray();
            ReplicationFactor = replicationFactor;
        }

        public readonly object Gate = new();
        public readonly PartitionLog[] Partitions;
        public readonly short ReplicationFactor;
        /// <summary>Every message of every partition in the order it was appended.</summary>
        public readonly List<KafkaMessage> All = new();
        public readonly List<Channel<KafkaMessage>> Subscribers = new();
        public readonly ConcurrentDictionary<(string group, int partition), long> CommittedOffsets = new();
        public int RoundRobin;
    }

    private sealed record LiveMember(string MemberId, string Topic);

    private readonly ConcurrentDictionary<string, TopicLog> _topics = new();

    /// <param name="brokerCount">How many brokers the simulated cluster reports (partition leaders are spread over them).</param>
    public InMemoryKafkaBroker(int brokerCount = 1)
    {
        BrokerCount = Math.Max(1, brokerCount);
    }

    public int BrokerCount { get; }

    /// <summary>Auto-created topics (first produce / consume) get <c>num.partitions</c> = 1, like the broker config reports.</summary>
    private TopicLog GetOrCreate(string topic) =>
        _topics.GetOrAdd(topic, static _ => new TopicLog(1, 1));

    private TopicLog? Find(string topic) => _topics.TryGetValue(topic, out var log) ? log : null;

    public IReadOnlyList<string> ListTopics() => _topics.Keys.OrderBy(t => t, StringComparer.Ordinal).ToArray();

    public void EnsureTopic(string topic) => GetOrCreate(topic);

    /// <summary>Creates a topic with an explicit layout. Throws if it exists or the layout is impossible.</summary>
    public void CreateTopic(string topic, int partitions, short replicationFactor, bool compacted = false)
    {
        if (partitions < 1) throw new ArgumentException("a topic needs at least one partition", nameof(partitions));
        if (replicationFactor < 1) throw new ArgumentException("the replication factor must be at least 1", nameof(replicationFactor));
        if (replicationFactor > BrokerCount)
        {
            throw new InvalidOperationException(
                $"replication factor {replicationFactor} is larger than the number of available brokers ({BrokerCount})");
        }
        if (!_topics.TryAdd(topic, new TopicLog(partitions, replicationFactor)))
        {
            throw new InvalidOperationException($"topic '{topic}' already exists");
        }
        if (compacted) SetCompacted(topic, true);
    }

    public bool TopicExists(string topic) => _topics.ContainsKey(topic);

    public int GetPartitionCount(string topic) => GetOrCreate(topic).Partitions.Length;

    public short GetReplicationFactor(string topic) => GetOrCreate(topic).ReplicationFactor;

    /// <summary>The broker that leads a partition: partitions are spread round-robin over the brokers.</summary>
    public int GetLeader(int partition) => partition % BrokerCount;

    /// <summary>The earliest and next-to-be-written offset of partition 0.</summary>
    public (long earliest, long latest) GetOffsets(string topic) => GetOffsets(topic, 0);

    public (long earliest, long latest) GetOffsets(string topic, int partition)
    {
        var log = GetOrCreate(topic);
        lock (log.Gate)
        {
            if (partition < 0 || partition >= log.Partitions.Length) return (0L, 0L);
            var p = log.Partitions[partition];
            return (0L, p.NextOffset); // compaction never moves the log start, like Kafka
        }
    }

    public KafkaMessage Append(string topic, string? key, string? value, byte[]? rawValue,
        IReadOnlyDictionary<string, string>? headers, DateTimeOffset timestamp, int? partition = null)
    {
        // Copy so a caller mutating its dictionary afterwards can't rewrite "stored" history.
        headers = headers is null ? null : new Dictionary<string, string>(headers);
        var log = GetOrCreate(topic);
        if (partition is { } explicitPartition && (explicitPartition < 0 || explicitPartition >= log.Partitions.Length))
        {
            throw new ArgumentOutOfRangeException(nameof(partition),
                $"topic '{topic}' has {log.Partitions.Length} partition(s); partition {explicitPartition} does not exist");
        }

        KafkaMessage message;
        lock (log.Gate)
        {
            var target = partition ?? ChoosePartition(log, key);
            var part = log.Partitions[target];
            message = new KafkaMessage
            {
                Topic = topic,
                Partition = target,
                Offset = part.NextOffset++,
                Key = key,
                Value = value,
                RawValue = rawValue,
                Headers = headers ?? new Dictionary<string, string>(),
                Timestamp = timestamp
            };
            part.Messages.Add(message);
            log.All.Add(message);

            // Delivered while holding the lock so subscribers see messages in append order even when
            // producers race (an out-of-order delivery would make the consumer skip the earlier offset).
            // The channels are unbounded, so TryWrite never blocks.
            foreach (var sub in log.Subscribers) sub.Writer.TryWrite(message);
        }

        return message;
    }

    /// <summary>Keyed messages hash to a stable partition (CRC32, the librdkafka default); keyless ones rotate.</summary>
    private static int ChoosePartition(TopicLog log, string? key)
    {
        var count = log.Partitions.Length;
        if (count == 1) return 0;
        if (key is null) return (int)((uint)Interlocked.Increment(ref log.RoundRobin) % (uint)count);
        return (int)(Crc32(System.Text.Encoding.UTF8.GetBytes(key)) % (uint)count);
    }

    private static readonly uint[] Crc32Table = Enumerable.Range(0, 256).Select(i =>
    {
        var c = (uint)i;
        for (var k = 0; k < 8; k++) c = (c & 1) != 0 ? 0xEDB88320u ^ (c >> 1) : c >> 1;
        return c;
    }).ToArray();

    private static uint Crc32(byte[] data)
    {
        var crc = 0xFFFFFFFFu;
        foreach (var b in data) crc = Crc32Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return ~crc;
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
            history = log.All.ToList();
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

    private readonly ConcurrentDictionary<string, bool> _compacted = new();

    /// <summary>Marks a topic as having <c>cleanup.policy=compact</c>.</summary>
    public void SetCompacted(string topic, bool compacted) => _compacted[topic] = compacted;

    public bool IsCompacted(string topic) => _compacted.TryGetValue(topic, out var c) && c;

    /// <summary>
    /// Runs one log-compaction pass over a topic: for every key only the newest message survives and a
    /// key whose newest message is a tombstone disappears entirely. Offsets are never reassigned (gaps
    /// remain, as in Kafka). Real brokers compact in the background; here it only happens when asked.
    /// Returns how many messages were removed.
    /// </summary>
    public int Compact(string topic)
    {
        var log = Find(topic);
        if (log is null) return 0;
        var removed = 0;
        lock (log.Gate)
        {
            foreach (var part in log.Partitions)
            {
                var newest = new Dictionary<string, long>(StringComparer.Ordinal);
                foreach (var m in part.Messages)
                {
                    if (m.Key is not null) newest[m.Key] = m.Offset;
                }
                removed += part.Messages.RemoveAll(m =>
                    m.Key is not null && (newest[m.Key] != m.Offset || m.Value is null && m.RawValue is null));
            }
            if (removed > 0)
            {
                log.All.Clear();
                log.All.AddRange(log.Partitions.SelectMany(p => p.Messages).OrderBy(m => m.Timestamp).ThenBy(m => m.Offset));
            }
        }
        return removed;
    }

    public long GetCommittedOffset(string topic, string consumerGroup, int partition = 0)
    {
        var log = GetOrCreate(topic);
        return log.CommittedOffsets.TryGetValue((consumerGroup, partition), out var offset) ? offset : -1;
    }

    /// <summary>Records that <paramref name="offset"/> was consumed (never moves a commit backwards).</summary>
    public void Commit(string topic, string consumerGroup, long offset, int partition = 0)
    {
        var log = GetOrCreate(topic);
        log.CommittedOffsets.AddOrUpdate((consumerGroup, partition), offset,
            (_, existing) => Math.Max(existing, offset));
    }

    // ---- consumer groups ----

    private readonly ConcurrentDictionary<string, int> _simulatedMembers = new();
    private readonly ConcurrentDictionary<string, List<LiveMember>> _liveMembers = new();
    private long _memberSeq;

    /// <summary>Every group that has committed an offset or has members (simulated or consuming right now).</summary>
    public IReadOnlyList<string> ListGroups() =>
        _topics.Values.SelectMany(t => t.CommittedOffsets.Keys.Select(k => k.group))
            .Concat(_simulatedMembers.Keys)
            .Concat(_liveMembers.Where(kv => GetLiveCount(kv.Key) > 0).Select(kv => kv.Key))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(g => g, StringComparer.Ordinal)
            .ToArray();

    /// <summary>The (topic, partition, next offset to read) triples a group has committed.</summary>
    public IReadOnlyList<(string topic, int partition, long nextOffset)> GetGroupOffsets(string group) =>
        _topics.SelectMany(t => t.Value.CommittedOffsets
                .Where(c => c.Key.group == group)
                .Select(c => (t.Key, c.Key.partition, c.Value + 1)))
            .OrderBy(t => t.Key, StringComparer.Ordinal)
            .ThenBy(t => t.partition)
            .ToArray();

    /// <summary>Overwrites (including moving backwards, unlike <see cref="Commit"/>) the group's next offset to read.</summary>
    public void SetNextOffset(string topic, string consumerGroup, long nextOffset, int partition = 0)
    {
        var log = GetOrCreate(topic);
        log.CommittedOffsets[(consumerGroup, partition)] = Math.Max(-1, nextOffset - 1);
    }

    /// <summary>Pretends a group has live members, so tests and the demo can exercise the "group is active" paths.</summary>
    public void SimulateActiveMembers(string group, int count)
    {
        if (count <= 0) _simulatedMembers.TryRemove(group, out _);
        else _simulatedMembers[group] = count;
    }

    /// <summary>Joins a group for as long as the returned handle lives (a consumer that is reading right now).</summary>
    internal IDisposable JoinGroup(string group, string topic)
    {
        var member = new LiveMember($"{group}-live-{Interlocked.Increment(ref _memberSeq)}", topic);
        var members = _liveMembers.GetOrAdd(group, static _ => new List<LiveMember>());
        lock (members) members.Add(member);
        return new Leave(() => { lock (members) members.Remove(member); });
    }

    private sealed class Leave(Action action) : IDisposable
    {
        private int _done;
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _done, 1) == 0) action();
        }
    }

    private int GetLiveCount(string group)
    {
        if (!_liveMembers.TryGetValue(group, out var members)) return 0;
        lock (members) return members.Count;
    }

    public int GetActiveMemberCount(string group) =>
        (_simulatedMembers.TryGetValue(group, out var n) ? n : 0) + GetLiveCount(group);

    /// <summary>
    /// The group's members and their partitions as "topic[partition]". Simulated members own every
    /// committed partition; live consumers split the partitions of the topic they read (range assignor).
    /// </summary>
    public IReadOnlyList<(string memberId, bool simulated, IReadOnlyList<string> assignment)> GetMembers(string group)
    {
        var result = new List<(string, bool, IReadOnlyList<string>)>();
        var committed = GetGroupOffsets(group).Select(o => $"{o.topic}[{o.partition}]").ToList();
        for (var i = 1; i <= (_simulatedMembers.TryGetValue(group, out var n) ? n : 0); i++)
        {
            result.Add(($"{group}-member-{i}", true, committed));
        }

        if (_liveMembers.TryGetValue(group, out var members))
        {
            LiveMember[] snapshot;
            lock (members) snapshot = members.ToArray();
            foreach (var byTopic in snapshot.GroupBy(m => m.Topic))
            {
                var partitionCount = Find(byTopic.Key)?.Partitions.Length ?? 0;
                var list = byTopic.ToList();
                for (var i = 0; i < list.Count; i++)
                {
                    var from = partitionCount * i / list.Count;
                    var to = partitionCount * (i + 1) / list.Count;
                    result.Add((list[i].MemberId, false,
                        Enumerable.Range(from, to - from).Select(p => $"{byTopic.Key}[{p}]").ToList()));
                }
            }
        }
        return result;
    }

    /// <summary>(timestamp, offset) of every message still in the partition, oldest first.</summary>
    public IReadOnlyList<(DateTimeOffset timestamp, long offset)> GetTimestamps(string topic, int partition = 0)
    {
        var log = GetOrCreate(topic);
        lock (log.Gate)
        {
            if (partition < 0 || partition >= log.Partitions.Length) return Array.Empty<(DateTimeOffset, long)>();
            return log.Partitions[partition].Messages.Select(m => (m.Timestamp, m.Offset)).ToArray();
        }
    }
}

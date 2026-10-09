using System.Runtime.CompilerServices;
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
        if (!_broker.TopicExists(topic))
        {
            return Task.FromException<TopicMetadata>(new KeyNotFoundException($"topic '{topic}' not found"));
        }

        var metadata = new TopicMetadata
        {
            Name = topic,
            ReplicationFactor = _broker.GetReplicationFactor(topic),
            Partitions = Enumerable.Range(0, _broker.GetPartitionCount(topic)).Select(id =>
            {
                var (earliest, latest) = _broker.GetOffsets(topic, id);
                return new PartitionInfo { Id = id, LeaderBrokerId = _broker.GetLeader(id), EarliestOffset = earliest, LatestOffset = latest };
            }).ToArray()
        };
        return Task.FromResult(metadata);
    }

    public Task<ClusterInfo> DescribeClusterAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(new ClusterInfo
        {
            ClusterId = "demo-cluster",
            ControllerId = 0,
            Brokers = Enumerable.Range(0, _broker.BrokerCount)
                .Select(id => new BrokerInfo { Id = id, Host = "localhost", Port = 9092 + id, IsController = id == 0 })
                .ToArray()
        });

    public Task<IReadOnlyList<BrokerConfigEntry>> GetBrokerConfigAsync(int brokerId,
        CancellationToken cancellationToken = default)
    {
        if (brokerId < 0 || brokerId >= _broker.BrokerCount) return Task.FromException<IReadOnlyList<BrokerConfigEntry>>(
            new KeyNotFoundException($"broker {brokerId} not found"));
        IReadOnlyList<BrokerConfigEntry> entries = new[]
        {
            new BrokerConfigEntry { Name = "auto.create.topics.enable", Value = "true", IsDefault = true, Source = "DefaultConfig" },
            new BrokerConfigEntry { Name = "log.retention.hours", Value = "168", IsDefault = true, Source = "DefaultConfig" },
            new BrokerConfigEntry { Name = "num.partitions", Value = "1", IsDefault = true, Source = "DefaultConfig" }
        };
        return Task.FromResult(entries);
    }

    public Task CreateTopicAsync(string topic, int partitions, short replicationFactor,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _broker.CreateTopic(topic, partitions, replicationFactor);
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
        {
            return Task.FromException(ex);
        }
        return Task.CompletedTask;
    }

    public Task<ProduceReceipt> ProduceAsync(ProduceRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var raw = request.GetValueBytes();
        KafkaMessage message;
        try
        {
            message = _broker.Append(
                request.Topic,
                request.Key,
                request.RawValue is not null ? KafkaMessage.DecodeText(raw) : request.Value,
                raw,
                request.Headers,
                _clock.UtcNow,
                request.Partition);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            return Task.FromException<ProduceReceipt>(ex);
        }

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
        // While reading, the consumer is a live member of its group (visible on the Consumer Groups
        // screen, and it blocks offset resets just like a real consumer would).
        using var membership = _broker.JoinGroup(options.ConsumerGroup, options.Topic);
        try
        {
            // The subscription above is registered atomically with the history snapshot, so from here
            // on nothing can be missed.
            try { options.OnReady?.Invoke(); } catch { /* caller bug - don't kill the subscription (same as the real gateway) */ }

            // First offset to read, per partition (the live stream continues from the end of history).
            var partitionCount = _broker.GetPartitionCount(options.Topic);
            var start = new long[partitionCount];
            var historyEnd = new long[partitionCount];
            foreach (var m in history) historyEnd[m.Partition] = Math.Max(historyEnd[m.Partition], m.Offset + 1);

            for (var p = 0; p < partitionCount; p++)
            {
                var partitionHistory = history.Where(m => m.Partition == p).ToList();
                var end = _broker.GetOffsets(options.Topic, p).latest;
                start[p] = StartOffset(options, p, partitionHistory, end);
            }

            var emitted = 0L;
            var nextExpected = (long[])start.Clone();

            foreach (var msg in history)
            {
                if (msg.Offset < start[msg.Partition]) continue;
                if (options.MaxMessages is { } cap && emitted >= cap) yield break;
                cancellationToken.ThrowIfCancellationRequested();

                yield return msg with { ConsumerGroup = options.ConsumerGroup };
                emitted++;
                nextExpected[msg.Partition] = msg.Offset + 1;

                if (options.AutoAcknowledge)
                {
                    _broker.Commit(options.Topic, options.ConsumerGroup, msg.Offset, msg.Partition);
                }
            }

            if (options.MaxMessages is { } cap2 && emitted >= cap2) yield break;

            // Bounded "scan and display" style reads (StopAtPartitionEnd) should stop once history has
            // been drained, mirroring how the real gateway stops at partition EOF, rather than blocking
            // on the live stream forever like an unbounded "watch" subscription does.
            if (options.StopAtPartitionEnd) yield break;

            await foreach (var msg in live.ReadAllAsync(cancellationToken))
            {
                // Already emitted from history (or before the requested start position).
                if (msg.Offset < nextExpected[msg.Partition] || msg.Offset < start[msg.Partition]) continue;
                nextExpected[msg.Partition] = msg.Offset + 1;

                var tagged = msg with { ConsumerGroup = options.ConsumerGroup };
                yield return tagged;
                emitted++;

                if (options.AutoAcknowledge)
                {
                    _broker.Commit(options.Topic, options.ConsumerGroup, tagged.Offset, tagged.Partition);
                }

                if (options.MaxMessages is { } cap3 && emitted >= cap3) yield break;
            }
        }
        finally
        {
            unsubscribe();
        }
    }

    /// <summary>The first offset to read on one partition for the requested start position.</summary>
    private long StartOffset(ConsumeOptions options, int partition, IReadOnlyList<KafkaMessage> partitionHistory, long end)
    {
        long ForFallback(ConsumeStartPosition position) => position == ConsumeStartPosition.Latest ? end : 0;

        return options.StartPosition switch
        {
            ConsumeStartPosition.Earliest => 0,
            ConsumeStartPosition.Latest => end,
            ConsumeStartPosition.Committed => _broker.GetCommittedOffset(options.Topic, options.ConsumerGroup, partition) is var committed and >= 0
                ? committed + 1
                : ForFallback(options.UncommittedStart),
            // No timestamp given: behave like Latest, as the real gateway does.
            ConsumeStartPosition.FromTimestamp => options.FromTimestamp is { } from ? FindFirstOffsetAtOrAfter(partitionHistory, from, end) : end,
            // Per partition, like the real gateway.
            ConsumeStartPosition.Tail => Math.Max(0, end - Math.Max(0, options.TailCount)),
            _ => end
        };
    }

    public Task<bool> IsTopicCompactedAsync(string topic, CancellationToken cancellationToken = default) =>
        _broker.TopicExists(topic)
            ? Task.FromResult(_broker.IsCompacted(topic))
            : Task.FromException<bool>(new KeyNotFoundException($"topic '{topic}' not found"));

    public Task AcknowledgeAsync(KafkaMessage message, CancellationToken cancellationToken = default)
    {
        if (message.ConsumerGroup is null)
        {
            throw new InvalidOperationException(
                $"Cannot acknowledge message at {message.Topic}#{message.Partition}@{message.Offset}: " +
                "it was not associated with a consumer group (was it produced rather than consumed?).");
        }

        _broker.Commit(message.Topic, message.ConsumerGroup, message.Offset, message.Partition);
        return Task.CompletedTask;
    }

    private static long FindFirstOffsetAtOrAfter(IReadOnlyList<KafkaMessage> partitionHistory, DateTimeOffset timestamp, long end)
    {
        foreach (var m in partitionHistory)
        {
            if (m.Timestamp >= timestamp) return m.Offset;
        }
        return end;
    }

    public Task<IReadOnlyList<ConsumerGroupSummary>> ListConsumerGroupsAsync(CancellationToken cancellationToken = default)
    {
        IReadOnlyList<ConsumerGroupSummary> groups = _broker.ListGroups().Select(g =>
        {
            var members = _broker.GetActiveMemberCount(g);
            return new ConsumerGroupSummary
            {
                GroupId = g,
                State = members > 0 ? "Stable" : ConsumerGroupStates.Empty,
                MemberCount = members
            };
        }).ToList();
        return Task.FromResult(groups);
    }

    public Task<ConsumerGroupDetail> DescribeConsumerGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        if (!_broker.ListGroups().Contains(groupId))
        {
            return Task.FromException<ConsumerGroupDetail>(new KeyNotFoundException($"consumer group '{groupId}' not found"));
        }

        var memberCount = _broker.GetActiveMemberCount(groupId);
        var offsets = _broker.GetGroupOffsets(groupId).Select(o =>
        {
            var (earliest, latest) = _broker.GetOffsets(o.topic, o.partition);
            return new ConsumerGroupOffset
            {
                Topic = o.topic,
                Partition = o.partition,
                CommittedOffset = o.nextOffset >= 0 ? o.nextOffset : null,
                EarliestOffset = earliest,
                EndOffset = latest
            };
        }).ToList();
        var memberList = _broker.GetMembers(groupId);

        return Task.FromResult(new ConsumerGroupDetail
        {
            GroupId = groupId,
            State = memberCount > 0 ? "Stable" : ConsumerGroupStates.Empty,
            PartitionAssignor = memberCount > 0 ? "range" : null,
            Members = memberList.Select((m, i) => new ConsumerGroupMember
            {
                MemberId = m.memberId,
                ClientId = $"client-{i + 1}",
                Host = "/127.0.0.1",
                Assignment = m.assignment
            }).ToList(),
            Offsets = offsets
        });
    }

    public async Task<IReadOnlyList<OffsetChange>> PlanOffsetResetAsync(OffsetResetRequest request,
        CancellationToken cancellationToken = default)
    {
        var detail = await DescribeConsumerGroupAsync(request.GroupId, cancellationToken).ConfigureAwait(false);
        var changes = new List<OffsetChange>();
        foreach (var o in detail.Offsets)
        {
            if (request.Topic is not null && o.Topic != request.Topic) continue;
            if (request.Partition is { } p && o.Partition != p) continue;

            long target = request.Target switch
            {
                OffsetResetTarget.Earliest => o.EarliestOffset,
                OffsetResetTarget.Latest => o.EndOffset,
                OffsetResetTarget.Timestamp => FindOffsetAtOrAfter(o.Topic, o.Partition, request.Timestamp
                    ?? throw new ArgumentException("a timestamp is required"), o.EndOffset),
                _ => Math.Clamp(request.Offset ?? throw new ArgumentException("an offset is required"),
                    o.EarliestOffset, o.EndOffset)
            };
            changes.Add(new OffsetChange { Topic = o.Topic, Partition = o.Partition, CurrentOffset = o.CommittedOffset, NewOffset = target });
        }

        if (changes.Count == 0)
        {
            throw new InvalidOperationException("no committed partitions of this group match the reset scope");
        }
        return changes;
    }

    public Task ApplyOffsetResetAsync(string groupId, IReadOnlyList<OffsetChange> changes,
        CancellationToken cancellationToken = default)
    {
        var members = _broker.GetActiveMemberCount(groupId);
        if (members > 0)
        {
            return Task.FromException(new InvalidOperationException(
                $"consumer group '{groupId}' is active ({members} member(s)); stop its consumers before changing offsets"));
        }

        foreach (var change in changes) _broker.SetNextOffset(change.Topic, groupId, change.NewOffset, change.Partition);
        return Task.CompletedTask;
    }

    private long FindOffsetAtOrAfter(string topic, int partition, DateTimeOffset timestamp, long endOffset)
    {
        foreach (var (ts, offset) in _broker.GetTimestamps(topic, partition))
        {
            if (ts >= timestamp) return offset;
        }
        return endOffset;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

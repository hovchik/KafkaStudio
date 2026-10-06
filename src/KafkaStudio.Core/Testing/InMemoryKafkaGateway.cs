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

    public Task<ClusterInfo> DescribeClusterAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(new ClusterInfo
        {
            ClusterId = "demo-cluster",
            ControllerId = 0,
            Brokers = new[] { new BrokerInfo { Id = 0, Host = "localhost", Port = 9092, IsController = true } }
        });

    public Task<IReadOnlyList<BrokerConfigEntry>> GetBrokerConfigAsync(int brokerId,
        CancellationToken cancellationToken = default)
    {
        if (brokerId != 0) return Task.FromException<IReadOnlyList<BrokerConfigEntry>>(
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
        if (_broker.TopicExists(topic))
        {
            return Task.FromException(new InvalidOperationException($"topic '{topic}' already exists"));
        }
        _broker.EnsureTopic(topic);
        return Task.CompletedTask;
    }

    public Task<ProduceReceipt> ProduceAsync(ProduceRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var raw = request.GetValueBytes();
        var message = _broker.Append(
            request.Topic,
            request.Key,
            request.RawValue is not null ? KafkaMessage.DecodeText(raw) : request.Value,
            raw,
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
            // The subscription above is registered atomically with the history snapshot, so from here
            // on nothing can be missed.
            options.OnReady?.Invoke();

            long startOffset = options.StartPosition switch
            {
                ConsumeStartPosition.Earliest => 0,
                ConsumeStartPosition.Latest => history.Count,
                ConsumeStartPosition.Committed => _broker.GetCommittedOffset(options.Topic, options.ConsumerGroup) + 1,
                ConsumeStartPosition.FromTimestamp => FindFirstIndexAtOrAfter(history, options.FromTimestamp),
                ConsumeStartPosition.Tail => Math.Max(0, history.Count - Math.Max(0, options.TailCount)),
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
            var (earliest, latest) = _broker.GetOffsets(o.topic);
            return new ConsumerGroupOffset
            {
                Topic = o.topic,
                Partition = 0,
                CommittedOffset = o.nextOffset > 0 ? o.nextOffset : null,
                EarliestOffset = earliest,
                EndOffset = latest
            };
        }).ToList();

        return Task.FromResult(new ConsumerGroupDetail
        {
            GroupId = groupId,
            State = memberCount > 0 ? "Stable" : ConsumerGroupStates.Empty,
            PartitionAssignor = memberCount > 0 ? "range" : null,
            Members = Enumerable.Range(1, memberCount).Select(i => new ConsumerGroupMember
            {
                MemberId = $"{groupId}-member-{i}",
                ClientId = $"client-{i}",
                Host = "/127.0.0.1",
                Assignment = offsets.Select(o => $"{o.Topic}[{o.Partition}]").ToList()
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
                OffsetResetTarget.Timestamp => FindOffsetAtOrAfter(o.Topic, request.Timestamp
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

        foreach (var change in changes) _broker.SetNextOffset(change.Topic, groupId, change.NewOffset);
        return Task.CompletedTask;
    }

    private long FindOffsetAtOrAfter(string topic, DateTimeOffset timestamp, long endOffset)
    {
        foreach (var (ts, offset) in _broker.GetTimestamps(topic))
        {
            if (ts >= timestamp) return offset;
        }
        return endOffset;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

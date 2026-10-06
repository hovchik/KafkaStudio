using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Connections;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Kafka;

/// <summary>
/// The real <see cref="IKafkaGateway"/>: a thin async adapter over Confluent.Kafka / librdkafka.
/// Confluent.Kafka's consumer API is synchronous and blocking by design (that's how librdkafka's
/// polling model works), so every consume subscription here runs its own dedicated background thread
/// that polls in a loop and hands messages to callers through a bounded <see cref="Channel{T}"/> - the
/// same bridge-to-async pattern <see cref="Core.Testing.InMemoryKafkaGateway"/> uses, so callers (the
/// KafScript interpreter, the rethrow engine, the UI) don't need to know or care which one they're
/// talking to.
///
/// Reads use explicit partition assignment with offsets resolved up front (watermarks, timestamps or
/// committed offsets) rather than a group subscription. That avoids the multi-second group join /
/// rebalance of a brand-new group, makes "from now" exact (the high watermark is pinned before
/// <see cref="ConsumeOptions.OnReady"/> fires, so nothing produced afterwards can be missed), and gives
/// bounded reads a precise end: "everything up to the end offsets that existed when the read started".
/// </summary>
public sealed class ConfluentKafkaGateway : IKafkaGateway
{
    private static readonly TimeSpan MetadataTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan AssignmentWaitTimeout = TimeSpan.FromSeconds(10);
    private const int ChannelCapacity = 2_000;

    public ConnectionProfile Profile { get; }

    private readonly object _clientsGate = new();
    private IProducer<byte[]?, byte[]?>? _producer;
    private IAdminClient? _admin;
    private readonly CancellationTokenSource _disposeCts = new();

    // Bounded ring of the most recent librdkafka debug/log lines (broker + security), so a bare
    // "Local: Timed out" can be enriched with the actual low-level reason (e.g. a failed SASL
    // handshake or DNS/connect failure) instead of leaving the user to guess.
    private readonly ConcurrentQueue<string> _recentLogs = new();
    private const int MaxRecentLogs = 20;

    private void OnLibrdkafkaLog<TClient>(TClient _, LogMessage message)
    {
        _recentLogs.Enqueue($"[{message.Level}] {message.Facility}: {message.Message}");
        while (_recentLogs.Count > MaxRecentLogs && _recentLogs.TryDequeue(out string? _)) { }
    }

    /// <summary>A live consumer plus the lock that serializes access to it (IConsumer is not thread-safe).</summary>
    private sealed class ConsumerEntry
    {
        public required IConsumer<byte[]?, byte[]?> Consumer { get; init; }
        public object Lock { get; } = new();
        public bool Closed { get; private set; }

        /// <summary>Closes (leaves the group, flushes commits) and disposes exactly once.</summary>
        public void CloseOnce()
        {
            lock (Lock)
            {
                if (Closed) return;
                Closed = true;
                try { Consumer.Close(); } catch { /* best effort */ }
                try { Consumer.Dispose(); } catch { /* best effort */ }
            }
        }
    }

    // Live consumers keyed by consumer group id so AcknowledgeAsync can route an explicit commit back
    // to the exact consumer instance that read the message.
    private readonly ConcurrentDictionary<string, ConsumerEntry> _activeConsumers = new();
    private readonly ConcurrentDictionary<Task, byte> _pumps = new();

    public ConfluentKafkaGateway(ConnectionProfile profile)
    {
        Profile = profile;
    }

    /// <summary>
    /// Wraps a local-transport <see cref="KafkaException"/> (e.g. "Local: Broker transport failure")
    /// with the connection settings that were actually used, so callers/logs/UI status messages show
    /// enough to diagnose a broker mismatch (wrong host:port, wrong security protocol, etc.) without
    /// needing to inspect this gateway's state directly.
    /// </summary>
    private KafkaException WrapTransportError(KafkaException ex)
    {
        if (!ex.Error.IsLocalError)
        {
            return ex;
        }

        var lastLogs = string.Join(" | ", _recentLogs.ToArray());
        var enrichedReason = $"{ex.Error.Reason} [bootstrap.servers='{Profile.BootstrapServers}', " +
            $"security.protocol={Profile.SecurityProtocol}, sasl.mechanism={Profile.SaslMechanism}]" +
            (lastLogs.Length > 0 ? $" - recent librdkafka logs: {lastLogs}" : string.Empty);
        return new KafkaException(new Error(ex.Error.Code, enrichedReason, ex.Error.IsFatal), ex);
    }

    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(Profile.BootstrapServers))
        {
            throw new ArgumentException("bootstrap servers are required (e.g. 'localhost:9092')");
        }

        lock (_clientsGate)
        {
            // Reconnecting replaces the clients instead of leaking the previous ones.
            _producer?.Dispose();
            _admin?.Dispose();

            _producer = new ProducerBuilder<byte[]?, byte[]?>(ConfigMapper.ToProducerConfig(Profile))
                .SetLogHandler(OnLibrdkafkaLog)
                .Build();

            _admin = new AdminClientBuilder(ConfigMapper.ToAdminConfig(Profile))
                .SetLogHandler(OnLibrdkafkaLog)
                .Build();
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<string>> ListTopicsAsync(CancellationToken cancellationToken = default) =>
        Task.Run(() =>
        {
            try
            {
                var metadata = RequireAdmin().GetMetadata(MetadataTimeout);
                IReadOnlyList<string> topics = metadata.Topics
                    .Where(t => !t.Error.IsError)
                    .Select(t => t.Topic)
                    .Where(name => !name.StartsWith("__", StringComparison.Ordinal)) // hide internal topics (__consumer_offsets etc.)
                    .OrderBy(name => name, StringComparer.Ordinal)
                    .ToList();
                return topics;
            }
            catch (KafkaException ex)
            {
                throw WrapTransportError(ex);
            }
        }, cancellationToken);

    public Task<Core.Messaging.TopicMetadata> DescribeTopicAsync(string topic, CancellationToken cancellationToken = default) =>
        Task.Run(() =>
        {
            var topicMeta = GetTopicMetadata(topic)
                ?? throw new KeyNotFoundException($"topic '{topic}' not found");

            using var probe = BuildConsumer($"kafka-studio-describe-{Guid.NewGuid():N}");
            var partitions = new List<PartitionInfo>();
            foreach (var p in topicMeta.Partitions.OrderBy(p => p.PartitionId))
            {
                cancellationToken.ThrowIfCancellationRequested();
                var watermarks = QueryWatermarks(probe, new TopicPartition(topic, new Partition(p.PartitionId)));
                partitions.Add(new PartitionInfo
                {
                    Id = p.PartitionId,
                    LeaderBrokerId = p.Leader,
                    EarliestOffset = watermarks.Low.Value,
                    LatestOffset = watermarks.High.Value
                });
            }

            return new Core.Messaging.TopicMetadata
            {
                Name = topic,
                ReplicationFactor = topicMeta.Partitions.Count == 0 ? 0 : topicMeta.Partitions[0].Replicas.Length,
                Partitions = partitions
            };
        }, cancellationToken);

    public async Task<ClusterInfo> DescribeClusterAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var result = await RequireAdmin()
                .DescribeClusterAsync(new DescribeClusterOptions { RequestTimeout = MetadataTimeout })
                .WaitAsync(cancellationToken).ConfigureAwait(false);
            var controllerId = result.Controller?.Id;
            return new ClusterInfo
            {
                ClusterId = result.ClusterId,
                ControllerId = controllerId,
                Brokers = result.Nodes
                    .Select(n => new BrokerInfo
                    {
                        Id = n.Id,
                        Host = n.Host,
                        Port = n.Port,
                        Rack = string.IsNullOrEmpty(n.Rack) ? null : n.Rack,
                        IsController = n.Id == controllerId
                    })
                    .OrderBy(b => b.Id)
                    .ToList()
            };
        }
        catch (KafkaException)
        {
            // Older brokers don't support DescribeCluster - fall back to plain metadata (no controller/rack).
        }

        return await Task.Run(() =>
        {
            try
            {
                var metadata = RequireAdmin().GetMetadata(MetadataTimeout);
                return new ClusterInfo
                {
                    Brokers = metadata.Brokers
                        .Select(b => new BrokerInfo { Id = b.BrokerId, Host = b.Host, Port = b.Port })
                        .OrderBy(b => b.Id)
                        .ToList()
                };
            }
            catch (KafkaException ex)
            {
                throw WrapTransportError(ex);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<BrokerConfigEntry>> GetBrokerConfigAsync(int brokerId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var results = await RequireAdmin().DescribeConfigsAsync(
                new[] { new ConfigResource { Type = ResourceType.Broker, Name = brokerId.ToString() } },
                new DescribeConfigsOptions { RequestTimeout = MetadataTimeout })
                .WaitAsync(cancellationToken).ConfigureAwait(false);
            return results
                .SelectMany(r => r.Entries)
                .Select(e => new BrokerConfigEntry
                {
                    Name = e.Key,
                    Value = e.Value.IsSensitive ? null : e.Value.Value,
                    IsDefault = e.Value.IsDefault,
                    IsSensitive = e.Value.IsSensitive,
                    Source = e.Value.Source.ToString()
                })
                .OrderBy(e => e.Name, StringComparer.Ordinal)
                .ToList();
        }
        catch (KafkaException ex)
        {
            throw WrapTransportError(ex);
        }
    }

    public async Task CreateTopicAsync(string topic, int partitions, short replicationFactor,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await RequireAdmin().CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = replicationFactor }
            }).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (CreateTopicsException ex)
        {
            var reason = ex.Results.FirstOrDefault(r => r.Error.IsError)?.Error.Reason ?? ex.Message;
            throw new InvalidOperationException($"could not create topic '{topic}': {reason}", ex);
        }
        catch (KafkaException ex)
        {
            throw WrapTransportError(ex);
        }
    }

    public async Task<ProduceReceipt> ProduceAsync(ProduceRequest request, CancellationToken cancellationToken = default)
    {
        var producer = RequireProducer();

        var message = new Message<byte[]?, byte[]?>
        {
            Key = request.Key is null ? null : Encoding.UTF8.GetBytes(request.Key),
            Value = request.GetValueBytes()
        };
        if (request.Headers is { Count: > 0 })
        {
            var headers = new Headers();
            foreach (var (key, value) in request.Headers)
            {
                headers.Add(key, Encoding.UTF8.GetBytes(value ?? string.Empty));
            }
            message.Headers = headers;
        }

        DeliveryResult<byte[]?, byte[]?> result;
        try
        {
            result = request.Partition is { } partition
                ? await producer.ProduceAsync(new TopicPartition(request.Topic, new Partition(partition)), message, cancellationToken).ConfigureAwait(false)
                : await producer.ProduceAsync(request.Topic, message, cancellationToken).ConfigureAwait(false);
        }
        catch (ProduceException<byte[]?, byte[]?> ex)
        {
            throw ex.Error.IsLocalError ? WrapTransportError(new KafkaException(ex.Error, ex)) : ex;
        }

        return new ProduceReceipt
        {
            Topic = result.Topic,
            Partition = result.Partition.Value,
            Offset = result.Offset.Value,
            Timestamp = result.Timestamp.Type == TimestampType.NotAvailable
                ? DateTimeOffset.UtcNow
                : new DateTimeOffset(result.Timestamp.UtcDateTime)
        };
    }

    public async Task<long> DeleteRecordsBeforeAsync(string topic, int partition, long beforeOffset,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var results = await RequireAdmin().DeleteRecordsAsync(new[]
            {
                new TopicPartitionOffset(topic, new Partition(partition), new Offset(beforeOffset))
            }).WaitAsync(cancellationToken).ConfigureAwait(false);
            return results.Count > 0 ? results[0].Offset.Value : beforeOffset;
        }
        catch (DeleteRecordsException ex)
        {
            var reason = ex.Results.FirstOrDefault(r => r.Error.IsError)?.Error.Reason ?? ex.Message;
            throw new InvalidOperationException($"could not delete records from '{topic}' [{partition}]: {reason}", ex);
        }
        catch (KafkaException ex)
        {
            throw WrapTransportError(ex);
        }
    }

    public async IAsyncEnumerable<KafkaMessage> ConsumeAsync(ConsumeOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Our own token source, so the pump thread can be stopped when the *caller* stops enumerating
        // (break / MaxMessages / Dispose) - not only when the caller's token is cancelled. Without it
        // an early break left the pump polling forever and the enumerator's cleanup hung on it.
        using var pumpCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCts.Token);
        var token = pumpCts.Token;

        var entry = new ConsumerEntry { Consumer = BuildConsumer(options.ConsumerGroup) };
        _activeConsumers[options.ConsumerGroup] = entry;

        var channel = Channel.CreateBounded<KafkaMessage>(new BoundedChannelOptions(ChannelCapacity)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait // back-pressure: a slow reader pauses polling instead of buffering unboundedly
        });

        var pump = Task.Factory.StartNew(
            () => PumpLoop(entry, options, channel.Writer, token),
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        _pumps[pump] = 0;

        try
        {
            var emitted = 0;
            var done = false;
            while (!done && await channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (channel.Reader.TryRead(out var message))
                {
                    yield return message;
                    emitted++;
                    if (options.MaxMessages is { } cap && emitted >= cap)
                    {
                        done = true;
                        break;
                    }
                }
            }
        }
        finally
        {
            pumpCts.Cancel();
            ((ICollection<KeyValuePair<string, ConsumerEntry>>)_activeConsumers)
                .Remove(new KeyValuePair<string, ConsumerEntry>(options.ConsumerGroup, entry));
            try { await pump.ConfigureAwait(false); } catch { /* the pump reports its own errors via the channel */ }
            _pumps.TryRemove(pump, out _);

            // Close()/Dispose() talk to the broker and can block for a while - run them on a
            // background thread so a caller awaiting this enumerator on the UI thread never freezes.
            await Task.Run(entry.CloseOnce).ConfigureAwait(false);
        }
    }

    private IConsumer<byte[]?, byte[]?> BuildConsumer(string groupId) =>
        new ConsumerBuilder<byte[]?, byte[]?>(ConfigMapper.ToConsumerConfig(Profile, groupId, AutoOffsetReset.Earliest))
            .SetLogHandler(OnLibrdkafkaLog)
            .Build();

    /// <summary>Read plan: where each partition starts, and (for bounded reads) where it ends.</summary>
    private sealed record ReadPlan(IReadOnlyList<TopicPartitionOffset> Start, IReadOnlyDictionary<TopicPartition, long> EndExclusive);

    private void PumpLoop(ConsumerEntry entry, ConsumeOptions options, ChannelWriter<KafkaMessage> writer, CancellationToken token)
    {
        var consumer = entry.Consumer;
        var readySignalled = 0;
        void SignalReady()
        {
            if (Interlocked.Exchange(ref readySignalled, 1) == 0)
            {
                try { options.OnReady?.Invoke(); } catch { /* caller bug - don't kill the subscription */ }
            }
        }

        Exception? failure = null;
        try
        {
            var plan = ResolveReadPlan(consumer, options, token);

            // Partitions still to be drained for a bounded read. For the subscribe fallback (topic
            // unknown at start) the end isn't known up front, so rely on partition-EOF events instead.
            HashSet<TopicPartition>? remaining = null;
            var eofPartitions = new HashSet<TopicPartition>();

            if (plan is null && options.StopAtPartitionEnd)
            {
                // A bounded read of a topic that doesn't exist is simply empty - don't wait for it to appear.
                return;
            }

            if (plan is null)
            {
                lock (entry.Lock) consumer.Subscribe(options.Topic);
            }
            else
            {
                lock (entry.Lock) consumer.Assign(plan.Start);
                if (options.StopAtPartitionEnd)
                {
                    remaining = plan.Start
                        .Where(s => s.Offset.Value < plan.EndExclusive[s.TopicPartition])
                        .Select(s => s.TopicPartition)
                        .ToHashSet();
                }
            }

            SignalReady();

            if (remaining is { Count: 0 }) return; // nothing to read - e.g. an empty topic

            while (!token.IsCancellationRequested)
            {
                ConsumeResult<byte[]?, byte[]?>? result;
                lock (entry.Lock)
                {
                    result = consumer.Consume(200); // short poll so we keep checking for cancellation
                }

                if (result is null) continue;

                if (result.IsPartitionEOF)
                {
                    if (options.StopAtPartitionEnd)
                    {
                        if (remaining is not null)
                        {
                            remaining.Remove(result.TopicPartition);
                            if (remaining.Count == 0) break;
                        }
                        else
                        {
                            eofPartitions.Add(result.TopicPartition);
                            List<TopicPartition> assignment;
                            lock (entry.Lock) assignment = consumer.Assignment;
                            if (assignment.Count > 0 && eofPartitions.IsSupersetOf(assignment)) break;
                        }
                    }
                    continue;
                }

                if (result.Message is null) continue;

                var message = ToKafkaMessage(result, options.ConsumerGroup);

                if (options.AutoAcknowledge)
                {
                    lock (entry.Lock)
                    {
                        consumer.Commit(new[] { new TopicPartitionOffset(result.TopicPartition, new Offset(result.Offset.Value + 1)) });
                    }
                }

                if (!writer.TryWrite(message))
                {
                    writer.WriteAsync(message, token).AsTask().GetAwaiter().GetResult();
                }

                if (remaining is not null && plan is not null &&
                    result.Offset.Value + 1 >= plan.EndExclusive.GetValueOrDefault(result.TopicPartition, long.MaxValue))
                {
                    remaining.Remove(result.TopicPartition);
                    if (remaining.Count == 0) break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // expected on shutdown / early stop
        }
        catch (ChannelClosedException)
        {
            // reader went away
        }
        catch (KafkaException ex)
        {
            failure = WrapTransportError(ex);
        }
        catch (Exception ex)
        {
            failure = ex;
        }
        finally
        {
            SignalReady(); // never leave a waiter hanging on a subscription that failed before becoming live
            writer.TryComplete(failure);
        }
    }

    /// <summary>
    /// Works out explicit start (and end) offsets for every partition of the topic. Returns null when the
    /// topic doesn't exist (yet) - the caller then falls back to a group subscription, which waits for
    /// the topic to appear (or be auto-created) exactly like a plain Kafka consumer would.
    /// </summary>
    private ReadPlan? ResolveReadPlan(IConsumer<byte[]?, byte[]?> consumer, ConsumeOptions options, CancellationToken token)
    {
        var topicMeta = GetTopicMetadata(options.Topic);
        if (topicMeta is null || topicMeta.Partitions.Count == 0) return null;

        var partitions = topicMeta.Partitions
            .Select(p => new TopicPartition(options.Topic, new Partition(p.PartitionId)))
            .OrderBy(tp => tp.Partition.Value)
            .ToList();

        var watermarks = new Dictionary<TopicPartition, WatermarkOffsets>();
        foreach (var tp in partitions)
        {
            token.ThrowIfCancellationRequested();
            watermarks[tp] = QueryWatermarks(consumer, tp);
        }

        long Clamp(TopicPartition tp, long offset)
        {
            var w = watermarks[tp];
            return Math.Clamp(offset, w.Low.Value, w.High.Value);
        }

        Dictionary<TopicPartition, long> start;
        switch (options.StartPosition)
        {
            case ConsumeStartPosition.Earliest:
                start = partitions.ToDictionary(tp => tp, tp => watermarks[tp].Low.Value);
                break;

            case ConsumeStartPosition.Tail:
                var tail = Math.Max(0, options.TailCount);
                start = partitions.ToDictionary(tp => tp, tp => Clamp(tp, watermarks[tp].High.Value - tail));
                break;

            case ConsumeStartPosition.FromTimestamp when options.FromTimestamp is { } from:
                var ts = new Timestamp(from.UtcDateTime, TimestampType.CreateTime);
                var byTime = consumer.OffsetsForTimes(partitions.Select(tp => new TopicPartitionTimestamp(tp, ts)), MetadataTimeout);
                start = byTime.ToDictionary(
                    r => r.TopicPartition,
                    // No message at/after the timestamp -> the partition's end.
                    r => r.Offset.Value < 0 ? watermarks[r.TopicPartition].High.Value : Clamp(r.TopicPartition, r.Offset.Value));
                break;

            case ConsumeStartPosition.Committed:
                var committed = consumer.Committed(partitions, MetadataTimeout);
                start = committed.ToDictionary(
                    c => c.TopicPartition,
                    // Nothing committed yet -> from the beginning (same as the in-memory gateway);
                    // committed offset already deleted by retention -> earliest still available.
                    c => c.Offset.Value < 0 ? watermarks[c.TopicPartition].Low.Value : Clamp(c.TopicPartition, c.Offset.Value));
                break;

            default: // Latest (and FromTimestamp without a timestamp)
                start = partitions.ToDictionary(tp => tp, tp => watermarks[tp].High.Value);
                break;
        }

        return new ReadPlan(
            partitions.Select(tp => new TopicPartitionOffset(tp, new Offset(start[tp]))).ToList(),
            partitions.ToDictionary(tp => tp, tp => watermarks[tp].High.Value));
    }

    private Confluent.Kafka.TopicMetadata? GetTopicMetadata(string topic)
    {
        Confluent.Kafka.Metadata metadata;
        try
        {
            metadata = RequireAdmin().GetMetadata(topic, MetadataTimeout);
        }
        catch (KafkaException ex)
        {
            throw WrapTransportError(ex);
        }

        var topicMeta = metadata.Topics.FirstOrDefault(t => t.Topic == topic);
        if (topicMeta is null) return null;
        if (topicMeta.Error.Code is ErrorCode.UnknownTopicOrPart or ErrorCode.Local_UnknownTopic) return null;
        if (topicMeta.Error.IsError)
        {
            throw new InvalidOperationException($"topic '{topic}': {topicMeta.Error.Reason}");
        }
        return topicMeta;
    }

    private WatermarkOffsets QueryWatermarks(IConsumer<byte[]?, byte[]?> consumer, TopicPartition tp)
    {
        try
        {
            return consumer.QueryWatermarkOffsets(tp, MetadataTimeout);
        }
        catch (KafkaException ex)
        {
            throw WrapTransportError(ex);
        }
    }

    private static KafkaMessage ToKafkaMessage(ConsumeResult<byte[]?, byte[]?> result, string consumerGroup)
    {
        // Kafka allows repeated header names; keep the last value rather than throwing (which used to
        // kill the whole subscription on the first such message).
        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        if (result.Message.Headers is { } rawHeaders)
        {
            foreach (var header in rawHeaders)
            {
                var bytes = header.GetValueBytes();
                headers[header.Key] = bytes is null ? string.Empty : Encoding.UTF8.GetString(bytes);
            }
        }

        var rawKey = result.Message.Key;
        var key = rawKey is null ? null : KafkaMessage.DecodeText(rawKey) ?? "0x" + Convert.ToHexString(rawKey.AsSpan(0, Math.Min(rawKey.Length, 64)));

        var rawValue = result.Message.Value;
        return new KafkaMessage
        {
            Topic = result.Topic,
            Partition = result.Partition.Value,
            Offset = result.Offset.Value,
            Key = key,
            Value = KafkaMessage.DecodeText(rawValue),
            RawValue = rawValue,
            Headers = headers,
            Timestamp = new DateTimeOffset(result.Message.Timestamp.UtcDateTime),
            ConsumerGroup = consumerGroup
        };
    }

    public Task AcknowledgeAsync(KafkaMessage message, CancellationToken cancellationToken = default)
    {
        if (message.ConsumerGroup is null)
        {
            throw new InvalidOperationException(
                $"Cannot acknowledge message at {message.Topic}#{message.Partition}@{message.Offset}: " +
                "it was not associated with a consumer group (was it produced rather than consumed?).");
        }

        var offsets = new[]
        {
            new TopicPartitionOffset(message.Topic, new Partition(message.Partition), new Offset(message.Offset + 1))
        };

        return Task.Run(() =>
        {
            try
            {
                if (_activeConsumers.TryGetValue(message.ConsumerGroup, out var entry))
                {
                    lock (entry.Lock)
                    {
                        if (!entry.Closed)
                        {
                            entry.Consumer.Commit(offsets);
                            return;
                        }
                    }
                }

                // The subscription that read the message has already ended (typical for "scan, then
                // acknowledge each scanned message"): commit through a short-lived consumer in the same
                // group. Kafka accepts offset commits for a group that has no active members.
                using var committer = BuildConsumer(message.ConsumerGroup);
                committer.Commit(offsets);
                committer.Close();
            }
            catch (KafkaException ex)
            {
                throw WrapTransportError(ex);
            }
        }, cancellationToken);
    }

    // ------------------------------------------------------------------ consumer groups ----

    public async Task<IReadOnlyList<ConsumerGroupSummary>> ListConsumerGroupsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var listed = await RequireAdmin().ListConsumerGroupsAsync(new ListConsumerGroupsOptions { RequestTimeout = MetadataTimeout })
                .WaitAsync(cancellationToken).ConfigureAwait(false);
            var ids = listed.Valid.Select(g => g.GroupId).OrderBy(g => g, StringComparer.Ordinal).ToList();
            if (ids.Count == 0) return Array.Empty<ConsumerGroupSummary>();

            // The listing has no member counts - one describe call covers every group.
            var described = await RequireAdmin().DescribeConsumerGroupsAsync(ids, new DescribeConsumerGroupsOptions { RequestTimeout = MetadataTimeout })
                .WaitAsync(cancellationToken).ConfigureAwait(false);
            var byId = described.ConsumerGroupDescriptions.ToDictionary(d => d.GroupId, StringComparer.Ordinal);

            return ids.Select(id => byId.TryGetValue(id, out var d) && !d.Error.IsError
                ? new ConsumerGroupSummary { GroupId = id, State = d.State.ToString(), MemberCount = d.Members.Count }
                : new ConsumerGroupSummary
                {
                    GroupId = id,
                    State = listed.Valid.First(g => g.GroupId == id).State.ToString()
                }).ToList();
        }
        catch (KafkaException ex)
        {
            throw WrapTransportError(ex);
        }
    }

    public async Task<ConsumerGroupDetail> DescribeConsumerGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        try
        {
            var admin = RequireAdmin();
            var described = await admin.DescribeConsumerGroupsAsync(new[] { groupId }, new DescribeConsumerGroupsOptions { RequestTimeout = MetadataTimeout })
                .WaitAsync(cancellationToken).ConfigureAwait(false);
            var description = described.ConsumerGroupDescriptions.FirstOrDefault()
                ?? throw new KeyNotFoundException($"consumer group '{groupId}' not found");
            if (description.Error.IsError) throw new KafkaException(description.Error);

            var offsetResults = await admin.ListConsumerGroupOffsetsAsync(
                    new[] { new ConsumerGroupTopicPartitions(groupId, null) },
                    new ListConsumerGroupOffsetsOptions { RequestTimeout = MetadataTimeout })
                .WaitAsync(cancellationToken).ConfigureAwait(false);
            var committed = offsetResults.SelectMany(r => r.Partitions).ToList();
            var failed = committed.FirstOrDefault(c => c.Error.IsError);
            if (failed is not null) throw new KafkaException(failed.Error);

            // A group can be assigned partitions it never committed on - show those too (lag counts from earliest).
            var partitions = committed.Select(c => (c.TopicPartition, Committed: c.Offset.Value >= 0 ? (long?)c.Offset.Value : null))
                .Concat(description.Members.SelectMany(m => m.Assignment.TopicPartitions)
                    .Where(tp => committed.All(c => c.TopicPartition != tp))
                    .Select(tp => (TopicPartition: tp, Committed: (long?)null)))
                .OrderBy(p => p.TopicPartition.Topic, StringComparer.Ordinal).ThenBy(p => p.TopicPartition.Partition.Value)
                .ToList();

            var offsets = await Task.Run(() =>
            {
                using var probe = BuildConsumer($"kafka-studio-groups-{Guid.NewGuid():N}");
                return partitions.Select(p =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var w = QueryWatermarks(probe, p.TopicPartition);
                    return new ConsumerGroupOffset
                    {
                        Topic = p.TopicPartition.Topic,
                        Partition = p.TopicPartition.Partition.Value,
                        CommittedOffset = p.Committed,
                        EarliestOffset = w.Low.Value,
                        EndOffset = w.High.Value
                    };
                }).ToList();
            }, cancellationToken).ConfigureAwait(false);

            return new ConsumerGroupDetail
            {
                GroupId = groupId,
                State = description.State.ToString(),
                PartitionAssignor = string.IsNullOrEmpty(description.PartitionAssignor) ? null : description.PartitionAssignor,
                Members = description.Members.Select(m => new ConsumerGroupMember
                {
                    MemberId = m.ConsumerId,
                    ClientId = m.ClientId,
                    Host = m.Host,
                    Assignment = m.Assignment.TopicPartitions.Select(tp => $"{tp.Topic}[{tp.Partition.Value}]").ToList()
                }).ToList(),
                Offsets = offsets
            };
        }
        catch (DescribeConsumerGroupsException ex)
        {
            throw new InvalidOperationException($"could not describe consumer group '{groupId}': {ex.Results.ConsumerGroupDescriptions.FirstOrDefault()?.Error.Reason ?? ex.Message}", ex);
        }
        catch (KafkaException ex)
        {
            throw WrapTransportError(ex);
        }
    }

    public async Task<IReadOnlyList<OffsetChange>> PlanOffsetResetAsync(OffsetResetRequest request,
        CancellationToken cancellationToken = default)
    {
        var detail = await DescribeConsumerGroupAsync(request.GroupId, cancellationToken).ConfigureAwait(false);
        var scope = detail.Offsets
            .Where(o => (request.Topic is null || o.Topic == request.Topic) &&
                        (request.Partition is null || o.Partition == request.Partition))
            .ToList();
        if (scope.Count == 0)
        {
            throw new InvalidOperationException("no committed partitions of this group match the reset scope");
        }

        return await Task.Run(() =>
        {
            Dictionary<TopicPartition, long>? byTime = null;
            if (request.Target == OffsetResetTarget.Timestamp)
            {
                var when = request.Timestamp ?? throw new ArgumentException("a timestamp is required");
                var ts = new Timestamp(when.UtcDateTime, TimestampType.CreateTime);
                using var probe = BuildConsumer($"kafka-studio-groups-{Guid.NewGuid():N}");
                byTime = probe.OffsetsForTimes(
                        scope.Select(o => new TopicPartitionTimestamp(new TopicPartition(o.Topic, new Partition(o.Partition)), ts)),
                        MetadataTimeout)
                    .ToDictionary(r => r.TopicPartition, r => r.Offset.Value);
            }

            return (IReadOnlyList<OffsetChange>)scope.Select(o =>
            {
                long target = request.Target switch
                {
                    OffsetResetTarget.Earliest => o.EarliestOffset,
                    OffsetResetTarget.Latest => o.EndOffset,
                    OffsetResetTarget.Timestamp =>
                        // No message at/after the timestamp -> the partition's end.
                        byTime![new TopicPartition(o.Topic, new Partition(o.Partition))] is var found and >= 0 ? found : o.EndOffset,
                    _ => Math.Clamp(request.Offset ?? throw new ArgumentException("an offset is required"),
                        o.EarliestOffset, o.EndOffset)
                };
                return new OffsetChange { Topic = o.Topic, Partition = o.Partition, CurrentOffset = o.CommittedOffset, NewOffset = target };
            }).ToList();
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task ApplyOffsetResetAsync(string groupId, IReadOnlyList<OffsetChange> changes,
        CancellationToken cancellationToken = default)
    {
        // Check up front so the user gets a plain explanation instead of a broker error code.
        var current = await DescribeConsumerGroupAsync(groupId, cancellationToken).ConfigureAwait(false);
        if (current.IsActive)
        {
            throw new InvalidOperationException(
                $"consumer group '{groupId}' is active (state {current.State}, {current.Members.Count} member(s)); " +
                "stop all of its consumers before changing offsets");
        }

        try
        {
            await RequireAdmin().AlterConsumerGroupOffsetsAsync(
                new[]
                {
                    new ConsumerGroupTopicPartitionOffsets(groupId, changes
                        .Select(c => new TopicPartitionOffset(new TopicPartition(c.Topic, new Partition(c.Partition)), new Offset(c.NewOffset)))
                        .ToList())
                },
                new AlterConsumerGroupOffsetsOptions { RequestTimeout = MetadataTimeout })
                .WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (AlterConsumerGroupOffsetsException ex)
        {
            var reason = ex.Results.SelectMany(r => r.Partitions).FirstOrDefault(p => p.Error.IsError)?.Error.Reason ?? ex.Message;
            throw new InvalidOperationException($"could not update offsets for consumer group '{groupId}': {reason}", ex);
        }
        catch (KafkaException ex)
        {
            throw WrapTransportError(ex);
        }
    }

    private IProducer<byte[]?, byte[]?> RequireProducer() =>
        _producer ?? throw new InvalidOperationException("not connected - call ConnectAsync first");

    private IAdminClient RequireAdmin() =>
        _admin ?? throw new InvalidOperationException("not connected - call ConnectAsync first");

    public async ValueTask DisposeAsync()
    {
        _disposeCts.Cancel();

        // Every pump observes the dispose token; give them (and their enumerators' cleanup) a moment.
        try { await Task.WhenAll(_pumps.Keys).WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false); }
        catch { /* best effort */ }

        await Task.Run(() =>
        {
            // Anything still registered belongs to an enumerator that was abandoned without being
            // disposed - its pump has stopped, so it's safe to close here.
            foreach (var entry in _activeConsumers.Values) entry.CloseOnce();
            _activeConsumers.Clear();

            lock (_clientsGate)
            {
                try { _producer?.Flush(TimeSpan.FromSeconds(5)); } catch { /* best effort */ }
                _producer?.Dispose();
                _admin?.Dispose();
                _producer = null;
                _admin = null;
            }
        }).ConfigureAwait(false);
    }
}

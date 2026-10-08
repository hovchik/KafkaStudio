using System.Collections.Concurrent;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Search;

/// <summary>What a scan callback wants to happen next.</summary>
public enum ScanDecision
{
    Continue,
    /// <summary>Stop reading this topic (e.g. "exists?" found its first hit) but keep scanning the others.</summary>
    StopTopic,
    /// <summary>Stop the whole scan (e.g. a result cap was reached).</summary>
    StopAll
}

/// <summary>Live progress of a scan (safe to read from any thread).</summary>
public sealed class ScanProgress
{
    private long _messagesScanned;
    private int _topicsScanned;

    public int TopicsTotal { get; init; }
    public long MessagesScanned => Interlocked.Read(ref _messagesScanned);
    public int TopicsScanned => Volatile.Read(ref _topicsScanned);

    internal void AddMessage() => Interlocked.Increment(ref _messagesScanned);
    internal int AddTopic() => Interlocked.Increment(ref _topicsScanned);
}

/// <summary>Outcome of a scan across topics.</summary>
public sealed record ScanSummary
{
    public required int TopicsTotal { get; init; }
    public required int TopicsScanned { get; init; }
    public required long MessagesScanned { get; init; }

    /// <summary>Topics that could not be read, with the error.</summary>
    public required IReadOnlyDictionary<string, string> FailedTopics { get; init; }

    /// <summary>True when the scan stopped early (a callback returned <see cref="ScanDecision.StopAll"/>).</summary>
    public required bool StoppedEarly { get; init; }

    public required bool Cancelled { get; init; }

    public string FailedSuffix => FailedTopics.Count == 0 ? "" : $" ({FailedTopics.Count} topic(s) could not be read)";
}

/// <summary>
/// Reads a set of topics (bounded by a <see cref="SearchRange"/>) concurrently and hands every message to a
/// callback. The one scanning loop behind search, existence checks, bulk checks, traces, similarity and
/// reconciliation: it bounds concurrency so a big cluster doesn't open hundreds of consumers at once,
/// uses a throwaway consumer group per topic (never touches real groups' offsets), and keeps going
/// when a single topic can't be read.
/// </summary>
public static class TopicScanner
{
    public const int DefaultParallelism = 8;

    /// <param name="onMessage">Called for every in-range message. Called concurrently for different topics,
    /// but sequentially within one topic.</param>
    /// <param name="onTopicDone">Optional; called after each topic finishes (or fails).</param>
    public static async Task<ScanSummary> ScanAsync(
        IKafkaGateway gateway,
        IReadOnlyCollection<string> topics,
        SearchRange range,
        Func<string, KafkaMessage, ScanDecision> onMessage,
        CancellationToken cancellationToken = default,
        ScanProgress? progress = null,
        Action<string>? onTopicDone = null,
        DateTimeOffset? now = null,
        int parallelism = DefaultParallelism)
    {
        // Deduplicate once so the total, the progress and the summary all count the same topics.
        var distinctTopics = topics.Distinct(StringComparer.Ordinal).ToList();
        progress ??= new ScanProgress { TopicsTotal = distinctTopics.Count };
        var at = now ?? DateTimeOffset.UtcNow;
        var failed = new ConcurrentDictionary<string, string>(StringComparer.Ordinal);
        using var stopAll = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var stoppedEarly = 0;

        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, parallelism),
            CancellationToken = stopAll.Token
        };

        try
        {
            await Parallel.ForEachAsync(distinctTopics, parallelOptions, async (topic, token) =>
            {
                try
                {
                    var options = range.ToConsumeOptions(topic, $"kafka-studio-search-{Guid.NewGuid():N}", at);
                    await foreach (var message in gateway.ConsumeAsync(options, token).ConfigureAwait(false))
                    {
                        if (token.IsCancellationRequested) break;
                        if (!range.Includes(message, at)) continue;
                        progress.AddMessage();

                        var decision = onMessage(topic, message);
                        if (decision == ScanDecision.StopTopic) break;
                        if (decision == ScanDecision.StopAll)
                        {
                            Interlocked.Exchange(ref stoppedEarly, 1);
                            stopAll.Cancel();
                            break;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // cancelled or stopped early - reported via the summary.
                }
                catch (Exception ex)
                {
                    // One unreachable/misbehaving topic shouldn't abort a scan across many.
                    failed[topic] = ex.Message;
                }
                finally
                {
                    progress.AddTopic();
                    onTopicDone?.Invoke(topic);
                }
            }).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // handled below
        }

        return new ScanSummary
        {
            TopicsTotal = distinctTopics.Count,
            TopicsScanned = progress.TopicsScanned,
            MessagesScanned = progress.MessagesScanned,
            FailedTopics = failed,
            StoppedEarly = stoppedEarly == 1,
            Cancelled = cancellationToken.IsCancellationRequested
        };
    }
}

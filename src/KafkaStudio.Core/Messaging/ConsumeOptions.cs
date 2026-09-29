namespace KafkaStudio.Core.Messaging;

public enum ConsumeStartPosition
{
    Earliest,
    Latest,
    FromTimestamp,

    /// <summary>Resume from whatever offsets are already committed for the consumer group.</summary>
    Committed,

    /// <summary>
    /// Start <see cref="ConsumeOptions.TailCount"/> messages before the current end of each partition,
    /// i.e. "show me the newest N". Used by the Topic Browser so a capped load shows the latest
    /// messages rather than the oldest ones.
    /// </summary>
    Tail
}

/// <summary>
/// Options for a streaming subscribe-and-consume operation (used by "watch topic", "expect message",
/// rethrow, and scan+acknowledge steps alike). A single shape covers all of these because they only
/// differ in how the resulting messages get handled by the caller.
/// </summary>
public sealed record ConsumeOptions
{
    public required string Topic { get; init; }

    /// <summary>
    /// Consumer group id. KafkaStudio steps default to a per-run unique group (so re-running a check
    /// doesn't skip messages because of stale committed offsets) unless the script pins one explicitly.
    /// </summary>
    public required string ConsumerGroup { get; init; }

    public ConsumeStartPosition StartPosition { get; init; } = ConsumeStartPosition.Latest;

    public DateTimeOffset? FromTimestamp { get; init; }

    /// <summary>Messages per partition to rewind from the end when <see cref="StartPosition"/> is
    /// <see cref="ConsumeStartPosition.Tail"/>.</summary>
    public int TailCount { get; init; } = 50;

    /// <summary>
    /// When true, the gateway commits each message's offset immediately after it is handed to the
    /// caller (at-most-once from the consumer group's point of view). When false, the caller is
    /// responsible for calling <see cref="Abstractions.IKafkaGateway.AcknowledgeAsync"/> explicitly -
    /// this is what the "scan ... and acknowledge" DSL step uses so a script can inspect a message
    /// before deciding to commit it.
    /// </summary>
    public bool AutoAcknowledge { get; init; }

    /// <summary>Optional cap so "scan topic" steps can bound how many records they pull.</summary>
    public int? MaxMessages { get; init; }

    /// <summary>
    /// When true, the gateway stops once every partition has been read up to the end offset it had
    /// when the read started (partition EOF), instead of waiting indefinitely for new messages to
    /// arrive. Used by bounded "scan and display" operations (e.g. the Topic Browser) so that a scan
    /// with no <see cref="MaxMessages"/> cap ("load all") still terminates once the topic's current
    /// backlog has been drained, rather than behaving like an unbounded live "watch" subscription.
    /// </summary>
    public bool StopAtPartitionEnd { get; init; }

    /// <summary>
    /// Invoked by the gateway exactly once, as soon as the read positions are pinned - i.e. from this
    /// point on, every message produced to the topic is guaranteed to be delivered (for positions
    /// relative to "now", like <see cref="ConsumeStartPosition.Latest"/>). This is what makes
    /// "watch topic B, then produce to A, then expect on B" race-free against a real cluster, where
    /// joining a group and fetching offsets takes a noticeable amount of time. Gateways also invoke it
    /// if the subscription fails before becoming ready, so waiters never hang.
    /// </summary>
    public Action? OnReady { get; init; }
}

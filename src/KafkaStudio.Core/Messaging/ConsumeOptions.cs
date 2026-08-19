namespace KafkaStudio.Core.Messaging;

public enum ConsumeStartPosition
{
    Earliest,
    Latest,
    FromTimestamp,

    /// <summary>Resume from whatever offsets are already committed for the consumer group.</summary>
    Committed
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
}

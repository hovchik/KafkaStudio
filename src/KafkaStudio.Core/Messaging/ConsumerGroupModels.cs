namespace KafkaStudio.Core.Messaging;

/// <summary>One row of the consumer group list.</summary>
public sealed record ConsumerGroupSummary
{
    public required string GroupId { get; init; }

    /// <summary>Kafka's group state: Stable, Empty, PreparingRebalance, CompletingRebalance, Dead, ...</summary>
    public required string State { get; init; }

    public int MemberCount { get; init; }

    /// <summary>
    /// True when Kafka will reject offset changes for this group because it has live members.
    /// Only <c>Empty</c> and <c>Dead</c> groups can have their committed offsets altered.
    /// </summary>
    public bool IsActive => ConsumerGroupStates.IsActive(State);
}

public static class ConsumerGroupStates
{
    public const string Empty = "Empty";
    public const string Dead = "Dead";

    public static bool IsActive(string state) =>
        !string.Equals(state, Empty, StringComparison.OrdinalIgnoreCase) &&
        !string.Equals(state, Dead, StringComparison.OrdinalIgnoreCase);
}

public sealed record ConsumerGroupMember
{
    public required string MemberId { get; init; }
    public string? ClientId { get; init; }
    public string? Host { get; init; }
    public IReadOnlyList<string> Assignment { get; init; } = Array.Empty<string>();
}

/// <summary>A group's committed position on one partition, next to what the partition currently holds.</summary>
public sealed record ConsumerGroupOffset
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }

    /// <summary>The next offset the group will read, or null when the group never committed on this partition.</summary>
    public long? CommittedOffset { get; init; }

    public required long EarliestOffset { get; init; }
    public required long EndOffset { get; init; }

    /// <summary>Messages the group still has to read (everything from the end back to its committed position).</summary>
    public long Lag => Math.Max(0, EndOffset - Math.Max(CommittedOffset ?? EarliestOffset, EarliestOffset));
}

public sealed record ConsumerGroupDetail
{
    public required string GroupId { get; init; }
    public required string State { get; init; }
    public string? PartitionAssignor { get; init; }
    public IReadOnlyList<ConsumerGroupMember> Members { get; init; } = Array.Empty<ConsumerGroupMember>();
    public IReadOnlyList<ConsumerGroupOffset> Offsets { get; init; } = Array.Empty<ConsumerGroupOffset>();

    public bool IsActive => ConsumerGroupStates.IsActive(State);
    public long TotalLag => Offsets.Sum(o => o.Lag);
}

public enum OffsetResetTarget
{
    Earliest,
    Latest,
    /// <summary>The first message at or after <see cref="OffsetResetRequest.Timestamp"/>.</summary>
    Timestamp,
    /// <summary>An explicit offset (clamped to what each partition still holds).</summary>
    Offset
}

/// <summary>Where to move a group: every partition it has offsets for, optionally narrowed to a topic / partition.</summary>
public sealed record OffsetResetRequest
{
    public required string GroupId { get; init; }
    public required OffsetResetTarget Target { get; init; }
    public string? Topic { get; init; }
    public int? Partition { get; init; }
    public DateTimeOffset? Timestamp { get; init; }
    public long? Offset { get; init; }
}

/// <summary>One partition's planned move, shown for confirmation before it is applied.</summary>
public sealed record OffsetChange
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public long? CurrentOffset { get; init; }
    public required long NewOffset { get; init; }
}

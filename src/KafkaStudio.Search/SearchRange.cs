using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Search;

public enum SearchRangeKind
{
    /// <summary>Every message currently on the topic.</summary>
    All,
    /// <summary>Messages from the last <see cref="SearchRange.Last"/> (e.g. 15 minutes).</summary>
    LastDuration,
    /// <summary>Messages with a timestamp between <see cref="SearchRange.From"/> and <see cref="SearchRange.To"/>.</summary>
    Between,
    /// <summary>Only the newest <see cref="SearchRange.NewestCount"/> messages of each partition.</summary>
    NewestPerPartition
}

/// <summary>
/// Which part of a topic a search reads. Narrowing the range is the single biggest speed-up for large
/// topics: the gateway seeks straight to the start timestamp/offset instead of reading from the
/// beginning.
/// </summary>
public sealed record SearchRange
{
    public SearchRangeKind Kind { get; init; } = SearchRangeKind.All;
    public TimeSpan? Last { get; init; }
    public DateTimeOffset? From { get; init; }
    public DateTimeOffset? To { get; init; }
    public int NewestCount { get; init; } = 1000;

    public static SearchRange All { get; } = new();

    public static SearchRange LastPeriod(TimeSpan period) => new() { Kind = SearchRangeKind.LastDuration, Last = period };
    public static SearchRange Between(DateTimeOffset? from, DateTimeOffset? to) => new() { Kind = SearchRangeKind.Between, From = from, To = to };
    public static SearchRange Newest(int count) => new() { Kind = SearchRangeKind.NewestPerPartition, NewestCount = count };

    /// <summary>Builds a bounded (stop-at-partition-end) read for <paramref name="topic"/>.</summary>
    public ConsumeOptions ToConsumeOptions(string topic, string consumerGroup, DateTimeOffset now)
    {
        var options = new ConsumeOptions
        {
            Topic = topic,
            ConsumerGroup = consumerGroup,
            StartPosition = ConsumeStartPosition.Earliest,
            StopAtPartitionEnd = true
        };

        return Kind switch
        {
            SearchRangeKind.LastDuration when Last is { } last => options with
            {
                StartPosition = ConsumeStartPosition.FromTimestamp,
                FromTimestamp = now - last
            },
            SearchRangeKind.Between when From is { } from => options with
            {
                StartPosition = ConsumeStartPosition.FromTimestamp,
                FromTimestamp = from
            },
            SearchRangeKind.NewestPerPartition => options with
            {
                StartPosition = ConsumeStartPosition.Tail,
                TailCount = Math.Max(1, NewestCount)
            },
            _ => options
        };
    }

    /// <summary>Client-side check for bounds the broker can't apply (the "to" end of a time window, and
    /// messages with out-of-order timestamps that a timestamp seek still returns).</summary>
    public bool Includes(KafkaMessage message, DateTimeOffset now) => Kind switch
    {
        SearchRangeKind.LastDuration when Last is { } last => message.Timestamp >= now - last,
        SearchRangeKind.Between => (From is not { } from || message.Timestamp >= from) &&
                                   (To is not { } to || message.Timestamp <= to),
        _ => true
    };

    public string Describe() => Kind switch
    {
        SearchRangeKind.LastDuration when Last is { } last => $"last {FormatDuration(last)}",
        SearchRangeKind.Between => $"{From?.ToString("yyyy-MM-dd HH:mm") ?? "beginning"} → {To?.ToString("yyyy-MM-dd HH:mm") ?? "now"}",
        SearchRangeKind.NewestPerPartition => $"newest {NewestCount:N0} per partition",
        _ => "all messages"
    };

    public static string FormatDuration(TimeSpan span) =>
        span.TotalDays >= 1 && span.TotalDays % 1 == 0 ? $"{span.TotalDays:0} day(s)"
        : span.TotalHours >= 1 && span.TotalHours % 1 == 0 ? $"{span.TotalHours:0} hour(s)"
        : span.TotalMinutes >= 1 ? $"{span.TotalMinutes:0.#} minute(s)"
        : $"{span.TotalSeconds:0.#} second(s)";
}

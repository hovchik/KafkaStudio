namespace KafkaStudio.Core.Abstractions;

/// <summary>
/// Indirection over wall-clock time so timing-sensitive logic (the cross-topic "within N seconds" check,
/// task scheduling) can be driven deterministically from unit tests instead of racing real timers.
/// </summary>
public interface IClock
{
    DateTimeOffset UtcNow { get; }

    /// <summary>Completes after the given delay, honoring cancellation. Real implementation is Task.Delay.</summary>
    Task Delay(TimeSpan delay, CancellationToken cancellationToken = default);
}

public sealed class SystemClock : IClock
{
    public static readonly SystemClock Instance = new();

    public DateTimeOffset UtcNow => DateTimeOffset.UtcNow;

    public Task Delay(TimeSpan delay, CancellationToken cancellationToken = default) =>
        Task.Delay(delay, cancellationToken);
}

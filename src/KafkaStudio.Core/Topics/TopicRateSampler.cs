namespace KafkaStudio.Core.Topics;

/// <summary>One topic's activity between two samples.</summary>
public sealed record TopicRate(string Topic, long TotalMessages, long NewMessages, double MessagesPerSecond);

/// <summary>
/// Turns repeated "end offset per topic" samples into messages per second. Pure arithmetic over the
/// numbers it is given, so it is easy to test; the caller decides how often to sample.
/// </summary>
public sealed class TopicRateSampler
{
    private Dictionary<string, long> _previous = new(StringComparer.Ordinal);
    private DateTimeOffset? _previousAt;

    /// <summary>Feeds one sample (topic -> total messages written so far). The first call returns rates of 0.</summary>
    public IReadOnlyList<TopicRate> Add(IReadOnlyDictionary<string, long> totals, DateTimeOffset at)
    {
        var seconds = _previousAt is { } p ? (at - p).TotalSeconds : 0;
        var rates = totals
            .Select(t =>
            {
                var delta = _previous.TryGetValue(t.Key, out var before) ? Math.Max(0, t.Value - before) : 0;
                var perSecond = seconds > 0 ? delta / seconds : 0;
                return new TopicRate(t.Key, t.Value, delta, perSecond);
            })
            .OrderByDescending(r => r.MessagesPerSecond)
            .ThenBy(r => r.Topic, StringComparer.Ordinal)
            .ToList();
        _previous = new Dictionary<string, long>(totals, StringComparer.Ordinal);
        _previousAt = at;
        return rates;
    }

    public void Reset()
    {
        _previous = new Dictionary<string, long>(StringComparer.Ordinal);
        _previousAt = null;
    }
}

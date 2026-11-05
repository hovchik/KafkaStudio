namespace KafkaStudio.Core.Messaging;

/// <summary>A consumer group's total lag at one moment.</summary>
public sealed record LagSample(DateTimeOffset At, long TotalLag);

/// <summary>The most recent lag samples of one group, with a text sparkline and a growing / draining verdict.</summary>
public sealed class LagHistory
{
    private static readonly char[] Bars = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    private readonly List<LagSample> _samples = new();
    private readonly int _capacity;

    public LagHistory(int capacity = 60) => _capacity = Math.Max(2, capacity);

    public IReadOnlyList<LagSample> Samples => _samples;

    public void Add(long totalLag, DateTimeOffset at)
    {
        _samples.Add(new LagSample(at, totalLag));
        if (_samples.Count > _capacity) _samples.RemoveAt(0);
    }

    public void Clear() => _samples.Clear();

    /// <summary>One bar per sample, scaled between the lowest and highest lag seen (a flat history is all low bars).</summary>
    public string Sparkline()
    {
        if (_samples.Count == 0) return "";
        var min = _samples.Min(s => s.TotalLag);
        var max = _samples.Max(s => s.TotalLag);
        var span = max - min;
        return new string(_samples.Select(s =>
            span == 0 ? Bars[0] : Bars[(int)((s.TotalLag - min) * (Bars.Length - 1) / span)]).ToArray());
    }

    /// <summary>"growing", "draining" or "steady" from the first to the last sample; null with fewer than two samples.</summary>
    public string? Trend()
    {
        if (_samples.Count < 2) return null;
        var delta = _samples[^1].TotalLag - _samples[0].TotalLag;
        return delta > 0 ? "growing" : delta < 0 ? "draining" : "steady";
    }

    public string Summary()
    {
        if (_samples.Count == 0) return "";
        var trend = Trend();
        return trend is null
            ? $"lag {_samples[^1].TotalLag:N0}"
            : $"{Sparkline()}  lag {_samples[^1].TotalLag:N0} ({trend} over {_samples.Count} samples)";
    }
}

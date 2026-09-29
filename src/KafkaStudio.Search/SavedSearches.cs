using KafkaStudio.Core.Persistence;

namespace KafkaStudio.Search;

/// <summary>A search with everything that shapes it - query, match options, range and topic scope -
/// saved under a name so it can be re-run with one click.</summary>
public sealed record SavedSearch
{
    public required string Name { get; init; }
    public required string Query { get; init; }
    public TextMatchMode Mode { get; init; } = TextMatchMode.Contains;
    public bool CaseSensitive { get; init; }
    public SearchFields Fields { get; init; } = SearchFields.All;
    public SearchRangeKind RangeKind { get; init; } = SearchRangeKind.All;
    public double? LastMinutes { get; init; }
    public DateTimeOffset? From { get; init; }
    public DateTimeOffset? To { get; init; }
    public int? NewestCount { get; init; }

    /// <summary>Name of the saved topic set to scope to (null = every topic).</summary>
    public string? TopicSetName { get; init; }

    /// <summary>Optional topic-name filter (contains) applied on top of the scope.</summary>
    public string? TopicNameFilter { get; init; }

    public bool ExistenceCheck { get; init; }

    public QueryOptions ToQueryOptions() => new() { Mode = Mode, CaseSensitive = CaseSensitive, Fields = Fields };

    public SearchRange ToRange() => RangeKind switch
    {
        SearchRangeKind.LastDuration => SearchRange.LastPeriod(TimeSpan.FromMinutes(LastMinutes ?? 60)),
        SearchRangeKind.Between => SearchRange.Between(From, To),
        SearchRangeKind.NewestPerPartition => SearchRange.Newest(NewestCount ?? 1000),
        _ => SearchRange.All
    };
}

/// <summary>Persists named <see cref="SavedSearch"/>es (<c>saved-searches.json</c>).</summary>
public static class SavedSearchStore
{
    private const string FileName = "saved-searches.json";

    public static IReadOnlyList<SavedSearch> Load() =>
        JsonFileStore.Load<List<SavedSearch>>(FileName, new List<SavedSearch>())
            .Where(s => !string.IsNullOrWhiteSpace(s.Name) && !string.IsNullOrWhiteSpace(s.Query))
            .ToList();

    /// <summary>Returns an error message on failure, or null on success.</summary>
    public static string? Save(IEnumerable<SavedSearch> searches) => JsonFileStore.TrySave(FileName, searches.ToList());
}

/// <summary>Most-recent-first list of queries that were run (<c>search-history.json</c>).</summary>
public static class SearchHistoryStore
{
    public const int MaxEntries = 30;
    private const string FileName = "search-history.json";

    public static IReadOnlyList<string> Load() =>
        JsonFileStore.Load<List<string>>(FileName, new List<string>())
            .Where(q => !string.IsNullOrWhiteSpace(q))
            .Distinct(StringComparer.Ordinal)
            .Take(MaxEntries)
            .ToList();

    /// <summary>Moves <paramref name="query"/> to the top of <paramref name="history"/> (trimmed to
    /// <see cref="MaxEntries"/>) and returns the new list.</summary>
    public static List<string> Push(IEnumerable<string> history, string query)
    {
        var q = query.Trim();
        var list = history.Where(h => h != q).ToList();
        if (q.Length > 0) list.Insert(0, q);
        if (list.Count > MaxEntries) list.RemoveRange(MaxEntries, list.Count - MaxEntries);
        return list;
    }

    public static string? Save(IEnumerable<string> history) => JsonFileStore.TrySave(FileName, history.Take(MaxEntries).ToList());
}

using KafkaStudio.Core.Persistence;

namespace KafkaStudio.Core.Topics;

/// <summary>
/// Persists saved topic filter terms to a small JSON file under the user's app data folder, so
/// frequently used topic searches survive an app restart.
/// </summary>
public static class SavedTopicFilterStore
{
    private const string FileName = "topic-filters.json";

    public static IReadOnlyList<string> Load() =>
        JsonFileStore.Load<List<string>>(FileName, new List<string>())
            .Where(f => !string.IsNullOrWhiteSpace(f))
            .Distinct(StringComparer.Ordinal)
            .ToList();

    /// <summary>Returns an error message on failure, or null on success.</summary>
    public static string? Save(IEnumerable<string> filters) => JsonFileStore.TrySave(FileName, filters.ToList());
}

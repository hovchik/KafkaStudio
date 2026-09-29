using KafkaStudio.Core.Persistence;

namespace KafkaStudio.Core.Topics;

/// <summary>
/// Persists named <see cref="SavedTopicSet"/>s to a small JSON file under the user's app data
/// folder, so topic sets captured from search results survive an app restart.
/// </summary>
public static class SavedTopicSetStore
{
    private const string FileName = "topic-sets.json";

    public static IReadOnlyList<SavedTopicSet> Load() =>
        JsonFileStore.Load<List<SavedTopicSet>>(FileName, new List<SavedTopicSet>())
            .Where(s => !string.IsNullOrWhiteSpace(s.Name) && s.Topics is not null)
            .ToList();

    /// <summary>Returns an error message on failure, or null on success.</summary>
    public static string? Save(IEnumerable<SavedTopicSet> sets) => JsonFileStore.TrySave(FileName, sets.ToList());
}

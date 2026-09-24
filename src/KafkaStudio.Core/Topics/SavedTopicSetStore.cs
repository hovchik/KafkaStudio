using System.Text.Json;

namespace KafkaStudio.Core.Topics;

/// <summary>
/// Persists named <see cref="SavedTopicSet"/>s to a small JSON file under the user's app data
/// folder, so topic sets captured from search results survive an app restart.
/// </summary>
public static class SavedTopicSetStore
{
    private static readonly JsonSerializerOptions SerializerOptions = new() { WriteIndented = true };

    private static string FilePath => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
        "KafkaStudio",
        "topic-sets.json");

    public static IReadOnlyList<SavedTopicSet> Load()
    {
        try
        {
            if (!File.Exists(FilePath)) return Array.Empty<SavedTopicSet>();
            var json = File.ReadAllText(FilePath);
            return JsonSerializer.Deserialize<List<SavedTopicSet>>(json) ?? new List<SavedTopicSet>();
        }
        catch
        {
            return Array.Empty<SavedTopicSet>();
        }
    }

    public static void Save(IEnumerable<SavedTopicSet> sets)
    {
        var path = FilePath;
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var json = JsonSerializer.Serialize(sets.ToList(), SerializerOptions);
        File.WriteAllText(path, json);
    }
}

using System.Text.Json;

namespace KafkaStudio.Core.Topics;

/// <summary>
/// Persists saved topic filter terms to a small JSON file under the user's app data folder, so
/// frequently used topic searches survive an app restart.
/// </summary>
public static class SavedTopicFilterStore
{
    private static readonly JsonSerializerOptions SerializerOptions = new() { WriteIndented = true };

    private static string FilePath => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
        "KafkaStudio",
        "topic-filters.json");

    public static IReadOnlyList<string> Load()
    {
        try
        {
            if (!File.Exists(FilePath)) return Array.Empty<string>();
            var json = File.ReadAllText(FilePath);
            return JsonSerializer.Deserialize<List<string>>(json) ?? new List<string>();
        }
        catch
        {
            return Array.Empty<string>();
        }
    }

    public static void Save(IEnumerable<string> filters)
    {
        var path = FilePath;
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var json = JsonSerializer.Serialize(filters.ToList(), SerializerOptions);
        File.WriteAllText(path, json);
    }
}

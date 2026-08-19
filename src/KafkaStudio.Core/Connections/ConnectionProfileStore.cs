using System.Text.Json;

namespace KafkaStudio.Core.Connections;

/// <summary>
/// Persists <see cref="ConnectionProfile"/>s to a small JSON file under the user's local app data
/// folder, so connections added on the Connections screen survive an app restart.
/// </summary>
public static class ConnectionProfileStore
{
    private static readonly JsonSerializerOptions SerializerOptions = new() { WriteIndented = true };

    private static string FilePath => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
        "KafkaStudio",
        "connections.json");

    public static IReadOnlyList<ConnectionProfile> Load()
    {
        try
        {
            if (!File.Exists(FilePath)) return Array.Empty<ConnectionProfile>();
            var json = File.ReadAllText(FilePath);
            return JsonSerializer.Deserialize<List<ConnectionProfile>>(json) ?? new List<ConnectionProfile>();
        }
        catch
        {
            // Corrupt or unreadable store: fall back to an empty list rather than crashing startup.
            return Array.Empty<ConnectionProfile>();
        }
    }

    public static void Save(IEnumerable<ConnectionProfile> profiles)
    {
        var path = FilePath;
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var json = JsonSerializer.Serialize(profiles.ToList(), SerializerOptions);
        File.WriteAllText(path, json);
    }
}

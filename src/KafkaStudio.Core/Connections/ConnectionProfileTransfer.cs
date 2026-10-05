using System.Text.Json;
using KafkaStudio.Core.Persistence;

namespace KafkaStudio.Core.Connections;

/// <summary>
/// Reads/writes connection profiles as a portable JSON file, so connections can be shared with a
/// teammate or moved to another machine. Passwords are left out unless explicitly requested, and are
/// always written as plain text (the local DPAPI encryption only works for the current user).
/// </summary>
public static class ConnectionProfileTransfer
{
    public const string FileExtension = ".json";

    public static string Export(IEnumerable<ConnectionProfile> profiles, bool includePasswords) =>
        JsonSerializer.Serialize(
            profiles.Select(p => includePasswords ? p : p with { SaslPassword = null }).ToList(),
            JsonFileStore.SerializerOptions);

    /// <summary>Parses an exported file. Throws <see cref="FormatException"/> when it isn't one.</summary>
    public static IReadOnlyList<ConnectionProfile> Import(string json)
    {
        List<ConnectionProfile>? profiles;
        try
        {
            profiles = JsonSerializer.Deserialize<List<ConnectionProfile>>(json, JsonFileStore.SerializerOptions);
        }
        catch (JsonException ex)
        {
            throw new FormatException($"not a KafkaStudio connections file ({ex.Message})", ex);
        }
        return (profiles ?? new List<ConnectionProfile>())
            .Where(p => p is not null && !string.IsNullOrWhiteSpace(p.Name) && !string.IsNullOrWhiteSpace(p.BootstrapServers))
            .Select(p => p with { Name = p.Name.Trim(), AdvancedProperties = p.AdvancedProperties ?? new Dictionary<string, string>() })
            .ToList();
    }
}

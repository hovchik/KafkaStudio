using System.Text.Json;
using System.Text.Json.Serialization;

namespace KafkaStudio.Core.Persistence;

/// <summary>
/// Shared plumbing for KafkaStudio's small JSON settings files (connections, saved filters, topic
/// sets, rethrow rules, tasks). Centralizes three things every store needs to get right:
/// <list type="bullet">
/// <item>one data directory, overridable (via <see cref="DataDirectory"/> or the
/// <c>KAFKASTUDIO_DATA_DIR</c> environment variable) so tests and portable installs never touch the
/// user's real profile;</item>
/// <item>atomic writes (write to a temp file, then replace), so a crash or full disk mid-save can't
/// leave a truncated file that silently loads as "nothing saved";</item>
/// <item>tolerant reads - a missing or corrupt file loads as the fallback instead of crashing startup
/// (a corrupt file is kept aside as <c>*.corrupt</c> so it isn't overwritten and lost).</item>
/// </list>
/// </summary>
public static class JsonFileStore
{
    private static readonly object Gate = new();

    // Files that exist but could not be read (locked by another process, permission problem). Saving
    // over one of those would replace the user's real data with whatever the fallback was.
    private static readonly HashSet<string> Unreadable = new(StringComparer.OrdinalIgnoreCase);

    public static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() },
        // The stores filter out incomplete entries themselves (a profile without bootstrap servers is
        // skipped). Enforcing C# `required` members at deserialization time instead would reject the
        // *whole* file for one bad entry and quarantine every connection the user has.
        TypeInfoResolver = new System.Text.Json.Serialization.Metadata.DefaultJsonTypeInfoResolver
        {
            Modifiers =
            {
                typeInfo =>
                {
                    foreach (var property in typeInfo.Properties) property.IsRequired = false;
                }
            }
        }
    };

    private static string? _dataDirectory;

    /// <summary>Folder every store file lives in. Defaults to <c>%APPDATA%/KafkaStudio</c>.</summary>
    public static string DataDirectory
    {
        get => _dataDirectory
               ?? Environment.GetEnvironmentVariable("KAFKASTUDIO_DATA_DIR")
               ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "KafkaStudio");
        set => _dataDirectory = value;
    }

    public static string PathFor(string fileName) => Path.Combine(DataDirectory, fileName);

    public static T Load<T>(string fileName, T fallback)
    {
        var path = PathFor(fileName);
        lock (Gate)
        {
            try
            {
                if (!File.Exists(path)) { Unreadable.Remove(path); return fallback; }
                var json = File.ReadAllText(path);
                Unreadable.Remove(path);
                if (string.IsNullOrWhiteSpace(json)) return fallback;
                return JsonSerializer.Deserialize<T>(json, SerializerOptions) ?? fallback;
            }
            catch (JsonException)
            {
                TryQuarantine(path);
                return fallback;
            }
            catch (Exception)
            {
                Unreadable.Add(path);
                return fallback;
            }
        }
    }

    /// <summary>Serializes and atomically writes <paramref name="value"/>. Throws on I/O failure so the
    /// caller can surface it; use <see cref="TrySave{T}"/> when a failure should just be reported.</summary>
    public static void Save<T>(string fileName, T value)
    {
        var path = PathFor(fileName);
        var json = JsonSerializer.Serialize(value, SerializerOptions);
        lock (Gate)
        {
            if (Unreadable.Contains(path))
            {
                // Whatever is in memory came from the fallback, not from the file: saving would replace
                // the user's real data with it. Nothing short of a reload can make that safe.
                throw new IOException(
                    $"{fileName} could not be read when it was loaded, so saving would overwrite its contents. " +
                    "Fix the file's permissions or close the program holding it, then restart KafkaStudio.");
            }

            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            var temp = path + ".tmp";
            File.WriteAllText(temp, json);
            File.Move(temp, path, overwrite: true);
        }
    }

    /// <summary>Like <see cref="Save{T}"/> but returns the error message instead of throwing.</summary>
    public static string? TrySave<T>(string fileName, T value)
    {
        try
        {
            Save(fileName, value);
            return null;
        }
        catch (Exception ex)
        {
            return $"could not save {fileName}: {ex.Message}";
        }
    }

    private static void TryQuarantine(string path)
    {
        try { File.Copy(path, path + ".corrupt", overwrite: true); }
        catch { /* best effort */ }
    }
}

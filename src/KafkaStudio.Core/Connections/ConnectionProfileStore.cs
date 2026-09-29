using KafkaStudio.Core.Persistence;

namespace KafkaStudio.Core.Connections;

/// <summary>
/// Encrypts/decrypts secrets (SASL passwords) before they are written to disk. The hosting app plugs
/// in a platform implementation (Windows DPAPI in KafkaStudio.App); without one, secrets are stored
/// as-is, exactly like before.
/// </summary>
public interface ISecretProtector
{
    /// <summary>Short scheme tag written in front of protected values, e.g. "dpapi".</summary>
    string Scheme { get; }

    string Protect(string plaintext);

    string Unprotect(string protectedValue);
}

/// <summary>
/// Persists <see cref="ConnectionProfile"/>s to a small JSON file under the user's local app data
/// folder, so connections added on the Connections screen survive an app restart.
/// </summary>
public static class ConnectionProfileStore
{
    private const string FileName = "connections.json";

    /// <summary>Optional secret protector; see <see cref="ISecretProtector"/>.</summary>
    public static ISecretProtector? Protector { get; set; }

    public static IReadOnlyList<ConnectionProfile> Load() =>
        JsonFileStore.Load<List<ConnectionProfile>>(FileName, new List<ConnectionProfile>())
            .Where(p => !string.IsNullOrWhiteSpace(p.Name) && p.BootstrapServers is not null)
            .Select(p => p with { SaslPassword = Reveal(p.SaslPassword) })
            .ToList();

    /// <summary>Returns an error message on failure, or null on success.</summary>
    public static string? Save(IEnumerable<ConnectionProfile> profiles) =>
        JsonFileStore.TrySave(FileName, profiles.Select(p => p with { SaslPassword = Hide(p.SaslPassword) }).ToList());

    private static string? Hide(string? secret)
    {
        if (string.IsNullOrEmpty(secret) || Protector is not { } protector) return secret;
        try
        {
            return $"{protector.Scheme}:{protector.Protect(secret)}";
        }
        catch
        {
            return secret;
        }
    }

    private static string? Reveal(string? stored)
    {
        if (string.IsNullOrEmpty(stored) || Protector is not { } protector) return stored;
        var prefix = protector.Scheme + ":";
        if (!stored.StartsWith(prefix, StringComparison.Ordinal)) return stored; // legacy plaintext value
        try
        {
            return protector.Unprotect(stored[prefix.Length..]);
        }
        catch
        {
            // Encrypted for another user/machine: better to ask for the password again than to send
            // ciphertext to the broker as if it were the password.
            return null;
        }
    }
}

using KafkaStudio.Automation.History;
using KafkaStudio.Automation.Rethrow;
using KafkaStudio.Automation.Scheduling;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Connections;
using KafkaStudio.Core.Testing;

namespace KafkaStudio.App.ViewModels.Shared;

/// <summary>
/// The app's composition root / shared service bag: every named connection's live gateway, the
/// automation scheduler, the rethrow manager, and run history. Every top-level ViewModel is
/// constructed with a reference to the same <see cref="AppState"/> instance, so (for example) a
/// connection added on the Connections screen is immediately visible to the Script Editor's "use
/// connection" autocomplete and the Rethrow Rules screen.
/// </summary>
public sealed class AppState : IAsyncDisposable
{
    /// <summary>
    /// Builds the real <see cref="IKafkaGateway"/> for a connection profile. Injected rather than
    /// referenced directly so this ViewModels project - and everything below it - never has to depend
    /// on KafkaStudio.Kafka (and therefore never has to depend on the Confluent.Kafka NuGet package):
    /// only the top-level Avalonia App project needs to know a concrete gateway type exists. The
    /// default here throws, so a caller that forgets to wire up the real factory fails loudly instead
    /// of silently no-op'ing.
    /// </summary>
    public Func<ConnectionProfile, IKafkaGateway> RealGatewayFactory { get; set; } =
        _ => throw new InvalidOperationException(
            "No Kafka gateway factory was configured. The hosting app must set AppState.RealGatewayFactory " +
            "(KafkaStudio.App does this at startup with 'profile => new ConfluentKafkaGateway(profile)').");

    public Dictionary<string, ConnectionProfile> ConnectionProfiles { get; } = new();
    public Dictionary<string, IKafkaGateway> Connections { get; } = new();

    public AutomationScheduler Scheduler { get; } = new();
    public RethrowManager RethrowManager { get; } = new();
    public RunHistoryStore RunHistory { get; } = new();

    /// <summary>Shared broker used when a connection profile is added in "offline / demo" mode
    /// (no real cluster configured yet) - lets every screen be exercised without Kafka installed.</summary>
    public InMemoryKafkaBroker DemoBroker { get; } = new();

    public event Action? ConnectionsChanged;

    public void AddDemoConnection(string name)
    {
        var profile = new ConnectionProfile { Name = name, BootstrapServers = "demo (in-memory)" };
        ConnectionProfiles[name] = profile;
        Connections[name] = new InMemoryKafkaGateway(profile, DemoBroker);
        ConnectionsChanged?.Invoke();
    }

    public void AddConnection(ConnectionProfile profile, IKafkaGateway gateway)
    {
        ConnectionProfiles[profile.Name] = profile;
        Connections[profile.Name] = gateway;
        ConnectionsChanged?.Invoke();
    }

    public async Task RemoveConnectionAsync(string name)
    {
        if (Connections.Remove(name, out var gateway))
        {
            await gateway.DisposeAsync().ConfigureAwait(false);
        }
        ConnectionProfiles.Remove(name);
        ConnectionsChanged?.Invoke();
    }

    public async ValueTask DisposeAsync()
    {
        await Scheduler.DisposeAsync().ConfigureAwait(false);
        await RethrowManager.DisposeAsync().ConfigureAwait(false);
        foreach (var gateway in Connections.Values)
        {
            await gateway.DisposeAsync().ConfigureAwait(false);
        }
    }
}

using System.Collections.Concurrent;
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

    /// <summary>
    /// Runs an action on the UI thread. Scheduler runs, rethrow relays and live consumers raise their
    /// events on background threads, and UI frameworks (Avalonia included) throw when bound
    /// properties/collections change off the UI thread - so every such handler goes through this. The
    /// App sets it to the Avalonia dispatcher; the default runs inline (tests, headless use).
    /// </summary>
    public Action<Action> PostToUi { get; set; } = action => action();

    /// <summary>Optional file open/save dialogs, provided by the hosting app (null in tests).</summary>
    public IFileDialogService? FileDialogs { get; set; }

    /// <summary>Optional clipboard access, provided by the hosting app (null in tests).</summary>
    public Func<string, Task>? SetClipboardText { get; set; }

    public Dictionary<string, ConnectionProfile> ConnectionProfiles { get; } = new();

    /// <summary>Live gateways by connection name. Concurrent because scheduled tasks and rethrow rules
    /// read it from background threads while the UI adds/removes connections.</summary>
    public ConcurrentDictionary<string, IKafkaGateway> Connections { get; } = new();

    /// <summary>Why a persisted connection couldn't be reconnected at startup, by name.</summary>
    public ConcurrentDictionary<string, string> ConnectionErrors { get; } = new();

    public AutomationScheduler Scheduler { get; } = new();
    public RethrowManager RethrowManager { get; } = new();
    public RunHistoryStore RunHistory { get; } = new();

    /// <summary>Shared broker used when a connection profile is added in "offline / demo" mode
    /// (no real cluster configured yet) - lets every screen be exercised without Kafka installed.</summary>
    public InMemoryKafkaBroker DemoBroker { get; } = new();

    /// <summary>Raised (on the UI thread) whenever a connection is added, removed or reconnected.</summary>
    public event Action? ConnectionsChanged;

    /// <summary>Raised when something goes wrong that no screen-specific status line covers (e.g. a
    /// settings file couldn't be written). The main window shows it as a notification.</summary>
    public event Action<string>? Notification;

    public void Notify(string message) => PostToUi(() => Notification?.Invoke(message));

    /// <summary>Raised when a screen asks to open a message in the Producer (to resend or edit it).</summary>
    public event Action<string?, Core.Messaging.KafkaMessage>? EditInProducerRequested;

    /// <summary>Opens <paramref name="message"/> in the Producer screen, pre-filled, on <paramref name="connection"/>.</summary>
    public void RequestEditInProducer(string? connection, Core.Messaging.KafkaMessage message) =>
        EditInProducerRequested?.Invoke(connection, message);

    /// <summary>Raised when a screen asks the Find Data screen to look for messages similar to one.</summary>
    public event Action<string?, Core.Messaging.KafkaMessage>? FindSimilarRequested;

    public void RequestFindSimilar(string? connection, Core.Messaging.KafkaMessage message) =>
        FindSimilarRequested?.Invoke(connection, message);

    /// <summary>Raised when a screen asks the Find Data screen to trace an id across topics.</summary>
    public event Action<string?, string>? TraceRequested;

    public void RequestTrace(string? connection, string id) => TraceRequested?.Invoke(connection, id);

    /// <summary>Raised when generated KafScript should be opened in the Script Editor.</summary>
    public event Action<string>? OpenScriptRequested;

    public void RequestOpenScript(string source) => OpenScriptRequested?.Invoke(source);

    /// <summary>Raised when the saved topic sets were changed by one screen, so others reload them.</summary>
    public event Action? SavedTopicSetsChanged;

    public void RaiseSavedTopicSetsChanged() => PostToUi(() => SavedTopicSetsChanged?.Invoke());

    private void RaiseConnectionsChanged() => PostToUi(() => ConnectionsChanged?.Invoke());

    private void PersistProfiles()
    {
        if (ConnectionProfileStore.Save(ConnectionProfiles.Values) is { } error) Notify(error);
    }

    public void AddDemoConnection(string name)
    {
        var profile = new ConnectionProfile
        {
            Name = name,
            BootstrapServers = ConnectionProfile.DemoBootstrapServers,
            IsDemo = true
        };
        ReplaceGateway(name, new InMemoryKafkaGateway(profile, DemoBroker));
        ConnectionProfiles[name] = profile;
        ConnectionErrors.TryRemove(name, out _);
        PersistProfiles();
        RaiseConnectionsChanged();
    }

    /// <summary>Adds (or replaces) a connection. A gateway previously registered under the same name
    /// is disposed rather than leaked.</summary>
    public void AddConnection(ConnectionProfile profile, IKafkaGateway gateway)
    {
        ReplaceGateway(profile.Name, gateway);
        ConnectionProfiles[profile.Name] = profile;
        ConnectionErrors.TryRemove(profile.Name, out _);
        PersistProfiles();
        RaiseConnectionsChanged();
    }

    private void ReplaceGateway(string name, IKafkaGateway gateway)
    {
        if (Connections.TryGetValue(name, out var old) && !ReferenceEquals(old, gateway))
        {
            _ = DisposeQuietlyAsync(old);
        }
        Connections[name] = gateway;
    }

    public async Task RemoveConnectionAsync(string name)
    {
        ConnectionProfiles.Remove(name);
        ConnectionErrors.TryRemove(name, out _);
        PersistProfiles();
        if (Connections.TryRemove(name, out var gateway))
        {
            await DisposeQuietlyAsync(gateway).ConfigureAwait(false);
        }
        RaiseConnectionsChanged();
    }

    /// <summary>(Re)creates the live gateway for a known profile - used by "Reconnect" and at startup.</summary>
    public async Task ReconnectAsync(string name)
    {
        if (!ConnectionProfiles.TryGetValue(name, out var profile)) return;
        try
        {
            IKafkaGateway gateway;
            if (profile.IsDemoConnection)
            {
                gateway = new InMemoryKafkaGateway(profile, DemoBroker);
            }
            else
            {
                gateway = RealGatewayFactory(profile);
                await gateway.ConnectAsync().ConfigureAwait(false);
            }
            ReplaceGateway(name, gateway);
            ConnectionErrors.TryRemove(name, out _);
        }
        catch (Exception ex)
        {
            ConnectionErrors[name] = ex.Message;
        }
        RaiseConnectionsChanged();
    }

    /// <summary>
    /// Loads any connections that were persisted by a previous session and reconnects each one
    /// (or recreates the in-memory demo gateway). Called once at app startup. Connections that fail
    /// to reconnect (e.g. broker unreachable) are still listed, without a live gateway, so the user
    /// can inspect / retry / remove them via the Connections screen.
    /// </summary>
    public async Task LoadPersistedConnectionsAsync()
    {
        foreach (var profile in ConnectionProfileStore.Load())
        {
            ConnectionProfiles[profile.Name] = profile;
        }

        foreach (var name in ConnectionProfiles.Keys.ToList())
        {
            await ReconnectAsync(name).ConfigureAwait(false);
        }

        RaiseConnectionsChanged();
    }

    private static async Task DisposeQuietlyAsync(IKafkaGateway gateway)
    {
        try { await gateway.DisposeAsync().ConfigureAwait(false); }
        catch { /* best effort */ }
    }

    public async ValueTask DisposeAsync()
    {
        try { await Scheduler.DisposeAsync().ConfigureAwait(false); } catch { /* best effort */ }
        try { await RethrowManager.DisposeAsync().ConfigureAwait(false); } catch { /* best effort */ }
        foreach (var gateway in Connections.Values)
        {
            await DisposeQuietlyAsync(gateway).ConfigureAwait(false);
        }
        Connections.Clear();
    }
}

/// <summary>File dialogs, implemented by the hosting UI (Avalonia's StorageProvider in KafkaStudio.App).</summary>
public interface IFileDialogService
{
    /// <summary>Returns the chosen file's path, or null if cancelled.</summary>
    Task<string?> PickOpenFileAsync(string title, string extension, string filterName);

    /// <summary>Returns the chosen file's path, or null if cancelled.</summary>
    Task<string?> PickSaveFileAsync(string title, string extension, string filterName, string suggestedName);
}

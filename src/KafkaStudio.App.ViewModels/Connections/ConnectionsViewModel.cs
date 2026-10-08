using System.Collections.ObjectModel;
using System.Globalization;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Connections;

namespace KafkaStudio.App.ViewModels.Connections;

public sealed class ConnectionRowViewModel : ObservableObject
{
    public required string Name { get; init; }
    public required string BootstrapServers { get; init; }
    public required bool IsDemo { get; init; }
    public required ConnectionProfile Profile { get; init; }

    private string _status = "Not tested";
    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }

    private bool _isConnected;
    /// <summary>True when a live gateway exists for this connection.</summary>
    public bool IsConnected { get => _isConnected; set => SetProperty(ref _isConnected, value); }

    private bool _hasError;
    public bool HasError { get => _hasError; set => SetProperty(ref _hasError, value); }

    public string Kind => IsDemo ? "demo" : Profile.SecurityProtocol.ToString();
}

/// <summary>Manage named Kafka connections: add a real cluster (host, security, SASL) or an in-memory
/// demo cluster for trying out the app / DSL without a broker. Existing connections can be tested,
/// reconnected, edited (loads them back into the form) and removed.</summary>
public sealed class ConnectionsViewModel : ObservableObject
{
    private readonly AppState _state;

    public ObservableCollection<ConnectionRowViewModel> Connections { get; } = new();

    /// <summary>Bound to the "Security" ComboBox's ItemsSource - AXAML has no clean built-in way to
    /// enumerate an enum's values, so the ViewModel exposes them directly.</summary>
    public IReadOnlyList<SecurityProtocolKind> SecurityProtocolOptions { get; } = Enum.GetValues<SecurityProtocolKind>();

    /// <summary>Bound to the "SASL mechanism" ComboBox's ItemsSource - same reasoning as
    /// <see cref="SecurityProtocolOptions"/>.</summary>
    public IReadOnlyList<SaslMechanismKind> SaslMechanismOptions { get; } = Enum.GetValues<SaslMechanismKind>();

    private string _newConnectionName = "";
    public string NewConnectionName
    {
        get => _newConnectionName;
        set
        {
            if (SetProperty(ref _newConnectionName, value))
            {
                OnPropertyChanged(nameof(IsEditingExisting));
                RaiseFormCommands();
            }
        }
    }

    private string _newBootstrapServers = "localhost:9092";
    public string NewBootstrapServers
    {
        get => _newBootstrapServers;
        set { if (SetProperty(ref _newBootstrapServers, value)) RaiseFormCommands(); }
    }

    private SecurityProtocolKind _newSecurityProtocol = SecurityProtocolKind.Plaintext;
    public SecurityProtocolKind NewSecurityProtocol
    {
        get => _newSecurityProtocol;
        set
        {
            if (SetProperty(ref _newSecurityProtocol, value))
            {
                OnPropertyChanged(nameof(UsesSasl));
                OnPropertyChanged(nameof(UsesSsl));
                // SASL_* protocols need a mechanism; pick the most common one instead of failing later.
                if (UsesSasl && NewSaslMechanism == SaslMechanismKind.None) NewSaslMechanism = SaslMechanismKind.Plain;
                if (!UsesSasl) NewSaslMechanism = SaslMechanismKind.None;
            }
        }
    }

    public bool UsesSasl => NewSecurityProtocol is SecurityProtocolKind.SaslPlaintext or SecurityProtocolKind.SaslSsl;
    public bool UsesSsl => NewSecurityProtocol is SecurityProtocolKind.Ssl or SecurityProtocolKind.SaslSsl;

    private SaslMechanismKind _newSaslMechanism = SaslMechanismKind.None;
    public SaslMechanismKind NewSaslMechanism { get => _newSaslMechanism; set => SetProperty(ref _newSaslMechanism, value); }

    private string? _newSaslUsername;
    public string? NewSaslUsername { get => _newSaslUsername; set => SetProperty(ref _newSaslUsername, value); }

    private string? _newSaslPassword;
    public string? NewSaslPassword { get => _newSaslPassword; set => SetProperty(ref _newSaslPassword, value); }

    private string? _newSslCaLocation;
    /// <summary>CA certificate file for clusters signed by a private CA.</summary>
    public string? NewSslCaLocation { get => _newSslCaLocation; set => SetProperty(ref _newSslCaLocation, value); }

    private bool _newSslSkipVerification;
    /// <summary>Disables broker certificate verification - for dev clusters with self-signed certs only.</summary>
    public bool NewSslSkipVerification { get => _newSslSkipVerification; set => SetProperty(ref _newSslSkipVerification, value); }

    private string _newClientId = "kafka-studio";
    public string NewClientId { get => _newClientId; set => SetProperty(ref _newClientId, value); }

    private string? _newAdvancedProperties;
    /// <summary>Extra librdkafka settings, one "key=value" per line.</summary>
    public string? NewAdvancedProperties { get => _newAdvancedProperties; set => SetProperty(ref _newAdvancedProperties, value); }

    /// <summary>True when the form's name matches an existing connection - "Save" will replace it.</summary>
    public bool IsEditingExisting => _state.ConnectionProfiles.ContainsKey(NewConnectionName.Trim());

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    private bool _isBusy;
    public bool IsBusy { get => _isBusy; private set => SetProperty(ref _isBusy, value); }

    public AsyncRelayCommand AddConnectionCommand { get; }
    public AsyncRelayCommand AddDemoConnectionCommand { get; }
    public AsyncRelayCommand<ConnectionRowViewModel> RemoveConnectionCommand { get; }
    public AsyncRelayCommand<ConnectionRowViewModel> TestConnectionCommand { get; }
    public AsyncRelayCommand<ConnectionRowViewModel> ReconnectCommand { get; }
    public RelayCommand<ConnectionRowViewModel> EditConnectionCommand { get; }
    public RelayCommand ResetFormCommand { get; }
    public AsyncRelayCommand ExportConnectionsCommand { get; }
    public AsyncRelayCommand ImportConnectionsCommand { get; }

    private bool _exportIncludesPasswords;
    /// <summary>When set, SASL passwords are written to the export file in plain text.</summary>
    public bool ExportIncludesPasswords { get => _exportIncludesPasswords; set => SetProperty(ref _exportIncludesPasswords, value); }

    public ConnectionsViewModel(AppState state)
    {
        _state = state;
        _state.ConnectionsChanged += RefreshFromState;

        AddConnectionCommand = new AsyncRelayCommand(AddConnectionAsync,
            () => !string.IsNullOrWhiteSpace(NewConnectionName) && !string.IsNullOrWhiteSpace(NewBootstrapServers));
        AddDemoConnectionCommand = new AsyncRelayCommand(AddDemoConnectionAsync, () => !string.IsNullOrWhiteSpace(NewConnectionName));
        RemoveConnectionCommand = new AsyncRelayCommand<ConnectionRowViewModel>(RemoveConnectionAsync, allowConcurrentExecutions: true);
        TestConnectionCommand = new AsyncRelayCommand<ConnectionRowViewModel>(TestConnectionAsync, allowConcurrentExecutions: true);
        ReconnectCommand = new AsyncRelayCommand<ConnectionRowViewModel>(ReconnectAsync, allowConcurrentExecutions: true);
        EditConnectionCommand = new RelayCommand<ConnectionRowViewModel>(LoadIntoForm);
        ResetFormCommand = new RelayCommand(ResetForm);
        ExportConnectionsCommand = new AsyncRelayCommand(ExportConnectionsAsync, () => Connections.Count > 0);
        ImportConnectionsCommand = new AsyncRelayCommand(ImportConnectionsAsync);

        RefreshFromState();
    }

    private void RaiseFormCommands()
    {
        AddConnectionCommand.RaiseCanExecuteChanged();
        AddDemoConnectionCommand.RaiseCanExecuteChanged();
    }

    private ConnectionProfile? BuildProfileFromForm(out string? error)
    {
        error = null;
        var name = NewConnectionName.Trim();
        var servers = NewBootstrapServers.Trim().Replace(" ", string.Empty);

        foreach (var server in servers.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            var colon = server.LastIndexOf(':');
            if (colon <= 0 || !int.TryParse(server[(colon + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var port) || port is <= 0 or > 65535)
            {
                error = $"'{server}' isn't a valid host:port";
                return null;
            }
        }

        if (UsesSasl && NewSaslMechanism is SaslMechanismKind.Plain or SaslMechanismKind.ScramSha256 or SaslMechanismKind.ScramSha512
            && string.IsNullOrWhiteSpace(NewSaslUsername))
        {
            error = $"{NewSaslMechanism} needs a username";
            return null;
        }

        var advanced = new Dictionary<string, string>();
        foreach (var rawLine in (NewAdvancedProperties ?? string.Empty).Split('\n'))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 || line.StartsWith('#')) continue;
            var eq = line.IndexOf('=');
            if (eq <= 0)
            {
                error = $"advanced property '{line}' must look like key=value";
                return null;
            }
            advanced[line[..eq].Trim()] = line[(eq + 1)..].Trim();
        }

        return new ConnectionProfile
        {
            Name = name,
            BootstrapServers = servers,
            SecurityProtocol = NewSecurityProtocol,
            SaslMechanism = UsesSasl ? NewSaslMechanism : SaslMechanismKind.None,
            SaslUsername = UsesSasl ? NewSaslUsername?.Trim() : null,
            SaslPassword = UsesSasl ? NewSaslPassword : null,
            SslCaLocation = UsesSsl && !string.IsNullOrWhiteSpace(NewSslCaLocation) ? NewSslCaLocation.Trim() : null,
            SslEnableVerification = !NewSslSkipVerification,
            ClientId = string.IsNullOrWhiteSpace(NewClientId) ? "kafka-studio" : NewClientId.Trim(),
            AdvancedProperties = advanced
        };
    }

    private async Task AddConnectionAsync()
    {
        var profile = BuildProfileFromForm(out var error);
        if (profile is null)
        {
            StatusMessage = error;
            return;
        }

        IsBusy = true;
        StatusMessage = $"Connecting to '{profile.Name}'...";
        try
        {
            var gateway = _state.RealGatewayFactory(profile);
            try
            {
                await gateway.ConnectAsync().ConfigureAwait(true);
                // Building the clients doesn't talk to the broker yet - list topics to prove the
                // settings actually work before saving them.
                var topics = await gateway.ListTopicsAsync().ConfigureAwait(true);
                var replaced = IsEditingExisting;
                _state.AddConnection(profile, gateway);
                StatusMessage = $"{(replaced ? "Updated" : "Connected to")} '{profile.Name}' - {topics.Count} topic(s) visible.";
                ResetForm();
            }
            catch
            {
                await gateway.DisposeAsync().ConfigureAwait(true);
                throw;
            }
        }
        catch (Exception ex)
        {
            StatusMessage = $"Failed to connect to '{profile.Name}': {ex.Message}";
        }
        finally
        {
            IsBusy = false;
        }
    }

    private Task AddDemoConnectionAsync()
    {
        var name = NewConnectionName.Trim();
        if (_state.ConnectionProfiles.TryGetValue(name, out var existing) && !existing.IsDemoConnection)
        {
            StatusMessage = $"'{name}' is already a real connection - pick another name for the demo.";
            return Task.CompletedTask;
        }
        _state.AddDemoConnection(name);
        StatusMessage = $"Added demo (in-memory) connection '{name}'.";
        ResetForm();
        return Task.CompletedTask;
    }

    private async Task RemoveConnectionAsync(ConnectionRowViewModel? row)
    {
        if (row is null) return;
        await _state.RemoveConnectionAsync(row.Name).ConfigureAwait(true);
        StatusMessage = $"Removed '{row.Name}'.";
    }

    private async Task ReconnectAsync(ConnectionRowViewModel? row)
    {
        if (row is null) return;
        row.Status = "Reconnecting...";
        await _state.ReconnectAsync(row.Name).ConfigureAwait(true);
    }

    private async Task TestConnectionAsync(ConnectionRowViewModel? row)
    {
        if (row is null) return;
        if (!_state.Connections.TryGetValue(row.Name, out var gateway))
        {
            row.Status = "Not connected - use Reconnect";
            row.HasError = true;
            return;
        }
        row.Status = "Testing...";
        try
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();
            var topics = await gateway.ListTopicsAsync().ConfigureAwait(true);
            row.Status = $"OK - {topics.Count} topic(s), {watch.ElapsedMilliseconds} ms";
            row.HasError = false;
        }
        catch (Exception ex)
        {
            row.Status = $"Error: {ex.Message}";
            row.HasError = true;
        }
    }

    private async Task ExportConnectionsAsync()
    {
        if (_state.FileDialogs is null) return;
        var path = await _state.FileDialogs.PickSaveFileAsync("Export connections", ConnectionProfileTransfer.FileExtension,
            "KafkaStudio connections", "kafkastudio-connections.json").ConfigureAwait(true);
        if (path is null) return;
        try
        {
            var profiles = _state.ConnectionProfiles.Values.OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase).ToList();
            await File.WriteAllTextAsync(path, ConnectionProfileTransfer.Export(profiles, ExportIncludesPasswords)).ConfigureAwait(true);
            StatusMessage = $"Exported {profiles.Count} connection(s) to {Path.GetFileName(path)}"
                            + (ExportIncludesPasswords ? " (passwords included in plain text)." : " (without passwords).");
        }
        catch (Exception ex)
        {
            StatusMessage = $"Export failed: {ex.Message}";
        }
    }

    private async Task ImportConnectionsAsync()
    {
        if (_state.FileDialogs is null) return;
        var path = await _state.FileDialogs.PickOpenFileAsync("Import connections", ConnectionProfileTransfer.FileExtension,
            "KafkaStudio connections").ConfigureAwait(true);
        if (path is null) return;
        await ImportConnectionsFromFileAsync(path).ConfigureAwait(true);
    }

    /// <summary>Imports connections from an exported file; same-named connections are replaced.</summary>
    public async Task ImportConnectionsFromFileAsync(string path)
    {
        IsBusy = true;
        try
        {
            var profiles = ConnectionProfileTransfer.Import(await File.ReadAllTextAsync(path).ConfigureAwait(true));
            if (profiles.Count == 0)
            {
                StatusMessage = $"No connections found in {Path.GetFileName(path)}.";
                return;
            }
            var replaced = profiles.Count(p => _state.ConnectionProfiles.ContainsKey(p.Name));
            StatusMessage = $"Importing {profiles.Count} connection(s)...";
            await _state.ImportConnectionsAsync(profiles).ConfigureAwait(true);
            StatusMessage = $"Imported {profiles.Count} connection(s)" + (replaced > 0 ? $", {replaced} replaced." : ".");
        }
        catch (Exception ex)
        {
            StatusMessage = $"Import failed: {ex.Message}";
        }
        finally
        {
            IsBusy = false;
        }
    }

    private void LoadIntoForm(ConnectionRowViewModel? row)
    {
        if (row is null) return;
        var p = row.Profile;
        NewConnectionName = p.Name;
        NewBootstrapServers = p.IsDemoConnection ? "" : p.BootstrapServers;
        NewSecurityProtocol = p.SecurityProtocol;
        NewSaslMechanism = p.SaslMechanism;
        NewSaslUsername = p.SaslUsername;
        NewSaslPassword = p.SaslPassword;
        NewSslCaLocation = p.SslCaLocation;
        NewSslSkipVerification = !p.SslEnableVerification;
        NewClientId = p.ClientId;
        NewAdvancedProperties = string.Join("\n", p.AdvancedProperties.Select(kv => $"{kv.Key}={kv.Value}"));
        StatusMessage = $"Editing '{p.Name}' - change settings and press Save & connect.";
    }

    private void ResetForm()
    {
        NewConnectionName = "";
        NewBootstrapServers = "localhost:9092";
        NewSecurityProtocol = SecurityProtocolKind.Plaintext;
        NewSaslMechanism = SaslMechanismKind.None;
        NewSaslUsername = null;
        NewSaslPassword = null;
        NewSslCaLocation = null;
        NewSslSkipVerification = false;
        NewClientId = "kafka-studio";
        NewAdvancedProperties = null;
    }

    private void RefreshFromState()
    {
        var previousStatus = Connections.ToDictionary(c => c.Name, c => c.Status);
        Connections.Clear();
        foreach (var (name, profile) in _state.ConnectionProfiles.OrderBy(p => p.Key, StringComparer.OrdinalIgnoreCase))
        {
            var connected = _state.Connections.ContainsKey(name);
            var error = _state.ConnectionErrors.TryGetValue(name, out var e) ? e : null;
            Connections.Add(new ConnectionRowViewModel
            {
                Name = name,
                BootstrapServers = profile.BootstrapServers,
                IsDemo = profile.IsDemoConnection,
                Profile = profile,
                IsConnected = connected,
                HasError = error is not null,
                Status = error is not null
                    ? $"Could not connect: {error}"
                    : previousStatus.TryGetValue(name, out var s) && !s.StartsWith("Could not", StringComparison.Ordinal) && !s.StartsWith("Reconnecting", StringComparison.Ordinal)
                        ? s
                        : connected ? "Ready" : "Not connected"
            });
        }
        OnPropertyChanged(nameof(IsEditingExisting));
        ExportConnectionsCommand.RaiseCanExecuteChanged();
    }
}

using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Connections;

namespace KafkaStudio.App.ViewModels.Connections;

public sealed class ConnectionRowViewModel : ObservableObject
{
    public required string Name { get; init; }
    public required string BootstrapServers { get; init; }
    public required bool IsDemo { get; init; }

    private string _status = "Not connected";
    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }
}

/// <summary>Manage named Kafka connections: add a real cluster (host, security, SASL) or an in-memory
/// demo cluster for trying out the app / DSL without a broker.</summary>
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
    public string NewConnectionName { get => _newConnectionName; set => SetProperty(ref _newConnectionName, value); }

    private string _newBootstrapServers = "localhost:9092";
    public string NewBootstrapServers { get => _newBootstrapServers; set => SetProperty(ref _newBootstrapServers, value); }

    private SecurityProtocolKind _newSecurityProtocol = SecurityProtocolKind.Plaintext;
    public SecurityProtocolKind NewSecurityProtocol { get => _newSecurityProtocol; set => SetProperty(ref _newSecurityProtocol, value); }

    private SaslMechanismKind _newSaslMechanism = SaslMechanismKind.None;
    public SaslMechanismKind NewSaslMechanism { get => _newSaslMechanism; set => SetProperty(ref _newSaslMechanism, value); }

    private string? _newSaslUsername;
    public string? NewSaslUsername { get => _newSaslUsername; set => SetProperty(ref _newSaslUsername, value); }

    private string? _newSaslPassword;
    public string? NewSaslPassword { get => _newSaslPassword; set => SetProperty(ref _newSaslPassword, value); }

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    public AsyncRelayCommand AddConnectionCommand { get; }
    public AsyncRelayCommand AddDemoConnectionCommand { get; }
    public AsyncRelayCommand<ConnectionRowViewModel> RemoveConnectionCommand { get; }
    public AsyncRelayCommand<ConnectionRowViewModel> TestConnectionCommand { get; }

    public ConnectionsViewModel(AppState state)
    {
        _state = state;
        _state.ConnectionsChanged += RefreshFromState;

        AddConnectionCommand = new AsyncRelayCommand(AddConnectionAsync, () => !string.IsNullOrWhiteSpace(NewConnectionName));
        AddDemoConnectionCommand = new AsyncRelayCommand(AddDemoConnectionAsync, () => !string.IsNullOrWhiteSpace(NewConnectionName));
        RemoveConnectionCommand = new AsyncRelayCommand<ConnectionRowViewModel>(RemoveConnectionAsync);
        TestConnectionCommand = new AsyncRelayCommand<ConnectionRowViewModel>(TestConnectionAsync);

        // The Connect / Add demo buttons are only enabled once a name is entered - re-evaluate
        // their CanExecute whenever it changes, otherwise the buttons stay disabled forever since
        // ICommand.CanExecute is only re-checked when CanExecuteChanged fires.
        PropertyChanged += (_, e) =>
        {
            if (e.PropertyName == nameof(NewConnectionName))
            {
                AddConnectionCommand.RaiseCanExecuteChanged();
                AddDemoConnectionCommand.RaiseCanExecuteChanged();
            }
        };

        RefreshFromState();
    }

    private async Task AddConnectionAsync()
    {
        var profile = new ConnectionProfile
        {
            Name = NewConnectionName.Trim(),
            BootstrapServers = NewBootstrapServers.Trim(),
            SecurityProtocol = NewSecurityProtocol,
            SaslMechanism = NewSaslMechanism,
            SaslUsername = NewSaslUsername,
            SaslPassword = NewSaslPassword
        };

        var gateway = _state.RealGatewayFactory(profile);
        try
        {
            await gateway.ConnectAsync().ConfigureAwait(true);
            _state.AddConnection(profile, gateway);
            StatusMessage = $"Connected to '{profile.Name}'.";
            ResetForm();
        }
        catch (Exception ex)
        {
            StatusMessage = $"Failed to connect to '{profile.Name}': {ex.Message}";
        }
    }

    private Task AddDemoConnectionAsync()
    {
        _state.AddDemoConnection(NewConnectionName.Trim());
        StatusMessage = $"Added demo (in-memory) connection '{NewConnectionName.Trim()}'.";
        ResetForm();
        return Task.CompletedTask;
    }

    private async Task RemoveConnectionAsync(ConnectionRowViewModel? row)
    {
        if (row is null) return;
        await _state.RemoveConnectionAsync(row.Name).ConfigureAwait(true);
    }

    private async Task TestConnectionAsync(ConnectionRowViewModel? row)
    {
        if (row is null || !_state.Connections.TryGetValue(row.Name, out var gateway)) return;
        row.Status = "Testing...";
        try
        {
            var topics = await gateway.ListTopicsAsync().ConfigureAwait(true);
            row.Status = $"OK - {topics.Count} topic(s) visible";
        }
        catch (Exception ex)
        {
            row.Status = $"Error: {ex.Message}";
        }
    }

    private void ResetForm()
    {
        NewConnectionName = "";
        NewSaslMechanism = SaslMechanismKind.None;
        NewSaslUsername = null;
        NewSaslPassword = null;
    }

    private void RefreshFromState()
    {
        Connections.Clear();
        foreach (var (name, profile) in _state.ConnectionProfiles)
        {
            Connections.Add(new ConnectionRowViewModel
            {
                Name = name,
                BootstrapServers = profile.BootstrapServers,
                IsDemo = profile.BootstrapServers.StartsWith("demo", StringComparison.OrdinalIgnoreCase)
            });
        }
    }
}

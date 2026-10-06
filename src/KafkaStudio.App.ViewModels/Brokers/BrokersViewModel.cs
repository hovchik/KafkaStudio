using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.App.ViewModels.Brokers;

/// <summary>Lists the brokers of the selected connection's cluster (id, address, rack, which one is the
/// controller) and, for the selected broker, its configuration.</summary>
public sealed class BrokersViewModel : ObservableObject
{
    private readonly AppState _state;
    private CancellationTokenSource? _loadCts;
    private CancellationTokenSource? _configCts;
    private List<BrokerConfigEntry> _allConfig = new();

    public ObservableCollection<string> ConnectionNames { get; } = new();
    public ObservableCollection<BrokerInfo> Brokers { get; } = new();
    public ObservableCollection<BrokerConfigEntry> Config { get; } = new();

    private string? _selectedConnection;
    public string? SelectedConnection
    {
        get => _selectedConnection;
        set
        {
            if (!SetProperty(ref _selectedConnection, value)) return;
            _loadCts?.Cancel();
            _configCts?.Cancel();
            Brokers.Clear();
            ClusterSummary = null;
            SelectedBroker = null;
            Status = null;
            RefreshCommand?.RaiseCanExecuteChanged();
            if (value is not null) _ = RefreshAsync();
        }
    }

    private BrokerInfo? _selectedBroker;
    public BrokerInfo? SelectedBroker
    {
        get => _selectedBroker;
        set
        {
            if (!SetProperty(ref _selectedBroker, value)) return;
            OnPropertyChanged(nameof(HasSelectedBroker));
            _ = LoadConfigAsync();
        }
    }

    public bool HasSelectedBroker => SelectedBroker is not null;

    private string? _configFilter;
    public string? ConfigFilter
    {
        get => _configFilter;
        set
        {
            if (SetProperty(ref _configFilter, value)) ApplyConfigFilter();
        }
    }

    private string? _clusterSummary;
    /// <summary>"cluster abc123 · 3 brokers · controller 1".</summary>
    public string? ClusterSummary { get => _clusterSummary; private set => SetProperty(ref _clusterSummary, value); }

    private string? _status;
    public string? Status { get => _status; private set => SetProperty(ref _status, value); }

    private bool _isLoading;
    public bool IsLoading { get => _isLoading; private set => SetProperty(ref _isLoading, value); }

    public AsyncRelayCommand RefreshCommand { get; }

    public BrokersViewModel(AppState state)
    {
        _state = state;
        RefreshCommand = new AsyncRelayCommand(RefreshAsync, () => SelectedConnection is not null);
        _state.ConnectionsChanged += RefreshConnectionNames;
        RefreshConnectionNames();
    }

    private void RefreshConnectionNames()
    {
        CollectionSync.SyncSorted(ConnectionNames, _state.Connections.Keys);
        if (SelectedConnection is not null && !ConnectionNames.Contains(SelectedConnection))
        {
            SelectedConnection = null;
        }
        if (SelectedConnection is null && ConnectionNames.Count == 1)
        {
            SelectedConnection = ConnectionNames[0];
        }
    }

    public async Task RefreshAsync()
    {
        if (SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;

        _loadCts?.Cancel();
        var cts = _loadCts = new CancellationTokenSource();
        IsLoading = true;
        Status = "Loading brokers…";
        try
        {
            var cluster = await gateway.DescribeClusterAsync(cts.Token);
            if (cts.IsCancellationRequested) return;

            var keepId = SelectedBroker?.Id;
            Brokers.Clear();
            foreach (var broker in cluster.Brokers) Brokers.Add(broker);
            SelectedBroker = Brokers.FirstOrDefault(b => b.Id == keepId) ?? Brokers.FirstOrDefault();

            ClusterSummary = string.Join(" · ", new[]
            {
                cluster.ClusterId is null ? null : $"cluster {cluster.ClusterId}",
                $"{cluster.Brokers.Count} broker{(cluster.Brokers.Count == 1 ? "" : "s")}",
                cluster.ControllerId is { } c ? $"controller {c}" : null
            }.Where(s => s is not null));
            Status = null;
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested) { }
        catch (Exception ex)
        {
            if (!cts.IsCancellationRequested) Status = $"Couldn't load brokers: {ex.Message}";
        }
        finally
        {
            if (ReferenceEquals(_loadCts, cts)) IsLoading = false;
        }
    }

    private async Task LoadConfigAsync()
    {
        _configCts?.Cancel();
        _allConfig = new List<BrokerConfigEntry>();
        Config.Clear();
        if (SelectedBroker is not { } broker || SelectedConnection is null ||
            !_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;

        var cts = _configCts = new CancellationTokenSource();
        try
        {
            var entries = await gateway.GetBrokerConfigAsync(broker.Id, cts.Token);
            if (cts.IsCancellationRequested) return;
            _allConfig = entries.ToList();
            ApplyConfigFilter();
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested) { }
        catch (Exception ex)
        {
            if (!cts.IsCancellationRequested) Status = $"Couldn't load config of broker {broker.Id}: {ex.Message}";
        }
    }

    private void ApplyConfigFilter()
    {
        var filter = ConfigFilter?.Trim();
        Config.Clear();
        foreach (var entry in _allConfig)
        {
            if (string.IsNullOrEmpty(filter) ||
                entry.Name.Contains(filter, StringComparison.OrdinalIgnoreCase) ||
                (entry.Value?.Contains(filter, StringComparison.OrdinalIgnoreCase) ?? false))
            {
                Config.Add(entry);
            }
        }
    }
}

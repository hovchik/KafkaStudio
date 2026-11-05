using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Schemas;
using KafkaStudio.Core.Topics;

namespace KafkaStudio.App.ViewModels.Admin;

/// <summary>An editable row of the topic settings list.</summary>
public sealed class TopicConfigRowViewModel : ObservableObject
{
    private string? _value;

    public TopicConfigRowViewModel(TopicConfigEntry entry)
    {
        Entry = entry;
        _value = entry.Value;
    }

    public TopicConfigEntry Entry { get; }
    public string Name => Entry.Name;
    public bool IsDefault => Entry.IsDefault;
    public bool CanEdit => !Entry.IsReadOnly && !Entry.IsSensitive;
    public string? Source => Entry.Source;

    public string? Value
    {
        get => _value;
        set
        {
            if (!SetProperty(ref _value, value)) return;
            OnPropertyChanged(nameof(IsChanged));
        }
    }

    public bool IsChanged => !string.Equals(_value, Entry.Value, StringComparison.Ordinal);
}

public sealed record SchemaVersionRow(string Subject, int Version);

/// <summary>
/// Cluster administration that goes beyond browsing: topic settings (edit, add partitions, delete),
/// ACLs, Schema Registry, Kafka Connect and per-topic message rates. Everything here reads or changes
/// cluster settings; nothing on this screen produces messages. Destructive actions need a second click.
/// </summary>
public sealed class ClusterAdminViewModel : ObservableObject
{
    private readonly AppState _state;
    private readonly TopicRateSampler _sampler = new();
    private CancellationTokenSource? _rateCts;
    private List<AclEntry> _allAcls = new();
    private Func<Task>? _pending;

    public ObservableCollection<string> ConnectionNames { get; } = new();
    public ObservableCollection<string> TopicNames { get; } = new();
    public ObservableCollection<TopicConfigRowViewModel> Config { get; } = new();
    public ObservableCollection<AclEntry> Acls { get; } = new();
    public ObservableCollection<string> Subjects { get; } = new();
    public ObservableCollection<SchemaVersionRow> Versions { get; } = new();
    public ObservableCollection<ConnectorInfo> Connectors { get; } = new();
    public ObservableCollection<TopicRate> Rates { get; } = new();

    private string? _selectedConnection;
    public string? SelectedConnection
    {
        get => _selectedConnection;
        set
        {
            if (!SetProperty(ref _selectedConnection, value)) return;
            StopRates();
            TopicNames.Clear();
            Config.Clear();
            Acls.Clear();
            _allAcls.Clear();
            Subjects.Clear();
            Versions.Clear();
            Connectors.Clear();
            Rates.Clear();
            SelectedTopic = null;
            CancelPending();
            Status = null;
            RaiseCommandStates();
            if (value is not null) _ = LoadTopicsAsync();
        }
    }

    private string? _selectedTopic;
    public string? SelectedTopic
    {
        get => _selectedTopic;
        set
        {
            if (!SetProperty(ref _selectedTopic, value)) return;
            Config.Clear();
            CancelPending();
            RaiseCommandStates();
            if (value is not null) _ = LoadConfigAsync();
        }
    }

    private int _newPartitionTotal = 1;
    /// <summary>The total partition count to grow the selected topic to.</summary>
    public int NewPartitionTotal { get => _newPartitionTotal; set => SetProperty(ref _newPartitionTotal, Math.Max(1, value)); }

    private string? _aclFilter;
    public string? AclFilter { get => _aclFilter; set { if (SetProperty(ref _aclFilter, value)) ApplyAclFilter(); } }

    private string? _selectedSubject;
    public string? SelectedSubject
    {
        get => _selectedSubject;
        set
        {
            if (!SetProperty(ref _selectedSubject, value)) return;
            Versions.Clear();
            SchemaText = null;
            if (value is not null) _ = LoadVersionsAsync(value);
        }
    }

    private SchemaVersionRow? _selectedVersion;
    public SchemaVersionRow? SelectedVersion
    {
        get => _selectedVersion;
        set
        {
            if (!SetProperty(ref _selectedVersion, value)) return;
            if (value is not null) _ = LoadSchemaAsync(value);
        }
    }

    private string? _schemaText;
    public string? SchemaText { get => _schemaText; private set => SetProperty(ref _schemaText, value); }

    private ConnectorInfo? _selectedConnector;
    public ConnectorInfo? SelectedConnector
    {
        get => _selectedConnector;
        set
        {
            if (!SetProperty(ref _selectedConnector, value)) return;
            OnPropertyChanged(nameof(ConnectorDetail));
        }
    }

    /// <summary>The selected connector's tasks (with failure traces) and settings, as plain text.</summary>
    public string? ConnectorDetail => SelectedConnector is not { } c ? null : string.Join(Environment.NewLine,
        new[] { $"{c.Name}  [{c.Type}] {c.State}  worker {c.WorkerId}" }
            .Concat(c.Tasks.Select(t => $"task {t.Id}: {t.State}{(t.Trace is null ? "" : Environment.NewLine + t.Trace)}"))
            .Append("")
            .Concat(c.Config.OrderBy(k => k.Key, StringComparer.Ordinal).Select(k => $"{k.Key} = {k.Value}")));

    private string? _status;
    public string? Status { get => _status; private set => SetProperty(ref _status, value); }

    private bool _isBusy;
    public bool IsBusy { get => _isBusy; private set => SetProperty(ref _isBusy, value); }

    private string? _confirmText;
    /// <summary>What the pending destructive action will do; shown with Confirm / Cancel buttons.</summary>
    public string? ConfirmText { get => _confirmText; private set { if (SetProperty(ref _confirmText, value)) OnPropertyChanged(nameof(HasPending)); } }

    public bool HasPending => ConfirmText is not null;

    private bool _isSampling;
    public bool IsSampling { get => _isSampling; private set { if (SetProperty(ref _isSampling, value)) RaiseCommandStates(); } }

    public bool HasTopic => SelectedTopic is not null;

    public AsyncRelayCommand RefreshCommand { get; }
    public AsyncRelayCommand ApplyConfigCommand { get; }
    public RelayCommand AddPartitionsCommand { get; }
    public RelayCommand DeleteTopicCommand { get; }
    public AsyncRelayCommand ConfirmCommand { get; }
    public RelayCommand CancelCommand { get; }
    public AsyncRelayCommand LoadAclsCommand { get; }
    public AsyncRelayCommand LoadSubjectsCommand { get; }
    public AsyncRelayCommand LoadConnectorsCommand { get; }
    public RelayCommand ToggleRatesCommand { get; }

    public ClusterAdminViewModel(AppState state)
    {
        _state = state;
        RefreshCommand = new AsyncRelayCommand(LoadTopicsAsync, () => SelectedConnection is not null);
        ApplyConfigCommand = new AsyncRelayCommand(ApplyConfigAsync,
            () => SelectedTopic is not null && Config.Any(c => c.IsChanged));
        AddPartitionsCommand = new RelayCommand(StageAddPartitions, () => SelectedTopic is not null);
        DeleteTopicCommand = new RelayCommand(StageDeleteTopic, () => SelectedTopic is not null);
        ConfirmCommand = new AsyncRelayCommand(ConfirmAsync, () => HasPending);
        CancelCommand = new RelayCommand(CancelPending);
        LoadAclsCommand = new AsyncRelayCommand(LoadAclsAsync, () => SelectedConnection is not null);
        LoadSubjectsCommand = new AsyncRelayCommand(LoadSubjectsAsync, () => SelectedConnection is not null);
        LoadConnectorsCommand = new AsyncRelayCommand(LoadConnectorsAsync, () => SelectedConnection is not null);
        ToggleRatesCommand = new RelayCommand(ToggleRates, () => SelectedConnection is not null);
        _state.ConnectionsChanged += RefreshConnectionNames;
        RefreshConnectionNames();
    }

    private void RefreshConnectionNames()
    {
        CollectionSync.SyncSorted(ConnectionNames, _state.Connections.Keys);
        if (SelectedConnection is not null && !ConnectionNames.Contains(SelectedConnection)) SelectedConnection = null;
        if (SelectedConnection is null && ConnectionNames.Count == 1) SelectedConnection = ConnectionNames[0];
    }

    private void RaiseCommandStates()
    {
        OnPropertyChanged(nameof(HasTopic));
        RefreshCommand.RaiseCanExecuteChanged();
        ApplyConfigCommand.RaiseCanExecuteChanged();
        AddPartitionsCommand.RaiseCanExecuteChanged();
        DeleteTopicCommand.RaiseCanExecuteChanged();
        ConfirmCommand.RaiseCanExecuteChanged();
        LoadAclsCommand.RaiseCanExecuteChanged();
        LoadSubjectsCommand.RaiseCanExecuteChanged();
        LoadConnectorsCommand.RaiseCanExecuteChanged();
        ToggleRatesCommand.RaiseCanExecuteChanged();
    }

    private bool TryGateway(out IKafkaGateway gateway) => _state.Connections.TryGetValue(SelectedConnection ?? "", out gateway!);

    // ---- topics and settings ----

    private async Task LoadTopicsAsync()
    {
        if (!TryGateway(out var gateway)) return;
        try
        {
            var names = await gateway.ListTopicsAsync().ConfigureAwait(true);
            var keep = SelectedTopic;
            CollectionSync.SyncSorted(TopicNames, names);
            if (keep is not null && TopicNames.Contains(keep)) SelectedTopic = keep;
            Status = $"{names.Count} topic{(names.Count == 1 ? "" : "s")}.";
        }
        catch (Exception ex) { Status = $"Couldn't list topics: {ex.Message}"; }
    }

    private async Task LoadConfigAsync()
    {
        if (!TryGateway(out var gateway) || SelectedTopic is not { } topic) return;
        try
        {
            var entries = await gateway.GetTopicConfigAsync(topic).ConfigureAwait(true);
            if (topic != SelectedTopic) return;
            Config.Clear();
            foreach (var entry in entries)
            {
                var row = new TopicConfigRowViewModel(entry);
                row.PropertyChanged += (_, e) =>
                {
                    if (e.PropertyName == nameof(TopicConfigRowViewModel.IsChanged)) ApplyConfigCommand.RaiseCanExecuteChanged();
                };
                Config.Add(row);
            }
            try
            {
                var meta = await gateway.DescribeTopicAsync(topic).ConfigureAwait(true);
                NewPartitionTotal = meta.Partitions.Count + 1;
                Status = $"{topic}: {meta.Partitions.Count} partition(s), replication factor {meta.ReplicationFactor}, {meta.TotalMessageCount:N0} messages.";
            }
            catch (Exception) { Status = null; }
            ApplyConfigCommand.RaiseCanExecuteChanged();
        }
        catch (Exception ex) { Status = $"Couldn't read settings of '{topic}': {ex.Message}"; }
    }

    private async Task ApplyConfigAsync()
    {
        if (!TryGateway(out var gateway) || SelectedTopic is not { } topic) return;
        var changes = Config.Where(c => c.IsChanged && c.CanEdit)
            .ToDictionary(c => c.Name, c => string.IsNullOrWhiteSpace(c.Value) ? null : c.Value, StringComparer.Ordinal);
        if (changes.Count == 0) return;
        IsBusy = true;
        try
        {
            await gateway.AlterTopicConfigAsync(topic, changes).ConfigureAwait(true);
            Status = $"Updated {changes.Count} setting{(changes.Count == 1 ? "" : "s")} on '{topic}' ({string.Join(", ", changes.Keys)}). An empty value resets a setting to the broker default.";
            await LoadConfigAsync().ConfigureAwait(true);
        }
        catch (Exception ex) { Status = $"Couldn't change settings of '{topic}': {ex.Message}"; }
        finally { IsBusy = false; }
    }

    private void StageAddPartitions()
    {
        if (SelectedTopic is not { } topic) return;
        var total = NewPartitionTotal;
        ConfirmText = $"Grow '{topic}' to {total} partitions? Partitions can never be removed, and keyed messages will map to different partitions for new data.";
        _pending = async () =>
        {
            if (!TryGateway(out var gateway)) return;
            await gateway.CreatePartitionsAsync(topic, total).ConfigureAwait(true);
            Status = $"'{topic}' now has {total} partitions.";
            await LoadConfigAsync().ConfigureAwait(true);
        };
        ConfirmCommand.RaiseCanExecuteChanged();
    }

    private void StageDeleteTopic()
    {
        if (SelectedTopic is not { } topic) return;
        ConfirmText = $"Delete topic '{topic}' and ALL of its messages? This cannot be undone.";
        _pending = async () =>
        {
            if (!TryGateway(out var gateway)) return;
            await gateway.DeleteTopicAsync(topic).ConfigureAwait(true);
            Status = $"Deleted topic '{topic}'.";
            SelectedTopic = null;
            await LoadTopicsAsync().ConfigureAwait(true);
        };
        ConfirmCommand.RaiseCanExecuteChanged();
    }

    private async Task ConfirmAsync()
    {
        var action = _pending;
        CancelPending();
        if (action is null) return;
        IsBusy = true;
        try { await action().ConfigureAwait(true); }
        catch (Exception ex) { Status = $"Failed: {ex.Message}"; }
        finally { IsBusy = false; }
    }

    private void CancelPending()
    {
        _pending = null;
        ConfirmText = null;
        ConfirmCommand?.RaiseCanExecuteChanged();
    }

    // ---- ACLs ----

    private async Task LoadAclsAsync()
    {
        if (!TryGateway(out var gateway)) return;
        IsBusy = true;
        try
        {
            _allAcls = (await gateway.ListAclsAsync().ConfigureAwait(true)).ToList();
            ApplyAclFilter();
            Status = _allAcls.Count == 0
                ? "No ACLs are defined (or the cluster has no authorizer)."
                : $"{_allAcls.Count} ACL{(_allAcls.Count == 1 ? "" : "s")}.";
        }
        catch (Exception ex) { Status = $"Couldn't list ACLs: {ex.Message}"; }
        finally { IsBusy = false; }
    }

    private void ApplyAclFilter()
    {
        var filter = AclFilter?.Trim();
        Acls.Clear();
        foreach (var acl in _allAcls)
        {
            if (string.IsNullOrEmpty(filter) ||
                acl.ResourceName.Contains(filter, StringComparison.OrdinalIgnoreCase) ||
                acl.Principal.Contains(filter, StringComparison.OrdinalIgnoreCase) ||
                acl.Operation.Contains(filter, StringComparison.OrdinalIgnoreCase))
            {
                Acls.Add(acl);
            }
        }
    }

    // ---- Schema Registry ----

    private SchemaRegistryClient? NewRegistry()
    {
        if (SelectedConnection is null || !_state.ConnectionProfiles.TryGetValue(SelectedConnection, out var p) ||
            string.IsNullOrWhiteSpace(p.SchemaRegistryUrl))
        {
            Status = "This connection has no Schema Registry URL. Add one in Connections, under \"Schemas, Connect and masking\".";
            return null;
        }
        return new SchemaRegistryClient(p.SchemaRegistryUrl, p.SchemaRegistryUsername, p.SchemaRegistryPassword);
    }

    private async Task LoadSubjectsAsync()
    {
        using var client = NewRegistry();
        if (client is null) return;
        IsBusy = true;
        try
        {
            var subjects = await client.ListSubjectsAsync().ConfigureAwait(true);
            CollectionSync.SyncSorted(Subjects, subjects);
            Status = $"{subjects.Count} subject{(subjects.Count == 1 ? "" : "s")}.";
        }
        catch (Exception ex) { Status = $"Schema Registry: {ex.Message}"; }
        finally { IsBusy = false; }
    }

    private async Task LoadVersionsAsync(string subject)
    {
        using var client = NewRegistry();
        if (client is null) return;
        try
        {
            var versions = await client.ListVersionsAsync(subject).ConfigureAwait(true);
            if (subject != SelectedSubject) return;
            foreach (var v in versions) Versions.Add(new SchemaVersionRow(subject, v));
            SelectedVersion = Versions.LastOrDefault();
        }
        catch (Exception ex) { Status = $"Schema Registry: {ex.Message}"; }
    }

    private async Task LoadSchemaAsync(SchemaVersionRow row)
    {
        using var client = NewRegistry();
        if (client is null) return;
        try
        {
            var version = await client.GetVersionAsync(row.Subject, row.Version).ConfigureAwait(true);
            if (!ReferenceEquals(row, SelectedVersion)) return;
            SchemaText = $"{version.SchemaType}  ·  id {version.Id}{Environment.NewLine}{Environment.NewLine}{PrettyJson(version.Schema)}";
        }
        catch (Exception ex) { Status = $"Schema Registry: {ex.Message}"; }
    }

    private static string PrettyJson(string text)
    {
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(text);
            return System.Text.Json.JsonSerializer.Serialize(doc.RootElement, new System.Text.Json.JsonSerializerOptions
            {
                WriteIndented = true,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            });
        }
        catch (System.Text.Json.JsonException) { return text; } // Protobuf schemas are .proto text
    }

    // ---- Kafka Connect ----

    private async Task LoadConnectorsAsync()
    {
        if (SelectedConnection is null || !_state.ConnectionProfiles.TryGetValue(SelectedConnection, out var p) ||
            string.IsNullOrWhiteSpace(p.ConnectUrl))
        {
            Status = "This connection has no Kafka Connect URL. Add one in Connections, under \"Schemas, Connect and masking\".";
            return;
        }
        IsBusy = true;
        try
        {
            using var client = new KafkaConnectClient(p.ConnectUrl);
            var connectors = await client.ListAsync().ConfigureAwait(true);
            var keep = SelectedConnector?.Name;
            Connectors.Clear();
            foreach (var c in connectors) Connectors.Add(c);
            SelectedConnector = Connectors.FirstOrDefault(c => c.Name == keep);
            var failed = connectors.Count(c => c.IsFailed);
            Status = $"{connectors.Count} connector{(connectors.Count == 1 ? "" : "s")}" + (failed > 0 ? $", {failed} failed." : ".");
        }
        catch (Exception ex) { Status = $"Kafka Connect: {ex.Message}"; }
        finally { IsBusy = false; }
    }

    // ---- message rates ----

    private void ToggleRates()
    {
        if (IsSampling) StopRates();
        else StartRates();
    }

    private void StartRates()
    {
        if (!TryGateway(out var gateway)) return;
        StopRates();
        _sampler.Reset();
        Rates.Clear();
        var cts = _rateCts = new CancellationTokenSource();
        IsSampling = true;
        Status = "Sampling topic sizes every 5 seconds. The first sample has no rate yet.";
        _ = Task.Run(async () =>
        {
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    var totals = new Dictionary<string, long>(StringComparer.Ordinal);
                    foreach (var topic in await gateway.ListTopicsAsync(cts.Token).ConfigureAwait(false))
                    {
                        try { totals[topic] = (await gateway.DescribeTopicAsync(topic, cts.Token).ConfigureAwait(false)).TotalMessageCount; }
                        catch (Exception ex) when (ex is not OperationCanceledException) { /* topic vanished mid-sample */ }
                    }
                    var rates = _sampler.Add(totals, DateTimeOffset.UtcNow);
                    _state.PostToUi(() =>
                    {
                        if (cts.IsCancellationRequested) return;
                        Rates.Clear();
                        foreach (var r in rates) Rates.Add(r);
                    });
                    await Task.Delay(TimeSpan.FromSeconds(5), cts.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _state.PostToUi(() => { Status = $"Sampling stopped: {ex.Message}"; IsSampling = false; });
            }
        });
    }

    private void StopRates()
    {
        _rateCts?.Cancel();
        _rateCts?.Dispose();
        _rateCts = null;
        IsSampling = false;
    }
}

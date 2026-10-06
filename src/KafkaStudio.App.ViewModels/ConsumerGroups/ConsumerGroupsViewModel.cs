using System.Collections.ObjectModel;
using System.Globalization;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.App.ViewModels.ConsumerGroups;

/// <summary>A consumer group's committed offset on one partition, as a list row.</summary>
public sealed class GroupOffsetRowViewModel
{
    public GroupOffsetRowViewModel(ConsumerGroupOffset offset) => Offset = offset;

    public ConsumerGroupOffset Offset { get; }
    public string Topic => Offset.Topic;
    public int Partition => Offset.Partition;
    public string Committed => Offset.CommittedOffset?.ToString(CultureInfo.InvariantCulture) ?? "-";
    public long Earliest => Offset.EarliestOffset;
    public long End => Offset.EndOffset;
    public long Lag => Offset.Lag;
}

/// <summary>One line of the reset confirmation: where a partition is now and where it will move to.</summary>
public sealed class OffsetChangeRowViewModel
{
    public OffsetChangeRowViewModel(OffsetChange change) => Change = change;

    public OffsetChange Change { get; }
    public string Label => $"{Change.Topic} #{Change.Partition}";
    public string From => Change.CurrentOffset?.ToString(CultureInfo.InvariantCulture) ?? "-";
    public long To => Change.NewOffset;
}

/// <summary>
/// Lists a cluster's consumer groups, shows one group's members, committed offsets, end offsets and
/// lag, and moves its committed offsets (to earliest, latest, a timestamp or an explicit offset).
///
/// Kafka only accepts offset changes for a group with no live members, so an active group shows a
/// warning and the reset is disabled. A reset is always a two step action: "Preview reset" plans the
/// new offsets (nothing is changed), the confirmation lists every partition's old and new offset, and
/// only "Apply" commits them.
/// </summary>
public sealed class ConsumerGroupsViewModel : ObservableObject
{
    private readonly AppState _state;
    private CancellationTokenSource? _groupsCts;
    private CancellationTokenSource? _detailCts;
    private readonly List<ConsumerGroupSummary> _allGroups = new();

    public ObservableCollection<string> ConnectionNames { get; } = new();
    public ObservableCollection<ConsumerGroupSummary> Groups { get; } = new();
    public ObservableCollection<GroupOffsetRowViewModel> Offsets { get; } = new();
    public ObservableCollection<ConsumerGroupMember> Members { get; } = new();
    public ObservableCollection<OffsetChangeRowViewModel> PendingChanges { get; } = new();

    public IReadOnlyList<OffsetResetTarget> ResetTargets { get; } = Enum.GetValues<OffsetResetTarget>();

    private string? _selectedConnection;
    public string? SelectedConnection
    {
        get => _selectedConnection;
        set
        {
            if (!SetProperty(ref _selectedConnection, value)) return;
            _detailCts?.Cancel();
            ClearDetail();
            SelectedGroup = null;
            _allGroups.Clear();
            Groups.Clear();
            RefreshCommand.RaiseCanExecuteChanged();
            _ = RefreshGroupsAsync();
        }
    }

    private string? _groupFilter;
    public string? GroupFilter
    {
        get => _groupFilter;
        set { if (SetProperty(ref _groupFilter, value)) ApplyGroupFilter(); }
    }

    private ConsumerGroupSummary? _selectedGroup;
    public ConsumerGroupSummary? SelectedGroup
    {
        get => _selectedGroup;
        set
        {
            if (!SetProperty(ref _selectedGroup, value)) return;
            _detailCts?.Cancel();
            ClearDetail();
            if (value is not null) _ = LoadDetailAsync(value.GroupId);
            RefreshDetailCommand.RaiseCanExecuteChanged();
        }
    }

    private ConsumerGroupDetail? _detail;
    public ConsumerGroupDetail? Detail
    {
        get => _detail;
        private set
        {
            if (!SetProperty(ref _detail, value)) return;
            OnPropertyChanged(nameof(HasDetail));
            OnPropertyChanged(nameof(DetailSummary));
            OnPropertyChanged(nameof(IsGroupActive));
            OnPropertyChanged(nameof(ActiveGroupWarning));
            OnPropertyChanged(nameof(Topics));
            RaiseResetCommands();
        }
    }

    public bool HasDetail => Detail is not null;
    public bool IsGroupActive => Detail?.IsActive == true;

    /// <summary>"Stable · 3 member(s) · total lag 1,204 · assignor range".</summary>
    public string? DetailSummary => Detail is null ? null :
        $"{Detail.State} · {Detail.Members.Count} member(s) · total lag {Detail.TotalLag:N0}" +
        (Detail.PartitionAssignor is null ? "" : $" · assignor {Detail.PartitionAssignor}");

    public string? ActiveGroupWarning => IsGroupActive
        ? $"This group is active ({Detail!.State}, {Detail.Members.Count} member(s)). Kafka only lets you change offsets of a group with no live members - stop its consumers, then refresh."
        : null;

    /// <summary>Topics the selected group has offsets for (reset can be narrowed to one).</summary>
    public IReadOnlyList<string> Topics =>
        Detail?.Offsets.Select(o => o.Topic).Distinct(StringComparer.Ordinal).OrderBy(t => t, StringComparer.Ordinal).ToList()
        ?? new List<string>();

    private GroupOffsetRowViewModel? _selectedOffset;
    public GroupOffsetRowViewModel? SelectedOffset
    {
        get => _selectedOffset;
        set
        {
            if (SetProperty(ref _selectedOffset, value))
            {
                if (value is null) OnlySelectedPartition = false;
                OnPropertyChanged(nameof(HasSelectedOffset));
            }
        }
    }

    public bool HasSelectedOffset => SelectedOffset is not null;

    // ------------------------------------------------------------------ reset inputs ----

    private OffsetResetTarget _resetTarget = OffsetResetTarget.Earliest;
    public OffsetResetTarget ResetTarget
    {
        get => _resetTarget;
        set
        {
            if (!SetProperty(ref _resetTarget, value)) return;
            OnPropertyChanged(nameof(IsTimestampTarget));
            OnPropertyChanged(nameof(IsOffsetTarget));
            CancelReset();
            RaiseResetCommands();
        }
    }

    public bool IsTimestampTarget => ResetTarget == OffsetResetTarget.Timestamp;
    public bool IsOffsetTarget => ResetTarget == OffsetResetTarget.Offset;

    private string? _resetTimestamp;
    /// <summary>Date/time to rewind to, e.g. "2026-10-06 08:00" (UTC unless it carries an offset).</summary>
    public string? ResetTimestamp
    {
        get => _resetTimestamp;
        set { if (SetProperty(ref _resetTimestamp, value)) { CancelReset(); RaiseResetCommands(); } }
    }

    private long? _resetOffset;
    public long? ResetOffset
    {
        get => _resetOffset;
        set { if (SetProperty(ref _resetOffset, value)) { CancelReset(); RaiseResetCommands(); } }
    }

    private string? _resetTopic;
    /// <summary>Null/empty = every topic of the group.</summary>
    public string? ResetTopic
    {
        get => _resetTopic;
        set { if (SetProperty(ref _resetTopic, string.IsNullOrEmpty(value) ? null : value)) CancelReset(); }
    }

    private bool _onlySelectedPartition;
    /// <summary>Restricts the reset to the partition row selected in the table.</summary>
    public bool OnlySelectedPartition
    {
        get => _onlySelectedPartition;
        set { if (SetProperty(ref _onlySelectedPartition, value && SelectedOffset is not null)) CancelReset(); }
    }

    private bool _isConfirming;
    /// <summary>True while the planned changes are shown waiting for "Apply".</summary>
    public bool IsConfirming { get => _isConfirming; private set => SetProperty(ref _isConfirming, value); }

    private string? _confirmationText;
    public string? ConfirmationText { get => _confirmationText; private set => SetProperty(ref _confirmationText, value); }

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    private bool _isLoading;
    public bool IsLoading { get => _isLoading; private set => SetProperty(ref _isLoading, value); }

    // ------------------------------------------------------------------ commands ----

    public AsyncRelayCommand RefreshCommand { get; }
    public AsyncRelayCommand RefreshDetailCommand { get; }
    public AsyncRelayCommand PreviewResetCommand { get; }
    public AsyncRelayCommand ApplyResetCommand { get; }
    public RelayCommand CancelResetCommand { get; }

    public ConsumerGroupsViewModel(AppState state)
    {
        _state = state;
        _state.ConnectionsChanged += RefreshConnectionNames;

        RefreshCommand = new AsyncRelayCommand(RefreshGroupsAsync, () => SelectedConnection is not null, allowConcurrentExecutions: true);
        RefreshDetailCommand = new AsyncRelayCommand(
            () => SelectedGroup is { } g ? LoadDetailAsync(g.GroupId) : Task.CompletedTask,
            () => SelectedGroup is not null, allowConcurrentExecutions: true);
        PreviewResetCommand = new AsyncRelayCommand(PreviewResetAsync, CanPreviewReset);
        ApplyResetCommand = new AsyncRelayCommand(ApplyResetAsync, () => IsConfirming && PendingChanges.Count > 0);
        CancelResetCommand = new RelayCommand(CancelReset, () => IsConfirming);

        RefreshConnectionNames();
    }

    // ------------------------------------------------------------------ loading ----

    private void RefreshConnectionNames()
    {
        CollectionSync.SyncSorted(ConnectionNames, _state.Connections.Keys);
        if (SelectedConnection is not null && !ConnectionNames.Contains(SelectedConnection)) SelectedConnection = null;
        if (SelectedConnection is null && ConnectionNames.Count == 1) SelectedConnection = ConnectionNames[0];
    }

    private void ApplyGroupFilter()
    {
        var selectedId = SelectedGroup?.GroupId;
        var term = GroupFilter?.Trim();
        Groups.Clear();
        foreach (var g in _allGroups.Where(g => string.IsNullOrEmpty(term) || g.GroupId.Contains(term, StringComparison.OrdinalIgnoreCase)))
        {
            Groups.Add(g);
        }
        // Clear() pushes null through the list binding - restore the selection when it survives.
        SelectedGroup = selectedId is null ? null : Groups.FirstOrDefault(g => g.GroupId == selectedId);
    }

    private async Task RefreshGroupsAsync()
    {
        _groupsCts?.Cancel();
        var cts = new CancellationTokenSource();
        _groupsCts = cts;

        if (SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway))
        {
            IsLoading = false;
            return;
        }

        StatusMessage = "Loading consumer groups...";
        IsLoading = true;
        try
        {
            var groups = await gateway.ListConsumerGroupsAsync(cts.Token).ConfigureAwait(true);
            if (cts.IsCancellationRequested) return;

            _allGroups.Clear();
            _allGroups.AddRange(groups.OrderBy(g => g.GroupId, StringComparer.Ordinal));
            var selectedId = SelectedGroup?.GroupId;
            ApplyGroupFilter();
            StatusMessage = _allGroups.Count == 0 ? "No consumer groups found." : $"{_allGroups.Count} consumer group(s).";

            // Keep the open group's detail current (its state/lag changed too).
            if (selectedId is not null && SelectedGroup is null && _allGroups.Any(g => g.GroupId == selectedId))
            {
                SelectedGroup = Groups.FirstOrDefault(g => g.GroupId == selectedId);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            if (!cts.IsCancellationRequested) StatusMessage = $"Could not load consumer groups: {ex.Message}";
        }
        finally
        {
            if (_groupsCts == cts) IsLoading = false;
        }
    }

    private async Task LoadDetailAsync(string groupId)
    {
        _detailCts?.Cancel();
        var cts = new CancellationTokenSource();
        _detailCts = cts;

        if (SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;

        StatusMessage = $"Loading offsets of '{groupId}'...";
        try
        {
            var detail = await gateway.DescribeConsumerGroupAsync(groupId, cts.Token).ConfigureAwait(true);
            if (cts.IsCancellationRequested) return;

            var selectedKey = SelectedOffset is { } s ? (s.Topic, s.Partition) : ((string, int)?)null;
            Offsets.Clear();
            foreach (var o in detail.Offsets) Offsets.Add(new GroupOffsetRowViewModel(o));
            Members.Clear();
            foreach (var m in detail.Members) Members.Add(m);
            SelectedOffset = selectedKey is { } k ? Offsets.FirstOrDefault(r => (r.Topic, r.Partition) == k) : null;
            Detail = detail;
            if (ResetTopic is not null && !Topics.Contains(ResetTopic)) ResetTopic = null;
            StatusMessage = $"{detail.GroupId}: {detail.Offsets.Count} partition(s), total lag {detail.TotalLag:N0}.";
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            if (!cts.IsCancellationRequested) StatusMessage = $"Could not load group '{groupId}': {ex.Message}";
        }
    }

    private void ClearDetail()
    {
        CancelReset();
        Offsets.Clear();
        Members.Clear();
        SelectedOffset = null;
        Detail = null;
    }

    // ------------------------------------------------------------------ resetting offsets ----

    private bool TryBuildRequest(out OffsetResetRequest? request, out string? error)
    {
        request = null;
        error = null;
        if (Detail is null) return false;

        DateTimeOffset? timestamp = null;
        if (ResetTarget == OffsetResetTarget.Timestamp)
        {
            if (!DateTimeOffset.TryParse(ResetTimestamp, CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
            {
                error = "Enter the timestamp as a date and time, e.g. 2026-10-06 08:00 (UTC unless it has an offset).";
                return false;
            }
            timestamp = parsed;
        }

        if (ResetTarget == OffsetResetTarget.Offset && (ResetOffset is null || ResetOffset < 0))
        {
            error = "Enter the offset to move to (0 or higher).";
            return false;
        }

        request = new OffsetResetRequest
        {
            GroupId = Detail.GroupId,
            Target = ResetTarget,
            Topic = OnlySelectedPartition ? SelectedOffset?.Topic : ResetTopic,
            Partition = OnlySelectedPartition ? SelectedOffset?.Partition : null,
            Timestamp = timestamp,
            Offset = ResetTarget == OffsetResetTarget.Offset ? ResetOffset : null
        };
        return true;
    }

    private bool CanPreviewReset() => SelectedConnection is not null && Detail is { IsActive: false };

    private void RaiseResetCommands()
    {
        PreviewResetCommand?.RaiseCanExecuteChanged();
        ApplyResetCommand?.RaiseCanExecuteChanged();
        CancelResetCommand?.RaiseCanExecuteChanged();
    }

    private async Task PreviewResetAsync()
    {
        if (SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;
        if (!TryBuildRequest(out var request, out var error))
        {
            if (error is not null) StatusMessage = error;
            return;
        }

        CancelReset();
        try
        {
            StatusMessage = "Working out the new offsets...";
            var changes = await gateway.PlanOffsetResetAsync(request!, CancellationToken.None).ConfigureAwait(true);
            foreach (var c in changes) PendingChanges.Add(new OffsetChangeRowViewModel(c));

            var scope = request!.Partition is not null ? "1 partition"
                : request.Topic is not null ? $"topic {request.Topic}" : "all topics";
            ConfirmationText = $"Move '{request.GroupId}' ({scope}, {changes.Count} partition(s)) to {Describe(request)}? " +
                               "Consumers will re-read or skip messages the next time the group starts.";
            IsConfirming = true;
            StatusMessage = "Review the changes below, then apply.";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Could not plan the reset: {ex.Message}";
        }
        RaiseResetCommands();
    }

    private static string Describe(OffsetResetRequest r) => r.Target switch
    {
        OffsetResetTarget.Earliest => "the earliest offset",
        OffsetResetTarget.Latest => "the latest offset",
        OffsetResetTarget.Timestamp => $"the first message at or after {r.Timestamp:yyyy-MM-dd HH:mm:ss} UTC",
        _ => $"offset {r.Offset}"
    };

    private async Task ApplyResetAsync()
    {
        if (Detail is null || SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;

        var groupId = Detail.GroupId;
        var changes = PendingChanges.Select(p => p.Change).ToList();
        try
        {
            StatusMessage = $"Updating offsets of '{groupId}'...";
            await gateway.ApplyOffsetResetAsync(groupId, changes, CancellationToken.None).ConfigureAwait(true);
            CancelReset();
            await LoadDetailAsync(groupId).ConfigureAwait(true);
            StatusMessage = $"Updated {changes.Count} partition offset(s) of '{groupId}'.";
        }
        catch (Exception ex)
        {
            // Most likely the group became active since the preview - show it and refresh its state.
            StatusMessage = $"Offsets were not changed: {ex.Message}";
            CancelReset();
            await LoadDetailAsync(groupId).ConfigureAwait(true);
            StatusMessage = $"Offsets were not changed: {ex.Message}";
        }
    }

    private void CancelReset()
    {
        PendingChanges.Clear();
        ConfirmationText = null;
        IsConfirming = false;
        RaiseResetCommands();
    }
}

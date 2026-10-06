using System.Collections.ObjectModel;
using System.Globalization;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.App.ViewModels.ConsumerGroups;

/// <summary>
/// A consumer group's committed offset on one partition, as a list row. While the partition has lag the
/// row offers an editable target offset (prefilled with the End offset, i.e. "skip the backlog") and an
/// Apply button that commits it straight away.
/// </summary>
public sealed class GroupOffsetRowViewModel : ObservableObject
{
    private readonly Func<GroupOffsetRowViewModel, Task> _apply;
    private readonly Func<bool> _groupActive;

    public GroupOffsetRowViewModel(ConsumerGroupOffset offset, Func<bool> groupActive, Func<GroupOffsetRowViewModel, Task> apply)
    {
        Offset = offset;
        _groupActive = groupActive;
        _apply = apply;
        _newOffset = offset.EndOffset;
        ApplyCommand = new AsyncRelayCommand(() => _apply(this), () => CanEdit);
    }

    public ConsumerGroupOffset Offset { get; }
    public string Topic => Offset.Topic;
    public int Partition => Offset.Partition;
    public string Committed => Offset.CommittedOffset?.ToString(CultureInfo.InvariantCulture) ?? "-";
    public long Earliest => Offset.EarliestOffset;
    public long End => Offset.EndOffset;
    public long Lag => Offset.Lag;

    /// <summary>Only partitions the group is behind on can be edited.</summary>
    public bool HasLag => Offset.Lag > 0;

    /// <summary>Lag, and the group has no live members (Kafka rejects offset changes otherwise).</summary>
    public bool CanEdit => HasLag && !_groupActive();

    private long? _newOffset;
    /// <summary>The offset to commit; defaults to the End offset.</summary>
    public long? NewOffset
    {
        get => _newOffset;
        set => SetProperty(ref _newOffset, value);
    }

    public AsyncRelayCommand ApplyCommand { get; }

    public void RaiseCanEditChanged()
    {
        OnPropertyChanged(nameof(CanEdit));
        ApplyCommand.RaiseCanExecuteChanged();
    }
}

/// <summary>
/// Lists a cluster's consumer groups, shows one group's members, committed offsets, end offsets and
/// lag, and moves its committed offsets (to earliest, latest, a timestamp or an explicit offset).
///
/// Kafka only accepts offset changes for a group with no live members, so an active group shows a
/// warning and editing is disabled. A partition with lag gets an editable target offset (prefilled with
/// its End offset) and an Apply button.
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
            foreach (var row in Offsets) row.RaiseCanEditChanged();
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

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    private bool _isLoading;
    public bool IsLoading { get => _isLoading; private set => SetProperty(ref _isLoading, value); }

    // ------------------------------------------------------------------ commands ----

    public AsyncRelayCommand RefreshCommand { get; }
    public AsyncRelayCommand RefreshDetailCommand { get; }

    public ConsumerGroupsViewModel(AppState state)
    {
        _state = state;
        _state.ConnectionsChanged += RefreshConnectionNames;

        RefreshCommand = new AsyncRelayCommand(RefreshGroupsAsync, () => SelectedConnection is not null, allowConcurrentExecutions: true);
        RefreshDetailCommand = new AsyncRelayCommand(
            () => SelectedGroup is { } g ? LoadDetailAsync(g.GroupId) : Task.CompletedTask,
            () => SelectedGroup is not null, allowConcurrentExecutions: true);

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

            Detail = detail;
            Offsets.Clear();
            foreach (var o in detail.Offsets) Offsets.Add(new GroupOffsetRowViewModel(o, () => IsGroupActive, ApplyRowAsync));
            Members.Clear();
            foreach (var m in detail.Members) Members.Add(m);
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
        Offsets.Clear();
        Members.Clear();
        Detail = null;
    }

    // ------------------------------------------------------------------ editing offsets ----

    private async Task ApplyRowAsync(GroupOffsetRowViewModel row)
    {
        if (Detail is null || SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;

        var groupId = Detail.GroupId;
        if (row.NewOffset is not { } target || target < row.Earliest || target > row.End)
        {
            StatusMessage = $"Enter an offset between {row.Earliest} and {row.End} for {row.Topic} #{row.Partition}.";
            return;
        }

        try
        {
            StatusMessage = $"Updating {row.Topic} #{row.Partition} of '{groupId}'...";
            await gateway.ApplyOffsetResetAsync(groupId,
                new[] { new OffsetChange { Topic = row.Topic, Partition = row.Partition, CurrentOffset = row.Offset.CommittedOffset, NewOffset = target } },
                CancellationToken.None).ConfigureAwait(true);
            await LoadDetailAsync(groupId).ConfigureAwait(true);
            StatusMessage = $"{row.Topic} #{row.Partition} of '{groupId}' now at offset {target}.";
        }
        catch (Exception ex)
        {
            // Most likely the group became active since it was loaded - show why and refresh its state.
            await LoadDetailAsync(groupId).ConfigureAwait(true);
            StatusMessage = $"Offset was not changed: {ex.Message}";
        }
    }
}

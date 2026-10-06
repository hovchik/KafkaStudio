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

/// <summary>A consumer group in the list, with its total lag once that has been looked up.</summary>
public sealed class GroupRowViewModel : ObservableObject
{
    public GroupRowViewModel(ConsumerGroupSummary summary) => Summary = summary;

    public ConsumerGroupSummary Summary { get; }
    public string GroupId => Summary.GroupId;
    public string State => Summary.State;
    public int MemberCount => Summary.MemberCount;

    private long? _lag;
    /// <summary>Total lag over all the group's partitions; null until looked up (or if the lookup failed).</summary>
    public long? Lag
    {
        get => _lag;
        set
        {
            if (!SetProperty(ref _lag, value)) return;
            OnPropertyChanged(nameof(HasLag));
            OnPropertyChanged(nameof(LagText));
        }
    }

    public bool HasLag => Lag is > 0;

    public string LagText => Lag switch
    {
        null => "lag …",
        0 => "no lag",
        var n => $"lag {n:N0}"
    };
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
    private readonly List<GroupRowViewModel> _allGroups = new();

    public ObservableCollection<string> ConnectionNames { get; } = new();
    public ObservableCollection<GroupRowViewModel> Groups { get; } = new();
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

    private bool _onlyWithLag;
    /// <summary>Shows only groups that are behind (and lists the most behind first).</summary>
    public bool OnlyWithLag
    {
        get => _onlyWithLag;
        set { if (SetProperty(ref _onlyWithLag, value)) ApplyGroupFilter(); }
    }

    private bool _rebuildingGroups;

    private GroupRowViewModel? _selectedGroup;
    public GroupRowViewModel? SelectedGroup
    {
        get => _selectedGroup;
        set
        {
            if (_rebuildingGroups) return; // list rebuild pushes transient nulls through the binding
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
        var selected = SelectedGroup;
        var term = GroupFilter?.Trim();
        IEnumerable<GroupRowViewModel> rows = _allGroups
            .Where(g => string.IsNullOrEmpty(term) || g.GroupId.Contains(term, StringComparison.OrdinalIgnoreCase));
        if (OnlyWithLag) rows = rows.Where(g => g.HasLag).OrderByDescending(g => g.Lag).ThenBy(g => g.GroupId, StringComparer.Ordinal);

        _rebuildingGroups = true;
        try
        {
            Groups.Clear();
            foreach (var g in rows) Groups.Add(g);
        }
        finally
        {
            _rebuildingGroups = false;
        }

        // Same row still listed: keep it (and its open detail) selected without reloading anything.
        if (selected is not null && Groups.Contains(selected))
        {
            _selectedGroup = selected;
            OnPropertyChanged(nameof(SelectedGroup));
        }
        else
        {
            SelectedGroup = null;
        }
    }

    /// <summary>
    /// The group listing carries no offsets, so each group's total lag is looked up with a describe call
    /// (a few at a time). Rows update as results arrive, and the "only groups with lag" view follows.
    /// </summary>
    private async Task LoadLagsAsync(KafkaStudio.Core.Abstractions.IKafkaGateway gateway, IReadOnlyList<GroupRowViewModel> rows, CancellationToken token)
    {
        using var gate = new SemaphoreSlim(4);
        async Task One(GroupRowViewModel row)
        {
            await gate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                var detail = await gateway.DescribeConsumerGroupAsync(row.GroupId, token).ConfigureAwait(false);
                _state.PostToUi(() => { if (!token.IsCancellationRequested) row.Lag = detail.TotalLag; });
            }
            catch (OperationCanceledException) { }
            catch
            {
                // This group's offsets could not be read - leave its lag unknown rather than failing the list.
            }
            finally
            {
                gate.Release();
            }
        }

        await Task.WhenAll(rows.Select(One)).ConfigureAwait(true);
        if (token.IsCancellationRequested) return;

        ApplyGroupFilter();
        var behind = _allGroups.Count(g => g.HasLag);
        StatusMessage = $"{_allGroups.Count} consumer group(s), {behind} with lag.";
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
            _allGroups.AddRange(groups.OrderBy(g => g.GroupId, StringComparer.Ordinal).Select(g => new GroupRowViewModel(g)));
            var selectedId = SelectedGroup?.GroupId;
            SelectedGroup = null;
            ApplyGroupFilter();
            StatusMessage = _allGroups.Count == 0 ? "No consumer groups found." : $"{_allGroups.Count} consumer group(s), checking lag...";
            if (_allGroups.Count > 0) _ = LoadLagsAsync(gateway, _allGroups.ToList(), cts.Token);

            // Keep the open group's detail current (its state/lag changed too).
            if (selectedId is not null)
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
            if (_allGroups.FirstOrDefault(g => g.GroupId == detail.GroupId) is { } listed)
            {
                listed.Lag = detail.TotalLag;
                if (OnlyWithLag) ApplyGroupFilter();
            }
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

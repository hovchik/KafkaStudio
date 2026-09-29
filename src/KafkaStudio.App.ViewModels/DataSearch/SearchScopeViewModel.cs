using System.Collections.ObjectModel;
using System.Globalization;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Topics;
using KafkaStudio.Search;

namespace KafkaStudio.App.ViewModels.DataSearch;

/// <summary>An item for a ComboBox bound to an enum-like choice (displays <see cref="Label"/>).</summary>
public sealed record Choice<T>(T Value, string Label)
{
    public override string ToString() => Label;
}

/// <summary>
/// Where the Find Data tools look: the connection, which topics (every topic, a saved topic set, and/or a
/// name filter) and which part of each topic (everything, the last N minutes, a time window, or the
/// newest N messages per partition). Shared by every tab so a scope set once applies everywhere.
/// </summary>
public sealed class SearchScopeViewModel : ObservableObject
{
    private readonly AppState _state;
    private CancellationTokenSource? _topicsCts;
    private readonly List<string> _allTopics = new();

    public ObservableCollection<string> ConnectionNames { get; } = new();

    /// <summary>Every topic on the connection (for the single-topic pickers).</summary>
    public ObservableCollection<string> TopicNames { get; } = new();

    public ObservableCollection<SavedTopicSet> SavedTopicSets { get; } = new();

    public IReadOnlyList<Choice<SearchRangeKind>> RangeKinds { get; } = new[]
    {
        new Choice<SearchRangeKind>(SearchRangeKind.All, "All messages"),
        new Choice<SearchRangeKind>(SearchRangeKind.LastDuration, "Last N minutes"),
        new Choice<SearchRangeKind>(SearchRangeKind.Between, "Between times"),
        new Choice<SearchRangeKind>(SearchRangeKind.NewestPerPartition, "Newest N per partition")
    };

    public SearchScopeViewModel(AppState state)
    {
        _state = state;
        _selectedRangeKind = RangeKinds[0];
        _state.ConnectionsChanged += RefreshConnectionNames;
        _state.SavedTopicSetsChanged += ReloadTopicSets;
        RefreshTopicsCommand = new AsyncRelayCommand(RefreshTopicsAsync, () => SelectedConnection is not null, allowConcurrentExecutions: true);
        ClearTopicSetCommand = new RelayCommand(() => SelectedTopicSet = null);
        ReloadTopicSets();
        RefreshConnectionNames();
    }

    public AsyncRelayCommand RefreshTopicsCommand { get; }
    public RelayCommand ClearTopicSetCommand { get; }

    /// <summary>Raised (UI thread) after the topic list for the connection has been (re)loaded.</summary>
    public event Action? TopicsLoaded;

    private string? _selectedConnection;
    public string? SelectedConnection
    {
        get => _selectedConnection;
        set
        {
            if (SetProperty(ref _selectedConnection, value))
            {
                RefreshTopicsCommand.RaiseCanExecuteChanged();
                _ = RefreshTopicsAsync();
            }
        }
    }

    private SavedTopicSet? _selectedTopicSet;
    /// <summary>When set, only this set's topics are scanned (null = every topic).</summary>
    public SavedTopicSet? SelectedTopicSet
    {
        get => _selectedTopicSet;
        set { if (SetProperty(ref _selectedTopicSet, value)) OnPropertyChanged(nameof(ScopeSummary)); }
    }

    private string? _topicNameFilter;
    /// <summary>Only topics whose name contains this (case-insensitive); empty = no name filter.</summary>
    public string? TopicNameFilter
    {
        get => _topicNameFilter;
        set { if (SetProperty(ref _topicNameFilter, value)) OnPropertyChanged(nameof(ScopeSummary)); }
    }

    private bool _isLoadingTopics;
    public bool IsLoadingTopics { get => _isLoadingTopics; private set => SetProperty(ref _isLoadingTopics, value); }

    private string? _topicsError;
    public string? TopicsError { get => _topicsError; private set => SetProperty(ref _topicsError, value); }

    // ------------------------------------------------------------------ range ----

    private Choice<SearchRangeKind> _selectedRangeKind;
    public Choice<SearchRangeKind> SelectedRangeKind
    {
        get => _selectedRangeKind;
        set
        {
            if (SetProperty(ref _selectedRangeKind, value ?? RangeKinds[0]))
            {
                OnPropertyChanged(nameof(IsLastDuration));
                OnPropertyChanged(nameof(IsBetween));
                OnPropertyChanged(nameof(IsNewest));
                OnPropertyChanged(nameof(ScopeSummary));
            }
        }
    }

    public bool IsLastDuration => SelectedRangeKind.Value == SearchRangeKind.LastDuration;
    public bool IsBetween => SelectedRangeKind.Value == SearchRangeKind.Between;
    public bool IsNewest => SelectedRangeKind.Value == SearchRangeKind.NewestPerPartition;

    private double _lastMinutes = 60;
    public double LastMinutes { get => _lastMinutes; set { if (SetProperty(ref _lastMinutes, Math.Max(0.1, value))) OnPropertyChanged(nameof(ScopeSummary)); } }

    private string? _fromText;
    /// <summary>Start of the window, local time (<c>2026-01-10 14:30</c>); empty = the beginning.</summary>
    public string? FromText { get => _fromText; set { if (SetProperty(ref _fromText, value)) OnPropertyChanged(nameof(ScopeSummary)); } }

    private string? _toText;
    /// <summary>End of the window, local time; empty = now.</summary>
    public string? ToText { get => _toText; set { if (SetProperty(ref _toText, value)) OnPropertyChanged(nameof(ScopeSummary)); } }

    private int _newestCount = 1000;
    public int NewestCount { get => _newestCount; set { if (SetProperty(ref _newestCount, Math.Max(1, value))) OnPropertyChanged(nameof(ScopeSummary)); } }

    /// <summary>"12 topic(s) · last 15 minute(s)" - shown next to every Run button.</summary>
    public string ScopeSummary
    {
        get
        {
            var topics = GetScopeTopics().Count;
            var range = TryGetRange(out var r, out var error) ? r.Describe() : error;
            var set = SelectedTopicSet is not null ? $" in set '{SelectedTopicSet.Name}'" : "";
            return $"{topics} topic(s){set} · {range}";
        }
    }

    /// <summary>The topics in scope: the saved set (or every topic), narrowed by the name filter.</summary>
    /// <summary>True once the connection's topic list has been loaded (it may legitimately be empty).</summary>
    public bool HasTopics => _allTopics.Count > 0;

    public IReadOnlyList<string> GetScopeTopics()
    {
        IEnumerable<string> topics = SelectedTopicSet is not null ? SelectedTopicSet.Topics : _allTopics;
        var filter = TopicNameFilter?.Trim();
        if (!string.IsNullOrEmpty(filter)) topics = topics.Where(t => t.Contains(filter, StringComparison.OrdinalIgnoreCase));
        return topics.Distinct(StringComparer.Ordinal).ToList();
    }

    public bool TryGetRange(out SearchRange range, out string error)
    {
        error = "";
        range = SearchRange.All;
        switch (SelectedRangeKind.Value)
        {
            case SearchRangeKind.LastDuration:
                range = SearchRange.LastPeriod(TimeSpan.FromMinutes(LastMinutes));
                return true;
            case SearchRangeKind.NewestPerPartition:
                range = SearchRange.Newest(NewestCount);
                return true;
            case SearchRangeKind.Between:
                if (!TryParseTime(FromText, out var from)) { error = $"can't read the start time '{FromText}' (use e.g. 2026-01-10 14:30)"; return false; }
                if (!TryParseTime(ToText, out var to)) { error = $"can't read the end time '{ToText}' (use e.g. 2026-01-10 15:00)"; return false; }
                if (from is not null && to is not null && to < from) { error = "the end time is before the start time"; return false; }
                range = SearchRange.Between(from, to);
                return true;
            default:
                return true;
        }
    }

    /// <summary>The From/To texts parsed (null where empty or unreadable).</summary>
    public void TryParseTimes(out DateTimeOffset? from, out DateTimeOffset? to)
    {
        if (!TryParseTime(FromText, out from)) from = null;
        if (!TryParseTime(ToText, out to)) to = null;
    }

    /// <summary>Parses a local date/time (or an ISO timestamp with offset); empty text means "no bound".</summary>
    public static bool TryParseTime(string? text, out DateTimeOffset? value)
    {
        value = null;
        if (string.IsNullOrWhiteSpace(text)) return true;
        if (DateTimeOffset.TryParse(text.Trim(), CultureInfo.CurrentCulture, DateTimeStyles.AssumeLocal, out var parsed) ||
            DateTimeOffset.TryParse(text.Trim(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out parsed))
        {
            value = parsed;
            return true;
        }
        return false;
    }

    /// <summary>The live gateway for <see cref="SelectedConnection"/>, or null (with a reason).</summary>
    public IKafkaGateway? GetGateway(out string? error)
    {
        error = null;
        if (SelectedConnection is null)
        {
            error = "Pick a connection first.";
            return null;
        }
        if (!_state.Connections.TryGetValue(SelectedConnection, out var gateway))
        {
            error = $"Connection '{SelectedConnection}' isn't connected.";
            return null;
        }
        return gateway;
    }

    /// <summary>Everything a run needs (gateway, topics, range), or an error explaining what's missing.</summary>
    public bool TryGetScope(out IKafkaGateway gateway, out IReadOnlyList<string> topics, out SearchRange range, out string error)
    {
        topics = Array.Empty<string>();
        range = SearchRange.All;
        gateway = GetGateway(out var gatewayError)!;
        if (gateway is null)
        {
            error = gatewayError!;
            return false;
        }
        if (!TryGetRange(out range, out error)) return false;
        topics = GetScopeTopics();
        if (topics.Count == 0)
        {
            error = _allTopics.Count == 0 ? "No topics listed for this connection - refresh topics." : "No topics match the scope.";
            return false;
        }
        return true;
    }

    private void RefreshConnectionNames()
    {
        CollectionSync.SyncSorted(ConnectionNames, _state.Connections.Keys);
        if (SelectedConnection is not null && !ConnectionNames.Contains(SelectedConnection)) SelectedConnection = null;
        if (SelectedConnection is null && ConnectionNames.Count > 0) SelectedConnection = ConnectionNames[0];
    }

    private void ReloadTopicSets()
    {
        var selected = SelectedTopicSet?.Name;
        SavedTopicSets.Clear();
        foreach (var set in SavedTopicSetStore.Load()) SavedTopicSets.Add(set);
        SelectedTopicSet = SavedTopicSets.FirstOrDefault(s => s.Name == selected);
    }

    /// <summary>Selects a saved topic set by name (used when applying a saved search).</summary>
    public void SelectTopicSet(string? name) =>
        SelectedTopicSet = name is null ? null : SavedTopicSets.FirstOrDefault(s => s.Name == name);

    public async Task RefreshTopicsAsync()
    {
        _topicsCts?.Cancel();
        var cts = new CancellationTokenSource();
        _topicsCts = cts;

        var gateway = SelectedConnection is not null && _state.Connections.TryGetValue(SelectedConnection, out var g) ? g : null;
        if (gateway is null)
        {
            _allTopics.Clear();
            TopicNames.Clear();
            OnPropertyChanged(nameof(ScopeSummary));
            return;
        }

        IsLoadingTopics = true;
        TopicsError = null;
        try
        {
            var names = await gateway.ListTopicsAsync(cts.Token).ConfigureAwait(true);
            if (cts.IsCancellationRequested) return;
            _allTopics.Clear();
            _allTopics.AddRange(names.OrderBy(n => n, StringComparer.OrdinalIgnoreCase));
            CollectionSync.SyncSorted(TopicNames, _allTopics);
            OnPropertyChanged(nameof(ScopeSummary));
            TopicsLoaded?.Invoke();
        }
        catch (OperationCanceledException)
        {
            // superseded
        }
        catch (Exception ex)
        {
            if (!cts.IsCancellationRequested) TopicsError = $"Failed to load topics: {ex.Message}";
        }
        finally
        {
            if (ReferenceEquals(_topicsCts, cts)) IsLoadingTopics = false;
        }
    }
}

/// <summary>
/// Base for a Find Data tab that runs one cancellable scan at a time: tracks running state, shows live
/// progress (topics / messages scanned) while it runs, and reports errors in <see cref="Status"/>.
/// </summary>
public abstract class ScanTabViewModel : ObservableObject
{
    protected readonly AppState State;
    protected readonly SearchScopeViewModel Scope;
    private readonly Action<KafkaMessage?> _showMessage;
    private CancellationTokenSource? _cts;

    protected ScanTabViewModel(AppState state, SearchScopeViewModel scope, Action<KafkaMessage?> showMessage)
    {
        State = state;
        Scope = scope;
        _showMessage = showMessage;
        CancelCommand = new RelayCommand(Cancel, () => IsRunning);
    }

    public RelayCommand CancelCommand { get; }

    private bool _isRunning;
    public bool IsRunning
    {
        get => _isRunning;
        private set
        {
            if (SetProperty(ref _isRunning, value))
            {
                CancelCommand.RaiseCanExecuteChanged();
                OnRunningChanged();
            }
        }
    }

    private string? _status;
    public string? Status { get => _status; protected set => SetProperty(ref _status, value); }

    private string? _progress;
    /// <summary>"3 / 12 topics · 120,442 messages" while a scan runs.</summary>
    public string? Progress { get => _progress; private set => SetProperty(ref _progress, value); }

    /// <summary>Lets subclasses re-evaluate their Run commands.</summary>
    protected virtual void OnRunningChanged() { }

    protected void ShowMessage(KafkaMessage? message) => _showMessage(message);

    /// <summary>Shows a message from a shared action (copy/export) in this tab's status line.</summary>
    public void ReportStatus(string status) => Status = status;

    private void Cancel()
    {
        var cts = _cts;
        if (cts is null) return;
        Status = "Cancelling…";
        _ = Task.Run(() => cts.Cancel()); // Cancel() runs consumer callbacks synchronously - keep the UI responsive
    }

    /// <summary>Runs <paramref name="work"/> with a fresh cancellation token and a progress pump; any
    /// exception ends up in <see cref="Status"/>. <paramref name="onTick"/> runs on the UI thread every
    /// ~250 ms while the scan runs (used to stream partial results in).</summary>
    protected async Task RunScanAsync(int topicsTotal, Func<CancellationToken, ScanProgress, Task> work, Action? onTick = null)
    {
        _cts?.Cancel();
        var cts = new CancellationTokenSource();
        _cts = cts;
        var progress = new ScanProgress { TopicsTotal = topicsTotal };
        IsRunning = true;
        Progress = $"0 / {topicsTotal} topic(s)…";

        using var timer = new Timer(_ => State.PostToUi(() =>
        {
            if (!ReferenceEquals(_cts, cts) || !IsRunning) return;
            Progress = $"{progress.TopicsScanned} / {topicsTotal} topic(s) · {progress.MessagesScanned:N0} message(s) scanned";
            onTick?.Invoke();
        }), null, TimeSpan.FromMilliseconds(250), TimeSpan.FromMilliseconds(250));

        try
        {
            await work(cts.Token, progress).ConfigureAwait(true);
        }
        catch (OperationCanceledException)
        {
            if (ReferenceEquals(_cts, cts)) Status = "Cancelled.";
        }
        catch (Exception ex)
        {
            if (ReferenceEquals(_cts, cts)) Status = $"Failed: {ex.Message}";
        }
        finally
        {
            if (ReferenceEquals(_cts, cts))
            {
                IsRunning = false;
                Progress = null;
                _cts = null;
            }
        }
    }
}

using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Topics;
using KafkaStudio.Search;

namespace KafkaStudio.App.ViewModels.Topics;

public sealed class TopicRowViewModel : ObservableObject
{
    public required string Name { get; init; }

    private int? _partitionCount;
    /// <summary>Null until this topic is opened and its metadata has been fetched.</summary>
    public int? PartitionCount { get => _partitionCount; set => SetProperty(ref _partitionCount, value); }

    private long? _totalMessageCount;
    public long? TotalMessageCount { get => _totalMessageCount; set => SetProperty(ref _totalMessageCount, value); }
}

/// <summary>A message found by <see cref="TopicBrowserViewModel.GlobalSearchCommand"/>, tagged with the
/// topic it came from so mixed-topic results are still identifiable in a single list.</summary>
public sealed class GlobalSearchHit
{
    public required string Topic { get; init; }
    public required KafkaMessage Message { get; init; }
}

/// <summary>Number of <see cref="TopicBrowserViewModel.GlobalSearchResults"/> hits found on a single topic,
/// used to show a per-topic breakdown alongside the overall total. Clicking its chip in the view toggles
/// <see cref="IsSelected"/> and filters the results list down to just this topic.</summary>
public sealed class TopicHitCount : ObservableObject
{
    public required string Topic { get; init; }

    private int _count;
    public int Count { get => _count; set => SetProperty(ref _count, value); }

    private bool _isSelected;
    public bool IsSelected { get => _isSelected; set => SetProperty(ref _isSelected, value); }
}

/// <summary>A message pinned into the "Compare messages" section, for side-by-side inspection.</summary>
public sealed class ComparisonEntry
{
    public required string Topic { get; init; }
    public required KafkaMessage Message { get; init; }

    /// <summary>"topic #p@o key" - how the entry is named in the diff side pickers.</summary>
    public string Label => $"{Topic} #{Message.Partition}@{Message.Offset}{(Message.Key is null ? "" : "  " + Message.Key)}";
}

/// <summary>Lists topics for the selected connection and lets you browse a topic's messages (newest
/// first), search across topics, compare messages side by side, export, and create topics.
///
/// Topics are (re)loaded automatically whenever <see cref="SelectedConnection"/> changes - just their
/// names, via a single ListTopicsAsync call, so the list populates immediately even for clusters with
/// many topics. Double-clicking (or pressing Enter on) a topic opens it: its partition/message counts
/// and its newest <see cref="ScanLimit"/> messages load in the background. Every load swaps in a fresh
/// <see cref="CancellationTokenSource"/>, so a rapid connection/topic switch cancels the now-stale load
/// instead of racing it.</summary>
public sealed class TopicBrowserViewModel : ObservableObject
{
    /// <summary>Cross-topic search stops collecting after this many hits, so a too-broad term can't
    /// exhaust memory or freeze the UI.</summary>
    public const int MaxGlobalSearchHits = 5_000;

    private readonly AppState _state;
    private CancellationTokenSource? _topicsLoadCts;
    private CancellationTokenSource? _messagesLoadCts;
    private CancellationTokenSource? _globalSearchCts;
    private readonly List<TopicRowViewModel> _allTopics = new();
    private readonly List<KafkaMessage> _allScannedMessages = new();
    private readonly Dictionary<string, TopicHitCount> _hitCountsByTopic = new(StringComparer.Ordinal);

    public ObservableCollection<string> ConnectionNames { get; } = new();
    public ObservableCollection<TopicRowViewModel> Topics { get; } = new();
    public ObservableCollection<KafkaMessage> ScannedMessages { get; } = new();
    public ObservableCollection<GlobalSearchHit> GlobalSearchResults { get; } = new();

    /// <summary>Per-topic breakdown of <see cref="GlobalSearchResults"/>, kept in sync as results stream in.</summary>
    public ObservableCollection<TopicHitCount> GlobalSearchResultsByTopic { get; } = new();

    /// <summary><see cref="GlobalSearchResults"/> narrowed down to <see cref="SelectedGlobalSearchTopicFilter"/>
    /// (or every hit, when no topic filter is active). This is what the results list actually binds to.</summary>
    public ObservableCollection<GlobalSearchHit> FilteredGlobalSearchResults { get; } = new();

    /// <summary>Named sets of topics captured from previous search results, persisted across restarts.</summary>
    public ObservableCollection<SavedTopicSet> SavedTopicSets { get; } = new();

    /// <summary>Messages pinned for side-by-side comparison.</summary>
    public ObservableCollection<ComparisonEntry> ComparisonMessages { get; } = new();

    /// <summary>Topic filter terms the user has saved, persisted across restarts.</summary>
    public ObservableCollection<string> SavedTopicFilters { get; } = new();

    public MessageActions Actions { get; }

    // ------------------------------------------------------------------ connection & topics ----

    private string? _selectedConnection;
    public string? SelectedConnection
    {
        get => _selectedConnection;
        set
        {
            if (SetProperty(ref _selectedConnection, value))
            {
                // Everything shown belongs to the previous cluster - drop it and cancel its loads.
                _messagesLoadCts?.Cancel();
                _globalSearchCts?.Cancel();
                SelectedTopicRow = null;
                _allTopics.Clear();
                Topics.Clear();
                ClearMessages();
                CloseSearchResults();
                GlobalSearchResults.Clear();
                IsLoadingMessages = false;
                _ = RefreshTopicsAsync();
                RaiseConnectionCommands();
            }
        }
    }

    private string? _topicFilter;
    /// <summary>Case-insensitive "contains" filter applied to <see cref="Topics"/> against the full set
    /// of loaded topic names.</summary>
    public string? TopicFilter
    {
        get => _topicFilter;
        set
        {
            if (SetProperty(ref _topicFilter, value))
            {
                ApplyTopicFilter();
                SaveTopicFilterCommand?.RaiseCanExecuteChanged();
            }
        }
    }

    private string? _selectedSavedTopicFilter;
    /// <summary>Selecting a saved filter applies it to <see cref="TopicFilter"/>.</summary>
    public string? SelectedSavedTopicFilter
    {
        get => _selectedSavedTopicFilter;
        set
        {
            if (SetProperty(ref _selectedSavedTopicFilter, value))
            {
                if (value is not null) TopicFilter = value;
                RemoveSavedTopicFilterCommand?.RaiseCanExecuteChanged();
            }
        }
    }

    private TopicRowViewModel? _selectedTopicRow;
    /// <summary>Bound to the topics list's selection. Selecting a row alone doesn't load messages -
    /// double-click / Enter (<see cref="OpenTopicCommand"/>) or "Load" does, so browsing the list
    /// with the keyboard doesn't spam the broker.</summary>
    public TopicRowViewModel? SelectedTopicRow
    {
        get => _selectedTopicRow;
        set
        {
            if (SetProperty(ref _selectedTopicRow, value))
            {
                OnPropertyChanged(nameof(SelectedTopic));
                ScanCommand.RaiseCanExecuteChanged();
            }
        }
    }

    public string? SelectedTopic => SelectedTopicRow?.Name;

    private string? _loadedTopic;
    /// <summary>The topic whose messages are currently shown (may differ from the selected row).</summary>
    public string? LoadedTopic { get => _loadedTopic; private set => SetProperty(ref _loadedTopic, value); }

    private int? _scanLimit = 50;
    /// <summary>How many of the newest messages to load. Empty/null loads the whole topic.</summary>
    public int? ScanLimit
    {
        get => _scanLimit;
        set => SetProperty(ref _scanLimit, value is { } v && v <= 0 ? null : value);
    }

    // ------------------------------------------------------------------ messages ----

    private string? _messageFilter;
    /// <summary>Filter over the loaded messages: plain text is a case-insensitive "contains" over key,
    /// value and headers; a structured query (<c>$.status = FAILED and key starts with ORD-</c>, see
    /// <see cref="MessageQuery"/>) filters on fields.</summary>
    public string? MessageFilter
    {
        get => _messageFilter;
        set
        {
            if (SetProperty(ref _messageFilter, value)) ApplyMessageFilter();
        }
    }

    private string? _messageFilterError;
    /// <summary>Why <see cref="MessageFilter"/> couldn't be parsed (null when it's fine).</summary>
    public string? MessageFilterError { get => _messageFilterError; private set => SetProperty(ref _messageFilterError, value); }

    private KafkaMessage? _selectedMessage;
    /// <summary>The message shown in the detail pane.</summary>
    public KafkaMessage? SelectedMessage
    {
        get => _selectedMessage;
        set
        {
            if (SetProperty(ref _selectedMessage, value))
            {
                AddSelectedToComparisonCommand?.RaiseCanExecuteChanged();
                DeleteSelectedCommand?.RaiseCanExecuteChanged();
                CancelDeleteCommand?.Execute(null);
            }
        }
    }

    // ------------------------------------------------------------------ delete message ----
    // Kafka can't remove one record from a log. The only per-message delete is a tombstone (null value)
    // for the record's key on a compacted topic, which makes compaction drop that key's records.

    private bool _isDeleteConfirmPending;
    /// <summary>True while the inline "are you sure?" panel for deleting the selected message is shown.</summary>
    public bool IsDeleteConfirmPending
    {
        get => _isDeleteConfirmPending;
        private set
        {
            if (SetProperty(ref _isDeleteConfirmPending, value)) ConfirmDeleteCommand?.RaiseCanExecuteChanged();
        }
    }

    private string? _deletePanelText;
    /// <summary>Confirmation question, or the reason this message can't be deleted. Null hides the panel.</summary>
    public string? DeletePanelText
    {
        get => _deletePanelText;
        private set { if (SetProperty(ref _deletePanelText, value)) OnPropertyChanged(nameof(HasDeletePanel)); }
    }

    public bool HasDeletePanel => DeletePanelText is not null;

    private KafkaMessage? _deleteTarget;

    /// <summary>Step 1: checks that the selected message can be deleted and asks for confirmation (or explains why not).</summary>
    public AsyncRelayCommand DeleteSelectedCommand { get; private set; } = null!;
    /// <summary>Step 2: produces the tombstone for the selected message's key.</summary>
    public AsyncRelayCommand ConfirmDeleteCommand { get; private set; } = null!;
    public RelayCommand CancelDeleteCommand { get; private set; } = null!;

    private void InitDeleteCommands()
    {
        DeleteSelectedCommand = new AsyncRelayCommand(PrepareDeleteAsync,
            () => SelectedMessage is not null && SelectedConnection is not null);
        CancelDeleteCommand = new RelayCommand(() =>
        {
            _deleteTarget = null;
            IsDeleteConfirmPending = false;
            DeletePanelText = null;
        });
        ConfirmDeleteCommand = new AsyncRelayCommand(ConfirmDeleteAsync, () => IsDeleteConfirmPending);
    }

    private async Task PrepareDeleteAsync()
    {
        CancelDeleteCommand.Execute(null);
        if (SelectedMessage is not { } m || SelectedConnection is null ||
            !_state.Connections.TryGetValue(SelectedConnection, out var gateway))
        {
            return;
        }

        if (m.IsTombstone)
        {
            DeletePanelText = "This message is already a tombstone (delete marker); there is nothing to delete.";
            return;
        }
        if (string.IsNullOrEmpty(m.Key))
        {
            DeletePanelText = "This message has no key. Kafka can only delete a message by key (with a tombstone on a compacted topic), so it can't be deleted.";
            return;
        }

        try
        {
            if (!await gateway.IsTopicCompactedAsync(m.Topic).ConfigureAwait(true))
            {
                DeletePanelText = $"Kafka can't delete a single message from '{m.Topic}'. " +
                    "Deleting works only on compacted topics (cleanup.policy=compact), where a tombstone for the key removes it.";
                return;
            }
        }
        catch (Exception ex)
        {
            DeletePanelText = $"Couldn't check whether '{m.Topic}' is compacted: {ex.Message}";
            return;
        }

        _deleteTarget = m;
        DeletePanelText = $"Delete the message with key \"{m.Key}\" from '{m.Topic}'? This sends a tombstone for that key; " +
            "once Kafka compacts the topic, every message with this key before the tombstone is removed, not just this one. This can't be undone.";
        IsDeleteConfirmPending = true;
    }

    private async Task ConfirmDeleteAsync()
    {
        var target = _deleteTarget;
        CancelDeleteCommand.Execute(null);
        if (target is null || SelectedConnection is null ||
            !_state.Connections.TryGetValue(SelectedConnection, out var gateway))
        {
            return;
        }

        StatusMessage = $"Sending tombstone for key \"{target.Key}\" to '{target.Topic}'...";
        try
        {
            var receipt = await gateway.ProduceAsync(new ProduceRequest
            {
                Topic = target.Topic,
                Key = target.Key,
                Value = null,
                Partition = target.Partition
            }).ConfigureAwait(true);
            StatusMessage = $"Tombstone for key \"{target.Key}\" written to '{target.Topic}' [{receipt.Partition}] at offset {receipt.Offset}. " +
                "The message disappears once Kafka compacts the topic.";
            if (SelectedTopic == target.Topic) await LoadMessagesAsync(null).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            StatusMessage = $"Delete failed: {ex.Message}";
        }
    }

    private GlobalSearchHit? _selectedGlobalSearchHit;
    /// <summary>The highlighted search result row; selecting one shows its message in the detail pane.</summary>
    public GlobalSearchHit? SelectedGlobalSearchHit
    {
        get => _selectedGlobalSearchHit;
        set { if (SetProperty(ref _selectedGlobalSearchHit, value) && value is not null) SelectedMessage = value.Message; }
    }

    private int _matchedMessageCount;
    public int MatchedMessageCount { get => _matchedMessageCount; private set => SetProperty(ref _matchedMessageCount, value); }

    private int _totalMessageCount;
    /// <summary>Total number of messages loaded for the current topic, before filtering.</summary>
    public int TotalMessageCount { get => _totalMessageCount; private set => SetProperty(ref _totalMessageCount, value); }

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    private bool _isLoadingTopics;
    public bool IsLoadingTopics { get => _isLoadingTopics; private set => SetProperty(ref _isLoadingTopics, value); }

    private bool _isLoadingMessages;
    public bool IsLoadingMessages
    {
        get => _isLoadingMessages;
        private set
        {
            if (SetProperty(ref _isLoadingMessages, value)) CancelLoadCommand?.RaiseCanExecuteChanged();
        }
    }

    // ------------------------------------------------------------------ create topic ----

    private bool _isCreateTopicOpen;
    public bool IsCreateTopicOpen { get => _isCreateTopicOpen; set => SetProperty(ref _isCreateTopicOpen, value); }

    private string _newTopicName = "";
    public string NewTopicName
    {
        get => _newTopicName;
        set { if (SetProperty(ref _newTopicName, value ?? "")) CreateTopicCommand.RaiseCanExecuteChanged(); }
    }

    private int _newTopicPartitions = 1;
    public int NewTopicPartitions { get => _newTopicPartitions; set => SetProperty(ref _newTopicPartitions, Math.Clamp(value, 1, 10_000)); }

    private int _newTopicReplicationFactor = 1;
    public int NewTopicReplicationFactor { get => _newTopicReplicationFactor; set => SetProperty(ref _newTopicReplicationFactor, Math.Clamp(value, 1, short.MaxValue)); }

    // ------------------------------------------------------------------ cross-topic search ----

    private string? _globalSearchTerm;
    /// <summary>Case-insensitive "contains" search executed by <see cref="GlobalSearchCommand"/> against
    /// every listed topic's message backlog (key, value and headers).</summary>
    public string? GlobalSearchTerm
    {
        get => _globalSearchTerm;
        set
        {
            if (SetProperty(ref _globalSearchTerm, value)) GlobalSearchCommand.RaiseCanExecuteChanged();
        }
    }

    private bool _isGlobalSearching;
    public bool IsGlobalSearching
    {
        get => _isGlobalSearching;
        private set
        {
            if (SetProperty(ref _isGlobalSearching, value))
            {
                CancelGlobalSearchCommand.RaiseCanExecuteChanged();
                GlobalSearchCommand.RaiseCanExecuteChanged();
            }
        }
    }

    private bool _isGlobalSearchActive;
    /// <summary>True while the search results replace the message list. Closing them (or opening a
    /// hit) brings the message list back; results are kept so "Back to results" can reopen them.</summary>
    public bool IsGlobalSearchActive
    {
        get => _isGlobalSearchActive;
        private set
        {
            if (SetProperty(ref _isGlobalSearchActive, value)) ShowSearchResultsCommand?.RaiseCanExecuteChanged();
        }
    }

    public bool HasGlobalSearchResults => GlobalSearchResults.Count > 0;

    private int _globalSearchTopicsScanned;
    public int GlobalSearchTopicsScanned { get => _globalSearchTopicsScanned; private set => SetProperty(ref _globalSearchTopicsScanned, value); }

    private int _globalSearchTopicsTotal;
    public int GlobalSearchTopicsTotal { get => _globalSearchTopicsTotal; private set => SetProperty(ref _globalSearchTopicsTotal, value); }

    private string? _selectedGlobalSearchTopicFilter;
    /// <summary>When set (by clicking a topic chip), <see cref="FilteredGlobalSearchResults"/> only
    /// shows hits from this topic. Clicking the same chip again clears the filter.</summary>
    public string? SelectedGlobalSearchTopicFilter
    {
        get => _selectedGlobalSearchTopicFilter;
        private set => SetProperty(ref _selectedGlobalSearchTopicFilter, value);
    }

    private SavedTopicSet? _selectedSavedTopicSet;
    /// <summary>When set, a cross-topic search only scans this set's topics instead of every listed topic.</summary>
    public SavedTopicSet? SelectedSavedTopicSet
    {
        get => _selectedSavedTopicSet;
        set
        {
            if (SetProperty(ref _selectedSavedTopicSet, value)) DeleteSavedTopicSetCommand.RaiseCanExecuteChanged();
        }
    }

    private string? _newTopicSetName;
    public string? NewTopicSetName
    {
        get => _newTopicSetName;
        set
        {
            if (SetProperty(ref _newTopicSetName, value)) SaveSearchResultsAsTopicSetCommand.RaiseCanExecuteChanged();
        }
    }

    // ------------------------------------------------------------------ commands ----

    public RelayCommand SaveTopicFilterCommand { get; }
    public RelayCommand RemoveSavedTopicFilterCommand { get; }
    public RelayCommand<TopicHitCount> ToggleGlobalSearchTopicFilterCommand { get; }
    public RelayCommand SaveSearchResultsAsTopicSetCommand { get; }
    public RelayCommand DeleteSavedTopicSetCommand { get; }
    public RelayCommand ClearTopicSetSelectionCommand { get; }
    public RelayCommand<GlobalSearchHit> AddToComparisonCommand { get; }
    public RelayCommand AddSelectedToComparisonCommand { get; }
    public RelayCommand<ComparisonEntry> RemoveFromComparisonCommand { get; }
    public RelayCommand ClearComparisonCommand { get; }
    public RelayCommand ToggleDiffCommand { get; }
    public RelayCommand SwapDiffSidesCommand { get; }
    public AsyncRelayCommand ExportMessagesCsvCommand { get; }
    public AsyncRelayCommand ExportSearchResultsCommand { get; }
    public AsyncRelayCommand ExportSearchResultsCsvCommand { get; }
    public AsyncRelayCommand RefreshTopicsCommand { get; }
    public AsyncRelayCommand ScanCommand { get; }
    public RelayCommand CancelLoadCommand { get; }
    public AsyncRelayCommand ExportMessagesCommand { get; }
    public AsyncRelayCommand GlobalSearchCommand { get; }
    public RelayCommand CancelGlobalSearchCommand { get; }
    public RelayCommand CloseSearchResultsCommand { get; }
    public RelayCommand ShowSearchResultsCommand { get; }
    public RelayCommand ToggleCreateTopicCommand { get; }
    public AsyncRelayCommand CreateTopicCommand { get; }

    /// <summary>Opens a topic (selects it and loads its messages) regardless of what's currently selected.</summary>
    public AsyncRelayCommand<TopicRowViewModel> OpenTopicCommand { get; }

    /// <summary>Jumps to a search hit's topic, loads it, and selects the hit in the message list.</summary>
    public AsyncRelayCommand<GlobalSearchHit> OpenGlobalSearchHitCommand { get; }

    public TopicBrowserViewModel(AppState state)
    {
        _state = state;
        _state.ConnectionsChanged += RefreshConnectionNames;
        Actions = new MessageActions(state, () => SelectedConnection, s => StatusMessage = s);
        InitDeleteCommands();

        RefreshTopicsCommand = new AsyncRelayCommand(RefreshTopicsAsync, () => SelectedConnection is not null, allowConcurrentExecutions: true);
        ScanCommand = new AsyncRelayCommand(ScanAsync, () => SelectedConnection is not null && SelectedTopic is not null, allowConcurrentExecutions: true);
        CancelLoadCommand = new RelayCommand(() => _messagesLoadCts?.Cancel(), () => IsLoadingMessages);
        OpenTopicCommand = new AsyncRelayCommand<TopicRowViewModel>(OpenTopicAsync, allowConcurrentExecutions: true);
        OpenGlobalSearchHitCommand = new AsyncRelayCommand<GlobalSearchHit>(OpenGlobalSearchHitAsync, allowConcurrentExecutions: true);
        ExportMessagesCommand = new AsyncRelayCommand(
            () => Actions.ExportAsync(ScannedMessages.ToList(), $"{MessageActions.SafeFileName(LoadedTopic ?? "messages")}.json"));
        ExportMessagesCsvCommand = new AsyncRelayCommand(
            () => Actions.ExportCsvAsync(ScannedMessages.ToList(), $"{MessageActions.SafeFileName(LoadedTopic ?? "messages")}.csv"));
        ExportSearchResultsCommand = new AsyncRelayCommand(
            () => Actions.ExportAsync(FilteredGlobalSearchResults.Select(h => h.Message).ToList(), "search-results.json"));
        ExportSearchResultsCsvCommand = new AsyncRelayCommand(
            () => Actions.ExportCsvAsync(FilteredGlobalSearchResults.Select(h => h.Message).ToList(), "search-results.csv"));

        GlobalSearchCommand = new AsyncRelayCommand(GlobalSearchAsync,
            () => SelectedConnection is not null && !IsGlobalSearching && !string.IsNullOrWhiteSpace(GlobalSearchTerm));
        CancelGlobalSearchCommand = new RelayCommand(CancelGlobalSearch, () => IsGlobalSearching);
        CloseSearchResultsCommand = new RelayCommand(CloseSearchResults);
        ShowSearchResultsCommand = new RelayCommand(() => IsGlobalSearchActive = true, () => !IsGlobalSearchActive && HasGlobalSearchResults);
        ToggleGlobalSearchTopicFilterCommand = new RelayCommand<TopicHitCount>(ToggleGlobalSearchTopicFilter);

        SaveTopicFilterCommand = new RelayCommand(SaveTopicFilter,
            () => !string.IsNullOrWhiteSpace(TopicFilter) && !SavedTopicFilters.Contains(TopicFilter.Trim()));
        RemoveSavedTopicFilterCommand = new RelayCommand(RemoveSavedTopicFilter, () => SelectedSavedTopicFilter is not null);
        SaveSearchResultsAsTopicSetCommand = new RelayCommand(SaveSearchResultsAsTopicSet,
            () => !string.IsNullOrWhiteSpace(NewTopicSetName) && GlobalSearchResults.Count > 0);
        DeleteSavedTopicSetCommand = new RelayCommand(DeleteSavedTopicSet, () => SelectedSavedTopicSet is not null);
        ClearTopicSetSelectionCommand = new RelayCommand(() => SelectedSavedTopicSet = null);

        AddToComparisonCommand = new RelayCommand<GlobalSearchHit>(hit => { if (hit is not null) AddToComparison(hit.Topic, hit.Message); });
        AddSelectedToComparisonCommand = new RelayCommand(
            () => { if (SelectedMessage is { } m) AddToComparison(m.Topic, m); },
            () => SelectedMessage is not null);
        RemoveFromComparisonCommand = new RelayCommand<ComparisonEntry>(entry => { if (entry is not null) ComparisonMessages.Remove(entry); });
        ClearComparisonCommand = new RelayCommand(ComparisonMessages.Clear);
        ToggleDiffCommand = new RelayCommand(() => IsDiffOpen = !IsDiffOpen, () => ComparisonMessages.Count >= 2);
        SwapDiffSidesCommand = new RelayCommand(() => (DiffLeft, DiffRight) = (DiffRight, DiffLeft));
        ComparisonMessages.CollectionChanged += (_, _) => OnComparisonChanged();
        _state.SavedTopicSetsChanged += ReloadSavedTopicSets;

        ToggleCreateTopicCommand = new RelayCommand(() => IsCreateTopicOpen = !IsCreateTopicOpen);
        CreateTopicCommand = new AsyncRelayCommand(CreateTopicAsync,
            () => SelectedConnection is not null && IsValidTopicName(NewTopicName.Trim()));

        foreach (var filter in SavedTopicFilterStore.Load()) SavedTopicFilters.Add(filter);
        foreach (var set in SavedTopicSetStore.Load()) SavedTopicSets.Add(set);

        GlobalSearchResults.CollectionChanged += (_, _) =>
        {
            OnPropertyChanged(nameof(HasGlobalSearchResults));
            ShowSearchResultsCommand.RaiseCanExecuteChanged();
            SaveSearchResultsAsTopicSetCommand.RaiseCanExecuteChanged();
        };

        RefreshConnectionNames();
    }

    private void RaiseConnectionCommands()
    {
        RefreshTopicsCommand.RaiseCanExecuteChanged();
        ScanCommand.RaiseCanExecuteChanged();
        GlobalSearchCommand.RaiseCanExecuteChanged();
        CreateTopicCommand.RaiseCanExecuteChanged();
    }

    /// <summary>Kafka's topic name rules: 1-249 chars of [a-zA-Z0-9._-], and not "." or "..".</summary>
    public static bool IsValidTopicName(string name) =>
        name.Length is > 0 and <= 249 && name is not "." and not ".." &&
        name.All(c => char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or '-');

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

    // ------------------------------------------------------------------ saved filters & sets ----

    private void ReportSaveError(string? error)
    {
        if (error is not null) StatusMessage = error;
    }

    private void SaveTopicFilter()
    {
        var filter = TopicFilter?.Trim();
        if (string.IsNullOrEmpty(filter) || SavedTopicFilters.Contains(filter)) return;
        SavedTopicFilters.Add(filter);
        ReportSaveError(SavedTopicFilterStore.Save(SavedTopicFilters));
        SelectedSavedTopicFilter = filter;
        SaveTopicFilterCommand.RaiseCanExecuteChanged();
    }

    private void RemoveSavedTopicFilter()
    {
        var filter = SelectedSavedTopicFilter;
        if (filter is null) return;
        SelectedSavedTopicFilter = null;
        SavedTopicFilters.Remove(filter);
        ReportSaveError(SavedTopicFilterStore.Save(SavedTopicFilters));
        SaveTopicFilterCommand.RaiseCanExecuteChanged();
    }

    private void SaveSearchResultsAsTopicSet()
    {
        var name = NewTopicSetName?.Trim();
        if (string.IsNullOrEmpty(name)) return;
        var topics = GlobalSearchResults.Select(h => h.Topic).Distinct(StringComparer.Ordinal).OrderBy(t => t, StringComparer.Ordinal).ToList();
        if (topics.Count == 0) return;

        var existing = SavedTopicSets.FirstOrDefault(s => s.Name == name);
        if (existing is not null) SavedTopicSets.Remove(existing);

        var set = new SavedTopicSet { Name = name, Topics = topics };
        SavedTopicSets.Add(set);
        ReportSaveError(SavedTopicSetStore.Save(SavedTopicSets));
        SelectedSavedTopicSet = set;
        _state.RaiseSavedTopicSetsChanged();
        NewTopicSetName = null;
        StatusMessage = $"Saved topic set '{name}' ({topics.Count} topic(s)).";
    }

    private void DeleteSavedTopicSet()
    {
        var set = SelectedSavedTopicSet;
        if (set is null) return;
        SelectedSavedTopicSet = null;
        SavedTopicSets.Remove(set);
        ReportSaveError(SavedTopicSetStore.Save(SavedTopicSets));
        _state.RaiseSavedTopicSetsChanged();
    }

    // ------------------------------------------------------------------ comparison & diff ----

    /// <summary>Field-by-field differences between <see cref="DiffLeft"/> and <see cref="DiffRight"/>.</summary>
    public ObservableCollection<DiffEntry> DiffRows { get; } = new();

    /// <summary>A diff needs two pinned messages.</summary>
    public bool CanDiff => ComparisonMessages.Count >= 2;

    private bool _isDiffOpen;
    /// <summary>Shows the structural diff instead of the side-by-side cards.</summary>
    public bool IsDiffOpen
    {
        get => _isDiffOpen;
        set { if (SetProperty(ref _isDiffOpen, value)) RecomputeDiff(); }
    }

    private ComparisonEntry? _diffLeft;
    public ComparisonEntry? DiffLeft { get => _diffLeft; set { if (SetProperty(ref _diffLeft, value)) RecomputeDiff(); } }

    private ComparisonEntry? _diffRight;
    public ComparisonEntry? DiffRight { get => _diffRight; set { if (SetProperty(ref _diffRight, value)) RecomputeDiff(); } }

    private bool _diffIgnoreVolatile = true;
    /// <summary>Hide fields expected to differ (timestamps, generated ids, trace ids).</summary>
    public bool DiffIgnoreVolatile { get => _diffIgnoreVolatile; set { if (SetProperty(ref _diffIgnoreVolatile, value)) RecomputeDiff(); } }

    private bool _diffShowUnchanged;
    public bool DiffShowUnchanged { get => _diffShowUnchanged; set { if (SetProperty(ref _diffShowUnchanged, value)) RecomputeDiff(); } }

    private string? _diffIgnorePaths;
    /// <summary>Comma separated paths (or prefixes) to leave out of the diff, e.g. <c>$.meta, headers.trace-id</c>.</summary>
    public string? DiffIgnorePaths { get => _diffIgnorePaths; set { if (SetProperty(ref _diffIgnorePaths, value)) RecomputeDiff(); } }

    private string? _diffSummary;
    public string? DiffSummary { get => _diffSummary; private set => SetProperty(ref _diffSummary, value); }

    private void OnComparisonChanged()
    {
        if (DiffLeft is not null && !ComparisonMessages.Contains(DiffLeft)) _diffLeft = null;
        if (DiffRight is not null && !ComparisonMessages.Contains(DiffRight)) _diffRight = null;
        _diffLeft ??= ComparisonMessages.FirstOrDefault(e => !ReferenceEquals(e, _diffRight));
        _diffRight ??= ComparisonMessages.FirstOrDefault(e => !ReferenceEquals(e, _diffLeft));
        OnPropertyChanged(nameof(DiffLeft));
        OnPropertyChanged(nameof(DiffRight));
        if (ComparisonMessages.Count < 2) IsDiffOpen = false;
        ToggleDiffCommand.RaiseCanExecuteChanged();
        OnPropertyChanged(nameof(CanDiff));
        RecomputeDiff();
    }

    private void RecomputeDiff()
    {
        DiffRows.Clear();
        if (!IsDiffOpen || DiffLeft is null || DiffRight is null)
        {
            DiffSummary = null;
            return;
        }
        var result = JsonDiff.Compare(DiffLeft.Message, DiffRight.Message, new DiffOptions
        {
            IgnoreVolatile = DiffIgnoreVolatile,
            IncludeUnchanged = DiffShowUnchanged,
            IgnorePaths = (DiffIgnorePaths ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        });
        foreach (var entry in result.Entries) DiffRows.Add(entry);
        DiffSummary = result.Summary;
    }

    private void ReloadSavedTopicSets()
    {
        var selected = SelectedSavedTopicSet?.Name;
        SavedTopicSets.Clear();
        foreach (var set in SavedTopicSetStore.Load()) SavedTopicSets.Add(set);
        SelectedSavedTopicSet = SavedTopicSets.FirstOrDefault(s => s.Name == selected);
    }

    private void AddToComparison(string topic, KafkaMessage message)
    {
        var alreadyPinned = ComparisonMessages.Any(e =>
            e.Topic == topic && e.Message.Partition == message.Partition && e.Message.Offset == message.Offset);
        if (alreadyPinned) return;
        ComparisonMessages.Add(new ComparisonEntry { Topic = topic, Message = message });
    }

    // ------------------------------------------------------------------ topics ----

    private void ApplyTopicFilter()
    {
        var filter = TopicFilter?.Trim();
        IEnumerable<TopicRowViewModel> matching = string.IsNullOrEmpty(filter)
            ? _allTopics
            : _allTopics.Where(t => t.Name.Contains(filter, StringComparison.OrdinalIgnoreCase));

        var selected = SelectedTopicRow;
        Topics.Clear();
        foreach (var topic in matching) Topics.Add(topic);
        // Keep the selection when it survives the filter (Clear() pushes null through the binding).
        SelectedTopicRow = selected is not null && Topics.Contains(selected) ? selected : null;
    }

    private async Task RefreshTopicsAsync()
    {
        _topicsLoadCts?.Cancel();
        var cts = new CancellationTokenSource();
        _topicsLoadCts = cts;

        if (SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway))
        {
            _allTopics.Clear();
            Topics.Clear();
            IsLoadingTopics = false;
            return;
        }

        StatusMessage = "Loading topics...";
        IsLoadingTopics = true;
        try
        {
            var names = await gateway.ListTopicsAsync(cts.Token).ConfigureAwait(true);
            if (cts.IsCancellationRequested) return;

            // Keep row objects (and their fetched counts) for topics that still exist.
            var existing = _allTopics.ToDictionary(t => t.Name, StringComparer.Ordinal);
            _allTopics.Clear();
            foreach (var name in names)
            {
                _allTopics.Add(existing.TryGetValue(name, out var row) ? row : new TopicRowViewModel { Name = name });
            }
            ApplyTopicFilter();
            StatusMessage = _allTopics.Count == 0 ? "No topics found." : $"{_allTopics.Count} topic(s).";
        }
        catch (OperationCanceledException)
        {
            // superseded by a newer connection selection - ignore.
        }
        catch (Exception ex)
        {
            if (!cts.IsCancellationRequested) StatusMessage = $"Failed to load topics: {ex.Message}";
        }
        finally
        {
            if (ReferenceEquals(_topicsLoadCts, cts)) IsLoadingTopics = false;
        }
    }

    private async Task CreateTopicAsync()
    {
        var name = NewTopicName.Trim();
        if (SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;
        if (_allTopics.Any(t => t.Name == name))
        {
            StatusMessage = $"Topic '{name}' already exists.";
            return;
        }

        StatusMessage = $"Creating topic '{name}'...";
        try
        {
            await gateway.CreateTopicAsync(name, NewTopicPartitions, (short)NewTopicReplicationFactor).ConfigureAwait(true);
            StatusMessage = $"Created topic '{name}'.";
            NewTopicName = "";
            IsCreateTopicOpen = false;
            await RefreshTopicsAsync().ConfigureAwait(true);
            SelectedTopicRow = _allTopics.FirstOrDefault(t => t.Name == name) is { } row && Topics.Contains(row) ? row : SelectedTopicRow;
        }
        catch (Exception ex)
        {
            StatusMessage = $"Could not create topic: {ex.Message}";
        }
    }

    // ------------------------------------------------------------------ messages ----

    private void ApplyMessageFilter()
    {
        var filter = MessageFilter?.Trim();
        MessageQuery? query = null;
        MessageFilterError = null;
        if (!string.IsNullOrEmpty(filter) && !MessageQuery.TryParse(filter, null, out query, out var error))
        {
            MessageFilterError = error;
        }
        IEnumerable<KafkaMessage> matching = query is null
            ? _allScannedMessages
            : _allScannedMessages.Where(query.Matches);

        var selected = SelectedMessage;
        ScannedMessages.Clear();
        foreach (var message in matching) ScannedMessages.Add(message);
        TotalMessageCount = _allScannedMessages.Count;
        MatchedMessageCount = ScannedMessages.Count;
        SelectedMessage = selected is not null && ScannedMessages.Contains(selected) ? selected : null;
    }

    private void ClearMessages()
    {
        _allScannedMessages.Clear();
        ScannedMessages.Clear();
        SelectedMessage = null;
        LoadedTopic = null;
        TotalMessageCount = 0;
        MatchedMessageCount = 0;
    }

    private Task OpenTopicAsync(TopicRowViewModel? topic)
    {
        if (topic is null) return Task.CompletedTask;
        SelectedTopicRow = topic;
        IsGlobalSearchActive = false;
        return ScanAsync();
    }

    private Task ScanAsync() => LoadMessagesAsync(null);

    /// <summary>Loads the newest <see cref="ScanLimit"/> messages of the selected topic (or all of them),
    /// newest first. <paramref name="select"/> optionally picks the message to select afterwards.</summary>
    private async Task LoadMessagesAsync(Func<KafkaMessage, bool>? select)
    {
        _messagesLoadCts?.Cancel();
        var cts = new CancellationTokenSource();
        _messagesLoadCts = cts;

        if (SelectedConnection is null || SelectedTopic is null ||
            !_state.Connections.TryGetValue(SelectedConnection, out var gateway))
        {
            IsLoadingMessages = false;
            return;
        }

        var topic = SelectedTopic;
        var row = SelectedTopicRow;
        var limit = ScanLimit;
        StatusMessage = limit is null ? $"Loading all messages of '{topic}'..." : $"Loading the newest {limit} message(s) of '{topic}'...";
        IsLoadingMessages = true;
        IsGlobalSearchActive = false;
        if (LoadedTopic != topic) ClearMessages();

        try
        {
            var options = new ConsumeOptions
            {
                Topic = topic,
                ConsumerGroup = $"kafka-studio-browser-{Guid.NewGuid():N}",
                // "Newest N": rewind N from the end of every partition, then keep the N newest overall.
                // (Reading from the beginning with a cap - what this used to do - showed the *oldest* N.)
                StartPosition = limit is null ? ConsumeStartPosition.Earliest : ConsumeStartPosition.Tail,
                TailCount = limit ?? 0,
                // A browser load is a bounded "current backlog" read - stop at partition end instead of
                // waiting for new messages (that's what the Consume screen is for).
                StopAtPartitionEnd = true
            };

            var describeTask = gateway.DescribeTopicAsync(topic, cts.Token);

            // Read on a background thread: for "load all" on a big topic this is a lot of work, and
            // buffering it before touching the UI collection keeps the list from re-rendering per message.
            var buffer = await Task.Run(async () =>
            {
                var list = new List<KafkaMessage>();
                await foreach (var message in gateway.ConsumeAsync(options, cts.Token).ConfigureAwait(false))
                {
                    list.Add(message);
                }
                return list;
            }, cts.Token).ConfigureAwait(true);

            TopicMetadata? metadata = null;
            try { metadata = await describeTask.ConfigureAwait(true); }
            catch (Exception) when (!cts.IsCancellationRequested) { /* counts are a nice-to-have */ }

            if (cts.IsCancellationRequested) return;

            if (row is not null && metadata is not null)
            {
                row.PartitionCount = metadata.Partitions.Count;
                row.TotalMessageCount = metadata.TotalMessageCount;
            }

            buffer.Sort((a, b) =>
            {
                var byTimestamp = b.Timestamp.CompareTo(a.Timestamp);
                if (byTimestamp != 0) return byTimestamp;
                var byPartition = a.Partition.CompareTo(b.Partition);
                return byPartition != 0 ? byPartition : b.Offset.CompareTo(a.Offset);
            });
            if (limit is { } cap && buffer.Count > cap) buffer.RemoveRange(cap, buffer.Count - cap);

            _allScannedMessages.Clear();
            _allScannedMessages.AddRange(buffer);
            LoadedTopic = topic;
            ApplyMessageFilter();
            if (select is not null) SelectedMessage = ScannedMessages.FirstOrDefault(select);

            var total = metadata is null ? "" : $" of {metadata.TotalMessageCount:N0}";
            StatusMessage = $"Loaded {_allScannedMessages.Count:N0}{total} message(s) from '{topic}', newest first.";
        }
        catch (OperationCanceledException)
        {
            if (ReferenceEquals(_messagesLoadCts, cts)) StatusMessage = "Load cancelled.";
        }
        catch (Exception ex)
        {
            if (!cts.IsCancellationRequested) StatusMessage = $"Load failed: {ex.Message}";
        }
        finally
        {
            if (ReferenceEquals(_messagesLoadCts, cts)) IsLoadingMessages = false;
        }
    }

    // ------------------------------------------------------------------ cross-topic search ----

    /// <summary>Requests cancellation of an in-flight cross-topic search.
    /// <see cref="CancellationTokenSource.Cancel()"/> synchronously invokes every callback registered on
    /// the token (one per concurrently scanned topic and its consumer) on the calling thread - doing that
    /// on the UI thread made the "Cancel" button appear to freeze the app, so it hops to a background
    /// thread first.</summary>
    private void CancelGlobalSearch()
    {
        var cts = _globalSearchCts;
        if (cts is null) return;
        StatusMessage = "Cancelling search...";
        _ = Task.Run(() => cts.Cancel());
    }

    private void CloseSearchResults()
    {
        if (IsGlobalSearching) CancelGlobalSearch();
        IsGlobalSearchActive = false;
    }

    private void ResetGlobalSearchResults()
    {
        GlobalSearchResults.Clear();
        FilteredGlobalSearchResults.Clear();
        GlobalSearchResultsByTopic.Clear();
        _hitCountsByTopic.Clear();
        SelectedGlobalSearchTopicFilter = null;
    }

    private void ToggleGlobalSearchTopicFilter(TopicHitCount? hit)
    {
        if (hit is null) return;

        SelectedGlobalSearchTopicFilter = SelectedGlobalSearchTopicFilter == hit.Topic ? null : hit.Topic;
        foreach (var item in GlobalSearchResultsByTopic) item.IsSelected = item.Topic == SelectedGlobalSearchTopicFilter;

        FilteredGlobalSearchResults.Clear();
        foreach (var result in GlobalSearchResults)
        {
            if (SelectedGlobalSearchTopicFilter is null || result.Topic == SelectedGlobalSearchTopicFilter)
            {
                FilteredGlobalSearchResults.Add(result);
            }
        }
    }

    /// <summary>UI thread: appends a batch of hits and updates the per-topic counts incrementally
    /// (the old implementation regrouped and rebuilt every list on every single hit - quadratic).</summary>
    private void AddHits(IReadOnlyList<GlobalSearchHit> hits)
    {
        foreach (var hit in hits)
        {
            if (GlobalSearchResults.Count >= MaxGlobalSearchHits) break;
            GlobalSearchResults.Add(hit);

            if (!_hitCountsByTopic.TryGetValue(hit.Topic, out var count))
            {
                count = new TopicHitCount { Topic = hit.Topic };
                _hitCountsByTopic[hit.Topic] = count;
                GlobalSearchResultsByTopic.Add(count);
            }
            count.Count++;

            if (SelectedGlobalSearchTopicFilter is null || hit.Topic == SelectedGlobalSearchTopicFilter)
            {
                FilteredGlobalSearchResults.Add(hit);
            }
        }
    }

    private void SortHitCounts()
    {
        var sorted = GlobalSearchResultsByTopic
            .OrderByDescending(c => c.Count)
            .ThenBy(c => c.Topic, StringComparer.OrdinalIgnoreCase)
            .ToList();
        for (var i = 0; i < sorted.Count; i++)
        {
            var current = GlobalSearchResultsByTopic.IndexOf(sorted[i]);
            if (current != i) GlobalSearchResultsByTopic.Move(current, i);
        }
    }

    private async Task GlobalSearchAsync()
    {
        _globalSearchCts?.Cancel();
        var cts = new CancellationTokenSource();
        _globalSearchCts = cts;

        var term = GlobalSearchTerm?.Trim();
        if (SelectedConnection is null || string.IsNullOrEmpty(term)) return;
        if (!_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;
        if (!MessageQuery.TryParse(term, null, out var query, out var queryError))
        {
            StatusMessage = $"Search: {queryError}";
            return;
        }

        var topics = SelectedSavedTopicSet is not null
            ? SelectedSavedTopicSet.Topics.ToList()
            : Topics.Select(t => t.Name).ToList();
        if (topics.Count == 0)
        {
            StatusMessage = SelectedSavedTopicSet is not null ? "Selected topic set is empty." : "No topics listed to search.";
            return;
        }

        IsGlobalSearching = true;
        IsGlobalSearchActive = true;
        ResetGlobalSearchResults();
        GlobalSearchTopicsScanned = 0;
        GlobalSearchTopicsTotal = topics.Count;
        StatusMessage = $"Searching {topics.Count} topic(s) for \"{term}\"...";

        var failedTopics = 0;
        var truncated = false;
        try
        {
            // Bound the number of topics scanned concurrently so a large cluster doesn't open hundreds
            // of consumer connections at once. The fan-out runs on the thread pool; hits are handed to
            // the UI in small batches.
            var scanned = 0;
            var totalHits = 0;
            var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = 8, CancellationToken = cts.Token };

            await Task.Run(() => Parallel.ForEachAsync(topics, parallelOptions, async (topic, token) =>
            {
                var pending = new List<GlobalSearchHit>();
                void FlushPending()
                {
                    if (pending.Count == 0) return;
                    var batch = pending.ToList();
                    pending.Clear();
                    _state.PostToUi(() =>
                    {
                        if (ReferenceEquals(_globalSearchCts, cts) && !cts.IsCancellationRequested) AddHits(batch);
                    });
                }

                try
                {
                    var options = new ConsumeOptions
                    {
                        Topic = topic,
                        ConsumerGroup = $"kafka-studio-global-search-{Guid.NewGuid():N}",
                        StartPosition = ConsumeStartPosition.Earliest,
                        StopAtPartitionEnd = true
                    };

                    await foreach (var message in gateway.ConsumeAsync(options, token).ConfigureAwait(false))
                    {
                        if (token.IsCancellationRequested) break;
                        if (!query!.Matches(message)) continue;

                        if (Interlocked.Increment(ref totalHits) > MaxGlobalSearchHits)
                        {
                            truncated = true;
                            cts.Cancel(); // enough - stop every topic
                            break;
                        }
                        pending.Add(new GlobalSearchHit { Topic = topic, Message = message });
                        if (pending.Count >= 50) FlushPending();
                    }
                }
                catch (OperationCanceledException)
                {
                    // superseded by a newer search / cancel.
                }
                catch (Exception)
                {
                    // One unreachable/misbehaving topic shouldn't abort the whole cross-topic search.
                    Interlocked.Increment(ref failedTopics);
                }
                finally
                {
                    FlushPending();
                    var count = Interlocked.Increment(ref scanned);
                    _state.PostToUi(() =>
                    {
                        if (ReferenceEquals(_globalSearchCts, cts)) GlobalSearchTopicsScanned = count;
                    });
                }
            })).ConfigureAwait(true);
        }
        catch (OperationCanceledException)
        {
            // handled below
        }
        finally
        {
            if (ReferenceEquals(_globalSearchCts, cts))
            {
                // Let already-posted hit batches land before reporting the totals.
                _state.PostToUi(() =>
                {
                    if (!ReferenceEquals(_globalSearchCts, cts)) return;
                    IsGlobalSearching = false;
                    SortHitCounts();
                    var failed = failedTopics > 0 ? $" ({failedTopics} topic(s) could not be read)" : "";
                    StatusMessage = truncated
                        ? $"Stopped after {MaxGlobalSearchHits:N0} matches - narrow the search term.{failed}"
                        : cts.IsCancellationRequested
                            ? $"Search cancelled - {GlobalSearchResults.Count:N0} match(es) so far."
                            : $"Found {GlobalSearchResults.Count:N0} message(s) matching \"{term}\" across {topics.Count} topic(s).{failed}";
                });
            }
        }
    }

    private async Task OpenGlobalSearchHitAsync(GlobalSearchHit? hit)
    {
        if (hit is null) return;

        var topicRow = _allTopics.FirstOrDefault(t => t.Name == hit.Topic);
        if (topicRow is null)
        {
            StatusMessage = $"Topic '{hit.Topic}' isn't in the topic list any more - refresh topics.";
            return;
        }

        // Make sure the row is visible in the (possibly filtered) list before selecting it.
        if (!Topics.Contains(topicRow)) TopicFilter = null;
        SelectedTopicRow = topicRow;
        IsGlobalSearchActive = false;

        var partition = hit.Message.Partition;
        var offset = hit.Message.Offset;
        bool IsHit(KafkaMessage m) => m.Partition == partition && m.Offset == offset;

        // Already loaded and contains the hit: just select it.
        if (LoadedTopic == hit.Topic && _allScannedMessages.Any(IsHit))
        {
            MessageFilter = null;
            SelectedMessage = ScannedMessages.FirstOrDefault(IsHit);
            return;
        }

        await LoadMessagesAsync(IsHit).ConfigureAwait(true);

        // Older than the loaded "newest N" window: still show the hit itself in the detail pane.
        if (SelectedMessage is null && LoadedTopic == hit.Topic) SelectedMessage = hit.Message;
    }
}

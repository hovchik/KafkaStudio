using System.Collections.ObjectModel;
using System.Linq;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Topics;

namespace KafkaStudio.App.ViewModels.Topics;

public sealed class TopicRowViewModel : ObservableObject
{
    public required string Name { get; init; }

    private int? _partitionCount;
    /// <summary>Null until this topic is selected and its metadata has been fetched.</summary>
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

/// <summary>A message pinned into the "Compare messages" section, for side-by-side inspection.</summary>
public sealed class ComparisonEntry
{
    public required string Topic { get; init; }
    public required KafkaMessage Message { get; init; }
}

/// <summary>Lists topics for the selected connection and lets you scan a backlog on demand (the same
/// "scan and acknowledge" capability the DSL exposes, surfaced as a point-and-click tool).
///
/// Topics are (re)loaded automatically whenever <see cref="SelectedConnection"/> changes - just their
/// names, via a single <see cref="IKafkaGateway.ListTopicsAsync"/> call, so the list populates
/// immediately even for clusters with many topics. The list can be narrowed with <see cref="TopicFilter"/>
/// (a simple case-insensitive "contains" match). Double-clicking a topic (bound to
/// <see cref="OpenTopicCommand"/>) loads its partition/message counts and its most recent messages,
/// newest first - selecting a row alone (e.g. via keyboard) does not trigger a load, so browsing the
/// list doesn't spam the broker. All loads run as fire-and-forget background work (the gateway calls are
/// all Task-based / non-blocking) so the UI thread is never blocked. A per-operation
/// <see cref="CancellationTokenSource"/> is swapped in on every trigger so a rapid connection/topic
/// switch cancels the now-stale load instead of racing it.</summary>
public sealed class TopicBrowserViewModel : ObservableObject
{
    private readonly AppState _state;
    private CancellationTokenSource? _topicsLoadCts;
    private CancellationTokenSource? _messagesLoadCts;
    private CancellationTokenSource? _globalSearchCts;
    private readonly List<TopicRowViewModel> _allTopics = new();

    public ObservableCollection<string> ConnectionNames { get; } = new();
    public ObservableCollection<TopicRowViewModel> Topics { get; } = new();
    public ObservableCollection<KafkaMessage> ScannedMessages { get; } = new();
    public ObservableCollection<GlobalSearchHit> GlobalSearchResults { get; } = new();

    /// <summary>Named sets of topics captured from previous search results, persisted across restarts.</summary>
    public ObservableCollection<SavedTopicSet> SavedTopicSets { get; } = new();

    /// <summary>Messages pinned for side-by-side comparison via <see cref="AddToComparisonCommand"/>.</summary>
    public ObservableCollection<ComparisonEntry> ComparisonMessages { get; } = new();

    private bool _isTopicsPanelExpanded = true;
    /// <summary>Whether the "Topics" list panel is expanded or collapsed to its header.</summary>
    public bool IsTopicsPanelExpanded
    {
        get => _isTopicsPanelExpanded;
        set
        {
            if (SetProperty(ref _isTopicsPanelExpanded, value)) OnPropertyChanged(nameof(AreTopicsAndMessagesCollapsed));
        }
    }

    private bool _isMessagesPanelExpanded = true;
    /// <summary>Whether the "Messages" panel is expanded or collapsed to its header.</summary>
    public bool IsMessagesPanelExpanded
    {
        get => _isMessagesPanelExpanded;
        set
        {
            if (SetProperty(ref _isMessagesPanelExpanded, value)) OnPropertyChanged(nameof(AreTopicsAndMessagesCollapsed));
        }
    }

    /// <summary>True when both the "Topics" and "Messages" panels are collapsed to their headers, so the
    /// "Search results (all topics)" panel below them should expand to fill the freed-up space instead of
    /// staying capped to its small default height.</summary>
    public bool AreTopicsAndMessagesCollapsed => !IsTopicsPanelExpanded && !IsMessagesPanelExpanded;

    private bool _isSearchResultsPanelExpanded = true;
    /// <summary>Whether the "Search results (all topics)" panel is expanded or collapsed to its header.</summary>
    public bool IsSearchResultsPanelExpanded { get => _isSearchResultsPanelExpanded; set => SetProperty(ref _isSearchResultsPanelExpanded, value); }

    private bool _arePanelsSwapped;
    /// <summary>When true, the "Topics" and "Messages" panels are shown in reverse order (Messages on the left).</summary>
    public bool ArePanelsSwapped { get => _arePanelsSwapped; set => SetProperty(ref _arePanelsSwapped, value); }

    /// <summary>Collapses/expands the "Topics", "Messages" and "Search results" panels to just their
    /// header, and lets the Topics/Messages panels be swapped left-to-right, so the layout can be
    /// reorganized to focus on whichever panel matters right now.</summary>
    public RelayCommand ToggleTopicsPanelCommand { get; }
    public RelayCommand ToggleMessagesPanelCommand { get; }
    public RelayCommand ToggleSearchResultsPanelCommand { get; }
    public RelayCommand SwapPanelsCommand { get; }

    private string? _selectedConnection;
    public string? SelectedConnection
    {
        get => _selectedConnection;
        set
        {
            if (SetProperty(ref _selectedConnection, value))
            {
                SelectedTopicRow = null;
                _allTopics.Clear();
                Topics.Clear();
                GlobalSearchResults.Clear();
                _ = RefreshTopicsAsync();
                GlobalSearchCommand.RaiseCanExecuteChanged();
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

    /// <summary>Topic filter terms the user has saved, persisted across restarts.</summary>
    public ObservableCollection<string> SavedTopicFilters { get; } = new();

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

    public RelayCommand SaveTopicFilterCommand { get; }
    public RelayCommand RemoveSavedTopicFilterCommand { get; }

    private TopicRowViewModel? _selectedTopicRow;
    /// <summary>Bound to the topics list's selection. Selecting a row (e.g. with the keyboard or a single
    /// click) does not, by itself, load messages - double-click (<see cref="OpenTopicCommand"/>) does.</summary>
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

    /// <summary>Max messages to pull, newest first. Leave empty/null to load the entire topic backlog.</summary>
    private int? _scanLimit = 50;
    public int? ScanLimit { get => _scanLimit; set => SetProperty(ref _scanLimit, value); }

    private string? _messageFilter;
    /// <summary>Case-insensitive "contains" search applied to key + value of <see cref="ScannedMessages"/>,
    /// against the full set of loaded messages.</summary>
    public string? MessageFilter
    {
        get => _messageFilter;
        set
        {
            if (SetProperty(ref _messageFilter, value))
            {
                ApplyMessageFilter();
            }
        }
    }

    private readonly List<KafkaMessage> _allScannedMessages = new();

    private int _matchedMessageCount;
    /// <summary>Number of messages currently shown in <see cref="ScannedMessages"/> after applying
    /// <see cref="MessageFilter"/> (equal to <see cref="TotalMessageCount"/> when the filter is empty).</summary>
    public int MatchedMessageCount { get => _matchedMessageCount; private set => SetProperty(ref _matchedMessageCount, value); }

    private int _totalMessageCount;
    /// <summary>Total number of messages loaded for the current topic, before filtering.</summary>
    public int TotalMessageCount { get => _totalMessageCount; private set => SetProperty(ref _totalMessageCount, value); }

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    private bool _isLoadingTopics;
    /// <summary>True while <see cref="RefreshTopicsAsync"/> is fetching the topic list.</summary>
    public bool IsLoadingTopics { get => _isLoadingTopics; private set => SetProperty(ref _isLoadingTopics, value); }

    private bool _isLoadingMessages;
    /// <summary>True while <see cref="ScanAsync"/> is loading messages for the selected topic.</summary>
    public bool IsLoadingMessages { get => _isLoadingMessages; private set => SetProperty(ref _isLoadingMessages, value); }

    private string? _globalSearchTerm;
    /// <summary>Case-insensitive "contains" search executed by <see cref="GlobalSearchCommand"/> against
    /// every currently loaded topic's message backlog (key + value), across the whole connection.</summary>
    public string? GlobalSearchTerm
    {
        get => _globalSearchTerm;
        set
        {
            if (SetProperty(ref _globalSearchTerm, value))
            {
                GlobalSearchCommand.RaiseCanExecuteChanged();
            }
        }
    }

    private bool _isGlobalSearching;
    /// <summary>True while <see cref="GlobalSearchCommand"/> is fanning out scans across topics.</summary>
    public bool IsGlobalSearching
    {
        get => _isGlobalSearching;
        private set
        {
            if (SetProperty(ref _isGlobalSearching, value))
            {
                CancelGlobalSearchCommand.RaiseCanExecuteChanged();
            }
        }
    }

    private int _globalSearchTopicsScanned;
    public int GlobalSearchTopicsScanned { get => _globalSearchTopicsScanned; private set => SetProperty(ref _globalSearchTopicsScanned, value); }

    private int _globalSearchTopicsTotal;
    public int GlobalSearchTopicsTotal { get => _globalSearchTopicsTotal; private set => SetProperty(ref _globalSearchTopicsTotal, value); }

    private SavedTopicSet? _selectedSavedTopicSet;
    /// <summary>When set, <see cref="GlobalSearchAsync"/> only scans this set's topics instead of every
    /// currently loaded topic.</summary>
    public SavedTopicSet? SelectedSavedTopicSet
    {
        get => _selectedSavedTopicSet;
        set
        {
            if (SetProperty(ref _selectedSavedTopicSet, value))
            {
                DeleteSavedTopicSetCommand.RaiseCanExecuteChanged();
            }
        }
    }

    private string? _newTopicSetName;
    /// <summary>Name to give the topic set created from the current <see cref="GlobalSearchResults"/> by
    /// <see cref="SaveSearchResultsAsTopicSetCommand"/>.</summary>
    public string? NewTopicSetName
    {
        get => _newTopicSetName;
        set
        {
            if (SetProperty(ref _newTopicSetName, value))
            {
                SaveSearchResultsAsTopicSetCommand.RaiseCanExecuteChanged();
            }
        }
    }

    /// <summary>Saves the distinct topics found in <see cref="GlobalSearchResults"/> as a new named
    /// <see cref="SavedTopicSet"/>, so a later search can be scoped to just those topics.</summary>
    public RelayCommand SaveSearchResultsAsTopicSetCommand { get; }
    public RelayCommand DeleteSavedTopicSetCommand { get; }

    /// <summary>Pins a search result's message into <see cref="ComparisonMessages"/>.</summary>
    public RelayCommand<GlobalSearchHit> AddToComparisonCommand { get; }
    public RelayCommand<ComparisonEntry> RemoveFromComparisonCommand { get; }
    public RelayCommand ClearComparisonCommand { get; }

    public AsyncRelayCommand RefreshTopicsCommand { get; }
    public AsyncRelayCommand ScanCommand { get; }

    /// <summary>Scans every loaded topic's current backlog in parallel and collects every message whose
    /// key or value contains <see cref="GlobalSearchTerm"/> - a "find this message, but I don't remember
    /// which topic it's on" tool. Heavier than the per-topic <see cref="MessageFilter"/> (it talks to the
    /// broker for every topic), so it only runs on demand.</summary>
    public AsyncRelayCommand GlobalSearchCommand { get; }
    public RelayCommand CancelGlobalSearchCommand { get; }

    /// <summary>Bound to a topic row's double-click in the view - opens that topic (selecting it and
    /// loading its messages) regardless of what's currently selected.</summary>
    public AsyncRelayCommand<TopicRowViewModel> OpenTopicCommand { get; }

    /// <summary>Bound to a global search result row's double-click - jumps to that message's topic and
    /// loads it in the main message pane.</summary>
    public AsyncRelayCommand<GlobalSearchHit> OpenGlobalSearchHitCommand { get; }

    public TopicBrowserViewModel(AppState state)
    {
        _state = state;
        _state.ConnectionsChanged += RefreshConnectionNames;
        ToggleTopicsPanelCommand = new RelayCommand(() => IsTopicsPanelExpanded = !IsTopicsPanelExpanded);
        ToggleMessagesPanelCommand = new RelayCommand(() => IsMessagesPanelExpanded = !IsMessagesPanelExpanded);
        ToggleSearchResultsPanelCommand = new RelayCommand(() => IsSearchResultsPanelExpanded = !IsSearchResultsPanelExpanded);
        SwapPanelsCommand = new RelayCommand(() => ArePanelsSwapped = !ArePanelsSwapped);
        RefreshTopicsCommand = new AsyncRelayCommand(RefreshTopicsAsync, () => SelectedConnection is not null);
        ScanCommand = new AsyncRelayCommand(ScanAsync, () => SelectedConnection is not null && SelectedTopic is not null);
        OpenTopicCommand = new AsyncRelayCommand<TopicRowViewModel>(OpenTopicAsync);
        GlobalSearchCommand = new AsyncRelayCommand(GlobalSearchAsync,
            () => SelectedConnection is not null && !string.IsNullOrWhiteSpace(GlobalSearchTerm));
        CancelGlobalSearchCommand = new RelayCommand(CancelGlobalSearch, () => IsGlobalSearching);
        OpenGlobalSearchHitCommand = new AsyncRelayCommand<GlobalSearchHit>(OpenGlobalSearchHitAsync);
        SaveTopicFilterCommand = new RelayCommand(SaveTopicFilter,
            () => !string.IsNullOrWhiteSpace(TopicFilter) && !SavedTopicFilters.Contains(TopicFilter.Trim()));
        RemoveSavedTopicFilterCommand = new RelayCommand(RemoveSavedTopicFilter, () => SelectedSavedTopicFilter is not null);
        foreach (var filter in SavedTopicFilterStore.Load()) SavedTopicFilters.Add(filter);
        SaveSearchResultsAsTopicSetCommand = new RelayCommand(SaveSearchResultsAsTopicSet,
            () => !string.IsNullOrWhiteSpace(NewTopicSetName) && GlobalSearchResults.Count > 0);
        DeleteSavedTopicSetCommand = new RelayCommand(DeleteSavedTopicSet, () => SelectedSavedTopicSet is not null);
        AddToComparisonCommand = new RelayCommand<GlobalSearchHit>(AddToComparison);
        RemoveFromComparisonCommand = new RelayCommand<ComparisonEntry>(entry => { if (entry is not null) ComparisonMessages.Remove(entry); });
        ClearComparisonCommand = new RelayCommand(ComparisonMessages.Clear);
        foreach (var set in SavedTopicSetStore.Load()) SavedTopicSets.Add(set);
        RefreshConnectionNames();
    }

    private void SaveSearchResultsAsTopicSet()
    {
        var name = NewTopicSetName?.Trim();
        if (string.IsNullOrEmpty(name)) return;
        var topics = GlobalSearchResults.Select(h => h.Topic).Distinct().ToList();
        if (topics.Count == 0) return;

        var existing = SavedTopicSets.FirstOrDefault(s => s.Name == name);
        if (existing is not null) SavedTopicSets.Remove(existing);

        var set = new SavedTopicSet { Name = name, Topics = topics };
        SavedTopicSets.Add(set);
        SavedTopicSetStore.Save(SavedTopicSets);
        SelectedSavedTopicSet = set;
        NewTopicSetName = null;
    }

    private void DeleteSavedTopicSet()
    {
        var set = SelectedSavedTopicSet;
        if (set is null) return;
        SelectedSavedTopicSet = null;
        SavedTopicSets.Remove(set);
        SavedTopicSetStore.Save(SavedTopicSets);
        DeleteSavedTopicSetCommand.RaiseCanExecuteChanged();
    }

    private void AddToComparison(GlobalSearchHit? hit)
    {
        if (hit is null) return;
        var alreadyPinned = ComparisonMessages.Any(e =>
            e.Topic == hit.Topic && e.Message.Partition == hit.Message.Partition && e.Message.Offset == hit.Message.Offset);
        if (alreadyPinned) return;
        ComparisonMessages.Add(new ComparisonEntry { Topic = hit.Topic, Message = hit.Message });
    }

    private void SaveTopicFilter()
    {
        var filter = TopicFilter?.Trim();
        if (string.IsNullOrEmpty(filter) || SavedTopicFilters.Contains(filter)) return;
        SavedTopicFilters.Add(filter);
        SavedTopicFilterStore.Save(SavedTopicFilters);
        SelectedSavedTopicFilter = filter;
        SaveTopicFilterCommand.RaiseCanExecuteChanged();
    }

    private void RemoveSavedTopicFilter()
    {
        var filter = SelectedSavedTopicFilter;
        if (filter is null) return;
        SelectedSavedTopicFilter = null;
        SavedTopicFilters.Remove(filter);
        SavedTopicFilterStore.Save(SavedTopicFilters);
        SaveTopicFilterCommand.RaiseCanExecuteChanged();
    }

    private void RefreshConnectionNames()
    {
        ConnectionNames.Clear();
        foreach (var name in _state.Connections.Keys) ConnectionNames.Add(name);
    }

    private void ApplyTopicFilter()
    {
        var filter = TopicFilter;
        IEnumerable<TopicRowViewModel> matching = string.IsNullOrWhiteSpace(filter)
            ? _allTopics
            : _allTopics.Where(t => t.Name.Contains(filter, StringComparison.OrdinalIgnoreCase));

        Topics.Clear();
        foreach (var topic in matching) Topics.Add(topic);
    }

    private void ApplyMessageFilter()
    {
        var filter = MessageFilter;
        IEnumerable<KafkaMessage> matching = string.IsNullOrWhiteSpace(filter)
            ? _allScannedMessages
            : _allScannedMessages.Where(m =>
                (m.Value is not null && m.Value.Contains(filter, StringComparison.OrdinalIgnoreCase)) ||
                (m.Key is not null && m.Key.Contains(filter, StringComparison.OrdinalIgnoreCase)));

        ScannedMessages.Clear();
        foreach (var message in matching) ScannedMessages.Add(message);
        TotalMessageCount = _allScannedMessages.Count;
        MatchedMessageCount = ScannedMessages.Count;
    }

    private Task OpenTopicAsync(TopicRowViewModel? topic)
    {
        if (topic is null) return Task.CompletedTask;
        SelectedTopicRow = topic;
        return ScanAsync();
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
            return;
        }

        StatusMessage = "Loading topics...";
        IsLoadingTopics = true;
        try
        {
            var names = await gateway.ListTopicsAsync(cts.Token).ConfigureAwait(true);

            if (cts.IsCancellationRequested) return;

            _allTopics.Clear();
            foreach (var name in names)
            {
                _allTopics.Add(new TopicRowViewModel { Name = name });
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
            StatusMessage = $"Failed to load topics: {ex.Message}";
        }
        finally
        {
            if (!cts.IsCancellationRequested) IsLoadingTopics = false;
        }
    }

    private async Task ScanAsync()
    {
        _messagesLoadCts?.Cancel();
        var cts = new CancellationTokenSource();
        _messagesLoadCts = cts;

        if (SelectedConnection is null || SelectedTopic is null) return;
        if (!_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;

        var topic = SelectedTopic;
        var row = SelectedTopicRow;
        StatusMessage = $"Loading messages for '{topic}'...";
        IsLoadingMessages = true;
        try
        {
            var options = new ConsumeOptions
            {
                Topic = topic,
                ConsumerGroup = $"kafka-studio-browser-{Guid.NewGuid():N}",
                StartPosition = ConsumeStartPosition.Earliest,
                MaxMessages = ScanLimit,
                // Whether or not a cap is set, a browser scan is a bounded "load current backlog" read -
                // stop once the topic's current messages are exhausted rather than waiting for more to
                // arrive (that's what live "watch" is for). This is also what makes an empty ScanLimit
                // mean "load all messages" instead of hanging forever.
                StopAtPartitionEnd = true
            };

            var describeTask = gateway.DescribeTopicAsync(topic, cts.Token);

            // Buffer the batch and sort it before touching the UI collection, so the ItemsControl isn't
            // re-ordered incrementally as messages arrive - and so the final view is newest-first.
            var buffer = new List<KafkaMessage>();
            await foreach (var message in gateway.ConsumeAsync(options, cts.Token).ConfigureAwait(true))
            {
                buffer.Add(message);
            }

            var metadata = await describeTask.ConfigureAwait(true);

            if (cts.IsCancellationRequested) return;

            if (row is not null)
            {
                row.PartitionCount = metadata.Partitions.Count;
                row.TotalMessageCount = metadata.TotalMessageCount;
            }

            buffer.Sort((a, b) =>
            {
                var byTimestamp = b.Timestamp.CompareTo(a.Timestamp);
                return byTimestamp != 0 ? byTimestamp : b.Offset.CompareTo(a.Offset);
            });

            _allScannedMessages.Clear();
            _allScannedMessages.AddRange(buffer);
            ApplyMessageFilter();
            StatusMessage = $"Loaded {_allScannedMessages.Count} message(s), newest first.";
        }
        catch (OperationCanceledException)
        {
            // superseded by a newer topic selection - ignore.
        }
        catch (Exception ex)
        {
            StatusMessage = $"Scan failed: {ex.Message}";
        }
        finally
        {
            if (!cts.IsCancellationRequested) IsLoadingMessages = false;
        }
    }

    /// <summary>Fans out a bounded, "load current backlog" scan across every currently loaded topic in
    /// parallel and collects every message whose key or value contains <see cref="GlobalSearchTerm"/>.
    /// This is deliberately heavier than <see cref="MessageFilter"/> - it talks to the broker for every
    /// topic - so it's only triggered explicitly via <see cref="GlobalSearchCommand"/>, not on every
    /// keystroke.</summary>
    /// <summary>Requests cancellation of an in-flight <see cref="GlobalSearchCommand"/>.
    /// <see cref="CancellationTokenSource.Cancel()"/> synchronously invokes every callback registered on
    /// the token (e.g. from the many per-topic <c>SemaphoreSlim.WaitAsync</c>/channel reads fanned out by
    /// <see cref="GlobalSearchAsync"/>, easily 1000+ for a large cluster) on the calling thread - doing
    /// that on the UI thread is what made the "Cancel" button appear to freeze the app. Hopping to a
    /// background thread first keeps that callback storm off the UI thread.</summary>
    private void CancelGlobalSearch()
    {
        var cts = _globalSearchCts;
        if (cts is null) return;
        _ = Task.Run(() => cts.Cancel());
    }

    private async Task GlobalSearchAsync()
    {
        _globalSearchCts?.Cancel();
        var cts = new CancellationTokenSource();
        _globalSearchCts = cts;

        var term = GlobalSearchTerm;
        if (SelectedConnection is null || string.IsNullOrWhiteSpace(term)) return;
        if (!_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;

        var topics = SelectedSavedTopicSet is not null
            ? SelectedSavedTopicSet.Topics.ToList()
            : Topics.Select(t => t.Name).ToList();
        if (topics.Count == 0)
        {
            StatusMessage = SelectedSavedTopicSet is not null ? "Selected topic set is empty." : "No topics loaded to search.";
            return;
        }

        IsGlobalSearching = true;
        GlobalSearchResults.Clear();
        GlobalSearchTopicsScanned = 0;
        GlobalSearchTopicsTotal = topics.Count;
        StatusMessage = $"Searching {topics.Count} topic(s) for \"{term}\"...";

        try
        {
            // Bound the number of topics scanned concurrently so a large cluster doesn't open hundreds
            // of consumer connections at once.
            //
            // The whole fan-out runs on the thread pool (ConfigureAwait(false)); only result/progress updates
            // are posted back to the UI thread. Previously every per-topic continuation (and the consumer
            // creation/teardown inside ConsumeAsync) ran on the UI thread, so cancelling - which unwinds all
            // of them at once - froze the UI. Parallel.ForEachAsync also avoids queuing a pending cancellable
            // wait for every one of the (possibly thousands of) topics up front.
            var ui = SynchronizationContext.Current;
            void OnUi(Action action)
            {
                if (ui is null) action();
                else ui.Post(_ => action(), null);
            }

            var scanned = 0;
            var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = 8, CancellationToken = cts.Token };

            await Task.Run(() => Parallel.ForEachAsync(topics, parallelOptions, async (topic, token) =>
            {
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
                        // ReadAllAsync only observes cancellation between buffered batches, so check
                        // explicitly to stop immediately instead of draining an already-fetched backlog.
                        if (token.IsCancellationRequested) break;

                        var isMatch = (message.Value is not null && message.Value.Contains(term, StringComparison.OrdinalIgnoreCase)) ||
                                      (message.Key is not null && message.Key.Contains(term, StringComparison.OrdinalIgnoreCase));

                        if (isMatch)
                        {
                            var hit = new GlobalSearchHit { Topic = topic, Message = message };
                            OnUi(() =>
                            {
                                if (!cts.IsCancellationRequested) GlobalSearchResults.Add(hit);
                            });
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // superseded by a newer search / cancel - ignore this topic's partial results.
                }
                catch (Exception)
                {
                    // one unreachable/misbehaving topic shouldn't abort the whole cross-topic search.
                }
                finally
                {
                    var count = Interlocked.Increment(ref scanned);
                    OnUi(() => GlobalSearchTopicsScanned = count);
                }
            })).ConfigureAwait(true);

            if (cts.IsCancellationRequested)
            {
                StatusMessage = "Search cancelled.";
                return;
            }

            StatusMessage = $"Found {GlobalSearchResults.Count} message(s) matching \"{term}\" across {topics.Count} topic(s).";
        }
        catch (OperationCanceledException)
        {
            StatusMessage = "Search cancelled.";
        }
        finally
        {
            IsGlobalSearching = false;
            SaveSearchResultsAsTopicSetCommand.RaiseCanExecuteChanged();
        }
    }

    private Task OpenGlobalSearchHitAsync(GlobalSearchHit? hit)
    {
        if (hit is null) return Task.CompletedTask;

        var topicRow = _allTopics.FirstOrDefault(t => t.Name == hit.Topic);
        if (topicRow is null) return Task.CompletedTask;

        SelectedTopicRow = topicRow;
        return ScanAsync();
    }
}

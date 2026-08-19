using System.Collections.ObjectModel;
using System.Linq;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;

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

    private bool _isTopicsPanelExpanded = true;
    /// <summary>Whether the "Topics" list panel is expanded or collapsed to its header.</summary>
    public bool IsTopicsPanelExpanded { get => _isTopicsPanelExpanded; set => SetProperty(ref _isTopicsPanelExpanded, value); }

    private bool _isMessagesPanelExpanded = true;
    /// <summary>Whether the "Messages" panel is expanded or collapsed to its header.</summary>
    public bool IsMessagesPanelExpanded { get => _isMessagesPanelExpanded; set => SetProperty(ref _isMessagesPanelExpanded, value); }

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
            }
        }
    }

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
        CancelGlobalSearchCommand = new RelayCommand(() => _globalSearchCts?.Cancel(), () => IsGlobalSearching);
        OpenGlobalSearchHitCommand = new AsyncRelayCommand<GlobalSearchHit>(OpenGlobalSearchHitAsync);
        RefreshConnectionNames();
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
    }

    /// <summary>Fans out a bounded, "load current backlog" scan across every currently loaded topic in
    /// parallel and collects every message whose key or value contains <see cref="GlobalSearchTerm"/>.
    /// This is deliberately heavier than <see cref="MessageFilter"/> - it talks to the broker for every
    /// topic - so it's only triggered explicitly via <see cref="GlobalSearchCommand"/>, not on every
    /// keystroke.</summary>
    private async Task GlobalSearchAsync()
    {
        _globalSearchCts?.Cancel();
        var cts = new CancellationTokenSource();
        _globalSearchCts = cts;

        var term = GlobalSearchTerm;
        if (SelectedConnection is null || string.IsNullOrWhiteSpace(term)) return;
        if (!_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;

        var topics = _allTopics.Select(t => t.Name).ToList();
        if (topics.Count == 0)
        {
            StatusMessage = "No topics loaded to search.";
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
            using var throttle = new SemaphoreSlim(8);
            var scanned = 0;

            var perTopicTasks = topics.Select(async topic =>
            {
                await throttle.WaitAsync(cts.Token).ConfigureAwait(true);
                try
                {
                    var options = new ConsumeOptions
                    {
                        Topic = topic,
                        ConsumerGroup = $"kafka-studio-global-search-{Guid.NewGuid():N}",
                        StartPosition = ConsumeStartPosition.Earliest,
                        StopAtPartitionEnd = true
                    };

                    await foreach (var message in gateway.ConsumeAsync(options, cts.Token).ConfigureAwait(true))
                    {
                        var isMatch = (message.Value is not null && message.Value.Contains(term, StringComparison.OrdinalIgnoreCase)) ||
                                      (message.Key is not null && message.Key.Contains(term, StringComparison.OrdinalIgnoreCase));
                        if (isMatch)
                        {
                            GlobalSearchResults.Add(new GlobalSearchHit { Topic = topic, Message = message });
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
                    Interlocked.Increment(ref scanned);
                    GlobalSearchTopicsScanned = scanned;
                    throttle.Release();
                }
            });

            await Task.WhenAll(perTopicTasks).ConfigureAwait(true);

            if (cts.IsCancellationRequested) return;

            StatusMessage = $"Found {GlobalSearchResults.Count} message(s) matching \"{term}\" across {topics.Count} topic(s).";
        }
        catch (OperationCanceledException)
        {
            StatusMessage = "Search cancelled.";
        }
        finally
        {
            IsGlobalSearching = false;
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

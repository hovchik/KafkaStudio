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
    private readonly List<TopicRowViewModel> _allTopics = new();

    public ObservableCollection<string> ConnectionNames { get; } = new();
    public ObservableCollection<TopicRowViewModel> Topics { get; } = new();
    public ObservableCollection<KafkaMessage> ScannedMessages { get; } = new();

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
                _ = RefreshTopicsAsync();
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

    private int _scanLimit = 50;
    public int ScanLimit { get => _scanLimit; set => SetProperty(ref _scanLimit, value); }

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    public AsyncRelayCommand RefreshTopicsCommand { get; }
    public AsyncRelayCommand ScanCommand { get; }

    /// <summary>Bound to a topic row's double-click in the view - opens that topic (selecting it and
    /// loading its messages) regardless of what's currently selected.</summary>
    public AsyncRelayCommand<TopicRowViewModel> OpenTopicCommand { get; }

    public TopicBrowserViewModel(AppState state)
    {
        _state = state;
        _state.ConnectionsChanged += RefreshConnectionNames;
        RefreshTopicsCommand = new AsyncRelayCommand(RefreshTopicsAsync, () => SelectedConnection is not null);
        ScanCommand = new AsyncRelayCommand(ScanAsync, () => SelectedConnection is not null && SelectedTopic is not null);
        OpenTopicCommand = new AsyncRelayCommand<TopicRowViewModel>(OpenTopicAsync);
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
                MaxMessages = ScanLimit
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

            ScannedMessages.Clear();
            foreach (var message in buffer)
            {
                ScannedMessages.Add(message);
            }
            StatusMessage = $"Loaded {ScannedMessages.Count} message(s), newest first.";
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
}

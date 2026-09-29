using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.App.ViewModels.Consumer;

/// <summary>Live "tail -f" view of a topic: start watching, messages stream in as they arrive, stop
/// whenever. Uses <see cref="ConsumeStartPosition.Latest"/> by default so it behaves like watching a
/// log rather than replaying history, with an option to start from the beginning instead.
///
/// Messages are received on a background thread and handed to the UI in batches (at most one pending
/// UI update at a time), so a busy topic can't flood the dispatcher. The stream can be paused
/// (messages keep being received and are shown on resume) and filtered live.</summary>
public sealed class ConsumerViewModel : ObservableObject
{
    /// <summary>Hard cap when <see cref="MessageLimit"/> is 0 ("unlimited"), so a firehose can't exhaust memory.</summary>
    public const int UnlimitedCap = 100_000;

    private readonly AppState _state;
    private CancellationTokenSource? _watchCts;
    private CancellationTokenSource? _topicNamesCts;
    private readonly ConcurrentQueue<KafkaMessage> _incoming = new();
    private int _flushScheduled;
    private int _watchGeneration;

    /// <summary>Everything received (newest first), capped at the limit; <see cref="Messages"/> is the filtered view.</summary>
    private readonly List<KafkaMessage> _all = new();

    public ObservableCollection<string> ConnectionNames { get; } = new();
    public ObservableCollection<KafkaMessage> Messages { get; } = new();

    /// <summary>All topic names for the selected connection - populated automatically whenever
    /// <see cref="SelectedConnection"/> changes, so the Topic field can offer them all while still
    /// letting the user filter by typing (the AutoCompleteBox in the view does the filtering).</summary>
    public ObservableCollection<string> TopicNames { get; } = new();

    public MessageActions Actions { get; }

    private string? _selectedConnection;
    public string? SelectedConnection
    {
        get => _selectedConnection;
        set
        {
            if (SetProperty(ref _selectedConnection, value))
            {
                _ = RefreshTopicNamesAsync();
                StartCommand.RaiseCanExecuteChanged();
            }
        }
    }

    private string _topic = "";
    public string Topic
    {
        get => _topic;
        set
        {
            if (SetProperty(ref _topic, value ?? ""))
            {
                StartCommand.RaiseCanExecuteChanged();
            }
        }
    }

    private bool _fromBeginning;
    public bool FromBeginning { get => _fromBeginning; set => SetProperty(ref _fromBeginning, value); }

    private int _messageLimit = 500;
    /// <summary>Caps how many of the most recent messages are kept/displayed - older messages are
    /// dropped off the end as new ones arrive. A value of 0 or less means "unlimited" (up to
    /// <see cref="UnlimitedCap"/>).</summary>
    public int MessageLimit
    {
        get => _messageLimit;
        set
        {
            if (SetProperty(ref _messageLimit, Math.Max(0, value)))
            {
                TrimAll();
                ApplyFilter();
            }
        }
    }

    private int EffectiveLimit => MessageLimit <= 0 ? UnlimitedCap : MessageLimit;

    private string? _filter;
    /// <summary>Case-insensitive "contains" filter over key, value and header values.</summary>
    public string? Filter
    {
        get => _filter;
        set { if (SetProperty(ref _filter, value)) ApplyFilter(); }
    }

    private bool _isPaused;
    /// <summary>While paused, new messages are still received but not shown until resumed.</summary>
    public bool IsPaused
    {
        get => _isPaused;
        set
        {
            if (SetProperty(ref _isPaused, value))
            {
                if (!value) ScheduleFlush();
                OnPropertyChanged(nameof(PauseLabel));
            }
        }
    }

    public string PauseLabel => IsPaused ? "Resume" : "Pause";

    private KafkaMessage? _selectedMessage;
    public KafkaMessage? SelectedMessage { get => _selectedMessage; set => SetProperty(ref _selectedMessage, value); }

    private long _receivedCount;
    /// <summary>Total messages received since Start (including ones dropped by the limit).</summary>
    public long ReceivedCount { get => _receivedCount; private set => SetProperty(ref _receivedCount, value); }

    private int _pendingCount;
    /// <summary>Messages received while paused and not shown yet.</summary>
    public int PendingCount { get => _pendingCount; private set => SetProperty(ref _pendingCount, value); }

    private bool _isWatching;
    public bool IsWatching
    {
        get => _isWatching;
        private set
        {
            if (SetProperty(ref _isWatching, value))
            {
                StartCommand.RaiseCanExecuteChanged();
                StopCommand.RaiseCanExecuteChanged();
            }
        }
    }

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    public RelayCommand StartCommand { get; }
    public RelayCommand StopCommand { get; }
    public RelayCommand ClearCommand { get; }
    public RelayCommand TogglePauseCommand { get; }
    public AsyncRelayCommand ExportCommand { get; }

    public ConsumerViewModel(AppState state)
    {
        _state = state;
        _state.ConnectionsChanged += RefreshConnectionNames;
        StartCommand = new RelayCommand(Start, () => !IsWatching && SelectedConnection is not null && !string.IsNullOrWhiteSpace(Topic));
        StopCommand = new RelayCommand(Stop, () => IsWatching);
        ClearCommand = new RelayCommand(Clear);
        TogglePauseCommand = new RelayCommand(() => IsPaused = !IsPaused);
        Actions = new MessageActions(state, () => SelectedConnection, s => StatusMessage = s);
        ExportCommand = new AsyncRelayCommand(() => Actions.ExportAsync(Messages.ToList(), $"{MessageActions.SafeFileName(Topic)}-live.json"));
        RefreshConnectionNames();
    }

    private void RefreshConnectionNames()
    {
        CollectionSync.SyncSorted(ConnectionNames, _state.Connections.Keys);
        if (SelectedConnection is not null && !ConnectionNames.Contains(SelectedConnection))
        {
            if (IsWatching) Stop();
            SelectedConnection = null;
        }
        if (SelectedConnection is null && ConnectionNames.Count == 1) SelectedConnection = ConnectionNames[0];
    }

    private async Task RefreshTopicNamesAsync()
    {
        _topicNamesCts?.Cancel();
        var cts = new CancellationTokenSource();
        _topicNamesCts = cts;

        TopicNames.Clear();
        if (SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;

        try
        {
            var names = await gateway.ListTopicsAsync(cts.Token).ConfigureAwait(true);
            if (cts.IsCancellationRequested) return;
            foreach (var name in names) TopicNames.Add(name);
        }
        catch (OperationCanceledException)
        {
            // superseded by a newer connection selection - ignore.
        }
        catch (Exception ex)
        {
            if (!cts.IsCancellationRequested) StatusMessage = $"Failed to load topic names: {ex.Message}";
        }
    }

    private void Start()
    {
        if (SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;

        var topic = Topic.Trim();
        var cts = new CancellationTokenSource();
        _watchCts = cts;
        var generation = Interlocked.Increment(ref _watchGeneration);
        while (_incoming.TryDequeue(out _)) { }

        var options = new ConsumeOptions
        {
            Topic = topic,
            ConsumerGroup = $"kafka-studio-watch-{Guid.NewGuid():N}",
            StartPosition = FromBeginning ? ConsumeStartPosition.Earliest : ConsumeStartPosition.Latest,
            OnReady = () => _state.PostToUi(() =>
            {
                if (generation == _watchGeneration && IsWatching) StatusMessage = $"Watching '{topic}'.";
            })
        };

        IsWatching = true;
        IsPaused = false;
        ReceivedCount = 0;
        StatusMessage = $"Connecting to '{topic}'...";

        var token = cts.Token;
        _ = Task.Run(async () =>
        {
            string? endMessage = null;
            try
            {
                await foreach (var message in gateway.ConsumeAsync(options, token).ConfigureAwait(false))
                {
                    _incoming.Enqueue(message);
                    ScheduleFlush();
                }
                endMessage = "Stream ended.";
            }
            catch (OperationCanceledException)
            {
                // expected on Stop()
            }
            catch (Exception ex)
            {
                endMessage = $"Watch error: {ex.Message}";
            }

            // The stream can end on its own (error, gateway disposed): reflect that in the UI instead
            // of showing "watching" forever - unless a newer Start() already took over.
            _state.PostToUi(() =>
            {
                if (generation != _watchGeneration) return;
                if (endMessage is not null) StatusMessage = endMessage;
                IsWatching = false;
            });
        });
    }

    private void ScheduleFlush()
    {
        if (Interlocked.Exchange(ref _flushScheduled, 1) == 1) return;
        _state.PostToUi(Flush);
    }

    /// <summary>Runs on the UI thread: moves everything received so far into the visible list.</summary>
    private void Flush()
    {
        Volatile.Write(ref _flushScheduled, 0);

        var batch = new List<KafkaMessage>();
        while (_incoming.TryDequeue(out var message)) batch.Add(message);
        List<KafkaMessage> trimmed = new();
        if (batch.Count > 0)
        {
            ReceivedCount += batch.Count;
            // Newest first: the batch arrives oldest->newest.
            batch.Reverse();
            _all.InsertRange(0, batch);
            trimmed = TrimAll();
        }

        if (IsPaused)
        {
            PendingCount += batch.Count;
            return;
        }

        if (PendingCount > 0)
        {
            PendingCount = 0;
            ApplyFilter();
            return;
        }

        // Incremental update: new matching messages go on top, trimmed (oldest) ones fall off the end.
        for (var i = batch.Count - 1; i >= 0; i--)
        {
            if (Matches(batch[i])) Messages.Insert(0, batch[i]);
        }
        if (trimmed.Count > 0)
        {
            var gone = new HashSet<KafkaMessage>(trimmed, ReferenceEqualityComparer.Instance);
            while (Messages.Count > 0 && gone.Contains(Messages[^1])) Messages.RemoveAt(Messages.Count - 1);
            if (SelectedMessage is not null && gone.Contains(SelectedMessage)) SelectedMessage = null;
        }
    }

    /// <summary>Drops the oldest messages beyond the limit and returns them.</summary>
    private List<KafkaMessage> TrimAll()
    {
        var limit = EffectiveLimit;
        if (_all.Count <= limit) return new List<KafkaMessage>();
        var removed = _all.GetRange(limit, _all.Count - limit);
        _all.RemoveRange(limit, _all.Count - limit);
        return removed;
    }

    private bool Matches(KafkaMessage m)
    {
        var filter = Filter;
        if (string.IsNullOrWhiteSpace(filter)) return true;
        return (m.Value?.Contains(filter, StringComparison.OrdinalIgnoreCase) ?? false)
               || (m.Key?.Contains(filter, StringComparison.OrdinalIgnoreCase) ?? false)
               || m.Headers.Values.Any(v => v.Contains(filter, StringComparison.OrdinalIgnoreCase));
    }

    private void ApplyFilter()
    {
        var selected = SelectedMessage;
        Messages.Clear();
        foreach (var m in _all)
        {
            if (Matches(m)) Messages.Add(m);
        }
        // Clear() makes a bound list push a null selection; restore it if the message is still visible.
        SelectedMessage = selected is not null && Messages.Any(m => ReferenceEquals(m, selected)) ? selected : null;
    }

    private void Clear()
    {
        _all.Clear();
        Messages.Clear();
        SelectedMessage = null;
        PendingCount = 0;
        ReceivedCount = 0;
    }

    private void Stop()
    {
        _watchCts?.Cancel();
        Interlocked.Increment(ref _watchGeneration);
        IsWatching = false;
        IsPaused = false;
        StatusMessage = "Stopped.";
    }
}

using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.App.ViewModels.Consumer;

/// <summary>Live "tail -f" view of a topic: start watching, messages stream in as they arrive, stop
/// whenever. Uses <see cref="ConsumeStartPosition.Latest"/> by default so it behaves like watching a
/// log rather than replaying history, with an option to start from the beginning instead.</summary>
public sealed class ConsumerViewModel : ObservableObject
{
    private readonly AppState _state;
    private CancellationTokenSource? _watchCts;
    private Task? _watchTask;
    private CancellationTokenSource? _topicNamesCts;

    public ObservableCollection<string> ConnectionNames { get; } = new();
    public ObservableCollection<KafkaMessage> Messages { get; } = new();

    /// <summary>All topic names for the selected connection - populated automatically whenever
    /// <see cref="SelectedConnection"/> changes, so the Topic field can offer them all while still
    /// letting the user filter by typing (the AutoCompleteBox in the view does the filtering).</summary>
    public ObservableCollection<string> TopicNames { get; } = new();

    private string? _selectedConnection;
    public string? SelectedConnection
    {
        get => _selectedConnection;
        set
        {
            if (SetProperty(ref _selectedConnection, value))
            {
                _ = RefreshTopicNamesAsync();
            }
        }
    }

    private string _topic = "";
    public string Topic { get => _topic; set => SetProperty(ref _topic, value); }

    private bool _fromBeginning;
    public bool FromBeginning { get => _fromBeginning; set => SetProperty(ref _fromBeginning, value); }

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

    public ConsumerViewModel(AppState state)
    {
        _state = state;
        _state.ConnectionsChanged += RefreshConnectionNames;
        StartCommand = new RelayCommand(Start, () => !IsWatching && SelectedConnection is not null && !string.IsNullOrWhiteSpace(Topic));
        StopCommand = new RelayCommand(Stop, () => IsWatching);
        ClearCommand = new RelayCommand(() => Messages.Clear());
        RefreshConnectionNames();
    }

    private void RefreshConnectionNames()
    {
        ConnectionNames.Clear();
        foreach (var name in _state.Connections.Keys) ConnectionNames.Add(name);
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
            StatusMessage = $"Failed to load topic names: {ex.Message}";
        }
    }

    private void Start()
    {
        if (SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;

        _watchCts = new CancellationTokenSource();
        var options = new ConsumeOptions
        {
            Topic = Topic,
            ConsumerGroup = $"kafka-studio-watch-{Guid.NewGuid():N}",
            StartPosition = FromBeginning ? ConsumeStartPosition.Earliest : ConsumeStartPosition.Latest
        };

        IsWatching = true;
        StatusMessage = $"Watching '{Topic}'...";

        var token = _watchCts.Token;
        _watchTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var message in gateway.ConsumeAsync(options, token).WithCancellation(token))
                {
                    // Marshal back onto whatever thread owns the UI - the App project's dispatcher
                    // wraps this ViewModel's collection mutations appropriately (see MainWindow.axaml.cs).
                    Messages.Insert(0, message);
                }
            }
            catch (OperationCanceledException)
            {
                // expected on Stop()
            }
            catch (Exception ex)
            {
                StatusMessage = $"Watch error: {ex.Message}";
            }
        }, token);
    }

    private void Stop()
    {
        _watchCts?.Cancel();
        IsWatching = false;
        StatusMessage = "Stopped.";
    }
}

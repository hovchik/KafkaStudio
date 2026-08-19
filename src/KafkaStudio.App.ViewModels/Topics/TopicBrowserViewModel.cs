using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.App.ViewModels.Topics;

public sealed class TopicRowViewModel : ObservableObject
{
    public required string Name { get; init; }
    public required int PartitionCount { get; init; }
    public required long TotalMessageCount { get; init; }
}

/// <summary>Lists topics for the selected connection and lets you scan a backlog on demand (the same
/// "scan and acknowledge" capability the DSL exposes, surfaced as a point-and-click tool).</summary>
public sealed class TopicBrowserViewModel : ObservableObject
{
    private readonly AppState _state;

    public ObservableCollection<string> ConnectionNames { get; } = new();
    public ObservableCollection<TopicRowViewModel> Topics { get; } = new();
    public ObservableCollection<KafkaMessage> ScannedMessages { get; } = new();

    private string? _selectedConnection;
    public string? SelectedConnection
    {
        get => _selectedConnection;
        set => SetProperty(ref _selectedConnection, value);
    }

    private string? _selectedTopic;
    public string? SelectedTopic
    {
        get => _selectedTopic;
        set => SetProperty(ref _selectedTopic, value);
    }

    private int _scanLimit = 50;
    public int ScanLimit { get => _scanLimit; set => SetProperty(ref _scanLimit, value); }

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    public AsyncRelayCommand RefreshTopicsCommand { get; }
    public AsyncRelayCommand ScanCommand { get; }

    public TopicBrowserViewModel(AppState state)
    {
        _state = state;
        _state.ConnectionsChanged += RefreshConnectionNames;
        RefreshTopicsCommand = new AsyncRelayCommand(RefreshTopicsAsync, () => SelectedConnection is not null);
        ScanCommand = new AsyncRelayCommand(ScanAsync, () => SelectedConnection is not null && SelectedTopic is not null);
        RefreshConnectionNames();
    }

    private void RefreshConnectionNames()
    {
        ConnectionNames.Clear();
        foreach (var name in _state.Connections.Keys) ConnectionNames.Add(name);
    }

    private async Task RefreshTopicsAsync()
    {
        if (SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;

        StatusMessage = "Loading topics...";
        Topics.Clear();
        try
        {
            var names = await gateway.ListTopicsAsync().ConfigureAwait(true);
            foreach (var name in names)
            {
                var metadata = await gateway.DescribeTopicAsync(name).ConfigureAwait(true);
                Topics.Add(new TopicRowViewModel
                {
                    Name = name,
                    PartitionCount = metadata.Partitions.Count,
                    TotalMessageCount = metadata.TotalMessageCount
                });
            }
            StatusMessage = $"{Topics.Count} topic(s).";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Failed to load topics: {ex.Message}";
        }
    }

    private async Task ScanAsync()
    {
        if (SelectedConnection is null || SelectedTopic is null) return;
        if (!_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;

        ScannedMessages.Clear();
        StatusMessage = $"Scanning '{SelectedTopic}'...";
        try
        {
            var options = new ConsumeOptions
            {
                Topic = SelectedTopic,
                ConsumerGroup = $"kafka-studio-browser-{Guid.NewGuid():N}",
                StartPosition = ConsumeStartPosition.Earliest,
                MaxMessages = ScanLimit
            };
            await foreach (var message in gateway.ConsumeAsync(options))
            {
                ScannedMessages.Add(message);
            }
            StatusMessage = $"Scanned {ScannedMessages.Count} message(s).";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Scan failed: {ex.Message}";
        }
    }
}

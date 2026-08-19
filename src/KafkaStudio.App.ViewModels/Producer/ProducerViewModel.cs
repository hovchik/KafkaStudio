using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.App.ViewModels.Producer;

public sealed class HeaderEntryViewModel : ObservableObject
{
    private string _name = "";
    public string Name { get => _name; set => SetProperty(ref _name, value); }

    private string _value = "";
    public string Value { get => _value; set => SetProperty(ref _value, value); }
}

/// <summary>Ad-hoc "send a message" form - the point-and-click equivalent of a KafScript "produce
/// message" step, handy for quick manual testing without writing a script.</summary>
public sealed class ProducerViewModel : ObservableObject
{
    private readonly AppState _state;
    private CancellationTokenSource? _topicNamesCts;

    public ObservableCollection<string> ConnectionNames { get; } = new();
    public ObservableCollection<HeaderEntryViewModel> Headers { get; } = new();
    public ObservableCollection<string> History { get; } = new();

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

    private string _key = "";
    public string Key { get => _key; set => SetProperty(ref _key, value); }

    private string _value = "";
    public string Value { get => _value; set => SetProperty(ref _value, value); }

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    public RelayCommand AddHeaderCommand { get; }
    public RelayCommand<HeaderEntryViewModel> RemoveHeaderCommand { get; }
    public AsyncRelayCommand SendCommand { get; }

    public ProducerViewModel(AppState state)
    {
        _state = state;
        _state.ConnectionsChanged += RefreshConnectionNames;
        AddHeaderCommand = new RelayCommand(() => Headers.Add(new HeaderEntryViewModel()));
        RemoveHeaderCommand = new RelayCommand<HeaderEntryViewModel>(h => { if (h is not null) Headers.Remove(h); });
        SendCommand = new AsyncRelayCommand(SendAsync, () => SelectedConnection is not null && !string.IsNullOrWhiteSpace(Topic));
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

    private async Task SendAsync()
    {
        if (SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;

        try
        {
            var headers = Headers
                .Where(h => !string.IsNullOrWhiteSpace(h.Name))
                .ToDictionary(h => h.Name, h => h.Value);

            var receipt = await gateway.ProduceAsync(new ProduceRequest
            {
                Topic = Topic,
                Key = string.IsNullOrEmpty(Key) ? null : Key,
                Value = Value,
                Headers = headers.Count > 0 ? headers : null
            }).ConfigureAwait(true);

            var line = $"{DateTimeOffset.Now:HH:mm:ss} -> {receipt.Topic}#{receipt.Partition}@{receipt.Offset}";
            History.Insert(0, line);
            StatusMessage = "Sent.";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Failed to send: {ex.Message}";
        }
    }
}

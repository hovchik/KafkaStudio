using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Automation.Rethrow;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.App.ViewModels.Rethrow;

public sealed class RethrowRuleRowViewModel : ObservableObject
{
    public required RethrowRule Rule { get; init; }

    public string Name => Rule.Name;
    public string Description => $"{Rule.SourceConnection}:{Rule.SourceTopic} -> {Rule.DestinationConnection}:{Rule.DestinationTopic}";

    private bool _isRunning;
    public bool IsRunning { get => _isRunning; set => SetProperty(ref _isRunning, value); }

    private int _relayedCount;
    public int RelayedCount { get => _relayedCount; set => SetProperty(ref _relayedCount, value); }

    private string? _lastError;
    public string? LastError { get => _lastError; set => SetProperty(ref _lastError, value); }
}

/// <summary>Point-and-click "rethrow messages from one topic to another" screen - the no-script
/// alternative to a KafScript "watch / message arrives / rethrow" scenario, meant for standing relays
/// you want running continuously rather than as a one-shot check.</summary>
public sealed class RethrowRulesViewModel : ObservableObject
{
    private readonly AppState _state;

    public ObservableCollection<string> ConnectionNames { get; } = new();
    public ObservableCollection<RethrowRuleRowViewModel> Rules { get; } = new();

    private string _newName = "";
    public string NewName { get => _newName; set => SetProperty(ref _newName, value); }

    private string? _newSourceConnection;
    public string? NewSourceConnection { get => _newSourceConnection; set => SetProperty(ref _newSourceConnection, value); }

    private string _newSourceTopic = "";
    public string NewSourceTopic { get => _newSourceTopic; set => SetProperty(ref _newSourceTopic, value); }

    private string? _newDestinationConnection;
    public string? NewDestinationConnection { get => _newDestinationConnection; set => SetProperty(ref _newDestinationConnection, value); }

    private string _newDestinationTopic = "";
    public string NewDestinationTopic { get => _newDestinationTopic; set => SetProperty(ref _newDestinationTopic, value); }

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    public RelayCommand AddRuleCommand { get; }
    public RelayCommand<RethrowRuleRowViewModel> StartCommand { get; }
    public AsyncRelayCommand<RethrowRuleRowViewModel> StopCommand { get; }

    public RethrowRulesViewModel(AppState state)
    {
        _state = state;
        _state.ConnectionsChanged += RefreshConnectionNames;
        _state.RethrowManager.MessageRelayed += OnMessageRelayed;
        _state.RethrowManager.RelayFailed += OnRelayFailed;

        AddRuleCommand = new RelayCommand(AddRule,
            () => !string.IsNullOrWhiteSpace(NewName) && NewSourceConnection is not null && NewDestinationConnection is not null);
        StartCommand = new RelayCommand<RethrowRuleRowViewModel>(Start);
        StopCommand = new AsyncRelayCommand<RethrowRuleRowViewModel>(StopAsync);

        RefreshConnectionNames();
    }

    private void RefreshConnectionNames()
    {
        ConnectionNames.Clear();
        foreach (var name in _state.Connections.Keys) ConnectionNames.Add(name);
    }

    private void AddRule()
    {
        var rule = new RethrowRule
        {
            Name = NewName.Trim(),
            SourceConnection = NewSourceConnection!,
            SourceTopic = NewSourceTopic.Trim(),
            DestinationConnection = NewDestinationConnection!,
            DestinationTopic = NewDestinationTopic.Trim()
        };
        Rules.Add(new RethrowRuleRowViewModel { Rule = rule });
        NewName = "";
        NewSourceTopic = "";
        NewDestinationTopic = "";
    }

    private void Start(RethrowRuleRowViewModel? row)
    {
        if (row is null) return;
        try
        {
            _state.RethrowManager.Start(row.Rule, _state.Connections);
            row.IsRunning = true;
            row.LastError = null;
        }
        catch (Exception ex)
        {
            StatusMessage = $"Could not start '{row.Name}': {ex.Message}";
        }
    }

    private async Task StopAsync(RethrowRuleRowViewModel? row)
    {
        if (row is null) return;
        await _state.RethrowManager.StopAsync(row.Name).ConfigureAwait(true);
        row.IsRunning = false;
    }

    private void OnMessageRelayed(RethrowRule rule, KafkaMessage message, ProduceReceipt receipt)
    {
        var row = Rules.FirstOrDefault(r => r.Rule.Name == rule.Name);
        if (row is not null) row.RelayedCount++;
    }

    private void OnRelayFailed(RethrowRule rule, Exception ex)
    {
        var row = Rules.FirstOrDefault(r => r.Rule.Name == rule.Name);
        if (row is not null) row.LastError = ex.Message;
    }
}

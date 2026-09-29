using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Automation.Rethrow;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Persistence;
using KafkaStudio.Scripting.Ast;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.App.ViewModels.Rethrow;

public sealed class RethrowRuleRowViewModel : ObservableObject
{
    public required RethrowRule Rule { get; init; }

    public string Name => Rule.Name;
    public string Description => $"{Rule.SourceConnection}:{Rule.SourceTopic}  →  {Rule.DestinationConnection}:{Rule.DestinationTopic}";

    public string FilterDescription => Rule.Filters.Count == 0
        ? "all messages"
        : "where " + string.Join(" and ", Rule.Filters.Select(f =>
            $"{(f.Field == ConditionField.Json ? $"json \"{f.JsonPath}\"" : f.Field.ToString().ToLowerInvariant())} {ConditionEvaluator.Describe(f.Comparator)} \"{f.Expected}\""));

    public string KeyDescription => Rule.KeepSourceKey ? "keeps key" : Rule.FixedKey is null ? "no key" : $"key \"{Rule.FixedKey}\"";

    private bool _isRunning;
    public bool IsRunning
    {
        get => _isRunning;
        set
        {
            if (SetProperty(ref _isRunning, value)) OnPropertyChanged(nameof(IsStopped));
        }
    }

    public bool IsStopped => !IsRunning;

    private int _relayedCount;
    public int RelayedCount { get => _relayedCount; set => SetProperty(ref _relayedCount, value); }

    private int _skippedCount;
    public int SkippedCount { get => _skippedCount; set => SetProperty(ref _skippedCount, value); }

    private string? _lastActivity;
    public string? LastActivity { get => _lastActivity; set => SetProperty(ref _lastActivity, value); }

    private string? _lastError;
    public string? LastError
    {
        get => _lastError;
        set
        {
            if (SetProperty(ref _lastError, value)) OnPropertyChanged(nameof(HasError));
        }
    }

    public bool HasError => !string.IsNullOrEmpty(LastError);
}

/// <summary>Point-and-click "rethrow messages from one topic to another" screen - the no-script
/// alternative to a KafScript "watch / message arrives / rethrow" scenario, meant for standing relays
/// you want running continuously rather than as a one-shot check. Rules are saved across restarts
/// (always loaded stopped, so nothing starts relaying on its own).</summary>
public sealed class RethrowRulesViewModel : ObservableObject
{
    private const string StoreFile = "rethrow-rules.json";

    private readonly AppState _state;

    public ObservableCollection<string> ConnectionNames { get; } = new();
    public ObservableCollection<RethrowRuleRowViewModel> Rules { get; } = new();

    public IReadOnlyList<string> FilterFieldOptions { get; } = new[] { "(no filter)", "key", "value", "json" };
    public IReadOnlyList<string> ComparatorOptions { get; } = new[] { "equals", "not equals", "contains", "matches" };

    private string _newName = "";
    public string NewName { get => _newName; set { if (SetProperty(ref _newName, value ?? "")) RaiseForm(); } }

    private string? _newSourceConnection;
    public string? NewSourceConnection { get => _newSourceConnection; set { if (SetProperty(ref _newSourceConnection, value)) RaiseForm(); } }

    private string _newSourceTopic = "";
    public string NewSourceTopic { get => _newSourceTopic; set { if (SetProperty(ref _newSourceTopic, value ?? "")) RaiseForm(); } }

    private string? _newDestinationConnection;
    public string? NewDestinationConnection { get => _newDestinationConnection; set { if (SetProperty(ref _newDestinationConnection, value)) RaiseForm(); } }

    private string _newDestinationTopic = "";
    public string NewDestinationTopic { get => _newDestinationTopic; set { if (SetProperty(ref _newDestinationTopic, value ?? "")) RaiseForm(); } }

    private string _newFilterField = "(no filter)";
    public string NewFilterField
    {
        get => _newFilterField;
        set
        {
            if (SetProperty(ref _newFilterField, value ?? "(no filter)"))
            {
                OnPropertyChanged(nameof(HasFilter));
                OnPropertyChanged(nameof(IsJsonFilter));
            }
        }
    }

    public bool HasFilter => NewFilterField != "(no filter)";
    public bool IsJsonFilter => NewFilterField == "json";

    private string _newFilterPath = "$.";
    public string NewFilterPath { get => _newFilterPath; set => SetProperty(ref _newFilterPath, value ?? ""); }

    private string _newFilterComparator = "equals";
    public string NewFilterComparator { get => _newFilterComparator; set => SetProperty(ref _newFilterComparator, value ?? "equals"); }

    private string _newFilterValue = "";
    public string NewFilterValue { get => _newFilterValue; set => SetProperty(ref _newFilterValue, value ?? ""); }

    private bool _newKeepSourceKey = true;
    public bool NewKeepSourceKey { get => _newKeepSourceKey; set => SetProperty(ref _newKeepSourceKey, value); }

    private string? _newHeaderName;
    /// <summary>Optional header stamped on every relayed message (e.g. "relayed-by").</summary>
    public string? NewHeaderName { get => _newHeaderName; set => SetProperty(ref _newHeaderName, value); }

    private string? _newHeaderValue;
    public string? NewHeaderValue { get => _newHeaderValue; set => SetProperty(ref _newHeaderValue, value); }

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    public RelayCommand AddRuleCommand { get; }
    public RelayCommand<RethrowRuleRowViewModel> StartCommand { get; }
    public AsyncRelayCommand<RethrowRuleRowViewModel> StopCommand { get; }
    public AsyncRelayCommand<RethrowRuleRowViewModel> RemoveCommand { get; }
    public RelayCommand<RethrowRuleRowViewModel> EditCommand { get; }
    public RelayCommand StartAllCommand { get; }
    public AsyncRelayCommand StopAllCommand { get; }

    public RethrowRulesViewModel(AppState state)
    {
        _state = state;
        _state.ConnectionsChanged += RefreshConnectionNames;
        _state.RethrowManager.MessageRelayed += (rule, message, receipt) => _state.PostToUi(() => OnMessageRelayed(rule, message, receipt));
        _state.RethrowManager.MessageSkipped += (rule, _) => _state.PostToUi(() => OnMessageSkipped(rule));
        _state.RethrowManager.RelayFailed += (rule, ex) => _state.PostToUi(() => OnRelayFailed(rule, ex));
        _state.RethrowManager.RuleStopped += (rule, ex) => _state.PostToUi(() => OnRuleStopped(rule, ex));

        AddRuleCommand = new RelayCommand(AddRule,
            () => !string.IsNullOrWhiteSpace(NewName) && NewSourceConnection is not null && NewDestinationConnection is not null
                  && !string.IsNullOrWhiteSpace(NewSourceTopic) && !string.IsNullOrWhiteSpace(NewDestinationTopic));
        StartCommand = new RelayCommand<RethrowRuleRowViewModel>(Start, row => row is { IsRunning: false });
        StopCommand = new AsyncRelayCommand<RethrowRuleRowViewModel>(StopAsync, row => row is { IsRunning: true }, allowConcurrentExecutions: true);
        RemoveCommand = new AsyncRelayCommand<RethrowRuleRowViewModel>(RemoveAsync, allowConcurrentExecutions: true);
        EditCommand = new RelayCommand<RethrowRuleRowViewModel>(LoadIntoForm);
        StartAllCommand = new RelayCommand(() => { foreach (var row in Rules.Where(r => !r.IsRunning).ToList()) Start(row); });
        StopAllCommand = new AsyncRelayCommand(async () =>
        {
            foreach (var row in Rules.Where(r => r.IsRunning).ToList()) await StopAsync(row).ConfigureAwait(true);
        });

        foreach (var rule in JsonFileStore.Load(StoreFile, new List<RethrowRule>()))
        {
            if (!string.IsNullOrWhiteSpace(rule.Name) && Rules.All(r => r.Name != rule.Name))
            {
                Rules.Add(new RethrowRuleRowViewModel { Rule = rule });
            }
        }

        RefreshConnectionNames();
    }

    private void RaiseForm() => AddRuleCommand.RaiseCanExecuteChanged();

    private void RaiseRowCommands()
    {
        StartCommand.RaiseCanExecuteChanged();
        StopCommand.RaiseCanExecuteChanged();
    }

    private void RefreshConnectionNames()
    {
        CollectionSync.SyncSorted(ConnectionNames, _state.Connections.Keys);
        if (NewSourceConnection is not null && !ConnectionNames.Contains(NewSourceConnection)) NewSourceConnection = null;
        if (NewDestinationConnection is not null && !ConnectionNames.Contains(NewDestinationConnection)) NewDestinationConnection = null;
        if (ConnectionNames.Count == 1)
        {
            NewSourceConnection ??= ConnectionNames[0];
            NewDestinationConnection ??= ConnectionNames[0];
        }
    }

    private void Persist()
    {
        var error = JsonFileStore.TrySave(StoreFile, Rules.Select(r => r.Rule).ToList());
        if (error is not null) StatusMessage = error;
    }

    private void AddRule()
    {
        var name = NewName.Trim();
        var filters = new List<Condition>();
        if (HasFilter)
        {
            var field = NewFilterField switch { "key" => ConditionField.Key, "json" => ConditionField.Json, _ => ConditionField.Value };
            var comparator = NewFilterComparator switch
            {
                "not equals" => Comparator.NotEquals,
                "contains" => Comparator.Contains,
                "matches" => Comparator.Matches,
                _ => Comparator.Equals
            };
            filters.Add(new Condition(field, field == ConditionField.Json ? NewFilterPath.Trim() : null, comparator, NewFilterValue));
        }

        var headers = new Dictionary<string, string>();
        if (!string.IsNullOrWhiteSpace(NewHeaderName)) headers[NewHeaderName.Trim()] = NewHeaderValue ?? "";

        var rule = new RethrowRule
        {
            Name = name,
            SourceConnection = NewSourceConnection!,
            SourceTopic = NewSourceTopic.Trim(),
            DestinationConnection = NewDestinationConnection!,
            DestinationTopic = NewDestinationTopic.Trim(),
            Filters = filters,
            KeepSourceKey = NewKeepSourceKey,
            ExtraHeaders = headers
        };

        try
        {
            RethrowEngine.Validate(rule, _state.Connections);
        }
        catch (ArgumentException ex)
        {
            StatusMessage = $"Can't add rule: {ex.Message}";
            return;
        }

        var existing = Rules.FirstOrDefault(r => r.Name == name);
        if (existing is not null)
        {
            if (existing.IsRunning)
            {
                StatusMessage = $"Rule '{name}' is running - stop it before replacing it.";
                return;
            }
            Rules[Rules.IndexOf(existing)] = new RethrowRuleRowViewModel { Rule = rule };
            StatusMessage = $"Updated rule '{name}'.";
        }
        else
        {
            Rules.Add(new RethrowRuleRowViewModel { Rule = rule });
            StatusMessage = $"Added rule '{name}' - press Start to begin relaying.";
        }

        Persist();
        NewName = "";
        NewSourceTopic = "";
        NewDestinationTopic = "";
        NewFilterField = "(no filter)";
        NewFilterValue = "";
    }

    private void LoadIntoForm(RethrowRuleRowViewModel? row)
    {
        if (row is null) return;
        var r = row.Rule;
        NewName = r.Name;
        NewSourceConnection = ConnectionNames.Contains(r.SourceConnection) ? r.SourceConnection : null;
        NewSourceTopic = r.SourceTopic;
        NewDestinationConnection = ConnectionNames.Contains(r.DestinationConnection) ? r.DestinationConnection : null;
        NewDestinationTopic = r.DestinationTopic;
        NewKeepSourceKey = r.KeepSourceKey;
        var filter = r.Filters.FirstOrDefault();
        NewFilterField = filter is null ? "(no filter)" : filter.Field.ToString().ToLowerInvariant();
        NewFilterPath = filter?.JsonPath ?? "$.";
        NewFilterComparator = filter is null ? "equals" : ConditionEvaluator.Describe(filter.Comparator);
        NewFilterValue = filter?.Expected ?? "";
        var header = r.ExtraHeaders.FirstOrDefault();
        NewHeaderName = header.Key;
        NewHeaderValue = header.Value;
        StatusMessage = $"Editing '{r.Name}' - change it and press Add / update.";
    }

    private void Start(RethrowRuleRowViewModel? row)
    {
        if (row is null || row.IsRunning) return;
        try
        {
            _state.RethrowManager.Start(row.Rule, _state.Connections);
            row.IsRunning = true;
            row.LastError = null;
            row.LastActivity = $"started {DateTime.Now:HH:mm:ss}";
        }
        catch (Exception ex)
        {
            row.LastError = ex.Message;
            StatusMessage = $"Could not start '{row.Name}': {ex.Message}";
        }
        RaiseRowCommands();
    }

    private async Task StopAsync(RethrowRuleRowViewModel? row)
    {
        if (row is null) return;
        await _state.RethrowManager.StopAsync(row.Name).ConfigureAwait(true);
        row.IsRunning = false;
        row.LastActivity = $"stopped {DateTime.Now:HH:mm:ss}";
        RaiseRowCommands();
    }

    private async Task RemoveAsync(RethrowRuleRowViewModel? row)
    {
        if (row is null) return;
        if (row.IsRunning) await _state.RethrowManager.StopAsync(row.Name).ConfigureAwait(true);
        Rules.Remove(row);
        Persist();
        StatusMessage = $"Removed rule '{row.Name}'.";
    }

    private RethrowRuleRowViewModel? Find(RethrowRule rule) => Rules.FirstOrDefault(r => r.Rule.Name == rule.Name);

    private void OnMessageRelayed(RethrowRule rule, KafkaMessage message, ProduceReceipt receipt)
    {
        if (Find(rule) is not { } row) return;
        row.RelayedCount++;
        row.LastActivity = $"{DateTime.Now:HH:mm:ss} relayed {message.Topic}@{message.Offset} → #{receipt.Partition}@{receipt.Offset}";
    }

    private void OnMessageSkipped(RethrowRule rule)
    {
        if (Find(rule) is { } row) row.SkippedCount++;
    }

    private void OnRelayFailed(RethrowRule rule, Exception ex)
    {
        if (Find(rule) is { } row) row.LastError = $"{DateTime.Now:HH:mm:ss} {ex.Message}";
    }

    private void OnRuleStopped(RethrowRule rule, Exception? ex)
    {
        if (Find(rule) is not { } row) return;
        row.IsRunning = false;
        row.LastActivity = $"stopped by itself {DateTime.Now:HH:mm:ss}";
        if (ex is not null) row.LastError = ex.Message;
        RaiseRowCommands();
    }
}

using System.Collections.ObjectModel;
using System.Text;
using System.Text.RegularExpressions;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Validation;
using KafkaStudio.Search;

namespace KafkaStudio.App.ViewModels.Testing;

/// <summary>A message that breaks the contract.</summary>
public sealed class ContractViolationRowViewModel
{
    public required KafkaMessage Message { get; init; }
    public required IReadOnlyList<SchemaViolation> Violations { get; init; }

    public string Location => $"#{Message.Partition}@{Message.Offset}";
    public string Key => Message.Key ?? "<null>";
    public string Timestamp => Message.Timestamp.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss");
    public string FirstProblem => Violations.Count == 0 ? "" : Violations[0].ToString();
    public string CountText => Violations.Count == 1 ? "" : $"+{Violations.Count - 1} more";
    public string AllProblems => string.Join("\n", Violations.Select(v => "• " + v));
}

/// <summary>How often one kind of problem occurs across the checked messages.</summary>
public sealed record ContractProblemGroup(string Path, string Example, int Count)
{
    public string CountText => $"{Count}×";
}

/// <summary>
/// Contract check: validate what's on a topic against a JSON Schema - the agreed message contract - and
/// see which messages break it and how (grouped by problem, so "312× $.currency: required field is
/// missing" stands out). No schema yet? Infer one from the topic's messages and tighten it. A contract
/// can be turned into a KafScript check with one click, so it keeps being verified in the regression pack.
/// </summary>
public sealed partial class ContractCheckViewModel : ObservableObject
{
    public const int MaxRows = 2000;
    public const int MaxMessages = 200_000;
    public const int InferenceSamples = 5000;

    private readonly AppState _state;
    private CancellationTokenSource? _cts;

    public ContractCheckViewModel(AppState state)
    {
        _state = state;
        Actions = new MessageActions(state, () => SelectedConnection, s => StatusMessage = s);
        LoadTopicsCommand = new AsyncRelayCommand(LoadTopicsAsync, () => SelectedConnection is not null);
        ValidateCommand = new AsyncRelayCommand(ValidateAsync, () => CanScan() && SchemaError is null && !string.IsNullOrWhiteSpace(SchemaText));
        InferCommand = new AsyncRelayCommand(InferAsync, CanScan);
        StopCommand = new RelayCommand(() => _cts?.Cancel(), () => IsBusy);
        OpenSchemaCommand = new AsyncRelayCommand(OpenSchemaAsync);
        SaveSchemaCommand = new AsyncRelayCommand(SaveSchemaAsync, () => !string.IsNullOrWhiteSpace(SchemaText));
        ToScriptCommand = new RelayCommand(ToScript, () => CanScan() && SchemaError is null && !string.IsNullOrWhiteSpace(SchemaText));
        _state.ConnectionsChanged += RefreshConnections;
        RefreshConnections();
    }

    public MessageActions Actions { get; }

    public ObservableCollection<string> ConnectionNames { get; } = new();
    public ObservableCollection<string> TopicNames { get; } = new();

    private string? _selectedConnection;
    public string? SelectedConnection
    {
        get => _selectedConnection;
        set
        {
            if (!SetProperty(ref _selectedConnection, value)) return;
            RaiseCommandStates();
            TopicNames.Clear();
            if (value is not null) _ = LoadTopicsAsync();
        }
    }

    private string? _topic;
    public string? Topic { get => _topic; set { if (SetProperty(ref _topic, value?.Trim())) RaiseCommandStates(); } }

    private bool _newestOnly = true;
    /// <summary>Read only the newest <see cref="NewestCount"/> messages per partition (faster on big topics).</summary>
    public bool NewestOnly { get => _newestOnly; set => SetProperty(ref _newestOnly, value); }

    private decimal _newestCount = 1000;
    public decimal NewestCount { get => _newestCount; set => SetProperty(ref _newestCount, Math.Clamp(value, 1, MaxMessages)); }

    private string _schemaText = """
        {
          "type": "object",
          "required": [],
          "properties": {}
        }
        """;
    public string SchemaText
    {
        get => _schemaText;
        set
        {
            if (!SetProperty(ref _schemaText, value ?? "")) return;
            SchemaError = string.IsNullOrWhiteSpace(_schemaText) ? null
                : JsonSchema.TryParse(_schemaText, out _, out var problem) ? null : problem;
            RaiseCommandStates();
        }
    }

    private string? _schemaError;
    public string? SchemaError { get => _schemaError; private set => SetProperty(ref _schemaError, value); }

    private bool _isBusy;
    public bool IsBusy { get => _isBusy; private set { if (SetProperty(ref _isBusy, value)) RaiseCommandStates(); } }

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    private bool? _lastPassed;
    /// <summary>True when every checked message matched; drives the summary colour.</summary>
    public bool? LastPassed { get => _lastPassed; private set => SetProperty(ref _lastPassed, value); }

    private int _checked, _valid, _invalid;
    public int CheckedCount { get => _checked; private set => SetProperty(ref _checked, value); }
    public int ValidCount { get => _valid; private set => SetProperty(ref _valid, value); }
    public int InvalidCount { get => _invalid; private set => SetProperty(ref _invalid, value); }

    public ObservableCollection<ContractViolationRowViewModel> Violations { get; } = new();
    public ObservableCollection<ContractProblemGroup> ProblemGroups { get; } = new();

    private ContractViolationRowViewModel? _selectedViolation;
    public ContractViolationRowViewModel? SelectedViolation
    {
        get => _selectedViolation;
        set { if (SetProperty(ref _selectedViolation, value)) OnPropertyChanged(nameof(SelectedMessage)); }
    }

    public KafkaMessage? SelectedMessage => SelectedViolation?.Message;

    public AsyncRelayCommand LoadTopicsCommand { get; }
    public AsyncRelayCommand ValidateCommand { get; }
    public AsyncRelayCommand InferCommand { get; }
    public RelayCommand StopCommand { get; }
    public AsyncRelayCommand OpenSchemaCommand { get; }
    public AsyncRelayCommand SaveSchemaCommand { get; }
    public RelayCommand ToScriptCommand { get; }

    private bool CanScan() => !IsBusy && SelectedConnection is not null && !string.IsNullOrWhiteSpace(Topic);

    private void RaiseCommandStates()
    {
        LoadTopicsCommand?.RaiseCanExecuteChanged();
        ValidateCommand?.RaiseCanExecuteChanged();
        InferCommand?.RaiseCanExecuteChanged();
        StopCommand?.RaiseCanExecuteChanged();
        SaveSchemaCommand?.RaiseCanExecuteChanged();
        ToScriptCommand?.RaiseCanExecuteChanged();
    }

    private void RefreshConnections()
    {
        CollectionSync.SyncSorted(ConnectionNames, _state.Connections.Keys);
        if (SelectedConnection is null || !_state.Connections.ContainsKey(SelectedConnection))
        {
            SelectedConnection = ConnectionNames.FirstOrDefault();
        }
    }

    private async Task LoadTopicsAsync()
    {
        if (SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;
        try
        {
            var topics = await gateway.ListTopicsAsync().ConfigureAwait(true);
            CollectionSync.SyncSorted(TopicNames, topics);
        }
        catch (Exception ex)
        {
            StatusMessage = $"Could not list topics: {ex.Message}";
        }
    }

    /// <summary>Reads the topic (newest N per partition, or everything up to <see cref="MaxMessages"/>).</summary>
    private async Task<(List<KafkaMessage> Messages, string? Problem, bool Capped)> ReadTopicAsync(int? cap, CancellationToken ct)
    {
        if (SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway))
        {
            return (new List<KafkaMessage>(), "Connection isn't connected.", false);
        }
        var topic = Topic!;
        var range = NewestOnly ? SearchRange.Newest((int)NewestCount) : SearchRange.All;
        var limit = cap ?? MaxMessages;
        var messages = new List<KafkaMessage>();
        var summary = await Task.Run(() => TopicScanner.ScanAsync(gateway, new[] { topic }, range, (_, message) =>
        {
            messages.Add(message);
            return messages.Count >= limit ? ScanDecision.StopAll : ScanDecision.Continue;
        }, ct), ct).ConfigureAwait(true);

        var problem = summary.FailedTopics.TryGetValue(topic, out var error) ? $"Could not read '{topic}': {error}" : null;
        return (messages, problem, summary.StoppedEarly);
    }

    private async Task ValidateAsync()
    {
        if (!JsonSchema.TryParse(SchemaText, out var schema, out var schemaProblem))
        {
            SchemaError = schemaProblem;
            return;
        }

        using var cts = new CancellationTokenSource();
        _cts = cts;
        IsBusy = true;
        Violations.Clear();
        ProblemGroups.Clear();
        SelectedViolation = null;
        LastPassed = null;
        CheckedCount = ValidCount = InvalidCount = 0;
        StatusMessage = $"Reading '{Topic}'…";
        try
        {
            var (messages, problem, capped) = await ReadTopicAsync(null, cts.Token).ConfigureAwait(true);
            if (problem is not null)
            {
                StatusMessage = problem;
                return;
            }

            StatusMessage = $"Validating {messages.Count:N0} message(s)…";
            var results = await Task.Run(() => messages
                .Select(m => (Message: m, Violations: schema!.Validate(m.Value)))
                .ToList(), cts.Token).ConfigureAwait(true);

            var invalid = results.Where(r => r.Violations.Count > 0).ToList();
            CheckedCount = results.Count;
            InvalidCount = invalid.Count;
            ValidCount = results.Count - invalid.Count;
            foreach (var (message, violations) in invalid.Take(MaxRows))
            {
                Violations.Add(new ContractViolationRowViewModel { Message = message, Violations = violations });
            }
            foreach (var group in GroupProblems(invalid.SelectMany(r => r.Violations))) ProblemGroups.Add(group);
            SelectedViolation = Violations.FirstOrDefault();

            LastPassed = results.Count > 0 && invalid.Count == 0;
            StatusMessage = results.Count == 0
                ? $"'{Topic}' has no messages in range - nothing to check."
                : invalid.Count == 0
                    ? $"All {results.Count:N0} message(s) match the contract."
                    : $"{invalid.Count:N0} of {results.Count:N0} message(s) break the contract" +
                      (invalid.Count > MaxRows ? $" (showing the first {MaxRows:N0})" : "") + ".";
            if (capped) StatusMessage += $" Stopped after {MaxMessages:N0} messages.";
        }
        catch (OperationCanceledException)
        {
            StatusMessage = "Stopped.";
        }
        finally
        {
            IsBusy = false;
            _cts = null;
        }
    }

    private async Task InferAsync()
    {
        using var cts = new CancellationTokenSource();
        _cts = cts;
        IsBusy = true;
        StatusMessage = $"Reading samples from '{Topic}'…";
        try
        {
            var (messages, problem, _) = await ReadTopicAsync(InferenceSamples, cts.Token).ConfigureAwait(true);
            if (problem is not null)
            {
                StatusMessage = problem;
                return;
            }
            var inferred = JsonSchemaInference.Infer(messages.Select(m => m.Value), InferenceSamples);
            if (inferred.SamplesUsed == 0)
            {
                StatusMessage = messages.Count == 0 ? $"'{Topic}' has no messages to learn from." : "None of the sampled messages are JSON.";
                return;
            }
            SchemaText = inferred.SchemaText;
            StatusMessage = $"Inferred from {inferred.SamplesUsed:N0} message(s)" +
                            (inferred.SamplesSkipped > 0 ? $" ({inferred.SamplesSkipped:N0} non-JSON skipped)" : "") +
                            ". Review it: add enums, patterns and ranges the samples can't show.";
        }
        catch (OperationCanceledException)
        {
            StatusMessage = "Stopped.";
        }
        finally
        {
            IsBusy = false;
            _cts = null;
        }
    }

    [GeneratedRegex(@"\[\d+\]")]
    private static partial Regex ArrayIndex();

    /// <summary>Groups violations by field (array indexes folded to [*]) and rule wording.</summary>
    public static IReadOnlyList<ContractProblemGroup> GroupProblems(IEnumerable<SchemaViolation> violations) =>
        violations
            .GroupBy(v => (Path: ArrayIndex().Replace(v.Path, "[*]"), Rule: RuleOf(v.Message)))
            .Select(g => new ContractProblemGroup(g.Key.Path, g.First().Message, g.Count()))
            .OrderByDescending(g => g.Count)
            .ThenBy(g => g.Path, StringComparer.Ordinal)
            .Take(50)
            .ToList();

    [GeneratedRegex("\"[^\"]*\"|-?\\d+(\\.\\d+)?")]
    private static partial Regex Literals();

    private static string RuleOf(string message) => Literals().Replace(message, "…");

    private async Task OpenSchemaAsync()
    {
        if (_state.FileDialogs is null) return;
        var path = await _state.FileDialogs.PickOpenFileAsync("Open JSON Schema", ".json", "JSON Schema").ConfigureAwait(true);
        if (path is null) return;
        try
        {
            SchemaText = await File.ReadAllTextAsync(path).ConfigureAwait(true);
            StatusMessage = $"Opened {Path.GetFileName(path)}.";
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            StatusMessage = $"Could not open: {ex.Message}";
        }
    }

    private async Task SaveSchemaAsync()
    {
        if (_state.FileDialogs is null) return;
        var suggested = (string.IsNullOrWhiteSpace(Topic) ? "contract" : MessageActions.SafeFileName(Topic)) + ".schema.json";
        var path = await _state.FileDialogs.PickSaveFileAsync("Save JSON Schema", ".json", "JSON Schema", suggested).ConfigureAwait(true);
        if (path is null) return;
        try
        {
            await File.WriteAllTextAsync(path, SchemaText).ConfigureAwait(true);
            StatusMessage = $"Saved {Path.GetFileName(path)}.";
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            StatusMessage = $"Could not save: {ex.Message}";
        }
    }

    /// <summary>The contract as a KafScript check, for the regression pack.</summary>
    public string BuildScript()
    {
        var limit = NewestOnly ? (int)NewestCount : 10_000;
        var sb = new StringBuilder();
        sb.AppendLine("@contract");
        sb.AppendLine($"Scenario: Messages on {Topic} match their contract");
        sb.AppendLine($"Given use connection \"{Escape(SelectedConnection ?? "")}\"");
        sb.AppendLine($"When scan topic \"{Escape(Topic ?? "")}\" from beginning limit {limit}");
        sb.AppendLine("Then validate each scanned message against schema \"\"\"");
        sb.AppendLine(SchemaText.Trim());
        sb.AppendLine("\"\"\"");
        return sb.ToString();
    }

    private void ToScript()
    {
        _state.RequestOpenScript(BuildScript());
        StatusMessage = "Added the contract check to the Script Editor.";
    }

    private static string Escape(string text) => text.Replace("\\", "\\\\").Replace("\"", "\\\"");
}

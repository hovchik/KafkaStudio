using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Search;

namespace KafkaStudio.App.ViewModels.DataSearch;

/// <summary>One field of the reference message in the Similar tab, tickable to build a "same values" search.</summary>
public sealed class ReferenceFieldViewModel : ObservableObject
{
    public required FieldSelector Selector { get; init; }
    public required string Path { get; init; }
    public required string? Value { get; init; }
    public bool IsVolatile { get; init; }
    public string DisplayValue => Value ?? "null";

    private bool _isChecked;
    public bool IsChecked { get => _isChecked; set => SetProperty(ref _isChecked, value); }
}

/// <summary>
/// The Similar tab: starting from a reference message, search for the same key, the same values of
/// chosen fields, or score every message in scope for content similarity (near-duplicates, typo'd ids)
/// or shape similarity (same JSON structure).
/// </summary>
public sealed class SimilarViewModel : ScanTabViewModel
{
    private readonly Func<string, Task> _runQuery;
    private readonly Func<KafkaMessage?> _currentMessage;

    public ObservableCollection<ReferenceFieldViewModel> ReferenceFields { get; } = new();
    public ObservableCollection<SimilarMatch> Matches { get; } = new();
    public ObservableCollection<DiffEntry> MatchDiff { get; } = new();

    public IReadOnlyList<Choice<SimilarityMode>> Modes { get; } = new[]
    {
        new Choice<SimilarityMode>(SimilarityMode.Content, "Content (near-duplicates, typos)"),
        new Choice<SimilarityMode>(SimilarityMode.Shape, "Shape (same JSON fields)")
    };

    public SimilarViewModel(AppState state, SearchScopeViewModel scope, Action<KafkaMessage?> showMessage,
        Func<string, Task> runQuery, Func<KafkaMessage?> currentMessage)
        : base(state, scope, showMessage)
    {
        _runQuery = runQuery;
        _currentMessage = currentMessage;
        _mode = Modes[0];
        UseCurrentMessageCommand = new RelayCommand(() => SetReference(_currentMessage()));
        FindSimilarCommand = new AsyncRelayCommand(RunAsync, () => !IsRunning && Reference is not null);
        SearchSameKeyCommand = new AsyncRelayCommand(
            () => _runQuery($"key equals {MessageQuery.Quote(Reference!.Key!)}"),
            () => Reference?.Key is not null);
        SearchCheckedFieldsCommand = new AsyncRelayCommand(SearchCheckedFieldsAsync, () => Reference is not null);
        ShowReferenceCommand = new RelayCommand(() => ShowMessage(Reference), () => Reference is not null);
    }

    public RelayCommand UseCurrentMessageCommand { get; }
    public AsyncRelayCommand FindSimilarCommand { get; }
    public AsyncRelayCommand SearchSameKeyCommand { get; }
    public AsyncRelayCommand SearchCheckedFieldsCommand { get; }
    public RelayCommand ShowReferenceCommand { get; }

    protected override void OnRunningChanged() => FindSimilarCommand.RaiseCanExecuteChanged();

    private KafkaMessage? _reference;
    public KafkaMessage? Reference { get => _reference; private set => SetProperty(ref _reference, value); }

    private Choice<SimilarityMode> _mode;
    public Choice<SimilarityMode> Mode { get => _mode; set => SetProperty(ref _mode, value ?? Modes[0]); }

    private double _threshold = 0.8;
    /// <summary>Minimum score (0.3 - 1.0) for a message to be listed.</summary>
    public double Threshold
    {
        get => _threshold;
        set { if (SetProperty(ref _threshold, Math.Clamp(value, 0.3, 1.0))) OnPropertyChanged(nameof(ThresholdText)); }
    }

    public string ThresholdText => $"{Threshold:P0}";

    private bool _ignoreVolatile = true;
    /// <summary>Ignore timestamps and generated ids when scoring (so retries score 100%).</summary>
    public bool IgnoreVolatile { get => _ignoreVolatile; set => SetProperty(ref _ignoreVolatile, value); }

    private string? _summary;
    public string? Summary { get => _summary; private set => SetProperty(ref _summary, value); }

    private SimilarMatch? _selectedMatch;
    public SimilarMatch? SelectedMatch
    {
        get => _selectedMatch;
        set
        {
            if (!SetProperty(ref _selectedMatch, value)) return;
            MatchDiff.Clear();
            if (value is null || Reference is null) return;
            ShowMessage(value.Message);
            var diff = JsonDiff.Compare(Reference, value.Message, new DiffOptions { IgnoreVolatile = IgnoreVolatile });
            foreach (var entry in diff.Entries) MatchDiff.Add(entry);
            MatchDiffSummary = diff.Summary;
        }
    }

    private string? _matchDiffSummary;
    /// <summary>How the selected match differs from the reference.</summary>
    public string? MatchDiffSummary { get => _matchDiffSummary; private set => SetProperty(ref _matchDiffSummary, value); }

    /// <summary>Makes <paramref name="message"/> the reference and lists its fields.</summary>
    public void SetReference(KafkaMessage? message)
    {
        if (message is null)
        {
            Status = "Select a message first (in the results of another tab), or use 'Find similar' on any message.";
            return;
        }
        Reference = message;
        ReferenceFields.Clear();
        Matches.Clear();
        MatchDiff.Clear();
        MatchDiffSummary = null;
        Summary = null;

        if (message.Key is not null)
        {
            ReferenceFields.Add(new ReferenceFieldViewModel { Selector = FieldSelector.Key, Path = "key", Value = message.Key });
        }
        foreach (var (name, value) in message.Headers)
        {
            ReferenceFields.Add(new ReferenceFieldViewModel
            {
                Selector = FieldSelector.Header(name), Path = $"header \"{name}\"", Value = value,
                IsVolatile = VolatileFields.IsVolatile(name, value)
            });
        }
        if (JsonFlattener.TryFlatten(message.Value, out var leaves))
        {
            foreach (var leaf in leaves.Take(300))
            {
                ReferenceFields.Add(new ReferenceFieldViewModel
                {
                    Selector = FieldSelector.Json(leaf.Path), Path = leaf.Path, Value = leaf.Value,
                    IsVolatile = VolatileFields.IsVolatile(leaf.Path, leaf.Value)
                });
            }
        }
        else if (message.Value is not null)
        {
            ReferenceFields.Add(new ReferenceFieldViewModel { Selector = FieldSelector.Value, Path = "value", Value = message.Value });
        }

        FindSimilarCommand.RaiseCanExecuteChanged();
        SearchSameKeyCommand.RaiseCanExecuteChanged();
        SearchCheckedFieldsCommand.RaiseCanExecuteChanged();
        ShowReferenceCommand.RaiseCanExecuteChanged();
        Status = $"Reference: {message.Topic} #{message.Partition}@{message.Offset}. Tick fields to search for the same values, or find similar messages.";
    }

    /// <summary>The structured query "every ticked field equals the reference's value".</summary>
    public string? BuildCheckedFieldsQuery()
    {
        var conditions = ReferenceFields.Where(f => f.IsChecked).Select(f => (QueryNode)new QueryCondition(
            f.Selector,
            f.Value is null ? QueryComparator.Missing : QueryComparator.Equals,
            f.Value)).ToList();
        var root = MessageQuery.AllOf(conditions);
        return root is null ? null : MessageQuery.Describe(root);
    }

    private Task SearchCheckedFieldsAsync()
    {
        var query = BuildCheckedFieldsQuery();
        if (query is null)
        {
            Status = "Tick at least one field of the reference message.";
            return Task.CompletedTask;
        }
        return _runQuery(query);
    }

    public async Task RunAsync()
    {
        var reference = Reference;
        if (reference is null) return;
        if (!Scope.TryGetScope(out var gateway, out var topics, out var range, out var scopeError))
        {
            Status = scopeError;
            return;
        }

        Matches.Clear();
        MatchDiff.Clear();
        Summary = null;
        Status = $"Scoring messages on {topics.Count} topic(s) against the reference…";
        var request = new SimilarityRequest
        {
            Reference = reference,
            Topics = topics,
            Mode = Mode.Value,
            Threshold = Threshold,
            IgnoreVolatileFields = IgnoreVolatile,
            Range = range
        };
        await RunScanAsync(topics.Count, async (token, progress) =>
        {
            var result = await Task.Run(() => SimilarityFinder.RunAsync(gateway, request, token, progress), token).ConfigureAwait(true);
            foreach (var match in result.Matches) Matches.Add(match);
            Summary = result.Summary;
            Status = Summary;
        }).ConfigureAwait(true);
    }
}

/// <summary>The Duplicates tab: groups a topic's messages by key, value, value-ignoring-timestamps, or
/// chosen fields, and lists every group with more than one message.</summary>
public sealed class DuplicatesViewModel : ScanTabViewModel
{
    public ObservableCollection<DuplicateGroup> Groups { get; } = new();

    public IReadOnlyList<Choice<DuplicateGroupBy>> GroupByChoices { get; } = new[]
    {
        new Choice<DuplicateGroupBy>(DuplicateGroupBy.Key, "Same key"),
        new Choice<DuplicateGroupBy>(DuplicateGroupBy.Value, "Identical value"),
        new Choice<DuplicateGroupBy>(DuplicateGroupBy.KeyAndValue, "Same key + identical value"),
        new Choice<DuplicateGroupBy>(DuplicateGroupBy.ValueIgnoringVolatile, "Same value, ignoring timestamps/ids (retries)"),
        new Choice<DuplicateGroupBy>(DuplicateGroupBy.Fields, "Same values of fields…")
    };

    public DuplicatesViewModel(AppState state, SearchScopeViewModel scope, Action<KafkaMessage?> showMessage)
        : base(state, scope, showMessage)
    {
        _groupBy = GroupByChoices[0];
        RunCommand = new AsyncRelayCommand(RunAsync, () => !IsRunning && Topic is not null);
    }

    public AsyncRelayCommand RunCommand { get; }

    protected override void OnRunningChanged() => RunCommand.RaiseCanExecuteChanged();

    private string? _topic;
    public string? Topic { get => _topic; set { if (SetProperty(ref _topic, value)) RunCommand.RaiseCanExecuteChanged(); } }

    private Choice<DuplicateGroupBy> _groupBy;
    public Choice<DuplicateGroupBy> GroupBy
    {
        get => _groupBy;
        set { if (SetProperty(ref _groupBy, value ?? GroupByChoices[0])) OnPropertyChanged(nameof(NeedsFields)); }
    }

    public bool NeedsFields => GroupBy.Value == DuplicateGroupBy.Fields;

    private string? _fieldsText;
    /// <summary>Comma separated fields for "same values of fields" (<c>$.orderId, $.eventType</c>).</summary>
    public string? FieldsText { get => _fieldsText; set => SetProperty(ref _fieldsText, value); }

    private string? _summary;
    public string? Summary { get => _summary; private set => SetProperty(ref _summary, value); }

    private DuplicateGroup? _selectedGroup;
    public DuplicateGroup? SelectedGroup
    {
        get => _selectedGroup;
        set { if (SetProperty(ref _selectedGroup, value) && value is not null) SelectedGroupMessage = value.Messages[0]; }
    }

    private KafkaMessage? _selectedGroupMessage;
    public KafkaMessage? SelectedGroupMessage
    {
        get => _selectedGroupMessage;
        set { if (SetProperty(ref _selectedGroupMessage, value) && value is not null) ShowMessage(value); }
    }

    public async Task RunAsync()
    {
        var topic = Topic;
        if (topic is null) return;
        IReadOnlyList<FieldSelector> fields = Array.Empty<FieldSelector>();
        if (NeedsFields)
        {
            if (!FieldSelector.TryParseList(FieldsText, out fields, out var fieldsError)) { Status = fieldsError; return; }
            if (fields.Count == 0) { Status = "Enter the fields to group by, e.g. $.orderId, $.eventType"; return; }
        }
        var gateway = Scope.GetGateway(out var gatewayError);
        if (gateway is null) { Status = gatewayError; return; }
        if (!Scope.TryGetRange(out var range, out var rangeError)) { Status = rangeError; return; }

        Groups.Clear();
        Summary = null;
        Status = $"Looking for duplicates on '{topic}' ({range.Describe()})…";
        var detector = new DuplicateDetector(GroupBy.Value, fields);
        await RunScanAsync(1, async (token, progress) =>
        {
            var scan = await Task.Run(() => TopicScanner.ScanAsync(gateway, new[] { topic }, range, (_, message) =>
            {
                detector.Add(message);
                return ScanDecision.Continue;
            }, token, progress), token).ConfigureAwait(true);

            var groups = detector.GetDuplicates();
            foreach (var group in groups.Take(2_000)) Groups.Add(group);
            var ungroupable = detector.Ungroupable > 0 ? $" · {detector.Ungroupable:N0} message(s) had nothing to group by" : "";
            var failed = scan.FailedTopics.Values.FirstOrDefault() is { } error ? $" - read failed: {error}" : "";
            Summary = groups.Count == 0
                ? $"No duplicates among {detector.MessagesSeen:N0} message(s){ungroupable}.{failed}"
                : $"{groups.Count:N0} duplicate group(s), {detector.ExtraCopies:N0} extra cop(ies) among {detector.MessagesSeen:N0} message(s){ungroupable}.{failed}";
            Status = Summary;
        }).ConfigureAwait(true);
    }
}

/// <summary>The Field stats tab: per-field presence, nulls, distinct values and top values for a topic;
/// any value can be turned into a search.</summary>
public sealed class FieldStatsViewModel : ScanTabViewModel
{
    private readonly Func<string, string, Task> _runQueryOnTopic;
    private IReadOnlyList<FieldStats> _all = Array.Empty<FieldStats>();

    public ObservableCollection<FieldStats> Fields { get; } = new();

    public FieldStatsViewModel(AppState state, SearchScopeViewModel scope, Action<KafkaMessage?> showMessage,
        Func<string, string, Task> runQueryOnTopic)
        : base(state, scope, showMessage)
    {
        _runQueryOnTopic = runQueryOnTopic;
        RunCommand = new AsyncRelayCommand(RunAsync, () => !IsRunning && Topic is not null);
        SearchValueCommand = new AsyncRelayCommand<ValueCount>(SearchValueAsync, v => v is not null && SelectedField is not null);
    }

    public AsyncRelayCommand RunCommand { get; }

    /// <summary>Searches the topic for messages whose selected field has this value.</summary>
    public AsyncRelayCommand<ValueCount> SearchValueCommand { get; }

    protected override void OnRunningChanged() => RunCommand.RaiseCanExecuteChanged();

    private string? _topic;
    public string? Topic { get => _topic; set { if (SetProperty(ref _topic, value)) RunCommand.RaiseCanExecuteChanged(); } }

    private bool _onlySometimesMissing;
    /// <summary>Only fields missing from some messages (schema drift).</summary>
    public bool OnlySometimesMissing { get => _onlySometimesMissing; set { if (SetProperty(ref _onlySometimesMissing, value)) ApplyFilter(); } }

    private string? _pathFilter;
    public string? PathFilter { get => _pathFilter; set { if (SetProperty(ref _pathFilter, value)) ApplyFilter(); } }

    private string? _summary;
    public string? Summary { get => _summary; private set => SetProperty(ref _summary, value); }

    private FieldStats? _selectedField;
    public FieldStats? SelectedField
    {
        get => _selectedField;
        set { if (SetProperty(ref _selectedField, value)) SearchValueCommand.RaiseCanExecuteChanged(); }
    }

    /// <summary>The query that finds messages where the selected field has <paramref name="value"/>.</summary>
    public string? BuildValueQuery(ValueCount value)
    {
        var field = SelectedField;
        if (field is null) return null;
        if (field.Selector is { } selector)
        {
            return value.Value is null
                ? MessageQuery.Describe(new QueryCondition(selector, QueryComparator.Missing, null))
                : MessageQuery.Describe(new QueryCondition(selector, QueryComparator.Equals, value.Value));
        }
        // Array-folded paths ($.items[*].sku) have no single value to compare - look for the value text.
        return value.Value is null ? null : MessageQuery.Describe(new QueryCondition(FieldSelector.Value, QueryComparator.Contains, value.Value));
    }

    private Task SearchValueAsync(ValueCount? value)
    {
        if (value is null || Topic is null) return Task.CompletedTask;
        var query = BuildValueQuery(value);
        return query is null ? Task.CompletedTask : _runQueryOnTopic(query, Topic);
    }

    public async Task RunAsync()
    {
        var topic = Topic;
        if (topic is null) return;
        var gateway = Scope.GetGateway(out var gatewayError);
        if (gateway is null) { Status = gatewayError; return; }
        if (!Scope.TryGetRange(out var range, out var rangeError)) { Status = rangeError; return; }

        Fields.Clear();
        _all = Array.Empty<FieldStats>();
        Summary = null;
        Status = $"Analysing '{topic}' ({range.Describe()})…";
        var collector = new FieldStatisticsCollector();
        await RunScanAsync(1, async (token, progress) =>
        {
            var scan = await Task.Run(() => TopicScanner.ScanAsync(gateway, new[] { topic }, range, (_, message) =>
            {
                collector.Add(message);
                return ScanDecision.Continue;
            }, token, progress), token).ConfigureAwait(true);

            _all = collector.GetStatistics();
            ApplyFilter();
            var drifting = _all.Count(f => f.IsSometimesMissing);
            var failed = scan.FailedTopics.Values.FirstOrDefault() is { } error ? $" - read failed: {error}" : "";
            Summary = $"{_all.Count:N0} field(s) across {collector.MessageCount:N0} message(s) ({collector.JsonMessageCount:N0} JSON)" +
                      (drifting > 0 ? $" · {drifting} field(s) missing from some messages" : "") + $".{failed}";
            Status = Summary;
        }).ConfigureAwait(true);
    }

    private void ApplyFilter()
    {
        var selected = SelectedField?.Path;
        Fields.Clear();
        var filter = PathFilter?.Trim();
        foreach (var field in _all)
        {
            if (OnlySometimesMissing && !field.IsSometimesMissing) continue;
            if (!string.IsNullOrEmpty(filter) && !field.Path.Contains(filter, StringComparison.OrdinalIgnoreCase)) continue;
            Fields.Add(field);
        }
        SelectedField = Fields.FirstOrDefault(f => f.Path == selected);
    }
}

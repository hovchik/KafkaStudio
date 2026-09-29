using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Search;

namespace KafkaStudio.App.ViewModels.DataSearch;

/// <summary>
/// The Bulk check tab: paste or import a list of ids and learn, in one pass per topic, which were found
/// (how often, where, when) and which are missing.
/// </summary>
public sealed class BulkCheckViewModel : ScanTabViewModel
{
    private readonly MessageActions _actions;
    private BulkExistenceResult? _result;

    public ObservableCollection<IdPresence> Rows { get; } = new();

    public BulkCheckViewModel(AppState state, SearchScopeViewModel scope, MessageActions actions, Action<KafkaMessage?> showMessage)
        : base(state, scope, showMessage)
    {
        _actions = actions;
        RunCommand = new AsyncRelayCommand(RunAsync, () => !IsRunning && ParsedIdCount > 0);
        ImportCommand = new AsyncRelayCommand(ImportAsync);
        CopyMissingCommand = new RelayCommand(
            () => _actions.CopyText(string.Join(Environment.NewLine, _result!.Ids.Where(i => !i.Found).Select(i => i.Id)), "missing ids"),
            () => _result is not null && _result.MissingCount > 0);
        ExportCsvCommand = new AsyncRelayCommand(
            () => _actions.SaveTextAsync(_result!.ToCsv(), "Export bulk check", ".csv", "CSV", "bulk-check.csv", "the bulk check"),
            () => _result is not null);
    }

    public AsyncRelayCommand RunCommand { get; }
    public AsyncRelayCommand ImportCommand { get; }
    public RelayCommand CopyMissingCommand { get; }
    public AsyncRelayCommand ExportCsvCommand { get; }

    protected override void OnRunningChanged() => RunCommand.RaiseCanExecuteChanged();

    private string? _idsText;
    /// <summary>Ids, one per line (or comma/semicolon/tab separated - a pasted CSV works).</summary>
    public string? IdsText
    {
        get => _idsText;
        set { if (SetProperty(ref _idsText, value)) OnIdsChanged(); }
    }

    private int? _csvColumn;
    /// <summary>1-based CSV column holding the ids (empty = every cell).</summary>
    public int? CsvColumn { get => _csvColumn; set { if (SetProperty(ref _csvColumn, value is <= 0 ? null : value)) OnIdsChanged(); } }

    private bool _skipHeader;
    public bool SkipHeader { get => _skipHeader; set { if (SetProperty(ref _skipHeader, value)) OnIdsChanged(); } }

    private string _matchField = "any";
    /// <summary>Where an id must appear: <c>any</c> (key, headers, or anywhere in the value), <c>key</c>,
    /// <c>$.path</c>, <c>header "name"</c>.</summary>
    public string MatchField { get => _matchField; set => SetProperty(ref _matchField, value ?? ""); }

    private bool _caseSensitive = true;
    public bool CaseSensitive { get => _caseSensitive; set => SetProperty(ref _caseSensitive, value); }

    private bool _onlyMissing;
    /// <summary>Show only the ids that weren't found.</summary>
    public bool OnlyMissing { get => _onlyMissing; set { if (SetProperty(ref _onlyMissing, value)) ApplyFilter(); } }

    private int _parsedIdCount;
    public int ParsedIdCount { get => _parsedIdCount; private set => SetProperty(ref _parsedIdCount, value); }

    private string? _summary;
    public string? Summary { get => _summary; private set => SetProperty(ref _summary, value); }

    private IdPresence? _selectedRow;
    public IdPresence? SelectedRow
    {
        get => _selectedRow;
        set { if (SetProperty(ref _selectedRow, value) && value?.First is { } first) ShowMessage(first); }
    }

    private IReadOnlyList<string> ParseIds() => IdMatcher.ParseIdList(IdsText ?? "", CsvColumn, SkipHeader);

    private void OnIdsChanged()
    {
        ParsedIdCount = ParseIds().Count;
        RunCommand.RaiseCanExecuteChanged();
    }

    private async Task ImportAsync()
    {
        if (State.FileDialogs is null)
        {
            Status = "File dialogs aren't available.";
            return;
        }
        var path = await State.FileDialogs.PickOpenFileAsync("Import ids (CSV or text, one per line)", ".csv", "CSV / text").ConfigureAwait(true);
        if (path is null) return;
        try
        {
            IdsText = await File.ReadAllTextAsync(path).ConfigureAwait(true);
            Status = $"Imported {ParsedIdCount:N0} id(s) from {Path.GetFileName(path)}.";
        }
        catch (Exception ex)
        {
            Status = $"Import failed: {ex.Message}";
        }
    }

    public async Task RunAsync()
    {
        var ids = ParseIds();
        if (ids.Count == 0)
        {
            Status = "Paste or import some ids first.";
            return;
        }
        if (!FieldSelector.TryParse(MatchField, out var field, out var fieldError))
        {
            Status = fieldError;
            return;
        }
        if (!Scope.TryGetScope(out var gateway, out var topics, out var range, out var scopeError))
        {
            Status = scopeError;
            return;
        }

        Rows.Clear();
        _result = null;
        Summary = null;
        Status = $"Checking {ids.Count:N0} id(s) against {topics.Count} topic(s)…";
        await RunScanAsync(topics.Count, async (token, progress) =>
        {
            _result = await Task.Run(() => BulkExistenceChecker.RunAsync(gateway, topics, ids, field, range, CaseSensitive, token, progress), token)
                .ConfigureAwait(true);
            Summary = _result.Summary;
            Status = Summary;
            ApplyFilter();
        }).ConfigureAwait(true);
        CopyMissingCommand.RaiseCanExecuteChanged();
        ExportCsvCommand.RaiseCanExecuteChanged();
    }

    private void ApplyFilter()
    {
        Rows.Clear();
        if (_result is null) return;
        foreach (var row in _result.Ids)
        {
            if (!OnlyMissing || !row.Found) Rows.Add(row);
        }
    }
}

/// <summary>The Trace tab: every message carrying an id across the scope, as a timeline with the time
/// between hops.</summary>
public sealed class TraceViewModel : ScanTabViewModel
{
    public ObservableCollection<TraceHop> Hops { get; } = new();

    public TraceViewModel(AppState state, SearchScopeViewModel scope, Action<KafkaMessage?> showMessage)
        : base(state, scope, showMessage)
    {
        RunCommand = new AsyncRelayCommand(RunAsync, () => !IsRunning && !string.IsNullOrWhiteSpace(Id));
    }

    public AsyncRelayCommand RunCommand { get; }

    protected override void OnRunningChanged() => RunCommand.RaiseCanExecuteChanged();

    private string? _id;
    public string? Id { get => _id; set { if (SetProperty(ref _id, value)) RunCommand.RaiseCanExecuteChanged(); } }

    private string _matchField = "any";
    public string MatchField { get => _matchField; set => SetProperty(ref _matchField, value ?? ""); }

    private bool _caseSensitive = true;
    public bool CaseSensitive { get => _caseSensitive; set => SetProperty(ref _caseSensitive, value); }

    private string? _summary;
    public string? Summary { get => _summary; private set => SetProperty(ref _summary, value); }

    private string? _path;
    /// <summary>"orders → payments → shipments".</summary>
    public string? PathText { get => _path; private set => SetProperty(ref _path, value); }

    private TraceHop? _selectedHop;
    public TraceHop? SelectedHop
    {
        get => _selectedHop;
        set { if (SetProperty(ref _selectedHop, value) && value is not null) ShowMessage(value.Message); }
    }

    public async Task RunAsync()
    {
        var id = Id?.Trim();
        if (string.IsNullOrEmpty(id)) return;
        if (!FieldSelector.TryParse(MatchField, out var field, out var fieldError))
        {
            Status = fieldError;
            return;
        }
        if (!Scope.TryGetScope(out var gateway, out var topics, out var range, out var scopeError))
        {
            Status = scopeError;
            return;
        }

        Hops.Clear();
        Summary = null;
        PathText = null;
        Status = $"Tracing '{id}' across {topics.Count} topic(s)…";
        await RunScanAsync(topics.Count, async (token, progress) =>
        {
            var result = await Task.Run(() => IdTracer.RunAsync(gateway, topics, id, field, range, CaseSensitive, token, progress), token)
                .ConfigureAwait(true);
            foreach (var hop in result.Hops) Hops.Add(hop);
            Summary = result.Summary;
            PathText = result.Hops.Count > 0 ? result.Path : null;
            Status = Summary;
        }).ConfigureAwait(true);
    }
}

/// <summary>The Reconcile tab: join topic A and topic B on a field and list what's missing on either
/// side, what matches, and which pairs differ.</summary>
public sealed class ReconcileViewModel : ScanTabViewModel
{
    private readonly MessageActions _actions;
    private ReconcileResult? _result;

    public ObservableCollection<ReconcileRow> Rows { get; } = new();
    public ObservableCollection<DiffEntry> RowDiff { get; } = new();

    public IReadOnlyList<Choice<ReconcileStatus?>> StatusFilters { get; } = new[]
    {
        new Choice<ReconcileStatus?>(null, "All rows"),
        new Choice<ReconcileStatus?>(ReconcileStatus.OnlyInA, "Only in A"),
        new Choice<ReconcileStatus?>(ReconcileStatus.OnlyInB, "Only in B"),
        new Choice<ReconcileStatus?>(ReconcileStatus.Mismatched, "Different"),
        new Choice<ReconcileStatus?>(ReconcileStatus.Matched, "Matched")
    };

    public ReconcileViewModel(AppState state, SearchScopeViewModel scope, MessageActions actions, Action<KafkaMessage?> showMessage)
        : base(state, scope, showMessage)
    {
        _actions = actions;
        _statusFilter = StatusFilters[0];
        RunCommand = new AsyncRelayCommand(RunAsync, () => !IsRunning && TopicA is not null && TopicB is not null);
        ExportCsvCommand = new AsyncRelayCommand(
            () => _actions.SaveTextAsync(_result!.ToCsv(), "Export reconciliation", ".csv", "CSV", "reconcile.csv", "the reconciliation"),
            () => _result is not null);
        ShowACommand = new RelayCommand(() => ShowMessage(SelectedRow?.A), () => SelectedRow?.A is not null);
        ShowBCommand = new RelayCommand(() => ShowMessage(SelectedRow?.B), () => SelectedRow?.B is not null);
    }

    public AsyncRelayCommand RunCommand { get; }
    public AsyncRelayCommand ExportCsvCommand { get; }
    public RelayCommand ShowACommand { get; }
    public RelayCommand ShowBCommand { get; }

    protected override void OnRunningChanged() => RunCommand.RaiseCanExecuteChanged();

    private string? _topicA;
    public string? TopicA { get => _topicA; set { if (SetProperty(ref _topicA, value)) RunCommand.RaiseCanExecuteChanged(); } }

    private string _joinA = "key";
    public string JoinA { get => _joinA; set => SetProperty(ref _joinA, value ?? ""); }

    private string? _topicB;
    public string? TopicB { get => _topicB; set { if (SetProperty(ref _topicB, value)) RunCommand.RaiseCanExecuteChanged(); } }

    private string _joinB = "key";
    public string JoinB { get => _joinB; set => SetProperty(ref _joinB, value ?? ""); }

    private string? _compareFields;
    /// <summary>Comma separated fields compared between matched pairs (<c>$.amount, $.currency</c>).</summary>
    public string? CompareFields { get => _compareFields; set => SetProperty(ref _compareFields, value); }

    private bool _caseSensitive = true;
    public bool CaseSensitive { get => _caseSensitive; set => SetProperty(ref _caseSensitive, value); }

    private Choice<ReconcileStatus?> _statusFilter;
    public Choice<ReconcileStatus?> StatusFilter { get => _statusFilter; set { if (SetProperty(ref _statusFilter, value ?? StatusFilters[0])) ApplyFilter(); } }

    private string? _summary;
    public string? Summary { get => _summary; private set => SetProperty(ref _summary, value); }

    private ReconcileRow? _selectedRow;
    public ReconcileRow? SelectedRow
    {
        get => _selectedRow;
        set
        {
            if (!SetProperty(ref _selectedRow, value)) return;
            ShowACommand.RaiseCanExecuteChanged();
            ShowBCommand.RaiseCanExecuteChanged();
            RowDiff.Clear();
            if (value is null) return;
            ShowMessage(value.A ?? value.B);
            if (value.A is not null && value.B is not null)
            {
                foreach (var entry in JsonDiff.Compare(value.A, value.B, new DiffOptions { IgnoreVolatile = true }).Entries) RowDiff.Add(entry);
            }
        }
    }

    public async Task RunAsync()
    {
        if (TopicA is null || TopicB is null) return;
        if (!FieldSelector.TryParse(JoinA, out var joinA, out var errorA)) { Status = $"Join field A: {errorA}"; return; }
        if (!FieldSelector.TryParse(JoinB, out var joinB, out var errorB)) { Status = $"Join field B: {errorB}"; return; }
        if (!FieldSelector.TryParseList(CompareFields, out var compare, out var compareError)) { Status = $"Compare fields: {compareError}"; return; }
        var gateway = Scope.GetGateway(out var gatewayError);
        if (gateway is null) { Status = gatewayError; return; }
        if (!Scope.TryGetRange(out var range, out var rangeError)) { Status = rangeError; return; }

        Rows.Clear();
        RowDiff.Clear();
        _result = null;
        Summary = null;
        Status = $"Reconciling '{TopicA}' with '{TopicB}'…";
        var request = new ReconcileRequest
        {
            TopicA = TopicA, JoinA = joinA, TopicB = TopicB, JoinB = joinB,
            CompareFields = compare, Range = range, CaseSensitive = CaseSensitive
        };
        await RunScanAsync(TopicA == TopicB ? 1 : 2, async (token, progress) =>
        {
            _result = await Task.Run(() => TopicReconciler.RunAsync(gateway, request, token, progress), token).ConfigureAwait(true);
            Summary = _result.Summary;
            Status = Summary;
            ApplyFilter();
        }).ConfigureAwait(true);
        ExportCsvCommand.RaiseCanExecuteChanged();
    }

    private void ApplyFilter()
    {
        Rows.Clear();
        if (_result is null) return;
        foreach (var row in _result.Rows)
        {
            if (StatusFilter.Value is null || row.Status == StatusFilter.Value) Rows.Add(row);
        }
    }
}

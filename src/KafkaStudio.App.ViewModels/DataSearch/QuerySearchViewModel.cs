using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Search;

namespace KafkaStudio.App.ViewModels.DataSearch;

/// <summary>
/// The Search tab: plain or structured queries (<see cref="MessageQuery"/>) with match modes, field
/// scope and case sensitivity; an "Exists?" quick check that stops at the first hit per topic; saved
/// searches and history; export to JSON/CSV; and "turn this search into a KafScript check".
/// </summary>
public sealed class QuerySearchViewModel : ScanTabViewModel
{
    public const int MaxHits = 5_000;

    private readonly MessageActions _actions;
    private readonly ConcurrentQueue<KafkaMessage> _pendingHits = new();
    private readonly object _drainGate = new();
    private SearchResult? _lastResult;
    private MessageQuery? _lastQuery;
    private IReadOnlyList<string> _lastTopics = Array.Empty<string>();

    public IReadOnlyList<Choice<TextMatchMode>> Modes { get; } = new[]
    {
        new Choice<TextMatchMode>(TextMatchMode.Contains, "Contains"),
        new Choice<TextMatchMode>(TextMatchMode.Exact, "Exact (whole field)"),
        new Choice<TextMatchMode>(TextMatchMode.WholeWord, "Whole word / id"),
        new Choice<TextMatchMode>(TextMatchMode.Regex, "Regular expression"),
        new Choice<TextMatchMode>(TextMatchMode.Fuzzy, "Fuzzy (similar)")
    };

    public ObservableCollection<KafkaMessage> Hits { get; } = new();

    /// <summary>Per-topic outcome (hits / first hit / failed) of the last run.</summary>
    public ObservableCollection<TopicSearchOutcome> TopicOutcomes { get; } = new();

    public ObservableCollection<SavedSearch> SavedSearches { get; } = new();
    public ObservableCollection<string> History { get; } = new();

    public QuerySearchViewModel(AppState state, SearchScopeViewModel scope, MessageActions actions, Action<KafkaMessage?> showMessage)
        : base(state, scope, showMessage)
    {
        _actions = actions;
        _mode = Modes[0];
        SearchCommand = new AsyncRelayCommand(() => RunAsync(existenceCheck: false), CanRun);
        ExistsCommand = new AsyncRelayCommand(() => RunAsync(existenceCheck: true), CanRun);
        ExportJsonCommand = new AsyncRelayCommand(() => _actions.ExportAsync(Hits.ToList(), "search-results.json"), () => Hits.Count > 0);
        ExportCsvCommand = new AsyncRelayCommand(() => _actions.ExportCsvAsync(Hits.ToList(), "search-results.csv"), () => Hits.Count > 0);
        CopySummaryCommand = new RelayCommand(() => _actions.CopyText(Summary, "summary"), () => Summary is not null);
        SaveSearchCommand = new RelayCommand(SaveSearch, () => !string.IsNullOrWhiteSpace(NewSearchName) && !string.IsNullOrWhiteSpace(QueryText));
        DeleteSavedSearchCommand = new RelayCommand(DeleteSavedSearch, () => SelectedSavedSearch is not null);
        ClearHistoryCommand = new RelayCommand(ClearHistory, () => History.Count > 0);
        ClearSingleTopicCommand = new RelayCommand(() => SingleTopic = null);
        ToScriptCommand = new RelayCommand(() => GenerateScript(asTask: false), () => !string.IsNullOrWhiteSpace(QueryText));
        ToTaskCommand = new RelayCommand(() => GenerateScript(asTask: true), () => !string.IsNullOrWhiteSpace(QueryText));

        foreach (var saved in SavedSearchStore.Load()) SavedSearches.Add(saved);
        foreach (var query in SearchHistoryStore.Load()) History.Add(query);
        Hits.CollectionChanged += (_, _) =>
        {
            ExportJsonCommand.RaiseCanExecuteChanged();
            ExportCsvCommand.RaiseCanExecuteChanged();
        };
        History.CollectionChanged += (_, _) => ClearHistoryCommand.RaiseCanExecuteChanged();
    }

    public AsyncRelayCommand SearchCommand { get; }
    public AsyncRelayCommand ExistsCommand { get; }
    public AsyncRelayCommand ExportJsonCommand { get; }
    public AsyncRelayCommand ExportCsvCommand { get; }
    public RelayCommand CopySummaryCommand { get; }
    public RelayCommand SaveSearchCommand { get; }
    public RelayCommand DeleteSavedSearchCommand { get; }
    public RelayCommand ClearHistoryCommand { get; }
    public RelayCommand ClearSingleTopicCommand { get; }
    public RelayCommand ToScriptCommand { get; }
    public RelayCommand ToTaskCommand { get; }

    private bool CanRun() => !IsRunning && !string.IsNullOrWhiteSpace(QueryText) && QueryError is null;

    protected override void OnRunningChanged()
    {
        SearchCommand.RaiseCanExecuteChanged();
        ExistsCommand.RaiseCanExecuteChanged();
    }

    // ------------------------------------------------------------------ query & options ----

    private string? _queryText;
    public string? QueryText
    {
        get => _queryText;
        set
        {
            if (SetProperty(ref _queryText, value))
            {
                Validate();
                SaveSearchCommand.RaiseCanExecuteChanged();
                ToScriptCommand.RaiseCanExecuteChanged();
                ToTaskCommand.RaiseCanExecuteChanged();
            }
        }
    }

    private Choice<TextMatchMode> _mode;
    public Choice<TextMatchMode> Mode { get => _mode; set { if (SetProperty(ref _mode, value ?? Modes[0])) Validate(); } }

    private bool _caseSensitive;
    public bool CaseSensitive { get => _caseSensitive; set { if (SetProperty(ref _caseSensitive, value)) Validate(); } }

    private bool _searchKey = true;
    public bool SearchKey { get => _searchKey; set { if (SetProperty(ref _searchKey, value)) Validate(); } }

    private bool _searchValue = true;
    public bool SearchValue { get => _searchValue; set { if (SetProperty(ref _searchValue, value)) Validate(); } }

    private bool _searchHeaders = true;
    public bool SearchHeaders { get => _searchHeaders; set { if (SetProperty(ref _searchHeaders, value)) Validate(); } }

    private string? _singleTopic;
    /// <summary>When set (e.g. by "search for this value" from field statistics) the search only reads
    /// this topic instead of the whole scope.</summary>
    public string? SingleTopic
    {
        get => _singleTopic;
        set { if (SetProperty(ref _singleTopic, value)) OnPropertyChanged(nameof(HasSingleTopic)); }
    }

    public bool HasSingleTopic => SingleTopic is not null;

    private string? _queryError;
    public string? QueryError { get => _queryError; private set => SetProperty(ref _queryError, value); }

    private string? _queryDescription;
    /// <summary>What the query will do: "Structured query: …" / "Text contains 'x' in key, value, headers".</summary>
    public string? QueryDescription { get => _queryDescription; private set => SetProperty(ref _queryDescription, value); }

    /// <summary>Plain-text options only matter for plain searches.</summary>
    private bool _isStructured;
    public bool IsStructured { get => _isStructured; private set => SetProperty(ref _isStructured, value); }

    public QueryOptions CurrentOptions => new()
    {
        Mode = Mode.Value,
        CaseSensitive = CaseSensitive,
        Fields = (SearchKey ? SearchFields.Key : 0) | (SearchValue ? SearchFields.Value : 0) | (SearchHeaders ? SearchFields.Headers : 0)
    };

    private void Validate()
    {
        var text = QueryText?.Trim();
        if (string.IsNullOrEmpty(text))
        {
            QueryError = null;
            QueryDescription = null;
            IsStructured = false;
        }
        else if (!SearchKey && !SearchValue && !SearchHeaders)
        {
            QueryError = "Tick at least one of key, value or headers.";
            QueryDescription = null;
        }
        else if (MessageQuery.TryParse(text, CurrentOptions, out var query, out var error))
        {
            QueryError = null;
            IsStructured = query!.IsStructured;
            QueryDescription = query.IsStructured
                ? $"Structured query: {MessageQuery.Describe(query.Root!)}"
                : $"Text {Mode.Label.ToLowerInvariant()} \"{query.Term}\" in {FieldsText()}{(CaseSensitive ? ", case-sensitive" : "")}";
        }
        else
        {
            QueryError = error;
            QueryDescription = null;
        }
        SearchCommand.RaiseCanExecuteChanged();
        ExistsCommand.RaiseCanExecuteChanged();
    }

    private string FieldsText() => string.Join(", ", new[] { SearchKey ? "key" : null, SearchValue ? "value" : null, SearchHeaders ? "headers" : null }
        .Where(f => f is not null));

    // ------------------------------------------------------------------ results ----

    private string? _summary;
    public string? Summary
    {
        get => _summary;
        private set { if (SetProperty(ref _summary, value)) CopySummaryCommand.RaiseCanExecuteChanged(); }
    }

    private bool? _lastRunFound;
    /// <summary>For the result badge: true = found, false = not found, null = no run yet.</summary>
    public bool? LastRunFound
    {
        get => _lastRunFound;
        private set
        {
            if (!SetProperty(ref _lastRunFound, value)) return;
            OnPropertyChanged(nameof(IsFound));
            OnPropertyChanged(nameof(IsNotFound));
        }
    }

    public bool IsFound => LastRunFound == true;
    public bool IsNotFound => LastRunFound == false;

    private KafkaMessage? _selectedHit;
    public KafkaMessage? SelectedHit
    {
        get => _selectedHit;
        set { if (SetProperty(ref _selectedHit, value) && value is not null) ShowMessage(value); }
    }

    private TopicSearchOutcome? _selectedOutcome;
    /// <summary>Selecting a topic row jumps to its first hit.</summary>
    public TopicSearchOutcome? SelectedOutcome
    {
        get => _selectedOutcome;
        set { if (SetProperty(ref _selectedOutcome, value) && value?.FirstHit is { } first) SelectedHit = first; }
    }

    /// <summary>Sets the query text (and optionally the topic) and runs the search.</summary>
    public Task RunQueryAsync(string query, string? singleTopic = null, bool existenceCheck = false)
    {
        QueryText = query;
        SingleTopic = singleTopic;
        return existenceCheck ? ExistsCommand.ExecuteAsync() : SearchCommand.ExecuteAsync();
    }

    private async Task RunAsync(bool existenceCheck)
    {
        var text = QueryText?.Trim();
        if (string.IsNullOrEmpty(text)) return;
        if (!MessageQuery.TryParse(text, CurrentOptions, out var query, out var error))
        {
            QueryError = error;
            return;
        }
        if (!Scope.TryGetScope(out var gateway, out var scopeTopics, out var range, out var scopeError) &&
            !(SingleTopic is not null && scopeError.StartsWith("No topics", StringComparison.Ordinal)))
        {
            Status = scopeError;
            return;
        }
        var topics = SingleTopic is not null ? new[] { SingleTopic } : scopeTopics;

        PushHistory(text);
        lock (_drainGate)
        {
            Hits.Clear();
            while (_pendingHits.TryDequeue(out _)) { }
        }
        TopicOutcomes.Clear();
        Summary = null;
        LastRunFound = null;
        _lastQuery = query;
        _lastTopics = topics;
        Status = existenceCheck
            ? $"Checking whether it exists on {topics.Count} topic(s)…"
            : $"Searching {topics.Count} topic(s) ({range.Describe()})…";

        await RunScanAsync(topics.Count, async (token, progress) =>
        {
            var result = await Task.Run(() => MessageSearch.RunAsync(gateway, new SearchRequest
            {
                Topics = topics,
                Query = query!,
                Range = range,
                FirstHitPerTopic = existenceCheck,
                MaxHits = MaxHits
            }, token, progress, onHit: _pendingHits.Enqueue), token).ConfigureAwait(true);

            _lastResult = result;
            DrainHits();
            foreach (var outcome in result.Topics.Where(o => o.Hits > 0 || o.Failed)) TopicOutcomes.Add(outcome);
            Summary = result.Summary(existenceCheck);
            Status = Summary;
            LastRunFound = result.Hits.Count > 0;
        }, DrainHits).ConfigureAwait(true);
    }

    /// <summary>Moves streamed hits into <see cref="Hits"/> (UI thread; guarded for inline test dispatch).</summary>
    private void DrainHits()
    {
        lock (_drainGate)
        {
            while (Hits.Count < MaxHits && _pendingHits.TryDequeue(out var hit)) Hits.Add(hit);
        }
    }

    // ------------------------------------------------------------------ history & saved searches ----

    private string? _selectedHistory;
    /// <summary>Picking a history entry puts it back in the search box.</summary>
    public string? SelectedHistory
    {
        get => _selectedHistory;
        set
        {
            if (SetProperty(ref _selectedHistory, value) && value is not null) QueryText = value;
        }
    }

    private void PushHistory(string query)
    {
        var updated = SearchHistoryStore.Push(History, query);
        _selectedHistory = null;
        OnPropertyChanged(nameof(SelectedHistory));
        CollectionSyncOrdered(History, updated);
        if (SearchHistoryStore.Save(History) is { } error) State.Notify(error);
    }

    private static void CollectionSyncOrdered(ObservableCollection<string> target, IReadOnlyList<string> desired)
    {
        target.Clear();
        foreach (var item in desired) target.Add(item);
    }

    private void ClearHistory()
    {
        History.Clear();
        if (SearchHistoryStore.Save(History) is { } error) State.Notify(error);
    }

    private string? _newSearchName;
    public string? NewSearchName
    {
        get => _newSearchName;
        set { if (SetProperty(ref _newSearchName, value)) SaveSearchCommand.RaiseCanExecuteChanged(); }
    }

    private SavedSearch? _selectedSavedSearch;
    /// <summary>Selecting a saved search restores its query, options and scope.</summary>
    public SavedSearch? SelectedSavedSearch
    {
        get => _selectedSavedSearch;
        set
        {
            if (SetProperty(ref _selectedSavedSearch, value))
            {
                DeleteSavedSearchCommand.RaiseCanExecuteChanged();
                if (value is not null) Apply(value);
            }
        }
    }

    private bool _existenceCheckDefault;
    /// <summary>Remembered with a saved search: re-run it as an "Exists?" check.</summary>
    public bool ExistenceCheckDefault { get => _existenceCheckDefault; set => SetProperty(ref _existenceCheckDefault, value); }

    public void Apply(SavedSearch saved)
    {
        QueryText = saved.Query;
        Mode = Modes.FirstOrDefault(m => m.Value == saved.Mode) ?? Modes[0];
        CaseSensitive = saved.CaseSensitive;
        SearchKey = saved.Fields.HasFlag(SearchFields.Key);
        SearchValue = saved.Fields.HasFlag(SearchFields.Value);
        SearchHeaders = saved.Fields.HasFlag(SearchFields.Headers);
        ExistenceCheckDefault = saved.ExistenceCheck;
        Scope.SelectedRangeKind = Scope.RangeKinds.FirstOrDefault(k => k.Value == saved.RangeKind) ?? Scope.RangeKinds[0];
        if (saved.LastMinutes is { } minutes) Scope.LastMinutes = minutes;
        if (saved.NewestCount is { } newest) Scope.NewestCount = newest;
        Scope.FromText = saved.From?.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss");
        Scope.ToText = saved.To?.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss");
        Scope.SelectTopicSet(saved.TopicSetName);
        Scope.TopicNameFilter = saved.TopicNameFilter;
        SingleTopic = null;
        Status = $"Loaded saved search '{saved.Name}'.";
    }

    private void SaveSearch()
    {
        var name = NewSearchName?.Trim();
        if (string.IsNullOrEmpty(name) || string.IsNullOrWhiteSpace(QueryText)) return;
        Scope.TryParseTimes(out var from, out var to);
        var saved = new SavedSearch
        {
            Name = name,
            Query = QueryText.Trim(),
            Mode = Mode.Value,
            CaseSensitive = CaseSensitive,
            Fields = CurrentOptions.Fields,
            RangeKind = Scope.SelectedRangeKind.Value,
            LastMinutes = Scope.LastMinutes,
            NewestCount = Scope.NewestCount,
            From = from,
            To = to,
            TopicSetName = Scope.SelectedTopicSet?.Name,
            TopicNameFilter = string.IsNullOrWhiteSpace(Scope.TopicNameFilter) ? null : Scope.TopicNameFilter.Trim(),
            ExistenceCheck = ExistenceCheckDefault
        };

        var existing = SavedSearches.FirstOrDefault(s => s.Name == name);
        if (existing is not null) SavedSearches.Remove(existing);
        SavedSearches.Add(saved);
        if (SavedSearchStore.Save(SavedSearches) is { } error) State.Notify(error);
        _selectedSavedSearch = saved;
        OnPropertyChanged(nameof(SelectedSavedSearch));
        DeleteSavedSearchCommand.RaiseCanExecuteChanged();
        NewSearchName = null;
        Status = $"Saved search '{name}'.";
    }

    private void DeleteSavedSearch()
    {
        var saved = SelectedSavedSearch;
        if (saved is null) return;
        _selectedSavedSearch = null;
        OnPropertyChanged(nameof(SelectedSavedSearch));
        SavedSearches.Remove(saved);
        if (SavedSearchStore.Save(SavedSearches) is { } error) State.Notify(error);
        DeleteSavedSearchCommand.RaiseCanExecuteChanged();
        Status = $"Deleted saved search '{saved.Name}'.";
    }

    // ------------------------------------------------------------------ to KafScript ----

    private string _taskSchedule = "every 15 minutes";
    /// <summary>Schedule used by "Make it a Task" (<c>every 15 minutes</c>, <c>at 9:30</c>, <c>run once</c>).</summary>
    public string TaskSchedule { get => _taskSchedule; set => SetProperty(ref _taskSchedule, value ?? ""); }

    /// <summary>Builds a KafScript check for the current query. Topics: those with hits in the last run
    /// of this query, else the single topic / scope. Opens it in the Script Editor.</summary>
    public string? GenerateScript(bool asTask)
    {
        var text = QueryText?.Trim();
        if (string.IsNullOrEmpty(text)) return null;
        if (!MessageQuery.TryParse(text, CurrentOptions, out var query, out var parseError))
        {
            Status = parseError;
            return null;
        }

        IReadOnlyList<string> topics;
        if (_lastResult is not null && _lastQuery?.Text == query!.Text && _lastResult.TopicsWithHits > 0)
        {
            topics = _lastResult.Topics.Where(t => t.Hits > 0).Select(t => t.Topic).ToList();
        }
        else if (SingleTopic is not null)
        {
            topics = new[] { SingleTopic };
        }
        else
        {
            topics = Scope.GetScopeTopics();
        }
        if (topics.Count > 20)
        {
            Status = $"{topics.Count} topics in scope - run the search first (the script uses the topics that had matches) or narrow the scope.";
            return null;
        }

        var name = SelectedSavedSearch?.Name ?? (text.Length > 50 ? text[..50] + "…" : text).Replace('\n', ' ');
        if (!KafScriptGenerator.TryGenerate(query!, new KafScriptGenerator.Options
            {
                Connection = Scope.SelectedConnection ?? "local",
                Topics = topics,
                Name = $"Exists {name}",
                TaskSchedule = asTask ? (string.IsNullOrWhiteSpace(TaskSchedule) ? "every 15 minutes" : TaskSchedule.Trim()) : null
            }, out var script, out var error))
        {
            Status = $"Can't turn this search into KafScript: {error}";
            return null;
        }

        State.RequestOpenScript(script);
        Status = $"Opened a generated {(asTask ? "Task" : "Scenario")} for {topics.Count} topic(s) in the Script Editor.";
        return script;
    }
}

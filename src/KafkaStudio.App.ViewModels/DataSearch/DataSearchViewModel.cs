using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Search;

namespace KafkaStudio.App.ViewModels.DataSearch;

/// <summary>
/// The "Find Data" screen - everything for answering "does this (or similar) data exist, and where?":
/// <list type="bullet">
/// <item><b>Search</b> - plain or structured queries, match modes, time ranges, "Exists?", saved searches,
/// history, export, and "turn into a KafScript check";</item>
/// <item><b>Bulk check</b> - which of a list of ids are present/missing/duplicated;</item>
/// <item><b>Trace</b> - one id's journey across topics as a timeline;</item>
/// <item><b>Reconcile</b> - join two topics and list gaps and differences;</item>
/// <item><b>Similar</b> - same key / same field values / near-duplicates / same shape as a reference message;</item>
/// <item><b>Duplicates</b> and <b>Field stats</b> - analyse one topic.</item>
/// </list>
/// All tabs share one <see cref="SearchScopeViewModel"/> (connection, topics, time range) and one message
/// detail pane.
/// </summary>
public sealed class DataSearchViewModel : ObservableObject
{
    public const int SearchTab = 0, BulkTab = 1, TraceTab = 2, ReconcileTab = 3, SimilarTab = 4, DuplicatesTab = 5, FieldStatsTab = 6;

    public SearchScopeViewModel Scope { get; }
    public QuerySearchViewModel Query { get; }
    public BulkCheckViewModel Bulk { get; }
    public TraceViewModel Trace { get; }
    public ReconcileViewModel Reconcile { get; }
    public SimilarViewModel Similar { get; }
    public DuplicatesViewModel Duplicates { get; }
    public FieldStatsViewModel FieldStats { get; }
    public MessageActions Actions { get; }

    public DataSearchViewModel(AppState state)
    {
        Scope = new SearchScopeViewModel(state);
        Actions = new MessageActions(state, () => Scope.SelectedConnection, s => CurrentTab.ReportStatus(s));
        Query = new QuerySearchViewModel(state, Scope, Actions, Show);
        Bulk = new BulkCheckViewModel(state, Scope, Actions, Show);
        Trace = new TraceViewModel(state, Scope, Show);
        Reconcile = new ReconcileViewModel(state, Scope, Actions, Show);
        Similar = new SimilarViewModel(state, Scope, Show, query => RunQueryAsync(query), () => SelectedMessage);
        Duplicates = new DuplicatesViewModel(state, Scope, Show);
        FieldStats = new FieldStatsViewModel(state, Scope, Show, (query, topic) => RunQueryAsync(query, topic));
        FindSimilarOfSelectedCommand = new RelayCommand(() =>
        {
            Similar.SetReference(SelectedMessage);
            SelectedTabIndex = SimilarTab;
        }, () => SelectedMessage is not null);
        Help = new HowToPanelViewModel(FindDataHelp.BuildTopics(), () => SelectedTabIndex, tab => SelectedTabIndex = tab, TryExample);
    }

    /// <summary>The "Help &amp; examples" panel (F1): how-tos for each tab.</summary>
    public HowToPanelViewModel Help { get; }

    /// <summary>"Try it" on a how-to: fills its example into the tab it belongs to (without running anything).</summary>
    private void TryExample(HowToTopicViewModel topic)
    {
        switch (topic.TabIndex)
        {
            case SearchTab: Query.QueryText = topic.TryText; break;
            case BulkTab: Bulk.IdsText = topic.TryText; break;
            case TraceTab: Trace.Id = topic.TryText; break;
            case ReconcileTab: Reconcile.CompareFields = topic.TryText; break;
            case DuplicatesTab:
                Duplicates.GroupBy = Duplicates.GroupByChoices.First(c => c.Value == DuplicateGroupBy.Fields);
                Duplicates.FieldsText = topic.TryText;
                break;
        }
    }

    /// <summary>Makes the message shown in the detail pane the Similar tab's reference.</summary>
    public RelayCommand FindSimilarOfSelectedCommand { get; }

    private int _selectedTabIndex;
    public int SelectedTabIndex { get => _selectedTabIndex; set { if (SetProperty(ref _selectedTabIndex, value)) Help.Refresh(); } }

    private KafkaMessage? _selectedMessage;
    /// <summary>The message shown in the shared detail pane.</summary>
    public KafkaMessage? SelectedMessage
    {
        get => _selectedMessage;
        set { if (SetProperty(ref _selectedMessage, value)) FindSimilarOfSelectedCommand.RaiseCanExecuteChanged(); }
    }

    private void Show(KafkaMessage? message) => SelectedMessage = message;

    /// <summary>The tab being shown (shared actions report their outcome in its status line).</summary>
    public ScanTabViewModel CurrentTab => SelectedTabIndex switch
    {
        BulkTab => Bulk,
        TraceTab => Trace,
        ReconcileTab => Reconcile,
        SimilarTab => Similar,
        DuplicatesTab => Duplicates,
        FieldStatsTab => FieldStats,
        _ => Query
    };

    /// <summary>Switches to the Search tab and runs <paramref name="query"/> (optionally on one topic).</summary>
    public Task RunQueryAsync(string query, string? singleTopic = null)
    {
        SelectedTabIndex = SearchTab;
        return Query.RunQueryAsync(query, singleTopic);
    }

    private void SelectConnection(string? connection)
    {
        if (connection is not null && Scope.ConnectionNames.Contains(connection)) Scope.SelectedConnection = connection;
    }

    /// <summary>Entry point for "Find similar" on any message in the app.</summary>
    public void FindSimilar(string? connection, KafkaMessage message)
    {
        SelectConnection(connection);
        Similar.SetReference(message);
        SelectedMessage = message;
        SelectedTabIndex = SimilarTab;
    }

    /// <summary>Entry point for "Trace key" on any message in the app: fills in the id and runs the trace.</summary>
    public async Task TraceAsync(string? connection, string id)
    {
        var previous = Scope.SelectedConnection;
        SelectConnection(connection);
        Trace.Id = id;
        SelectedTabIndex = TraceTab;
        // A different connection (or a first visit) needs its topic list before the trace can run.
        if (Scope.SelectedConnection != previous || !Scope.HasTopics) await Scope.RefreshTopicsAsync().ConfigureAwait(true);
        await Trace.RunCommand.ExecuteAsync().ConfigureAwait(true);
    }
}

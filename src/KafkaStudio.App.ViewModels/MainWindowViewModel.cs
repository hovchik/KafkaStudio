using KafkaStudio.App.ViewModels.Brokers;
using KafkaStudio.App.ViewModels.Connections;
using KafkaStudio.App.ViewModels.Consumer;
using KafkaStudio.App.ViewModels.ConsumerGroups;
using KafkaStudio.App.ViewModels.DataSearch;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Producer;
using KafkaStudio.App.ViewModels.Rethrow;
using KafkaStudio.App.ViewModels.Scripts;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.App.ViewModels.Tasks;
using KafkaStudio.App.ViewModels.Testing;
using KafkaStudio.App.ViewModels.Topics;

namespace KafkaStudio.App.ViewModels;

/// <summary>One sidebar screen. <paramref name="Group"/> is shown as a small section label above the first item of each group.</summary>
public sealed record NavigationItem(string Key, string Label, string Icon, string Shortcut, ObservableObject ViewModel, string Group = "", bool StartsGroup = false);

/// <summary>One entry of the command palette (Ctrl+K): a screen to open or an app action to run.</summary>
public sealed record PaletteCommand(string Title, string Hint, Action Run);

/// <summary>
/// Root ViewModel for the whole app: owns the shared <see cref="AppState"/> and every top-level
/// screen's ViewModel, and tracks which one is currently shown. The Avalonia shell
/// (MainWindow.axaml) binds its sidebar to <see cref="NavigationItems"/> and its content area to
/// <see cref="SelectedItem"/>.ViewModel via a ViewLocator-style DataTemplate, so adding a new screen
/// here is enough to make it show up in the app - no XAML changes needed beyond the initial wiring.
/// Connections is not a sidebar item - it's opened as a flyout panel from the top-right corner
/// button (see <see cref="IsConnectionsOpen"/>), since it's more of a settings/setup screen than a
/// day-to-day workspace.
/// </summary>
public sealed class MainWindowViewModel : ObservableObject, IAsyncDisposable
{
    public AppState State { get; }

    public ConnectionsViewModel Connections { get; }
    public TopicBrowserViewModel Topics { get; }
    public ProducerViewModel Producer { get; }
    public ConsumerViewModel Consumer { get; }
    public ConsumerGroupsViewModel ConsumerGroups { get; }
    public ScriptEditorViewModel Scripts { get; }
    public TasksViewModel Tasks { get; }
    public RethrowRulesViewModel Rethrow { get; }
    public DataSearchViewModel DataSearch { get; }
    public QaLabViewModel QaLab { get; }
    public BrokersViewModel Brokers { get; }

    public IReadOnlyList<NavigationItem> NavigationItems { get; }

    private NavigationItem _selectedItem;
    public NavigationItem SelectedItem
    {
        get => _selectedItem;
        // A ListBox can momentarily push null while its items are re-templated - keep the last screen.
        set
        {
            if (!SetProperty(ref _selectedItem, value ?? _selectedItem)) return;
            // The Test Runner lists the Script Editor's scenarios too - pick up edits made since.
            if (_selectedItem.Key == "qa") QaLab.Tests.Refresh();
        }
    }

    private bool _isConnectionsOpen;
    /// <summary>Whether the Connections panel (opened from the top-right corner button) is currently shown.</summary>
    public bool IsConnectionsOpen
    {
        get => _isConnectionsOpen;
        set => SetProperty(ref _isConnectionsOpen, value);
    }

    private string? _notification;
    /// <summary>Transient message shown at the bottom of the window (errors no screen reported).</summary>
    public string? Notification { get => _notification; set => SetProperty(ref _notification, value); }

    /// <summary>"3 connections" / "no connections" - shown on the Connections button.</summary>
    public string ConnectionsSummary => State.ConnectionProfiles.Count switch
    {
        0 => "Connections",
        var n => $"Connections ({n})"
    };

    public bool HasNoConnections => State.ConnectionProfiles.Count == 0;

    private bool _isSidebarCollapsed;
    /// <summary>Sidebar shows icons only (56px) instead of icons + labels (244px).</summary>
    public bool IsSidebarCollapsed
    {
        get => _isSidebarCollapsed;
        set
        {
            if (!SetProperty(ref _isSidebarCollapsed, value)) return;
            OnPropertyChanged(nameof(SidebarWidth));
            SidebarCollapsedChanged?.Invoke(value);
        }
    }

    public double SidebarWidth => _isSidebarCollapsed ? 56 : 244;

    /// <summary>Raised when the user collapses/expands the sidebar, so the view can persist the choice.</summary>
    public event Action<bool>? SidebarCollapsedChanged;

    /// <summary>"2 connected" / "1 of 2 connected" / "No connection" - the cluster chip in the header.</summary>
    public string ClusterStatusText
    {
        get
        {
            var total = State.ConnectionProfiles.Count;
            var live = State.Connections.Count;
            if (total == 0) return "No connection";
            if (State.Connections.Count == 1) return State.Connections.Keys.First();
            return live == total ? $"{live} connected" : $"{live} of {total} connected";
        }
    }

    /// <summary>Header dot is green when something is connected.</summary>
    public bool ClusterIsOk => State.Connections.Count > 0;

    /// <summary>Header dot is red when nothing is connected and a connection failed.</summary>
    public bool ClusterHasError => State.Connections.Count == 0 && !State.ConnectionErrors.IsEmpty;

    private bool _isPaletteOpen;
    public bool IsPaletteOpen
    {
        get => _isPaletteOpen;
        set
        {
            if (!SetProperty(ref _isPaletteOpen, value)) return;
            if (value) { PaletteQuery = ""; RefreshPalette(); }
        }
    }

    private string _paletteQuery = "";
    public string PaletteQuery
    {
        get => _paletteQuery;
        set { if (SetProperty(ref _paletteQuery, value ?? "")) RefreshPalette(); }
    }

    private IReadOnlyList<PaletteCommand> _paletteResults = Array.Empty<PaletteCommand>();
    public IReadOnlyList<PaletteCommand> PaletteResults { get => _paletteResults; private set => SetProperty(ref _paletteResults, value); }

    private PaletteCommand? _selectedPaletteCommand;
    public PaletteCommand? SelectedPaletteCommand { get => _selectedPaletteCommand; set => SetProperty(ref _selectedPaletteCommand, value); }

    /// <summary>Extra palette actions owned by the view (theme toggle); set by MainWindow.</summary>
    public IReadOnlyList<PaletteCommand> ExtraPaletteCommands { get; set; } = Array.Empty<PaletteCommand>();

    private IEnumerable<PaletteCommand> AllPaletteCommands()
    {
        foreach (var item in NavigationItems)
        {
            var target = item;
            yield return new PaletteCommand($"Go to {item.Label}", item.Shortcut, () => SelectedItem = target);
        }
        yield return new PaletteCommand("Manage connections", "Ctrl+Shift+K", () => IsConnectionsOpen = true);
        yield return new PaletteCommand(IsSidebarCollapsed ? "Expand sidebar" : "Collapse sidebar", "Ctrl+B", () => IsSidebarCollapsed = !IsSidebarCollapsed);
        foreach (var extra in ExtraPaletteCommands) yield return extra;
    }

    private void RefreshPalette()
    {
        var q = _paletteQuery.Trim();
        var all = AllPaletteCommands();
        PaletteResults = (q.Length == 0
            ? all
            : all.Where(c => q.Split(' ', StringSplitOptions.RemoveEmptyEntries)
                .All(w => c.Title.Contains(w, StringComparison.OrdinalIgnoreCase)))).ToList();
        SelectedPaletteCommand = PaletteResults.FirstOrDefault();
    }

    /// <summary>Runs the selected palette entry (Enter / click) and closes the palette first so the action can open its own overlay.</summary>
    public void RunPaletteCommand(PaletteCommand? command = null)
    {
        command ??= SelectedPaletteCommand;
        IsPaletteOpen = false;
        command?.Run();
    }

    public void MovePaletteSelection(int delta)
    {
        if (PaletteResults.Count == 0) return;
        var index = SelectedPaletteCommand is null ? -1 : PaletteResults.ToList().IndexOf(SelectedPaletteCommand);
        SelectedPaletteCommand = PaletteResults[Math.Clamp(index + delta, 0, PaletteResults.Count - 1)];
    }

    public RelayCommand OpenPaletteCommand { get; }
    public RelayCommand ToggleSidebarCommand { get; }
    public RelayCommand OpenConnectionsCommand { get; }
    public RelayCommand CloseConnectionsCommand { get; }
    public RelayCommand DismissNotificationCommand { get; }
    public RelayCommand<string> NavigateCommand { get; }

    public MainWindowViewModel(AppState state)
    {
        State = state;

        Connections = new ConnectionsViewModel(state);
        Topics = new TopicBrowserViewModel(state);
        Producer = new ProducerViewModel(state);
        Consumer = new ConsumerViewModel(state);
        ConsumerGroups = new ConsumerGroupsViewModel(state);
        Scripts = new ScriptEditorViewModel(state);
        Tasks = new TasksViewModel(state);
        Rethrow = new RethrowRulesViewModel(state);
        DataSearch = new DataSearchViewModel(state);
        QaLab = new QaLabViewModel(state, () => Scripts.Source, () => Scripts.FilePath);
        Brokers = new BrokersViewModel(state);

        NavigationItems = new List<NavigationItem>
        {
            new("topics", "Topics", "▤", "Ctrl+1", Topics, "DATA", true),
            new("consumer", "Consume", "↓", "Ctrl+3", Consumer, "DATA"),
            new("search", "Find Data", "⌕", "Ctrl+7", DataSearch, "DATA"),
            new("producer", "Produce", "↑", "Ctrl+2", Producer, "AUTOMATION", true),
            new("scripts", "Scripts", "{ }", "Ctrl+4", Scripts, "AUTOMATION"),
            new("tasks", "Tasks & Checks", "⏱", "Ctrl+5", Tasks, "AUTOMATION"),
            new("rethrow", "Rethrow Rules", "⇄", "Ctrl+6", Rethrow, "AUTOMATION"),
            new("qa", "QA Lab", "✓", "Ctrl+8", QaLab, "AUTOMATION"),
            new("brokers", "Brokers", "▦", "Ctrl+9", Brokers, "CLUSTER", true),
            new("groups", "Consumer Groups", "☰", "Ctrl+0", ConsumerGroups, "CLUSTER")
        };

        _selectedItem = NavigationItems[0];
        OpenConnectionsCommand = new RelayCommand(() => IsConnectionsOpen = true);
        OpenPaletteCommand = new RelayCommand(() => IsPaletteOpen = !IsPaletteOpen);
        ToggleSidebarCommand = new RelayCommand(() => IsSidebarCollapsed = !IsSidebarCollapsed);
        CloseConnectionsCommand = new RelayCommand(() => IsConnectionsOpen = false);
        DismissNotificationCommand = new RelayCommand(() => Notification = null);
        NavigateCommand = new RelayCommand<string>(key =>
        {
            if (NavigationItems.FirstOrDefault(i => i.Key == key) is { } item) SelectedItem = item;
        });

        state.Notification += message => Notification = message;
        state.ConnectionsChanged += () =>
        {
            OnPropertyChanged(nameof(ConnectionsSummary));
            OnPropertyChanged(nameof(HasNoConnections));
            OnPropertyChanged(nameof(ClusterStatusText));
            OnPropertyChanged(nameof(ClusterIsOk));
            OnPropertyChanged(nameof(ClusterHasError));
        };
        state.EditInProducerRequested += (connection, message) =>
        {
            Producer.LoadMessage(connection, message);
            SelectedItem = NavigationItems.First(i => i.Key == "producer");
        };
        state.FindSimilarRequested += (connection, message) =>
        {
            DataSearch.FindSimilar(connection, message);
            SelectedItem = NavigationItems.First(i => i.Key == "search");
        };
        state.TraceRequested += (connection, id) =>
        {
            SelectedItem = NavigationItems.First(i => i.Key == "search");
            _ = DataSearch.TraceAsync(connection, id);
        };
        state.OpenScriptRequested += source =>
        {
            Scripts.AppendScript(source);
            SelectedItem = NavigationItems.First(i => i.Key == "scripts");
        };
        CommandErrors.Unhandled += ex => state.PostToUi(() => Notification = $"Unexpected error: {ex.Message}");
    }

    public async ValueTask DisposeAsync() => await State.DisposeAsync().ConfigureAwait(false);
}

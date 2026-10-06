using KafkaStudio.App.ViewModels.Brokers;
using KafkaStudio.App.ViewModels.Connections;
using KafkaStudio.App.ViewModels.Consumer;
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

public sealed record NavigationItem(string Key, string Label, string Icon, string Shortcut, ObservableObject ViewModel);

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
        Scripts = new ScriptEditorViewModel(state);
        Tasks = new TasksViewModel(state);
        Rethrow = new RethrowRulesViewModel(state);
        DataSearch = new DataSearchViewModel(state);
        QaLab = new QaLabViewModel(state, () => Scripts.Source, () => Scripts.FilePath);
        Brokers = new BrokersViewModel(state);

        NavigationItems = new List<NavigationItem>
        {
            new("topics", "Topics", "▤", "Ctrl+1", Topics),
            new("producer", "Produce", "↑", "Ctrl+2", Producer),
            new("consumer", "Consume", "↓", "Ctrl+3", Consumer),
            new("scripts", "Scripts", "{ }", "Ctrl+4", Scripts),
            new("tasks", "Tasks & Checks", "⏱", "Ctrl+5", Tasks),
            new("rethrow", "Rethrow Rules", "⇄", "Ctrl+6", Rethrow),
            new("search", "Find Data", "⌕", "Ctrl+7", DataSearch),
            new("qa", "QA Lab", "✓", "Ctrl+8", QaLab),
            new("brokers", "Brokers", "▦", "Ctrl+9", Brokers)
        };

        _selectedItem = NavigationItems[0];
        OpenConnectionsCommand = new RelayCommand(() => IsConnectionsOpen = true);
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

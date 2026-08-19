using KafkaStudio.App.ViewModels.Connections;
using KafkaStudio.App.ViewModels.Consumer;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Producer;
using KafkaStudio.App.ViewModels.Rethrow;
using KafkaStudio.App.ViewModels.Scripts;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.App.ViewModels.Tasks;
using KafkaStudio.App.ViewModels.Topics;

namespace KafkaStudio.App.ViewModels;

public sealed record NavigationItem(string Key, string Label, string Icon, ObservableObject ViewModel);

/// <summary>
/// Root ViewModel for the whole app: owns the shared <see cref="AppState"/> and every top-level
/// screen's ViewModel, and tracks which one is currently shown. The Avalonia shell
/// (MainWindow.axaml) binds its sidebar to <see cref="NavigationItems"/> and its content area to
/// <see cref="SelectedItem"/>.ViewModel via a ViewLocator-style DataTemplate, so adding a new screen
/// here is enough to make it show up in the app - no XAML changes needed beyond the initial wiring.
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

    public IReadOnlyList<NavigationItem> NavigationItems { get; }

    private NavigationItem _selectedItem;
    public NavigationItem SelectedItem
    {
        get => _selectedItem;
        set => SetProperty(ref _selectedItem, value);
    }

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

        NavigationItems = new List<NavigationItem>
        {
            new("connections", "Connections", "", Connections),
            new("topics", "Topics", "", Topics),
            new("producer", "Produce", "", Producer),
            new("consumer", "Consume", "", Consumer),
            new("scripts", "Scripts (KafScript)", "", Scripts),
            new("tasks", "Tasks & Checks", "", Tasks),
            new("rethrow", "Rethrow Rules", "", Rethrow)
        };

        _selectedItem = NavigationItems[0];
    }

    public async ValueTask DisposeAsync() => await State.DisposeAsync().ConfigureAwait(false);
}

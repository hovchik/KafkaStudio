using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;

namespace KafkaStudio.App.ViewModels.Shared;

/// <summary>A single "how to" entry in a screen's in-app help panel (Find Data, QA Lab): a task-oriented
/// title, the steps to do it on that screen, and an optional example. <see cref="TabIndex"/> ties it to one
/// of the screen's tabs (-1 = applies to the whole screen), so the panel can show just the current tab's
/// topics and jump to the right tab. <see cref="TryText"/>, when set, is something the screen can fill in
/// for you (a search query, an id to trace, a tag filter) via "Try it".</summary>
public sealed class HowToTopicViewModel
{
    public const int AllTabs = -1;

    public required string Title { get; init; }
    public required int TabIndex { get; init; }
    /// <summary>The tab's header, shown as a badge ("Search", "Test Runner"…); "All tabs" for general topics.</summary>
    public required string TabName { get; init; }
    /// <summary>What to do, one step per line.</summary>
    public required string Steps { get; init; }
    /// <summary>A worked example (a query, a snippet, a tag filter…), or null.</summary>
    public string? Example { get; init; }
    /// <summary>Text "Try it" puts into the screen (e.g. into the search box); null when there's nothing to fill in.</summary>
    public string? TryText { get; init; }

    public bool HasExample => !string.IsNullOrEmpty(Example);
    public bool HasTry => !string.IsNullOrEmpty(TryText);
    public bool IsGeneral => TabIndex == AllTabs;
}

/// <summary>
/// The state behind a screen's "Help &amp; examples" panel: whether it's open (F1), whether it lists only the
/// current tab's topics, and the resulting visible list. The host screen supplies the topics, its current tab,
/// and what "Open tab" / "Try it" do.
/// </summary>
public sealed class HowToPanelViewModel : ObservableObject
{
    private readonly IReadOnlyList<HowToTopicViewModel> _all;
    private readonly Func<int> _currentTab;

    public HowToPanelViewModel(IReadOnlyList<HowToTopicViewModel> topics, Func<int> currentTab,
                               Action<int> openTab, Action<HowToTopicViewModel> tryIt)
    {
        _all = topics;
        _currentTab = currentTab;
        ToggleCommand = new RelayCommand(() => IsVisible = !IsVisible);
        OpenTabCommand = new RelayCommand<HowToTopicViewModel>(t => { if (t is { IsGeneral: false }) openTab(t.TabIndex); });
        TryCommand = new RelayCommand<HowToTopicViewModel>(t => { if (t is { HasTry: true }) { openTab(t.TabIndex); tryIt(t); } });
        Refresh();
    }

    public ObservableCollection<HowToTopicViewModel> Topics { get; } = new();

    private bool _isVisible;
    public bool IsVisible { get => _isVisible; set => SetProperty(ref _isVisible, value); }

    private bool _currentTabOnly = true;
    /// <summary>List only the current tab's topics (plus the ones that apply to every tab).</summary>
    public bool CurrentTabOnly { get => _currentTabOnly; set { if (SetProperty(ref _currentTabOnly, value)) Refresh(); } }

    public RelayCommand ToggleCommand { get; }
    /// <summary>Switches the screen to the topic's tab.</summary>
    public RelayCommand<HowToTopicViewModel> OpenTabCommand { get; }
    /// <summary>Switches to the topic's tab and fills in its <see cref="HowToTopicViewModel.TryText"/>.</summary>
    public RelayCommand<HowToTopicViewModel> TryCommand { get; }

    /// <summary>Re-filters the list; the host calls this when its tab changes.</summary>
    public void Refresh()
    {
        var tab = _currentTab();
        Topics.Clear();
        foreach (var topic in _all)
            if (!CurrentTabOnly || topic.IsGeneral || topic.TabIndex == tab) Topics.Add(topic);
    }
}

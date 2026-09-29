using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;

namespace KafkaStudio.App.ViewModels.Testing;

/// <summary>
/// The "QA Lab" screen - tools for QA engineers testing Kafka-based systems:
/// <list type="bullet">
/// <item><b>Test Runner</b> - run .kafscript regression packs by tag, with retries, live results and
/// JUnit/HTML/Markdown reports and one-click bug reports;</item>
/// <item><b>Contract check</b> - validate a topic against a JSON Schema, infer a schema from real
/// traffic, and turn a contract into a KafScript check.</item>
/// </list>
/// </summary>
public sealed class QaLabViewModel : ObservableObject
{
    public const int TestsTab = 0, ContractsTab = 1;

    public QaLabViewModel(AppState state, Func<string?> editorSource, Func<string?> editorPath)
    {
        Tests = new TestRunnerViewModel(state, editorSource, editorPath);
        Contracts = new ContractCheckViewModel(state);
    }

    public TestRunnerViewModel Tests { get; }
    public ContractCheckViewModel Contracts { get; }

    private int _selectedTabIndex;
    public int SelectedTabIndex { get => _selectedTabIndex; set => SetProperty(ref _selectedTabIndex, value); }
}

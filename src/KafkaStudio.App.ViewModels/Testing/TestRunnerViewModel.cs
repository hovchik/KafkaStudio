using System.Collections.ObjectModel;
using System.Globalization;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Scripts;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Automation.Testing;
using KafkaStudio.Automation.Testing.Reports;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Persistence;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.App.ViewModels.Testing;

public enum TestItemState { NotRun, Queued, Running, Passed, Failed, Error, Skipped, Cancelled }

/// <summary>One test in the Test Runner list, with its latest result and (live) step results.</summary>
public sealed class TestItemViewModel : ObservableObject
{
    public TestItemViewModel(TestCase testCase) => Case = testCase;

    public TestCase Case { get; }
    public string Id => Case.Id;
    public string Name => Case.Name;
    public string Location => Case.Line > 0 ? $"{Case.FileName}:{Case.Line}" : Case.FileName;
    public string TagsText => string.Join(" ", Case.Tags.Select(t => "@" + t));
    public bool HasTags => Case.Tags.Count > 0;

    public ObservableCollection<StepResultRowViewModel> Steps { get; } = new();

    private TestItemState _state;
    public TestItemState State
    {
        get => _state;
        set
        {
            if (SetProperty(ref _state, value))
            {
                OnPropertyChanged(nameof(StatusGlyph));
                OnPropertyChanged(nameof(StatusText));
                OnPropertyChanged(nameof(IsProblem));
            }
        }
    }

    private TestCaseResult? _result;
    public TestCaseResult? Result
    {
        get => _result;
        private set
        {
            if (SetProperty(ref _result, value))
            {
                OnPropertyChanged(nameof(DurationText));
                OnPropertyChanged(nameof(Message));
                OnPropertyChanged(nameof(HasMessage));
                OnPropertyChanged(nameof(IsFlaky));
                OnPropertyChanged(nameof(AttemptsText));
            }
        }
    }

    public bool IsProblem => State is TestItemState.Failed or TestItemState.Error;
    public bool IsFlaky => Result?.IsFlaky == true;
    public string DurationText => Result is null ? "" : TestRunReport.FormatDuration(Result.Duration);
    public string? Message => Result?.Message ?? Case.LoadError;
    public bool HasMessage => !string.IsNullOrEmpty(Message);
    public string AttemptsText => Result is { Attempts: > 1 } r ? $"{r.Attempts} attempts" : "";

    public string StatusGlyph => State switch
    {
        TestItemState.Passed => IsFlaky ? "≈" : "✓",
        TestItemState.Failed => "✗",
        TestItemState.Error => "!",
        TestItemState.Running => "▶",
        TestItemState.Queued => "…",
        TestItemState.Skipped or TestItemState.Cancelled => "–",
        _ => "○"
    };

    public string StatusText => State switch
    {
        TestItemState.NotRun => "not run",
        TestItemState.Passed when IsFlaky => "flaky",
        _ => State.ToString().ToLowerInvariant()
    };

    public void Apply(TestCaseResult result)
    {
        Result = result;
        State = result.Outcome switch
        {
            TestOutcome.Passed => TestItemState.Passed,
            TestOutcome.Failed => TestItemState.Failed,
            TestOutcome.Error => TestItemState.Error,
            TestOutcome.Cancelled => TestItemState.Cancelled,
            _ => TestItemState.Skipped
        };
        OnPropertyChanged(nameof(StatusGlyph));
        OnPropertyChanged(nameof(StatusText));
    }

    public void Reset(TestItemState state)
    {
        Result = null;
        Steps.Clear();
        State = state;
    }

    /// <summary>Keeps the last result when the list is re-discovered.</summary>
    public void CopyFrom(TestItemViewModel previous)
    {
        if (previous.Result is { } result) Apply(result);
        foreach (var step in previous.Steps) Steps.Add(step);
    }
}

/// <summary>
/// The QA Test Runner: pick .kafscript files/folders (plus, optionally, what's in the Script Editor),
/// filter the tests by tag expression and name, run them all / just the failed ones / one, watch results
/// stream in, and share the outcome as JUnit XML, HTML or Markdown - or copy a ready-made bug report for
/// one failed test. Runs use the same <see cref="TestSuiteRunner"/> as the <c>kafkastudio test</c> CLI, so
/// a pack behaves identically on a QA engineer's machine and in CI.
/// </summary>
public sealed class TestRunnerViewModel : ObservableObject
{
    private const string SettingsFile = "test-runner.json";

    private readonly AppState _state;
    private readonly Func<string?> _editorSource;
    private readonly Func<string?> _editorPath;
    private readonly Dictionary<string, string[]> _sourceLines = new(StringComparer.Ordinal);
    private IReadOnlyList<TestCase> _allCases = Array.Empty<TestCase>();
    private CancellationTokenSource? _runCts;
    private bool _loading;
    private int _runGeneration;

    /// <summary>Settings remembered between sessions.</summary>
    public sealed record Settings
    {
        public List<string> Paths { get; init; } = new();
        public bool IncludeEditor { get; init; } = true;
        public string? TagFilter { get; init; }
        public string? NameFilter { get; init; }
        public int Retries { get; init; }
        public bool FailFast { get; init; }
        public double TimeoutSeconds { get; init; }
        public string? Variables { get; init; }
        public string? DefaultConnection { get; init; }
    }

    public TestRunnerViewModel(AppState state, Func<string?> editorSource, Func<string?> editorPath)
    {
        _state = state;
        _editorSource = editorSource;
        _editorPath = editorPath;

        AddFileCommand = new AsyncRelayCommand(AddFileAsync);
        AddFolderCommand = new AsyncRelayCommand(AddFolderAsync);
        RemovePathCommand = new RelayCommand<string>(p => { if (p is not null && Paths.Remove(p)) PathsChanged(); }, _ => !IsRunning);
        RefreshCommand = new RelayCommand(Refresh, () => !IsRunning);
        RunAllCommand = new AsyncRelayCommand(() => RunAsync(Tests.ToList()), CanRun);
        RunFailedCommand = new AsyncRelayCommand(() => RunAsync(Tests.Where(t => t.IsProblem).ToList()),
            () => CanRun() && Tests.Any(t => t.IsProblem));
        RunSelectedCommand = new AsyncRelayCommand(() => RunAsync(SelectedTest is null ? new List<TestItemViewModel>() : new List<TestItemViewModel> { SelectedTest }),
            () => CanRun() && SelectedTest is not null);
        StopCommand = new RelayCommand(() => _runCts?.Cancel(), () => IsRunning);
        ExportJUnitCommand = new AsyncRelayCommand(() => ExportAsync(".xml", "JUnit XML", JUnitReportWriter.Write), () => LastReport is not null);
        ExportHtmlCommand = new AsyncRelayCommand(() => ExportAsync(".html", "HTML report", HtmlReportWriter.Write), () => LastReport is not null);
        ExportMarkdownCommand = new AsyncRelayCommand(() => ExportAsync(".md", "Markdown", MarkdownReportWriter.Write), () => LastReport is not null);
        CopySummaryCommand = new AsyncRelayCommand(() => CopyAsync(MarkdownReportWriter.Write(LastReport!), "Markdown summary copied."), () => LastReport is not null);
        CopyBugReportCommand = new AsyncRelayCommand(
            () => CopyAsync(MarkdownReportWriter.BugReport(SelectedTest!.Result!, Environment()), "Bug report copied - paste it into your tracker."),
            () => SelectedTest?.Result is { Outcome: TestOutcome.Failed or TestOutcome.Error });

        _state.ConnectionsChanged += RefreshConnectionNames;
        RefreshConnectionNames();
        Load();
        Refresh();
    }

    // ------------------------------------------------------------------ sources ----

    /// <summary>.kafscript files and folders the tests come from.</summary>
    public ObservableCollection<string> Paths { get; } = new();

    private bool _includeEditor = true;
    /// <summary>Also treat the Script Editor's current contents as a test file.</summary>
    public bool IncludeEditor
    {
        get => _includeEditor;
        set { if (SetProperty(ref _includeEditor, value)) { Save(); Refresh(); } }
    }

    // ------------------------------------------------------------------ filter & options ----

    private string? _tagFilter;
    /// <summary>E.g. "@smoke and not @wip".</summary>
    public string? TagFilter
    {
        get => _tagFilter;
        set { if (SetProperty(ref _tagFilter, value)) { Save(); ApplyFilter(); } }
    }

    private string? _tagFilterError;
    public string? TagFilterError { get => _tagFilterError; private set => SetProperty(ref _tagFilterError, value); }

    private string? _nameFilter;
    public string? NameFilter
    {
        get => _nameFilter;
        set { if (SetProperty(ref _nameFilter, value)) { Save(); ApplyFilter(); } }
    }

    private decimal _retries;
    /// <summary>Extra attempts for a failed test (flaky detection).</summary>
    public decimal Retries { get => _retries; set { if (SetProperty(ref _retries, Math.Clamp(value, 0, 10))) Save(); } }

    private bool _failFast;
    public bool FailFast { get => _failFast; set { if (SetProperty(ref _failFast, value)) Save(); } }

    private decimal _timeoutSeconds;
    /// <summary>Per-test time limit in seconds; 0 = none.</summary>
    public decimal TimeoutSeconds { get => _timeoutSeconds; set { if (SetProperty(ref _timeoutSeconds, Math.Max(0, value))) Save(); } }

    private string? _variablesText;
    /// <summary>"name=value" lines - starting variables for every test (test data per environment).</summary>
    public string? VariablesText
    {
        get => _variablesText;
        set { if (SetProperty(ref _variablesText, value)) { Save(); OnPropertyChanged(nameof(VariablesError)); RunAllCommand.RaiseCanExecuteChanged(); } }
    }

    public string? VariablesError => TryParseVariables(VariablesText, out _, out var error) ? null : error;

    public ObservableCollection<string> ConnectionNames { get; } = new();

    private string? _defaultConnection;
    /// <summary>The connection a test with no "use connection" step runs against (so one pack can be
    /// pointed at dev, staging...). Empty = the first connection.</summary>
    public string? DefaultConnection { get => _defaultConnection; set { if (SetProperty(ref _defaultConnection, value)) Save(); } }

    // ------------------------------------------------------------------ tests & results ----

    public ObservableCollection<TestItemViewModel> Tests { get; } = new();

    private TestItemViewModel? _selectedTest;
    public TestItemViewModel? SelectedTest
    {
        get => _selectedTest;
        set
        {
            if (SetProperty(ref _selectedTest, value))
            {
                RunSelectedCommand.RaiseCanExecuteChanged();
                CopyBugReportCommand.RaiseCanExecuteChanged();
            }
        }
    }

    private string _discoverySummary = "";
    /// <summary>"9 of 12 test(s) in 3 file(s)" or a load problem.</summary>
    public string DiscoverySummary { get => _discoverySummary; private set => SetProperty(ref _discoverySummary, value); }

    private bool _isRunning;
    public bool IsRunning
    {
        get => _isRunning;
        private set
        {
            if (!SetProperty(ref _isRunning, value)) return;
            OnPropertyChanged(nameof(ProgressText));
            RaiseCommandStates();
        }
    }

    private int _completedCount;
    public int CompletedCount { get => _completedCount; private set { if (SetProperty(ref _completedCount, value)) OnPropertyChanged(nameof(ProgressText)); } }

    private int _runTotal;
    public int RunTotal { get => _runTotal; private set { if (SetProperty(ref _runTotal, value)) OnPropertyChanged(nameof(ProgressText)); } }

    public string ProgressText => IsRunning ? $"{CompletedCount}/{RunTotal}" : "";

    private int _passed, _failed, _errors, _skipped;
    public int PassedCount { get => _passed; private set => SetProperty(ref _passed, value); }
    public int FailedCount { get => _failed; private set => SetProperty(ref _failed, value); }
    public int ErrorCount { get => _errors; private set => SetProperty(ref _errors, value); }
    public int SkippedCount { get => _skipped; private set => SetProperty(ref _skipped, value); }

    private string? _runSummary;
    public string? RunSummary { get => _runSummary; private set => SetProperty(ref _runSummary, value); }

    private bool? _lastRunPassed;
    public bool? LastRunPassed { get => _lastRunPassed; private set => SetProperty(ref _lastRunPassed, value); }

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; private set => SetProperty(ref _statusMessage, value); }

    private TestRunReport? _lastReport;
    public TestRunReport? LastReport
    {
        get => _lastReport;
        private set { if (SetProperty(ref _lastReport, value)) RaiseCommandStates(); }
    }

    public AsyncRelayCommand AddFileCommand { get; }
    public AsyncRelayCommand AddFolderCommand { get; }
    public RelayCommand<string> RemovePathCommand { get; }
    public RelayCommand RefreshCommand { get; }
    public AsyncRelayCommand RunAllCommand { get; }
    public AsyncRelayCommand RunFailedCommand { get; }
    public AsyncRelayCommand RunSelectedCommand { get; }
    public RelayCommand StopCommand { get; }
    public AsyncRelayCommand ExportJUnitCommand { get; }
    public AsyncRelayCommand ExportHtmlCommand { get; }
    public AsyncRelayCommand ExportMarkdownCommand { get; }
    public AsyncRelayCommand CopySummaryCommand { get; }
    public AsyncRelayCommand CopyBugReportCommand { get; }

    // ------------------------------------------------------------------ discovery ----

    /// <summary>Adds a file or folder of tests (used by the pickers, and by tests).</summary>
    public void AddPath(string path)
    {
        var full = System.IO.Path.GetFullPath(path);
        if (Paths.Contains(full, StringComparer.Ordinal)) return;
        Paths.Add(full);
        PathsChanged();
    }

    private void PathsChanged()
    {
        Save();
        Refresh();
    }

    private async Task AddFileAsync()
    {
        if (_state.FileDialogs is null) return;
        var path = await _state.FileDialogs.PickOpenFileAsync("Add a test file", ".kafscript", "KafScript").ConfigureAwait(true);
        if (path is not null) AddPath(path);
    }

    private async Task AddFolderAsync()
    {
        if (_state.FileDialogs is null) return;
        var path = await _state.FileDialogs.PickFolderAsync("Add a folder of .kafscript tests").ConfigureAwait(true);
        if (path is not null) AddPath(path);
    }

    /// <summary>Re-reads every source (files and the editor) and rebuilds the list, keeping earlier results.</summary>
    public void Refresh()
    {
        if (IsRunning) return;
        _sourceLines.Clear();
        var cases = new List<TestCase>();
        if (Paths.Count > 0)
        {
            var discovered = TestDiscovery.Discover(Paths);
            cases.AddRange(discovered);
            foreach (var file in discovered.Select(c => c.FilePath).Distinct(StringComparer.Ordinal))
            {
                try { _sourceLines[file] = File.ReadAllText(file).Replace("\r\n", "\n").Split('\n'); }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { /* reported as a load error */ }
            }
        }
        if (IncludeEditor && _editorSource() is { Length: > 0 } source)
        {
            var editorPath = _editorPath() ?? "";
            if (!cases.Any(c => c.FilePath == editorPath && editorPath.Length > 0))
            {
                cases.AddRange(TestDiscovery.FromSource(source, editorPath));
                _sourceLines[editorPath] = source.Replace("\r\n", "\n").Split('\n');
            }
        }
        _allCases = cases;
        ApplyFilter();
    }

    private TestRunOptions? BuildOptions(out string? problem)
    {
        if (!TagExpression.TryParse(TagFilter, out var tags, out var tagError))
        {
            problem = $"tag filter: {tagError}";
            return null;
        }
        if (!TryParseVariables(VariablesText, out var variables, out var varError))
        {
            problem = varError;
            return null;
        }
        problem = null;
        return new TestRunOptions
        {
            Tags = tags,
            NameFilter = NameFilter,
            Retries = (int)Retries,
            FailFast = FailFast,
            Timeout = TimeoutSeconds > 0 ? TimeSpan.FromSeconds((double)TimeoutSeconds) : null,
            Variables = variables
        };
    }

    private void ApplyFilter()
    {
        if (IsRunning) return;
        TagExpression.TryParse(TagFilter, out var tags, out var tagError);
        TagFilterError = tagError;
        var options = new TestRunOptions { Tags = tags, NameFilter = NameFilter };
        var selected = TestDiscovery.Select(_allCases, options);

        var previous = Tests.ToDictionary(t => t.Id, StringComparer.Ordinal);
        var selectedId = SelectedTest?.Id;
        Tests.Clear();
        foreach (var testCase in selected)
        {
            var item = new TestItemViewModel(testCase);
            if (testCase.LoadError is not null) item.State = TestItemState.Error;
            else if (previous.TryGetValue(item.Id, out var old)) item.CopyFrom(old);
            Tests.Add(item);
        }
        SelectedTest = Tests.FirstOrDefault(t => t.Id == selectedId) ?? Tests.FirstOrDefault();

        var runnable = _allCases.Count(c => c.Block is not null && TestDiscovery.IsSelected(c, new TestRunOptions()));
        var files = _allCases.Select(c => c.FilePath).Distinct(StringComparer.Ordinal).Count();
        var broken = _allCases.Count(c => c.LoadError is not null);
        DiscoverySummary = _allCases.Count == 0
            ? "No tests yet - add a .kafscript file or folder, or write a Scenario in the Script Editor."
            : $"{selected.Count(c => c.Block is not null)} of {runnable} test(s) selected · {files} source(s)" +
              (broken > 0 ? $" · {broken} can't be loaded" : "");
        RaiseCommandStates();
    }

    // ------------------------------------------------------------------ running ----

    private bool CanRun() => !IsRunning && TagFilterError is null && VariablesError is null && Tests.Count > 0;

    private void RaiseCommandStates()
    {
        RunAllCommand?.RaiseCanExecuteChanged();
        RunFailedCommand?.RaiseCanExecuteChanged();
        RunSelectedCommand?.RaiseCanExecuteChanged();
        StopCommand?.RaiseCanExecuteChanged();
        RefreshCommand?.RaiseCanExecuteChanged();
        RemovePathCommand?.RaiseCanExecuteChanged();
        ExportJUnitCommand?.RaiseCanExecuteChanged();
        ExportHtmlCommand?.RaiseCanExecuteChanged();
        ExportMarkdownCommand?.RaiseCanExecuteChanged();
        CopySummaryCommand?.RaiseCanExecuteChanged();
        CopyBugReportCommand?.RaiseCanExecuteChanged();
    }

    /// <summary>Runs <paramref name="items"/> in list order.</summary>
    public async Task RunAsync(IReadOnlyList<TestItemViewModel> items)
    {
        if (items.Count == 0 || IsRunning) return;
        var options = BuildOptions(out var problem);
        if (options is null)
        {
            StatusMessage = problem;
            return;
        }

        var connections = new Dictionary<string, IKafkaGateway>(_state.Connections);
        IKafkaGateway? defaultGateway = null;
        if (!string.IsNullOrEmpty(DefaultConnection) && !connections.TryGetValue(DefaultConnection, out defaultGateway))
        {
            StatusMessage = $"Connection '{DefaultConnection}' isn't connected.";
            return;
        }

        var byId = items.ToDictionary(i => i.Id, StringComparer.Ordinal);
        foreach (var item in items) item.Reset(TestItemState.Queued);
        using var cts = new CancellationTokenSource();
        _runCts = cts;
        IsRunning = true;
        CompletedCount = 0;
        RunTotal = items.Count;
        PassedCount = FailedCount = ErrorCount = SkippedCount = 0;
        RunSummary = "Running…";
        LastRunPassed = null;
        StatusMessage = null;

        var suite = new TestSuiteRunner(connections, defaultGateway);
        // Live updates arrive as UI posts that may be handled after the run itself has finished, so they
        // are idempotent and ignored once a newer run started; the final report is applied at the end.
        var generation = ++_runGeneration;
        var finished = new HashSet<string>(StringComparer.Ordinal);
        void OnUi(Action action) => _state.PostToUi(() => { if (generation == _runGeneration) action(); });

        suite.CaseStarted += (testCase, attempt) => OnUi(() =>
        {
            if (!byId.TryGetValue(testCase.Id, out var item) || finished.Contains(testCase.Id)) return;
            if (attempt > 1) item.Steps.Clear();
            item.State = TestItemState.Running;
        });
        suite.StepCompleted += (testCase, step) => OnUi(() =>
        {
            if (byId.TryGetValue(testCase.Id, out var item) && item.State == TestItemState.Running) item.Steps.Add(ToRow(testCase, step));
        });
        suite.CaseCompleted += result => OnUi(() => ApplyResult(result, byId, finished));

        try
        {
            var cases = items.Select(i => i.Case).ToList();
            var report = await Task.Run(() => suite.RunAsync(cases, options, cts.Token, "KafkaStudio test run", Environment())).ConfigureAwait(true);
            foreach (var result in report.Results) ApplyResult(result, byId, finished);
            LastReport = report;
            RunSummary = (report.WasCancelled ? "Stopped. " : "") + report.Summary;
            LastRunPassed = report.WasCancelled ? null : report.Success;
        }
        catch (Exception ex)
        {
            RunSummary = $"Run failed: {ex.Message}";
            LastRunPassed = false;
        }
        finally
        {
            IsRunning = false;
            _runCts = null;
            RaiseCommandStates();
        }
    }

    private void ApplyResult(TestCaseResult result, Dictionary<string, TestItemViewModel> byId, HashSet<string> finished)
    {
        if (!byId.TryGetValue(result.Case.Id, out var item) || !finished.Add(result.Case.Id)) return;
        item.Steps.Clear();
        foreach (var step in result.Steps) item.Steps.Add(ToRow(result.Case, step));
        item.Apply(result);
        CompletedCount = finished.Count;
        var done = byId.Values.Where(i => finished.Contains(i.Id)).ToList();
        PassedCount = done.Count(i => i.State == TestItemState.Passed);
        FailedCount = done.Count(i => i.State == TestItemState.Failed);
        ErrorCount = done.Count(i => i.State == TestItemState.Error);
        SkippedCount = done.Count(i => i.State is TestItemState.Skipped or TestItemState.Cancelled);
    }

    private StepResultRowViewModel ToRow(TestCase testCase, StepResult step)
    {
        var description = step.Step.Action.GetType().Name.Replace("Action", "");
        if (_sourceLines.TryGetValue(testCase.FilePath, out var lines) && step.Step.Line - 1 is var index && index >= 0 && index < lines.Length)
        {
            var text = lines[index].Trim();
            var space = text.IndexOf(' ');
            if (space > 0) text = text[(space + 1)..].Trim();
            description = text.Length > 160 ? text[..160] + "…" : text;
        }
        return new StepResultRowViewModel
        {
            Keyword = step.Step.Keyword.ToString(),
            Description = description,
            Status = step.Status,
            Message = step.Message,
            Line = step.Step.Line,
            Duration = step.Duration
        };
    }

    private Dictionary<string, string> Environment()
    {
        var env = new Dictionary<string, string>
        {
            ["connections"] = _state.Connections.IsEmpty ? "(none)" : string.Join(", ", _state.Connections.Keys.OrderBy(k => k, StringComparer.OrdinalIgnoreCase)),
            ["machine"] = System.Environment.MachineName
        };
        if (!string.IsNullOrEmpty(DefaultConnection)) env["default connection"] = DefaultConnection;
        if (!string.IsNullOrWhiteSpace(TagFilter)) env["tag filter"] = TagFilter.Trim();
        return env;
    }

    // ------------------------------------------------------------------ sharing ----

    private async Task ExportAsync(string extension, string filterName, Func<TestRunReport, string> write)
    {
        if (LastReport is null || _state.FileDialogs is null) return;
        var name = $"kafkastudio-tests-{LastReport.StartedAt.ToString("yyyyMMdd-HHmm", CultureInfo.InvariantCulture)}{extension}";
        var path = await _state.FileDialogs.PickSaveFileAsync($"Export {filterName}", extension, filterName, name).ConfigureAwait(true);
        if (path is null) return;
        try
        {
            await File.WriteAllTextAsync(path, write(LastReport)).ConfigureAwait(true);
            StatusMessage = $"Saved {System.IO.Path.GetFileName(path)}.";
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            StatusMessage = $"Could not save: {ex.Message}";
        }
    }

    private async Task CopyAsync(string text, string done)
    {
        if (_state.SetClipboardText is null)
        {
            StatusMessage = "Clipboard isn't available.";
            return;
        }
        await _state.SetClipboardText(text).ConfigureAwait(true);
        StatusMessage = done;
    }

    // ------------------------------------------------------------------ helpers ----

    /// <summary>Parses "name=value" lines (blank lines and # comments ignored).</summary>
    public static bool TryParseVariables(string? text, out Dictionary<string, string> variables, out string? error)
    {
        variables = new Dictionary<string, string>(StringComparer.Ordinal);
        error = null;
        if (string.IsNullOrWhiteSpace(text)) return true;
        var lineNo = 0;
        foreach (var raw in text.Replace("\r\n", "\n").Split('\n'))
        {
            lineNo++;
            var line = raw.Trim();
            if (line.Length == 0 || line.StartsWith('#')) continue;
            var eq = line.IndexOf('=');
            var name = eq > 0 ? line[..eq].Trim() : "";
            if (name.Length == 0 || !name.All(c => char.IsLetterOrDigit(c) || c is '_' or '-' or '.'))
            {
                error = $"variables line {lineNo}: expected name=value, got '{line}'";
                return false;
            }
            variables[name] = line[(eq + 1)..].Trim();
        }
        return true;
    }

    private void RefreshConnectionNames()
    {
        CollectionSync.SyncSorted(ConnectionNames, _state.Connections.Keys);
    }

    private void Load()
    {
        _loading = true;
        try
        {
            var s = JsonFileStore.Load(SettingsFile, new Settings());
            foreach (var path in s.Paths.Where(p => !string.IsNullOrWhiteSpace(p))) Paths.Add(path);
            _includeEditor = s.IncludeEditor;
            _tagFilter = s.TagFilter;
            _nameFilter = s.NameFilter;
            _retries = Math.Clamp(s.Retries, 0, 10);
            _failFast = s.FailFast;
            _timeoutSeconds = (decimal)Math.Max(0, s.TimeoutSeconds);
            _variablesText = s.Variables;
            _defaultConnection = s.DefaultConnection;
        }
        finally
        {
            _loading = false;
        }
    }

    private void Save()
    {
        if (_loading) return;
        var error = JsonFileStore.TrySave(SettingsFile, new Settings
        {
            Paths = Paths.ToList(),
            IncludeEditor = IncludeEditor,
            TagFilter = TagFilter,
            NameFilter = NameFilter,
            Retries = (int)Retries,
            FailFast = FailFast,
            TimeoutSeconds = (double)TimeoutSeconds,
            Variables = VariablesText,
            DefaultConnection = DefaultConnection
        });
        if (error is not null) StatusMessage = error;
    }
}

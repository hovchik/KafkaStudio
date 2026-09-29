using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Scripting;
using KafkaStudio.Scripting.Ast;
using KafkaStudio.Scripting.Parsing;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.App.ViewModels.Scripts;

public sealed class StepResultRowViewModel
{
    public required string Keyword { get; init; }
    public required string Description { get; init; }
    public required StepStatus Status { get; init; }
    public required string Message { get; init; }
    public int Line { get; init; }
    public string BlockName { get; init; } = "";
    public TimeSpan Duration { get; init; }

    /// <summary>True for the per-block header row rather than a step.</summary>
    public bool IsBlockHeader { get; init; }

    public string DurationText => IsBlockHeader || Duration == TimeSpan.Zero ? "" :
        Duration.TotalSeconds >= 1 ? $"{Duration.TotalSeconds:0.0} s" : $"{Duration.TotalMilliseconds:0} ms";

    public string StatusGlyph => Status switch
    {
        StepStatus.Passed => "✓",
        StepStatus.Failed => "✗",
        StepStatus.Cancelled => "■",
        _ => "·"
    };
}

/// <summary>
/// The KafScript editor/runner screen: type a Scenario or Task, get live parse errors, and run it
/// against any connection. This is the primary surface for everything the user asked KafkaStudio to
/// do - rethrow checks, scan+acknowledge checks, and cross-topic timing checks are all just scripts
/// run from here (or scheduled headlessly via the Tasks screen). Scripts can be opened from and saved
/// to .kafscript files; a run streams its step results in live and can be stopped at any time.
/// </summary>
public sealed class ScriptEditorViewModel : ObservableObject
{
    private readonly AppState _state;
    private CancellationTokenSource? _runCts;

    public ObservableCollection<StepResultRowViewModel> StepResults { get; } = new();

    private string _source = DefaultSample;
    public string Source
    {
        get => _source;
        set
        {
            if (SetProperty(ref _source, value ?? ""))
            {
                IsDirty = true;
                Reparse();
            }
        }
    }

    private string? _parseError;
    public string? ParseError { get => _parseError; set => SetProperty(ref _parseError, value); }

    private int? _parseErrorLine;
    /// <summary>1-based line of the current parse error, for "go to error".</summary>
    public int? ParseErrorLine { get => _parseErrorLine; private set => SetProperty(ref _parseErrorLine, value); }

    private ScriptDocument? _document;
    public ScriptDocument? Document
    {
        get => _document;
        private set
        {
            if (SetProperty(ref _document, value)) OnPropertyChanged(nameof(DocumentSummary));
        }
    }

    /// <summary>E.g. "2 scenario(s), 1 task" - shown next to the editor title.</summary>
    public string DocumentSummary
    {
        get
        {
            if (Document is null) return "";
            var scenarios = Document.Blocks.Count(b => b.Kind == BlockKind.Scenario);
            var tasks = Document.Blocks.Count(b => b.Kind == BlockKind.Task);
            var steps = Document.Blocks.Sum(b => b.Steps.Count);
            return $"{scenarios} scenario(s), {tasks} task(s), {steps} step(s)";
        }
    }

    private string? _runSummary;
    public string? RunSummary { get => _runSummary; set => SetProperty(ref _runSummary, value); }

    private bool? _lastRunPassed;
    /// <summary>Null before the first run; drives the summary's colour.</summary>
    public bool? LastRunPassed { get => _lastRunPassed; private set => SetProperty(ref _lastRunPassed, value); }

    private bool _isRunning;
    /// <summary>True while <see cref="RunAllAsync"/> is executing the document's blocks.</summary>
    public bool IsRunning
    {
        get => _isRunning;
        private set
        {
            if (SetProperty(ref _isRunning, value))
            {
                RunAllCommand.RaiseCanExecuteChanged();
                StopCommand.RaiseCanExecuteChanged();
            }
        }
    }

    private bool _isHelpVisible;
    /// <summary>Toggles the in-app KafScript help/examples panel.</summary>
    public bool IsHelpVisible { get => _isHelpVisible; set => SetProperty(ref _isHelpVisible, value); }

    private string? _filePath;
    /// <summary>File the script was opened from / saved to, if any.</summary>
    public string? FilePath
    {
        get => _filePath;
        private set
        {
            if (SetProperty(ref _filePath, value)) OnPropertyChanged(nameof(Title));
        }
    }

    private bool _isDirty;
    public bool IsDirty
    {
        get => _isDirty;
        private set
        {
            if (SetProperty(ref _isDirty, value)) OnPropertyChanged(nameof(Title));
        }
    }

    /// <summary>Editor pane title: file name (or "untitled") plus a dirty marker.</summary>
    public string Title => (FilePath is null ? "untitled.kafscript" : Path.GetFileName(FilePath)) + (IsDirty ? " •" : "");

    /// <summary>Connection names usable in 'use connection "..."' - shown as a hint.</summary>
    public string ConnectionsHint => _state.Connections.IsEmpty
        ? "No connections yet - add one via Connections (top right)."
        : "Connections: " + string.Join(", ", _state.Connections.Keys.OrderBy(k => k, StringComparer.OrdinalIgnoreCase).Select(k => $"\"{k}\""));

    public ObservableCollection<HelpTopicViewModel> HelpTopics { get; } = KafScriptHelp.BuildTopics();

    public RelayCommand ReparseCommand { get; }
    public AsyncRelayCommand RunAllCommand { get; }
    public RelayCommand StopCommand { get; }
    public RelayCommand ToggleHelpCommand { get; }
    public RelayCommand<HelpTopicViewModel> InsertExampleCommand { get; }
    public RelayCommand NewCommand { get; }
    public AsyncRelayCommand OpenCommand { get; }
    public AsyncRelayCommand SaveCommand { get; }
    public AsyncRelayCommand SaveAsCommand { get; }
    public RelayCommand ClearResultsCommand { get; }

    public ScriptEditorViewModel(AppState state)
    {
        _state = state;
        _state.ConnectionsChanged += () => OnPropertyChanged(nameof(ConnectionsHint));
        ReparseCommand = new RelayCommand(Reparse);
        RunAllCommand = new AsyncRelayCommand(RunAllAsync, () => !IsRunning && Document is not null && ParseError is null);
        StopCommand = new RelayCommand(() => _runCts?.Cancel(), () => IsRunning);
        ToggleHelpCommand = new RelayCommand(() => IsHelpVisible = !IsHelpVisible);
        InsertExampleCommand = new RelayCommand<HelpTopicViewModel>(InsertExample);
        NewCommand = new RelayCommand(NewScript);
        OpenCommand = new AsyncRelayCommand(OpenAsync);
        SaveCommand = new AsyncRelayCommand(() => SaveAsync(saveAs: false));
        SaveAsCommand = new AsyncRelayCommand(() => SaveAsync(saveAs: true));
        ClearResultsCommand = new RelayCommand(() => { StepResults.Clear(); RunSummary = null; LastRunPassed = null; });
        Reparse();
        IsDirty = false;
    }

    private void InsertExample(HelpTopicViewModel? topic)
    {
        if (topic is null) return;
        var separator = string.IsNullOrEmpty(Source) || Source.EndsWith('\n') ? string.Empty : "\n";
        Source += separator + topic.Example + "\n";
    }

    /// <summary>
    /// Adds generated KafScript (e.g. from Find Data's "turn into a check") to the editor: it replaces the
    /// untouched starter sample, and is appended after a blank line otherwise, so nothing typed is lost.
    /// </summary>
    public void AppendScript(string script)
    {
        if (!IsDirty && FilePath is null && Source == DefaultSample)
        {
            Source = script;
            return;
        }
        var separator = string.IsNullOrEmpty(Source) ? "" : Source.EndsWith('\n') ? "\n" : "\n\n";
        Source += separator + script;
    }

    private void NewScript()
    {
        Source = "Scenario: My check\nGiven use connection \"local\"\n";
        FilePath = null;
        IsDirty = false;
        StepResults.Clear();
        RunSummary = null;
        LastRunPassed = null;
    }

    private async Task OpenAsync()
    {
        if (_state.FileDialogs is null) return;
        var path = await _state.FileDialogs.PickOpenFileAsync("Open KafScript", ".kafscript", "KafScript").ConfigureAwait(true);
        if (path is null) return;
        try
        {
            var text = await File.ReadAllTextAsync(path).ConfigureAwait(true);
            Source = text;
            FilePath = path;
            IsDirty = false;
            RunSummary = $"Opened {Path.GetFileName(path)}.";
        }
        catch (Exception ex)
        {
            RunSummary = $"Could not open file: {ex.Message}";
        }
    }

    private async Task SaveAsync(bool saveAs)
    {
        var path = FilePath;
        if (saveAs || path is null)
        {
            if (_state.FileDialogs is null) return;
            var suggested = Document?.Blocks.FirstOrDefault()?.Name is { Length: > 0 } name
                ? MessageActions.SafeFileName(name) + ".kafscript"
                : "script.kafscript";
            path = await _state.FileDialogs.PickSaveFileAsync("Save KafScript", ".kafscript", "KafScript", suggested).ConfigureAwait(true);
            if (path is null) return;
        }

        try
        {
            await File.WriteAllTextAsync(path, Source).ConfigureAwait(true);
            FilePath = path;
            IsDirty = false;
            RunSummary = $"Saved {Path.GetFileName(path)}.";
        }
        catch (Exception ex)
        {
            RunSummary = $"Could not save file: {ex.Message}";
        }
    }

    private void Reparse()
    {
        try
        {
            Document = Parser.Parse(Source);
            ParseError = null;
            ParseErrorLine = null;
        }
        catch (KafScriptException ex)
        {
            Document = null;
            ParseError = ex.Message;
            ParseErrorLine = ex.Line;
        }
        catch (Exception ex)
        {
            // The parser should only ever throw KafScriptException, but this runs on every keystroke:
            // an unexpected exception here must never take the editor (or the app) down.
            Document = null;
            ParseError = $"internal parser error: {ex.Message}";
            ParseErrorLine = null;
        }
        RunAllCommand?.RaiseCanExecuteChanged();
    }

    private async Task RunAllAsync()
    {
        if (Document is null) return;

        var document = Document;
        var sourceLines = Source.Replace("\r\n", "\n").Split('\n');
        var cts = new CancellationTokenSource();
        _runCts = cts;

        StepResults.Clear();
        RunSummary = "Running...";
        LastRunPassed = null;
        IsRunning = true;
        var passed = 0;
        var failed = 0;
        var cancelled = false;
        try
        {
            // Snapshot: connections added/removed mid-run don't affect this run.
            var connections = new Dictionary<string, Core.Abstractions.IKafkaGateway>(_state.Connections);

            foreach (var block in document.Blocks)
            {
                if (cts.IsCancellationRequested) break;

                var header = new StepResultRowViewModel
                {
                    Keyword = block.Kind.ToString(),
                    Description = block.Name,
                    Status = StepStatus.Skipped,
                    Message = "",
                    BlockName = block.Name,
                    Line = block.Line,
                    IsBlockHeader = true
                };
                StepResults.Add(header);

                // Rows stream in live from the runner's thread. Each block owns its rows (inserted right
                // after its own header), and once the block finishes they're replaced with the
                // authoritative result - so a late-arriving UI post can never land under the wrong
                // block or go missing.
                var blockRows = new List<StepResultRowViewModel>();
                var blockDone = false;
                var runner = new ScriptRunner(connections);
                runner.StepCompleted += step => _state.PostToUi(() =>
                {
                    if (blockDone) return;
                    var row = ToRow(step, sourceLines);
                    var at = StepResults.IndexOf(header) + 1 + blockRows.Count;
                    if (at <= 0) return;
                    StepResults.Insert(Math.Min(at, StepResults.Count), row);
                    blockRows.Add(row);
                });

                var result = await Task.Run(() => runner.RunAsync(block, cts.Token)).ConfigureAwait(true);
                blockDone = true;
                foreach (var row in blockRows) StepResults.Remove(row);
                var insertAt = StepResults.IndexOf(header) + 1;
                foreach (var step in result.Steps) StepResults.Insert(insertAt++, ToRow(step, sourceLines));

                _state.RunHistory.Add(block.Name, DateTimeOffset.Now, result);

                if (result.Cancelled) cancelled = true;
                else if (result.Success) passed++;
                else failed++;
            }

            RunSummary = cancelled
                ? $"Stopped. {passed} passed, {failed} failed before stopping."
                : $"{passed} scenario(s)/task(s) passed, {failed} failed.";
            LastRunPassed = !cancelled && failed == 0;
        }
        finally
        {
            IsRunning = false;
            _runCts = null;
            cts.Dispose();
        }
    }

    private static StepResultRowViewModel ToRow(StepResult step, string[] sourceLines) => new()
    {
        Keyword = step.Step.Keyword.ToString(),
        Description = DescribeStep(step.Step, sourceLines),
        Status = step.Status,
        Message = step.Message,
        Line = step.Step.Line,
        Duration = step.Duration
    };

    /// <summary>The step's own source text (minus its Given/When/Then keyword) - far more useful than
    /// the AST node's type name.</summary>
    private static string DescribeStep(Step step, string[] sourceLines)
    {
        var index = step.Line - 1;
        if (index < 0 || index >= sourceLines.Length) return step.Action.GetType().Name.Replace("Action", "");
        var text = sourceLines[index].Trim();
        var space = text.IndexOf(' ');
        if (space > 0) text = text[(space + 1)..].Trim();
        return text.Length > 160 ? text[..160] + "…" : text;
    }

    private const string DefaultSample = """
        Scenario: Order confirmation triggers shipment notice
        Given use connection "local"
        Given watch topic "shipment-notices" from now
        When produce message to topic "orders" key "{{$uuid}}" value "{ \"status\": \"CONFIRMED\" }"
        Then expect message on topic "shipment-notices" within 30 seconds where json "$.status" equals "NOTIFIED"
        """;
}

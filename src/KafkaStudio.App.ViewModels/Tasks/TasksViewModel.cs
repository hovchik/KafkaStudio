using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Automation.Scheduling;
using KafkaStudio.Core.Persistence;
using KafkaStudio.Scripting;
using KafkaStudio.Scripting.Ast;
using KafkaStudio.Scripting.Parsing;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.App.ViewModels.Tasks;

public sealed class TaskRowViewModel : ObservableObject
{
    public required string Id { get; init; }
    public required ScheduledJob Job { get; init; }

    /// <summary>Source of the single block this row runs (what gets persisted).</summary>
    public required string Source { get; init; }

    public string Name => Job.Block.Name;
    public string Schedule => Job.Block.Schedule is { } s
        ? s.Kind switch
        {
            ScheduleKind.RunOnce => "run once",
            ScheduleKind.Every => $"every {s.Every}",
            ScheduleKind.At => $"daily at {s.At:HH:mm}",
            _ => "-"
        }
        : "manual only";

    public int StepCount => Job.Block.Steps.Count;

    private string _lastResult = "never run";
    public string LastResult { get => _lastResult; set => SetProperty(ref _lastResult, value); }

    private bool? _lastPassed;
    public bool? LastPassed { get => _lastPassed; set => SetProperty(ref _lastPassed, value); }

    private int _runCount;
    public int RunCount { get => _runCount; set => SetProperty(ref _runCount, value); }

    private int _failCount;
    public int FailCount { get => _failCount; set => SetProperty(ref _failCount, value); }

    private bool _isRunning;
    public bool IsRunning { get => _isRunning; set => SetProperty(ref _isRunning, value); }

    private bool _isEnabled = true;
    public bool IsEnabled { get => _isEnabled; set => SetProperty(ref _isEnabled, value); }

    private string _nextRun = "";
    public string NextRun { get => _nextRun; set => SetProperty(ref _nextRun, value); }

    private string _lastRun = "";
    public string LastRun { get => _lastRun; set => SetProperty(ref _lastRun, value); }

    public void RefreshTimes()
    {
        NextRun = "manual only";
        LastRun = Job.LastRunAt is { } last ? $"last {Format(last)}" : "";
    }

    private static string Format(DateTimeOffset when)
    {
        var local = when.ToLocalTime();
        return local.Date == DateTime.Today ? local.ToString("HH:mm:ss") : local.ToString("MMM d HH:mm");
    }
}

/// <summary>One finished run in the run-history list.</summary>
public sealed class RunHistoryRow
{
    public required string When { get; init; }
    public required string Name { get; init; }
    public required bool Passed { get; init; }
    public required string Detail { get; init; }
}

/// <summary>Register KafScript Task blocks with the <see cref="AutomationScheduler"/> and watch them
/// run - the "automation" half of the app, as distinct from the Script Editor's on-demand runs.
/// Registered tasks (and whether they're paused) are saved and re-registered on the next start.</summary>
public sealed class TasksViewModel : ObservableObject
{
    private const string StoreFile = "tasks.json";
    private const int MaxHistoryRows = 200;

    private sealed record PersistedTask(string Source, bool Enabled);

    private readonly AppState _state;

    public ObservableCollection<TaskRowViewModel> Jobs { get; } = new();
    public ObservableCollection<RunHistoryRow> History { get; } = new();

    private string _newTaskSource = DefaultSample;
    public string NewTaskSource
    {
        get => _newTaskSource;
        set
        {
            if (SetProperty(ref _newTaskSource, value ?? "")) ValidateSource();
        }
    }

    private string? _parseError;
    /// <summary>Live parse error for the editor, so problems show before pressing Register.</summary>
    public string? ParseError { get => _parseError; private set => SetProperty(ref _parseError, value); }

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    private bool _isHelpVisible;
    /// <summary>Toggles the in-app KafScript help/examples panel.</summary>
    public bool IsHelpVisible { get => _isHelpVisible; set => SetProperty(ref _isHelpVisible, value); }

    public ObservableCollection<HelpTopicViewModel> HelpTopics { get; } = KafScriptHelp.BuildTopics();

    public RelayCommand RegisterTaskCommand { get; }
    public AsyncRelayCommand<TaskRowViewModel> RunNowCommand { get; }
    public RelayCommand<TaskRowViewModel> RemoveCommand { get; }
    public RelayCommand<TaskRowViewModel> ToggleEnabledCommand { get; }
    public RelayCommand<TaskRowViewModel> EditCommand { get; }
    public RelayCommand ToggleHelpCommand { get; }
    public RelayCommand ClearHistoryCommand { get; }
    public RelayCommand<HelpTopicViewModel> InsertExampleCommand { get; }

    private readonly Timer _clockTimer;

    public TasksViewModel(AppState state)
    {
        _state = state;
        _state.Scheduler.RunStarted += job => _state.PostToUi(() => OnRunStarted(job));
        _state.Scheduler.RunCompleted += (job, result) => _state.PostToUi(() => OnRunCompleted(job, result));
        _state.Scheduler.RunFailed += (job, ex) => _state.PostToUi(() => OnRunFailed(job, ex));
        // The scheduler's timer loop is intentionally never started: the app must not produce (or run
        // anything that produces) on its own. Tasks run only when the user presses "Run now".

        RegisterTaskCommand = new RelayCommand(RegisterTask);
        RunNowCommand = new AsyncRelayCommand<TaskRowViewModel>(RunNowAsync, row => row is { IsRunning: false }, allowConcurrentExecutions: true);
        RemoveCommand = new RelayCommand<TaskRowViewModel>(Remove);
        ToggleEnabledCommand = new RelayCommand<TaskRowViewModel>(ToggleEnabled);
        EditCommand = new RelayCommand<TaskRowViewModel>(row => { if (row is not null) NewTaskSource = row.Source; });
        ToggleHelpCommand = new RelayCommand(() => IsHelpVisible = !IsHelpVisible);
        ClearHistoryCommand = new RelayCommand(History.Clear);
        InsertExampleCommand = new RelayCommand<HelpTopicViewModel>(InsertExample);

        LoadPersistedTasks();
        ValidateSource();

        // "next 10:32:05" labels go stale as time passes; refresh them every few seconds.
        _clockTimer = new Timer(_ => _state.PostToUi(() => { foreach (var row in Jobs) row.RefreshTimes(); }),
            null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
    }

    private void InsertExample(HelpTopicViewModel? topic)
    {
        if (topic is null) return;
        var separator = string.IsNullOrEmpty(NewTaskSource) || NewTaskSource.EndsWith('\n') ? string.Empty : "\n";
        NewTaskSource += separator + topic.Example + "\n";
    }

    private void ValidateSource()
    {
        try
        {
            Parser.Parse(NewTaskSource);
            ParseError = null;
        }
        catch (KafScriptException ex)
        {
            ParseError = ex.Message;
        }
        catch (Exception ex)
        {
            ParseError = $"internal parser error: {ex.Message}";
        }
    }

    /// <summary>Splits a document's source into one source string per block, so each registered job
    /// can be persisted and edited on its own.</summary>
    private static List<(ScriptBlock Block, string Source)> SplitBlocks(string source, ScriptDocument document)
    {
        var lines = source.Replace("\r\n", "\n").Split('\n');
        var result = new List<(ScriptBlock, string)>();
        for (var i = 0; i < document.Blocks.Count; i++)
        {
            var start = document.Blocks[i].Line - 1;
            var end = i + 1 < document.Blocks.Count ? document.Blocks[i + 1].Line - 1 : lines.Length;
            result.Add((document.Blocks[i], string.Join("\n", lines[start..end]).TrimEnd() + "\n"));
        }
        return result;
    }

    private void RegisterTask()
    {
        ScriptDocument document;
        try
        {
            document = Parser.Parse(NewTaskSource);
        }
        catch (KafScriptException ex)
        {
            StatusMessage = $"Could not parse: {ex.Message}";
            return;
        }

        if (document.Blocks.Count == 0)
        {
            StatusMessage = "Nothing to register - write a 'Task: name' block first.";
            return;
        }

        var added = 0;
        foreach (var (block, source) in SplitBlocks(NewTaskSource, document))
        {
            // Re-registering a task with the same name replaces it (the usual "edit and register again").
            var existing = Jobs.FirstOrDefault(j => j.Name == block.Name);
            if (existing is not null) Remove(existing, persist: false);
            AddJob(block, source, enabled: true);
            added++;
        }

        Persist();
        var unscheduled = document.Blocks.Count(b => b.Schedule is null);
        StatusMessage = $"Registered {added} block(s)." +
                        (unscheduled > 0 ? $" {unscheduled} without a schedule - they only run via Run now." : "");
    }

    private TaskRowViewModel AddJob(ScriptBlock block, string source, bool enabled)
    {
        var id = $"{block.Name}-{Guid.NewGuid():N}";
        // The live connections dictionary: a job picks up connections added after registration.
        var job = _state.Scheduler.Register(id, block, _state.Connections);
        var row = new TaskRowViewModel { Id = id, Job = job, Source = source, IsEnabled = enabled };
        if (!enabled) _state.Scheduler.SetEnabled(id, false);
        row.RefreshTimes();
        Jobs.Add(row);
        return row;
    }

    private void LoadPersistedTasks()
    {
        foreach (var task in JsonFileStore.Load(StoreFile, new List<PersistedTask>()))
        {
            try
            {
                var document = Parser.Parse(task.Source);
                foreach (var block in document.Blocks)
                {
                    // Don't re-fire "run once" tasks on every app start - they already ran.
                    var enabled = task.Enabled && block.Schedule?.Kind != ScheduleKind.RunOnce;
                    AddJob(block, task.Source, enabled);
                }
            }
            catch (Exception)
            {
                // A saved task that no longer parses (e.g. language change) is skipped, not fatal.
            }
        }
    }

    private void Persist()
    {
        var error = JsonFileStore.TrySave(StoreFile, Jobs.Select(j => new PersistedTask(j.Source, j.IsEnabled)).ToList());
        if (error is not null) StatusMessage = error;
    }

    private async Task RunNowAsync(TaskRowViewModel? row)
    {
        if (row is null) return;
        try
        {
            await _state.Scheduler.RunNowAsync(row.Id).ConfigureAwait(true);
        }
        catch (Exception ex) when (ex is InvalidOperationException or KeyNotFoundException)
        {
            StatusMessage = ex.Message;
        }
    }

    private void ToggleEnabled(TaskRowViewModel? row)
    {
        if (row is null) return;
        row.IsEnabled = !row.IsEnabled;
        _state.Scheduler.SetEnabled(row.Id, row.IsEnabled);
        row.RefreshTimes();
        Persist();
    }

    private void Remove(TaskRowViewModel? row) => Remove(row, persist: true);

    private void Remove(TaskRowViewModel? row, bool persist)
    {
        if (row is null) return;
        _state.Scheduler.Unregister(row.Id);
        Jobs.Remove(row);
        if (persist) Persist();
    }

    private void OnRunStarted(ScheduledJob job)
    {
        var row = Jobs.FirstOrDefault(r => r.Job == job);
        if (row is null) return;
        row.IsRunning = true;
        row.LastResult = "running...";
        row.RefreshTimes();
        RunNowCommand.RaiseCanExecuteChanged();
    }

    private void OnRunCompleted(ScheduledJob job, ScriptRunResult result)
    {
        _state.RunHistory.Add(job.Id, DateTimeOffset.Now, result);
        AddHistory(job.Block.Name, result.Success, result.Success
            ? $"passed in {result.Duration.TotalMilliseconds:0} ms"
            : result.Cancelled ? "cancelled" : result.FailureMessage ?? "failed");

        var row = Jobs.FirstOrDefault(r => r.Job == job);
        if (row is null) return;
        row.IsRunning = false;
        row.LastPassed = result.Success;
        row.LastResult = result.Success
            ? $"passed ({result.Duration.TotalMilliseconds:0} ms)"
            : result.Cancelled ? "cancelled" : $"FAILED: {result.FailureMessage}";
        row.RunCount = job.RunCount;
        if (!result.Success && !result.Cancelled) row.FailCount++;
        row.RefreshTimes();
        RunNowCommand.RaiseCanExecuteChanged();
    }

    private void OnRunFailed(ScheduledJob job, Exception ex)
    {
        AddHistory(job.Block.Name, false, $"error: {ex.Message}");
        var row = Jobs.FirstOrDefault(r => r.Job == job);
        if (row is null) return;
        row.IsRunning = false;
        row.LastPassed = false;
        row.LastResult = $"ERROR: {ex.Message}";
        row.RunCount = job.RunCount;
        row.FailCount++;
        row.RefreshTimes();
        RunNowCommand.RaiseCanExecuteChanged();
    }

    private void AddHistory(string name, bool passed, string detail)
    {
        History.Insert(0, new RunHistoryRow { When = DateTime.Now.ToString("HH:mm:ss"), Name = name, Passed = passed, Detail = detail });
        while (History.Count > MaxHistoryRows) History.RemoveAt(History.Count - 1);
    }

    private const string DefaultSample = """
        Task: Example task
        Given use connection "local"
        When produce message to topic "example" key "{{$uuid}}" value "hello at {{$now}}"
        """;
}

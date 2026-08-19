using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Automation.Scheduling;
using KafkaStudio.Scripting;
using KafkaStudio.Scripting.Ast;
using KafkaStudio.Scripting.Parsing;

namespace KafkaStudio.App.ViewModels.Tasks;

public sealed class TaskRowViewModel : ObservableObject
{
    public required string Id { get; init; }
    public required ScheduledJob Job { get; init; }

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

    private string _lastResult = "never run";
    public string LastResult { get => _lastResult; set => SetProperty(ref _lastResult, value); }

    private int _runCount;
    public int RunCount { get => _runCount; set => SetProperty(ref _runCount, value); }
}

/// <summary>Register KafScript Task blocks with the <see cref="AutomationScheduler"/> and watch them
/// run - the "automation" half of the app, as distinct from the Script Editor's on-demand runs.</summary>
public sealed class TasksViewModel : ObservableObject
{
    private readonly AppState _state;

    public ObservableCollection<TaskRowViewModel> Jobs { get; } = new();

    private string _newTaskSource = DefaultSample;
    public string NewTaskSource { get => _newTaskSource; set => SetProperty(ref _newTaskSource, value); }

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    public RelayCommand RegisterTaskCommand { get; }
    public AsyncRelayCommand<TaskRowViewModel> RunNowCommand { get; }
    public RelayCommand<TaskRowViewModel> RemoveCommand { get; }

    public TasksViewModel(AppState state)
    {
        _state = state;
        _state.Scheduler.RunCompleted += OnRunCompleted;
        _state.Scheduler.RunFailed += OnRunFailed;
        _state.Scheduler.Start();

        RegisterTaskCommand = new RelayCommand(RegisterTask);
        RunNowCommand = new AsyncRelayCommand<TaskRowViewModel>(RunNowAsync);
        RemoveCommand = new RelayCommand<TaskRowViewModel>(Remove);
    }

    private void RegisterTask()
    {
        try
        {
            var document = Parser.Parse(NewTaskSource);
            foreach (var block in document.Blocks)
            {
                var id = $"{block.Name}-{Guid.NewGuid():N}";
                var job = _state.Scheduler.Register(id, block, _state.Connections);
                Jobs.Add(new TaskRowViewModel { Id = id, Job = job });
            }
            StatusMessage = $"Registered {document.Blocks.Count} block(s).";
        }
        catch (KafScriptException ex)
        {
            StatusMessage = $"Could not parse: {ex.Message}";
        }
    }

    private async Task RunNowAsync(TaskRowViewModel? row)
    {
        if (row is null) return;
        await _state.Scheduler.RunNowAsync(row.Id).ConfigureAwait(true);
    }

    private void Remove(TaskRowViewModel? row)
    {
        if (row is null) return;
        _state.Scheduler.Unregister(row.Id);
        Jobs.Remove(row);
    }

    private void OnRunCompleted(ScheduledJob job, KafkaStudio.Scripting.Runtime.ScriptRunResult result)
    {
        var row = Jobs.FirstOrDefault(r => r.Job == job);
        if (row is null) return;
        row.LastResult = result.Success ? "passed" : $"FAILED: {result.Steps.LastOrDefault(s => s.Status == KafkaStudio.Scripting.Runtime.StepStatus.Failed)?.Message}";
        row.RunCount = job.RunCount;
        _state.RunHistory.Add(job.Id, DateTimeOffset.Now, result);
    }

    private void OnRunFailed(ScheduledJob job, Exception ex)
    {
        var row = Jobs.FirstOrDefault(r => r.Job == job);
        if (row is null) return;
        row.LastResult = $"ERROR: {ex.Message}";
        row.RunCount = job.RunCount;
    }

    private const string DefaultSample = """
        Task: Heartbeat producer
        schedule every 5 minutes
        Given use connection "local"
        When produce message to topic "heartbeats" value "ping"
        """;
}

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
}

/// <summary>
/// The KafScript editor/runner screen: type a Scenario or Task, get live parse errors, and run it
/// against any connection. This is the primary surface for everything the user asked KafkaStudio to
/// do - rethrow checks, scan+acknowledge checks, and cross-topic timing checks are all just scripts
/// run from here (or scheduled headlessly via the Tasks screen).
/// </summary>
public sealed class ScriptEditorViewModel : ObservableObject
{
    private readonly AppState _state;

    public ObservableCollection<StepResultRowViewModel> StepResults { get; } = new();

    private string _source = DefaultSample;
    public string Source
    {
        get => _source;
        set
        {
            if (SetProperty(ref _source, value)) ReparseCommand.Execute(null);
        }
    }

    private string? _parseError;
    public string? ParseError { get => _parseError; set => SetProperty(ref _parseError, value); }

    private ScriptDocument? _document;
    public ScriptDocument? Document { get => _document; private set => SetProperty(ref _document, value); }

    private string? _runSummary;
    public string? RunSummary { get => _runSummary; set => SetProperty(ref _runSummary, value); }

    public RelayCommand ReparseCommand { get; }
    public AsyncRelayCommand RunAllCommand { get; }

    public ScriptEditorViewModel(AppState state)
    {
        _state = state;
        ReparseCommand = new RelayCommand(Reparse);
        RunAllCommand = new AsyncRelayCommand(RunAllAsync, () => Document is not null && ParseError is null);
        Reparse();
    }

    private void Reparse()
    {
        try
        {
            Document = Parser.Parse(Source);
            ParseError = null;
        }
        catch (KafScriptException ex)
        {
            Document = null;
            ParseError = ex.Message;
        }
        RunAllCommand.RaiseCanExecuteChanged();
    }

    private async Task RunAllAsync()
    {
        if (Document is null) return;

        StepResults.Clear();
        RunSummary = "Running...";

        var runner = new ScriptRunner(_state.Connections);
        var passed = 0;
        var failed = 0;

        foreach (var block in Document.Blocks)
        {
            var result = await runner.RunAsync(block).ConfigureAwait(true);
            _state.RunHistory.Add(block.Name, DateTimeOffset.Now, result);

            foreach (var step in result.Steps)
            {
                StepResults.Add(new StepResultRowViewModel
                {
                    Keyword = step.Step.Keyword.ToString(),
                    Description = DescribeAction(step.Step),
                    Status = step.Status,
                    Message = step.Message
                });
            }

            if (result.Success) passed++; else failed++;
        }

        RunSummary = $"{passed} scenario(s)/task(s) passed, {failed} failed.";
    }

    private static string DescribeAction(Step step) => step.Action.GetType().Name;

    private const string DefaultSample = """
        Scenario: Order confirmation triggers shipment notice
        Given use connection "local"
        Given watch topic "shipment-notices" from now
        When produce message to topic "orders" value "{ \"status\": \"CONFIRMED\" }"
        Then expect message on topic "shipment-notices" within 30 seconds where json "$.status" equals "NOTIFIED"
        """;
}

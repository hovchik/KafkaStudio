using KafkaStudio.App.ViewModels;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.App.ViewModels.Testing;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

/// <summary>The QA Lab screen (Test Runner and Contract check tabs) against the demo broker.</summary>
public static class QaLabViewModelTests
{
    private static async Task WaitUntil(Func<bool> condition, int timeoutMs = 5000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new AssertionFailedException("timed out waiting for condition");
            await Task.Delay(10);
        }
    }

    public static void Register(TestRunner runner)
    {
        runner.Add("ViewModels: QA Lab", "Test Runner discovers files + editor, filters by tag, runs and reports", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var dir = Path.Combine(Path.GetTempPath(), $"ks-vm-{Guid.NewGuid():N}");
            Directory.CreateDirectory(dir);
            try
            {
                await File.WriteAllTextAsync(Path.Combine(dir, "pack.kafscript"), """
                    @smoke
                    Scenario: File test passes
                    When produce message to topic "t" value "{{env}}"
                    Then assert last message where value equals "qa"

                    @slow
                    Scenario: File test is filtered out
                    Given wait for 1 hour
                    """);

                var editor = "@smoke\nScenario: Editor test fails\nGiven set variable x to \"1\"\nThen assert x equals \"2\"";
                var vm = new TestRunnerViewModel(state, () => editor, () => null) { VariablesText = "env=qa\n# comment" };
                vm.AddPath(dir);
                Assert.Equal(3, vm.Tests.Count);

                vm.TagFilter = "@smoke and";
                Assert.NotNull(vm.TagFilterError);
                Assert.False(vm.RunAllCommand.CanExecute(null), "a broken tag filter blocks running");
                vm.TagFilter = "@smoke";
                Assert.Null(vm.TagFilterError);
                Assert.Equal(2, vm.Tests.Count);
                Assert.Contains("2 of 3 test(s) selected", vm.DiscoverySummary);

                await vm.RunAllCommand.ExecuteAsync();
                Assert.Equal(1, vm.PassedCount);
                Assert.Equal(1, vm.FailedCount);
                Assert.False(vm.LastRunPassed!.Value);
                var failed = vm.Tests.Single(t => t.State == TestItemState.Failed);
                Assert.Equal("Editor test fails", failed.Name);
                Assert.Contains("expected equals \"2\"", failed.Message!);
                Assert.True(failed.Steps.Count == 2 && failed.Steps[1].Description.StartsWith("assert x", StringComparison.Ordinal),
                    "step rows show the step's own source text");
                Assert.True(vm.ExportJUnitCommand.CanExecute(null));

                // Re-running only the failed test leaves the passing one's result in place.
                editor = editor.Replace("\"2\"", "\"1\"");
                vm.Refresh();
                Assert.Equal(TestItemState.Passed, vm.Tests.Single(t => t.Name == "File test passes").State, "results survive a refresh");
                await vm.RunFailedCommand.ExecuteAsync();
                Assert.True(vm.Tests.All(t => t.State == TestItemState.Passed));
                Assert.True(vm.LastRunPassed!.Value);

                string? copied = null;
                state.SetClipboardText = text => { copied = text; return Task.CompletedTask; };
                await vm.CopySummaryCommand.ExecuteAsync();
                Assert.Contains("✅ PASSED", copied!);

                // Settings persist across sessions.
                var reloaded = new TestRunnerViewModel(state, () => null, () => null);
                Assert.Equal(dir, reloaded.Paths.Single());
                Assert.Equal("@smoke", reloaded.TagFilter);
                Assert.Equal("env=qa\n# comment", reloaded.VariablesText);

                vm.VariablesText = "not a pair";
                Assert.Contains("expected name=value", vm.VariablesError!);
                vm.VariablesText = "";
            }
            finally
            {
                Directory.Delete(dir, recursive: true);
            }
        });

        runner.Add("ViewModels: QA Lab", "same-named scenarios and identical example rows are separate tests", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            File.Delete(KafkaStudio.Core.Persistence.JsonFileStore.PathFor("test-runner.json")); // no settings from earlier tests
            var source = "Scenario: Twin\nGiven log \"a\"\n\nScenario: Twin\nGiven log \"b\"\n\n" +
                         "Scenario Outline: Row <v>\nGiven log \"<v>\"\nExamples:\n| v |\n| 1 |\n| 1 |";
            var vm = new TestRunnerViewModel(state, () => source, () => null);
            Assert.Equal(4, vm.Tests.Count);
            await vm.RunAllCommand.ExecuteAsync();
            Assert.Equal(4, vm.PassedCount);
            Assert.True(vm.Tests.All(t => t.State == TestItemState.Passed));
        });

        runner.Add("ViewModels: QA Lab", "a failed test's bug report and the main window's QA Lab entry", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var main = new MainWindowViewModel(state);
            main.Scripts.Source = "@api\nScenario: Payment is captured\nGiven use connection \"local\"\nWhen produce message to topic \"p\" value \"{ \\\"state\\\": \\\"AUTHORIZED\\\" }\"\nThen assert last message where json \"$.state\" equals \"CAPTURED\"";
            main.NavigateCommand.Execute("qa");
            Assert.Equal("qa", main.SelectedItem.Key);

            var tests = main.QaLab.Tests;
            tests.TagFilter = "";
            var item = tests.Tests.Single(t => t.Name == "Payment is captured");
            tests.SelectedTest = item;
            await tests.RunSelectedCommand.ExecuteAsync();
            Assert.Equal(TestItemState.Failed, item.State);

            string? copied = null;
            state.SetClipboardText = text => { copied = text; return Task.CompletedTask; };
            Assert.True(tests.CopyBugReportCommand.CanExecute(null));
            await tests.CopyBugReportCommand.ExecuteAsync();
            Assert.Contains("## ❌ Payment is captured", copied!);
            Assert.Contains("expected json \"$.state\" equals \"CAPTURED\", but it was \"AUTHORIZED\"", copied!);
            Assert.Contains("- connections: local", copied!);
            await main.DisposeAsync();
        });

        runner.Add("ViewModels: QA Lab", "Contract check validates a topic, groups problems, infers a schema and makes a script", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var gateway = state.Connections["local"];
            await gateway.ProduceAsync(new ProduceRequest { Topic = "orders", Key = "1", Value = """{ "id": "ORD-1", "amount": 10 }""" });
            await gateway.ProduceAsync(new ProduceRequest { Topic = "orders", Key = "2", Value = """{ "id": "ORD-2" }""" });
            await gateway.ProduceAsync(new ProduceRequest { Topic = "orders", Key = "3", Value = """{ "amount": "ten" }""" });
            await gateway.ProduceAsync(new ProduceRequest { Topic = "orders", Key = "4", Value = """{ "id": "ORD-4", "amount": 5, "items": [ {"q": 1}, {"q": "x"} ] }""" });

            var vm = new ContractCheckViewModel(state);
            Assert.Equal("local", vm.SelectedConnection);
            await WaitUntil(() => vm.TopicNames.Contains("orders"));
            vm.Topic = "orders";

            vm.SchemaText = """{ "type": "objekt" }""";
            Assert.Contains("unknown type 'objekt'", vm.SchemaError!);
            Assert.False(vm.ValidateCommand.CanExecute(null));

            vm.SchemaText = """
                { "type": "object", "required": ["id", "amount"],
                  "properties": { "amount": { "type": "number" }, "items": { "type": "array", "items": { "properties": { "q": { "type": "integer" } } } } } }
                """;
            Assert.Null(vm.SchemaError);
            await vm.ValidateCommand.ExecuteAsync();
            Assert.Equal(4, vm.CheckedCount);
            Assert.Equal(1, vm.ValidCount);
            Assert.Equal(3, vm.InvalidCount);
            Assert.False(vm.LastPassed!.Value);
            Assert.Equal(3, vm.Violations.Count);
            Assert.True(vm.ProblemGroups.Any(g => g.Path == "$.items[*].q"), string.Join(" | ", vm.ProblemGroups.Select(g => g.Path)));
            Assert.Equal("$.amount", vm.ProblemGroups[0].Path, "the most frequent problem comes first");
            Assert.NotNull(vm.SelectedMessage);

            await vm.InferCommand.ExecuteAsync();
            Assert.Contains("Inferred from 4 message(s)", vm.StatusMessage!);
            Assert.Contains("\"required\": [", vm.SchemaText);

            string? script = null;
            state.OpenScriptRequested += s => script = s;
            vm.ToScriptCommand.Execute(null);
            var cases = KafkaStudio.Automation.Testing.TestDiscovery.FromSource(script!);
            Assert.Null(cases.Single().LoadError, "the generated check parses");
            Assert.Contains("validate each scanned message against schema", script!);
            Assert.Equal("contract", cases.Single().Tags.Single());

            // The generated check runs: the schema was inferred from these very messages, so they all pass.
            var report = await new KafkaStudio.Automation.Testing.TestSuiteRunner(state.Connections).RunAsync(cases, new());
            Assert.Equal(KafkaStudio.Automation.Testing.TestOutcome.Passed, report.Results[0].Outcome, report.Results[0].Message);
            Assert.Contains("all 4 scanned message(s) match the schema", report.Results[0].Steps.Last().Message);
        });

        runner.Add("ViewModels: QA Lab", "every KafScript help example parses", () =>
        {
            foreach (var topic in KafScriptHelp.BuildTopics())
            {
                var example = topic.Example.TrimStart();
                var isBlock = example.StartsWith('@') || example.StartsWith("Scenario", StringComparison.Ordinal) ||
                              example.StartsWith("Task", StringComparison.Ordinal) || example.StartsWith("Feature", StringComparison.Ordinal);
                var source = isBlock ? example : "Scenario: help example\n" + example;
                try
                {
                    KafkaStudio.Scripting.Parsing.Parser.Parse(source);
                }
                catch (KafkaStudio.Scripting.KafScriptException ex)
                {
                    throw new AssertionFailedException($"help example '{topic.Title}' doesn't parse: {ex.Message}");
                }
            }
            return Task.CompletedTask;
        });

        runner.Add("ViewModels: QA Lab", "help & examples: how-tos per tab, valid tag filters and schema via 'Try it'", () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var vm = new QaLabViewModel(state, () => null, () => null);
            var topics = QaLabHelp.BuildTopics();
            Assert.True(topics.Any(t => t.TabIndex == QaLabViewModel.TestsTab));
            Assert.True(topics.Any(t => t.TabIndex == QaLabViewModel.ContractsTab));

            vm.Help.ToggleCommand.Execute(null);
            Assert.True(vm.Help.IsVisible);
            Assert.False(vm.Help.Topics.Any(t => t.TabIndex == QaLabViewModel.ContractsTab), "Test Runner tab lists only its how-tos");

            foreach (var topic in topics.Where(t => t.HasTry))
            {
                vm.Help.TryCommand.Execute(topic);
                Assert.Equal(topic.TabIndex, vm.SelectedTabIndex);
                if (topic.TabIndex == QaLabViewModel.TestsTab)
                    Assert.Null(vm.Tests.TagFilterError, $"tag filter of '{topic.Title}' is invalid: {vm.Tests.TagFilterError}");
                else
                    Assert.Null(vm.Contracts.SchemaError, $"schema of '{topic.Title}' is invalid: {vm.Contracts.SchemaError}");
            }
            Assert.Equal(QaLabHelp.SampleSchema, vm.Contracts.SchemaText);
            Assert.True(vm.Help.Topics.Any(t => t.TabIndex == QaLabViewModel.ContractsTab), "list follows the tab 'Try it' opened");

            // Every tag expression shown as an example parses too, one per line.
            foreach (var topic in topics.Where(t => t.TabIndex == QaLabViewModel.TestsTab && t.HasTry && t.HasExample && !t.Example!.Contains("Scenario")))
                foreach (var line in topic.Example!.Split('\n'))
                    Assert.True(KafkaStudio.Automation.Testing.TagExpression.TryParse(line, out _, out var error), $"'{line}' doesn't parse: {error}");

            // Snippets that are KafScript parse.
            foreach (var topic in topics.Where(t => t.HasExample && t.Example!.Contains("Scenario", StringComparison.Ordinal)))
            {
                var source = topic.Example!.Replace("\n…", "\nGiven wait for 1 ms");
                try { KafkaStudio.Scripting.Parsing.Parser.Parse(source); }
                catch (KafkaStudio.Scripting.KafScriptException ex) { throw new AssertionFailedException($"example '{topic.Title}' doesn't parse: {ex.Message}"); }
            }
            return Task.CompletedTask;
        });

        runner.Add("ViewModels: QA Lab", "Producer 'send N times' numbers each copy with {{$index}}", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var producer = new KafkaStudio.App.ViewModels.Producer.ProducerViewModel(state)
            {
                SelectedConnection = "local",
                Topic = "numbered",
                Key = "K-{{$index}}",
                Value = "{ \"n\": {{$index}} }",
                RepeatCount = 3
            };
            await producer.SendCommand.ExecuteAsync();

            var keys = new List<string?>();
            await foreach (var m in state.Connections["local"].ConsumeAsync(new ConsumeOptions
            {
                Topic = "numbered", ConsumerGroup = "check", StartPosition = ConsumeStartPosition.Earliest, StopAtPartitionEnd = true
            })) keys.Add(m.Key);
            Assert.Equal("K-1,K-2,K-3", string.Join(",", keys));
        });
    }
}

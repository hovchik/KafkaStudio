using KafkaStudio.Automation.Loading;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Testing;
using KafkaStudio.Scripting.Ast;
using KafkaStudio.Scripting.Runtime;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

/// <summary>
/// Verifies the .kafscript files under /samples - the ones referenced from the README - actually
/// parse, and that every self-contained Scenario in them (one that doesn't depend on an external
/// system reacting to it) runs and passes. This is what stands in for "check every written code" on
/// the sample scripts specifically, since they're deliverables just as much as the C# is.
/// </summary>
public static class SampleScriptsTests
{
    private static string SamplesDirectory =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "samples"));

    public static void Register(TestRunner runner)
    {
        runner.Add("Samples", "every .kafscript file under /samples parses without error", () =>
        {
            var loaded = ScriptLibrary.LoadDirectory(SamplesDirectory);
            Assert.True(loaded.Count >= 4, $"expected at least 4 sample scripts, found {loaded.Count} in {SamplesDirectory}");
            foreach (var script in loaded)
            {
                Assert.True(script.Document.Blocks.Count > 0, $"{script.FilePath} parsed with zero blocks");
            }
        });

        runner.Add("Samples", "every QA sample under /samples/qa passes against the demo broker", async () =>
        {
            var cases = KafkaStudio.Automation.Testing.TestDiscovery.Discover(new[] { Path.Combine(SamplesDirectory, "qa") });
            Assert.True(cases.Count >= 9, $"expected at least 9 QA sample tests, found {cases.Count}");
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(new InMemoryKafkaBroker()) };
            var report = await new KafkaStudio.Automation.Testing.TestSuiteRunner(connections)
                .RunAsync(cases, new KafkaStudio.Automation.Testing.TestRunOptions());
            var problems = report.Results.Where(r => r.Outcome != KafkaStudio.Automation.Testing.TestOutcome.Passed)
                .Select(r => $"{r.Case.FileName} '{r.Case.Name}': {r.Message}");
            Assert.True(report.Success, string.Join("; ", problems));
        });

        runner.Add("Samples", "rethrow.kafscript runs and passes standalone", async () =>
        {
            var result = await RunFirstScenarioAsync("rethrow.kafscript");
            Assert.True(result.Success, string.Join("; ", result.Steps.Select(s => $"{s.Status}:{s.Message}")));
        });

        runner.Add("Samples", "scan-and-acknowledge.kafscript runs and passes standalone", async () =>
        {
            var result = await RunFirstScenarioAsync("scan-and-acknowledge.kafscript");
            Assert.True(result.Success, string.Join("; ", result.Steps.Select(s => $"{s.Status}:{s.Message}")));
        });

        runner.Add("Samples", "scheduled-task.kafscript defines two schedulable Task blocks", () =>
        {
            var script = ScriptLibrary.LoadFile(Path.Combine(SamplesDirectory, "scheduled-task.kafscript"));
            Assert.Equal(2, script.Document.Blocks.Count);
            Assert.True(script.Document.Blocks.All(b => b.Kind == BlockKind.Task));
            Assert.NotNull(script.Document.Blocks[0].Schedule);
            Assert.NotNull(script.Document.Blocks[1].Schedule);
        });

        runner.Add("Samples", "cross-topic-timing-check.kafscript parses with a watch + expect pair", () =>
        {
            var script = ScriptLibrary.LoadFile(Path.Combine(SamplesDirectory, "cross-topic-timing-check.kafscript"));
            var steps = script.Document.Blocks[0].Steps;
            Assert.True(steps.Any(s => s.Action is WatchTopicAction), "expected a watch step");
            Assert.True(steps.Any(s => s.Action is AwaitMessageAction { IsAssertion: true }), "expected an assertion-style await step");
        });
    }

    private static async Task<ScriptRunResult> RunFirstScenarioAsync(string fileName)
    {
        var script = ScriptLibrary.LoadFile(Path.Combine(SamplesDirectory, fileName));
        var broker = new InMemoryKafkaBroker();
        var connections = new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(broker) };
        return await new ScriptRunner(connections).RunAsync(script.Document.Blocks[0]);
    }
}

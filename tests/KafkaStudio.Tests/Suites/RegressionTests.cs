using System.Text;
using KafkaStudio.App.ViewModels;
using KafkaStudio.App.ViewModels.Consumer;
using KafkaStudio.App.ViewModels.Producer;
using KafkaStudio.App.ViewModels.Rethrow;
using KafkaStudio.App.ViewModels.Scripts;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.App.ViewModels.Tasks;
using KafkaStudio.App.ViewModels.Topics;
using KafkaStudio.Automation.Rethrow;
using KafkaStudio.Automation.Scheduling;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Connections;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Persistence;
using KafkaStudio.Core.Testing;
using KafkaStudio.Scripting;
using KafkaStudio.Scripting.Ast;
using KafkaStudio.Scripting.Parsing;
using KafkaStudio.Scripting.Runtime;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

/// <summary>Regression tests for bugs found in the deep review, plus coverage for the features added with it.</summary>
public static class RegressionTests
{
    private static ScriptBlock ParseOne(string source) => Parser.Parse(source).Blocks[0];

    private static Dictionary<string, IKafkaGateway> Local(InMemoryKafkaBroker broker) =>
        new() { ["local"] = TestKafka.NewGateway(broker) };

    public static void Register(TestRunner runner)
    {
        // ------------------------------------------------------------------ parser robustness ----

        runner.Add("Regression: parser", "invalid values raise KafScriptException, never raw exceptions", () =>
        {
            var bad = new[]
            {
                "Task: t\nschedule at 25:00\nGiven use connection \"x\"",
                "Task: t\nschedule at 9:75\nGiven use connection \"x\"",
                "Task: t\nschedule every 0 seconds\nGiven use connection \"x\"",
                "Scenario: s\nThen scan topic \"t\" from beginning limit 1.5",
                "Scenario: s\nThen scan topic \"t\" from beginning limit 0",
                "Scenario: s\nThen wait for 1.2.3 seconds",
                "Scenario: s\nThen expect message on topic \"t\" within 1 seconds where json \"$.a[\" equals \"x\"",
                "Scenario: s\nThen expect message on topic \"t\" within 1 seconds where value matches \"(unclosed\"",
                "Scenario: s\nThen scan topic \"t\" from committed",
                "Scenario:\nGiven use connection \"x\"",
            };
            foreach (var source in bad)
            {
                try
                {
                    Parser.Parse(source);
                    throw new AssertionFailedException($"expected a parse error for: {source.Replace('\n', '|')}");
                }
                catch (KafScriptException)
                {
                    // expected
                }
            }
        });

        runner.Add("Regression: parser", "a mistyped step keyword gets a helpful message", () =>
        {
            try
            {
                Parser.Parse("Scenario: s\nGiven use connection \"x\"\nThn produce message to topic \"t\" value \"v\"");
                throw new AssertionFailedException("expected an error");
            }
            catch (KafScriptException ex)
            {
                Assert.Contains("Given/When/Then/And/But", ex.Message);
                Assert.Equal(3, ex.Line ?? -1);
            }
        });

        runner.Add("Regression: parser", "scan accepts a pinned group and 'from committed'", () =>
        {
            var block = ParseOne("Scenario: s\nThen scan topic \"t\" from committed group \"sweeper\" limit 10");
            var scan = (ScanTopicAction)block.Steps[0].Action;
            Assert.Equal(TopicPosition.Committed, scan.Position);
            Assert.Equal("sweeper", scan.ConsumerGroup);
            Assert.Equal(10, scan.Limit ?? -1);
        });

        runner.Add("Regression: gateway", "compacted topics are reported so a tombstone can delete a message", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            broker.EnsureTopic("plain");
            broker.EnsureTopic("compacted");
            broker.SetCompacted("compacted", true);
            var gateway = TestKafka.NewGateway(broker);

            Assert.False(await gateway.IsTopicCompactedAsync("plain"));
            Assert.True(await gateway.IsTopicCompactedAsync("compacted"));
            await Assert.ThrowsAsync<KeyNotFoundException>(() => gateway.IsTopicCompactedAsync("nope"));
        });

        runner.Add("Regression: lexer", "CRLF doc strings carry no carriage returns", () =>
        {
            var source = "Scenario: s\r\nWhen produce message to topic \"t\" value \"\"\"\r\n{\r\n  \"a\": 1\r\n}\r\n\"\"\"\r\n";
            var produce = (ProduceMessageAction)ParseOne(source).Steps[0].Action;
            Assert.False(produce.Value!.Contains('\r'), "value still contains \\r");
            Assert.Equal("{\n  \"a\": 1\n}", produce.Value);
        });

        // ------------------------------------------------------------------ json path & templates ----

        runner.Add("Regression: JsonPath", "negative indexes, bracket names, malformed paths", () =>
        {
            const string json = """{ "items": [1, 2, 3], "odd.key": { "x y": "ok" } }""";
            Assert.Equal("3", JsonPathEvaluator.Evaluate(json, "$.items[-1]"));
            Assert.Null(JsonPathEvaluator.Evaluate(json, "$.items[-9]"));
            Assert.Equal("ok", JsonPathEvaluator.Evaluate(json, "$['odd.key'][\"x y\"]"));
            Assert.Null(JsonPathEvaluator.Evaluate(json, "$.items[abc]")); // used to throw FormatException
            Assert.NotNull(JsonPathEvaluator.Validate("$.items[abc]"));
            Assert.Null(JsonPathEvaluator.Validate("$.a.b[0]"));
        });

        runner.Add("Regression: templates", "built-ins are fresh per occurrence; unknown ones stay visible", () =>
        {
            var rendered = TemplateEngine.Render("{{$uuid}}|{{$uuid}}|{{$nope}}", new Dictionary<string, string>());
            var parts = rendered.Split('|');
            Assert.True(Guid.TryParse(parts[0], out _), rendered);
            Assert.NotEqual(parts[0], parts[1]);
            Assert.Equal("{{$nope}}", parts[2]);
        });

        runner.Add("Regression: conditions", "an invalid regex at run time is a readable script error", () =>
        {
            Assert.Throws<KafScriptException>(() => ConditionEvaluator.Compare("abc", Comparator.Matches, "(unclosed"));
        });

        // ------------------------------------------------------------------ interpreter ----

        runner.Add("Regression: interpreter", "watches are case-sensitive, like Kafka topic names", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var block = ParseOne("""
                Scenario: case
                Given use connection "local"
                Given watch topic "Orders" from now
                When produce message to topic "orders" value "lower"
                Then expect message on topic "Orders" within 500 ms
                """);
            var result = await new ScriptRunner(Local(broker)).RunAsync(block);
            Assert.False(result.Success, "a message on 'orders' must not satisfy a watch on 'Orders'");
        });

        runner.Add("Regression: interpreter", "duplicate header names keep the last value", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var block = ParseOne("""
                Scenario: headers
                Given use connection "local"
                When produce message to topic "t" value "v" header "h" to "1" header "h" to "2"
                """);
            var result = await new ScriptRunner(Local(broker)).RunAsync(block);
            Assert.True(result.Success, result.Summary);
            var stored = await ReadAll(TestKafka.NewGateway(broker), "t");
            Assert.Equal("2", stored[0].Headers["h"]);
        });

        runner.Add("Regression: interpreter", "cancelling returns a Cancelled result instead of throwing", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var block = ParseOne("""
                Scenario: slow
                Given use connection "local"
                When wait for 30 seconds
                Then log "never"
                """);
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
            var result = await new ScriptRunner(Local(broker)).RunAsync(block, cts.Token);
            Assert.True(result.Cancelled);
            Assert.Equal(StepStatus.Cancelled, result.Steps[1].Status);
            Assert.Equal(StepStatus.Skipped, result.Steps[2].Status);
        });

        runner.Add("Regression: interpreter", "rethrow keeps binary values byte-for-byte and source headers", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gateway = TestKafka.NewGateway(broker);
            var bytes = new byte[] { 0xff, 0x00, 0xfe, 0x41 };
            var block = ParseOne("""
                Scenario: relay
                Given use connection "local"
                Given watch topic "src" from now
                When a message arrives on topic "src" within 5 seconds
                Then rethrow last message to topic "dst" with key same header "via" to "script"
                """);
            var run = new ScriptRunner(Local(broker)).RunAsync(block);
            await Task.Delay(200);
            await gateway.ProduceAsync(new ProduceRequest
            {
                Topic = "src", Key = "k", Value = null, RawValue = bytes,
                Headers = new Dictionary<string, string> { ["trace"] = "abc" }
            });
            var result = await run;
            Assert.True(result.Success, result.Summary);

            var relayed = (await ReadAll(gateway, "dst"))[0];
            Assert.True(relayed.RawValue!.SequenceEqual(bytes), "binary payload was altered");
            Assert.Equal("abc", relayed.Headers["trace"]);
            Assert.Equal("script", relayed.Headers["via"]);
            Assert.Equal("k", relayed.Key);
        });

        runner.Add("Regression: interpreter", "scans stop at the end of the backlog instead of idling", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gateway = TestKafka.NewGateway(broker);
            for (var i = 0; i < 5; i++) await gateway.ProduceAsync(new ProduceRequest { Topic = "dlq", Value = $"m{i}" });

            var block = ParseOne("Scenario: s\nGiven use connection \"local\"\nThen scan topic \"dlq\" from beginning");
            var watch = System.Diagnostics.Stopwatch.StartNew();
            var result = await new ScriptRunner(Local(broker)).RunAsync(block);
            Assert.True(result.Success, result.Summary);
            Assert.Contains("scanned 5", result.Steps[1].Message);
            Assert.True(watch.Elapsed < TimeSpan.FromSeconds(2), $"scan took {watch.Elapsed.TotalSeconds:0.0}s");
        });

        runner.Add("Regression: interpreter", "scan with a pinned group resumes after acknowledged messages", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gateway = TestKafka.NewGateway(broker);
            for (var i = 0; i < 3; i++) await gateway.ProduceAsync(new ProduceRequest { Topic = "dlq", Value = $"m{i}" });

            var block = ParseOne("""
                Scenario: sweep
                Given use connection "local"
                Then scan topic "dlq" from committed group "sweeper"
                And acknowledge each scanned message
                """);
            var runner = new ScriptRunner(Local(broker));
            var first = await runner.RunAsync(block);
            Assert.Contains("scanned 3", first.Steps[1].Message);

            await gateway.ProduceAsync(new ProduceRequest { Topic = "dlq", Value = "late" });
            var second = await runner.RunAsync(block);
            Assert.Contains("scanned 1 ", second.Steps[1].Message);
        });

        // ------------------------------------------------------------------ scheduler & rethrow ----

        runner.Add("Regression: scheduler", "'at HH:MM' is local wall-clock time, not UTC", () =>
        {
            var zone = TimeZoneInfo.CreateCustomTimeZone("plus5", TimeSpan.FromHours(5), "plus5", "plus5");
            var now = new DateTimeOffset(2026, 1, 10, 1, 0, 0, TimeSpan.Zero); // 06:00 local
            var next = AutomationScheduler.NextDailyOccurrence(now, new TimeOnly(9, 30), zone);
            Assert.Equal(new DateTimeOffset(2026, 1, 10, 4, 30, 0, TimeSpan.Zero), next); // 09:30 local = 04:30 UTC

            var later = AutomationScheduler.NextDailyOccurrence(now, new TimeOnly(5, 0), zone);
            Assert.Equal(new DateTimeOffset(2026, 1, 11, 0, 0, 0, TimeSpan.Zero), later); // already passed today
        });

        runner.Add("Regression: scheduler", "a slow job never overlaps with itself", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var block = ParseOne("""
                Task: slow
                schedule every 1 seconds
                Given use connection "local"
                When wait for 3 seconds
                """);
            await using var scheduler = new AutomationScheduler();
            var concurrent = 0;
            var maxConcurrent = 0;
            scheduler.RunStarted += _ => { var c = Interlocked.Increment(ref concurrent); maxConcurrent = Math.Max(maxConcurrent, c); };
            scheduler.RunCompleted += (_, _) => Interlocked.Decrement(ref concurrent);
            var job = scheduler.Register("slow", block, Local(broker));
            scheduler.Start();
            await Task.Delay(3500);
            Assert.Equal(1, maxConcurrent);
            await Assert.ThrowsAsync<InvalidOperationException>(() => scheduler.RunNowAsync("slow"));
            _ = job;
        });

        runner.Add("Regression: rethrow", "rules that relay a topic into itself are rejected", () =>
        {
            var broker = new InMemoryKafkaBroker();
            var rule = new RethrowRule
            {
                Name = "loop", SourceConnection = "local", SourceTopic = "a", DestinationConnection = "local", DestinationTopic = "a"
            };
            Assert.Throws<ArgumentException>(() => new RethrowManager().Start(rule, Local(broker)));
        });

        runner.Add("Regression: rethrow", "a relay that dies is reported instead of showing 'running' forever", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var connections = new Dictionary<string, IKafkaGateway>
            {
                ["local"] = new ClosingGateway(TestKafka.NewGateway(broker))
            };
            await using var manager = new RethrowManager();
            var stopped = new TaskCompletionSource<Exception?>();
            manager.RuleStopped += (_, ex) => stopped.TrySetResult(ex);
            manager.Start(new RethrowRule
            {
                Name = "r", SourceConnection = "local", SourceTopic = "a", DestinationConnection = "local", DestinationTopic = "b"
            }, connections);
            await stopped.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.False(manager.IsRunning("r"));
        });

        // ------------------------------------------------------------------ core ----

        runner.Add("Regression: core", "in-memory 'Tail' reads the newest N messages", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gateway = TestKafka.NewGateway(broker);
            for (var i = 0; i < 10; i++) await gateway.ProduceAsync(new ProduceRequest { Topic = "t", Value = $"{i}" });
            var tail = await ReadAll(gateway, "t", ConsumeStartPosition.Tail, 3);
            Assert.Equal("7,8,9", string.Join(",", tail.Select(m => m.Value)));
        });

        runner.Add("Regression: core", "a hostname starting with 'demo' is not a demo connection", () =>
        {
            Assert.False(new ConnectionProfile { Name = "x", BootstrapServers = "demo-kafka:9092" }.IsDemoConnection);
            Assert.True(new ConnectionProfile { Name = "x", BootstrapServers = ConnectionProfile.DemoBootstrapServers }.IsDemoConnection);
        });

        runner.Add("Regression: core", "a corrupt settings file loads as empty and is kept aside", () =>
        {
            File.WriteAllText(JsonFileStore.PathFor("corrupt-test.json"), "{ not json");
            var loaded = JsonFileStore.Load("corrupt-test.json", new List<string> { "fallback" });
            Assert.Equal("fallback", loaded[0]);
            Assert.True(File.Exists(JsonFileStore.PathFor("corrupt-test.json.corrupt")));
        });

        runner.Add("Regression: core", "binary and tombstone values render safely", () =>
        {
            var binary = new KafkaMessage { Topic = "t", Partition = 0, Offset = 0, RawValue = new byte[] { 0xff, 0x41 }, Timestamp = DateTimeOffset.UtcNow };
            Assert.True(binary.IsBinary);
            Assert.Contains("binary, 2 bytes", binary.PrettyValue);
            var tombstone = new KafkaMessage { Topic = "t", Partition = 0, Offset = 0, Timestamp = DateTimeOffset.UtcNow };
            Assert.True(tombstone.IsTombstone);
            Assert.Null(KafkaMessage.DecodeText(new byte[] { 0xff }));
            Assert.Equal("é", KafkaMessage.DecodeText(Encoding.UTF8.GetBytes("é")));
        });

        // ------------------------------------------------------------------ view models ----

        runner.Add("Regression: view models", "Topics loads the NEWEST N messages, newest first", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var gateway = state.Connections["local"];
            for (var i = 0; i < 10; i++) await gateway.ProduceAsync(new ProduceRequest { Topic = "orders", Value = $"{i}" });

            var vm = new TopicBrowserViewModel(state);
            vm.SelectedConnection = "local";
            await WaitUntil(() => vm.Topics.Count == 1);
            vm.ScanLimit = 3;
            await vm.OpenTopicCommand.ExecuteAsync(vm.Topics[0]);

            Assert.Equal("9,8,7", string.Join(",", vm.ScannedMessages.Select(m => m.Value)));
            Assert.Equal(10L, vm.Topics[0].TotalMessageCount ?? -1);
        });

        runner.Add("Regression: view models", "switching connection clears the previous cluster's messages", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("a");
            state.AddDemoConnection("b");
            await state.Connections["a"].ProduceAsync(new ProduceRequest { Topic = "orders", Value = "x" });

            var vm = new TopicBrowserViewModel(state);
            vm.SelectedConnection = "a";
            await WaitUntil(() => vm.Topics.Count > 0);
            await vm.OpenTopicCommand.ExecuteAsync(vm.Topics[0]);
            Assert.Equal(1, vm.ScannedMessages.Count);

            vm.SelectedConnection = "b";
            Assert.Equal(0, vm.ScannedMessages.Count);
            Assert.False(vm.IsLoadingMessages);
        });

        runner.Add("Regression: view models", "cross-topic search finds hits and can be closed", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var gateway = state.Connections["local"];
            await gateway.ProduceAsync(new ProduceRequest { Topic = "a", Value = "needle one" });
            await gateway.ProduceAsync(new ProduceRequest { Topic = "b", Value = "hay" , Headers = new Dictionary<string, string> { ["x"] = "needle-in-header" } });
            await gateway.ProduceAsync(new ProduceRequest { Topic = "c", Value = "hay" });

            var vm = new TopicBrowserViewModel(state);
            vm.SelectedConnection = "local";
            await WaitUntil(() => vm.Topics.Count == 3);
            vm.GlobalSearchTerm = "needle";
            await vm.GlobalSearchCommand.ExecuteAsync();
            await WaitUntil(() => !vm.IsGlobalSearching);

            Assert.Equal(2, vm.GlobalSearchResults.Count);
            Assert.True(vm.IsGlobalSearchActive);
            vm.CloseSearchResultsCommand.Execute(null);
            Assert.False(vm.IsGlobalSearchActive);
            Assert.True(vm.ShowSearchResultsCommand.CanExecute(null));
        });

        runner.Add("Regression: view models", "Producer: Send enables once a topic is typed; repeat + templates", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var vm = new ProducerViewModel(state);
            var canExecuteChanged = false;
            vm.SendCommand.CanExecuteChanged += (_, _) => canExecuteChanged = true;

            vm.Topic = "orders";
            Assert.True(canExecuteChanged, "SendCommand never re-evaluated CanExecute when the topic changed");
            Assert.True(vm.SendCommand.CanExecute(null));

            vm.Key = "{{$uuid}}";
            vm.Value = "hello";
            vm.RepeatCount = 3;
            await vm.SendCommand.ExecuteAsync();

            var sent = await ReadAll(state.Connections["local"], "orders");
            Assert.Equal(3, sent.Count);
            Assert.Equal(3, sent.Select(m => m.Key).Distinct().Count());
        });

        runner.Add("Regression: view models", "Producer: tombstones and duplicate headers", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var vm = new ProducerViewModel(state) { Topic = "t", IsTombstone = true };
            vm.Headers.Add(new HeaderEntryViewModel { Name = "h", Value = "1" });
            vm.Headers.Add(new HeaderEntryViewModel { Name = "h", Value = "2" });
            await vm.SendCommand.ExecuteAsync();

            var sent = await ReadAll(state.Connections["local"], "t");
            Assert.True(sent[0].IsTombstone);
            Assert.Equal("2", sent[0].Headers["h"]);
        });

        runner.Add("Regression: view models", "Rethrow: Add enables when the form is complete; rules persist", () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var vm = new RethrowRulesViewModel(state);
            var changed = false;
            vm.AddRuleCommand.CanExecuteChanged += (_, _) => changed = true;

            vm.NewName = "relay";
            vm.NewSourceTopic = "a";
            vm.NewDestinationTopic = "b";
            Assert.True(changed, "AddRuleCommand never re-evaluated CanExecute");
            Assert.True(vm.AddRuleCommand.CanExecute(null));
            vm.AddRuleCommand.Execute(null);
            Assert.Equal(1, vm.Rules.Count);

            var reloaded = new RethrowRulesViewModel(state);
            Assert.True(reloaded.Rules.Any(r => r.Name == "relay"), "rule was not persisted");
        });

        runner.Add("Regression: view models", "Consumer shows live messages newest first and filters them", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var vm = new ConsumerViewModel(state) { Topic = "live" };
            vm.StartCommand.Execute(null);
            await Task.Delay(200);

            var gateway = state.Connections["local"];
            await gateway.ProduceAsync(new ProduceRequest { Topic = "live", Value = "first" });
            await gateway.ProduceAsync(new ProduceRequest { Topic = "live", Value = "second" });
            await WaitUntil(() => vm.Messages.Count == 2);
            Assert.Equal("second", vm.Messages[0].Value);

            vm.Filter = "fir";
            Assert.Equal(1, vm.Messages.Count);
            vm.StopCommand.Execute(null);
            Assert.False(vm.IsWatching);
        });

        runner.Add("Regression: view models", "removing a connection clears stale selections", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("a");
            state.AddDemoConnection("b");
            var main = new MainWindowViewModel(state);
            main.Producer.SelectedConnection = "a";
            main.Topics.SelectedConnection = "a";

            await state.RemoveConnectionAsync("a");

            // "a" is gone; with a single connection left, screens pick it automatically.
            Assert.Equal("b", main.Producer.SelectedConnection);
            Assert.Equal("b", main.Topics.SelectedConnection);
            Assert.Equal(1, main.Producer.ConnectionNames.Count);
        });

        runner.Add("Regression: view models", "Script editor Stop cancels a long-running scenario", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var vm = new ScriptEditorViewModel(state)
            {
                Source = "Scenario: slow\nGiven use connection \"local\"\nWhen wait for 60 seconds\n"
            };
            var run = vm.RunAllCommand.ExecuteAsync();
            await WaitUntil(() => vm.IsRunning);
            await Task.Delay(100);
            vm.StopCommand.Execute(null);
            await run.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Contains("Stopped", vm.RunSummary ?? "");
        });

        runner.Add("Regression: view models", "typing an out-of-range schedule doesn't crash the editor", () =>
        {
            var vm = new ScriptEditorViewModel(new AppState())
            {
                Source = "Task: t\nschedule at 99:99\nGiven use connection \"x\"\n"
            };
            Assert.NotNull(vm.ParseError);
        });

        runner.Add("Regression: view models", "registered tasks survive a restart", () =>
        {
            var state = new AppState();
            var vm = new TasksViewModel(state)
            {
                NewTaskSource = "Task: Persisted nightly\nschedule at 3:15\nGiven use connection \"local\"\n"
            };
            vm.RegisterTaskCommand.Execute(null);

            var restarted = new TasksViewModel(new AppState());
            Assert.True(restarted.Jobs.Any(j => j.Name == "Persisted nightly"), "task was not restored");
        });

        runner.Add("Regression: view models", "exported JSON embeds JSON payloads and base64 for binary", () =>
        {
            var json = MessageExport.ToJson(new[]
            {
                new KafkaMessage { Topic = "t", Partition = 0, Offset = 1, Value = "{\"a\":1}", Timestamp = DateTimeOffset.UtcNow },
                new KafkaMessage { Topic = "t", Partition = 0, Offset = 2, RawValue = new byte[] { 0xff }, Timestamp = DateTimeOffset.UtcNow }
            });
            Assert.Contains("\"a\": 1", json);
            Assert.Contains("\"valueBase64\": \"/w==\"", json);
        });
    }

    private static async Task<List<KafkaMessage>> ReadAll(IKafkaGateway gateway, string topic,
        ConsumeStartPosition position = ConsumeStartPosition.Earliest, int tail = 0)
    {
        var list = new List<KafkaMessage>();
        await foreach (var m in gateway.ConsumeAsync(new ConsumeOptions
        {
            Topic = topic,
            ConsumerGroup = $"reader-{Guid.NewGuid():N}",
            StartPosition = position,
            TailCount = tail,
            StopAtPartitionEnd = true
        }))
        {
            list.Add(m);
        }
        return list;
    }

    private static async Task WaitUntil(Func<bool> condition, int timeoutMs = 5000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new AssertionFailedException("timed out waiting for condition");
            await Task.Delay(20);
        }
    }

    /// <summary>A gateway whose consume stream ends immediately - simulates a relay whose subscription dies.</summary>
    private sealed class ClosingGateway(IKafkaGateway inner) : IKafkaGateway
    {
        public ConnectionProfile Profile => inner.Profile;
        public Task ConnectAsync(CancellationToken cancellationToken = default) => inner.ConnectAsync(cancellationToken);
        public Task<IReadOnlyList<string>> ListTopicsAsync(CancellationToken cancellationToken = default) => inner.ListTopicsAsync(cancellationToken);
        public Task<TopicMetadata> DescribeTopicAsync(string topic, CancellationToken cancellationToken = default) => inner.DescribeTopicAsync(topic, cancellationToken);
        public Task<ClusterInfo> DescribeClusterAsync(CancellationToken cancellationToken = default) => inner.DescribeClusterAsync(cancellationToken);
        public Task<IReadOnlyList<BrokerConfigEntry>> GetBrokerConfigAsync(int brokerId, CancellationToken cancellationToken = default) => inner.GetBrokerConfigAsync(brokerId, cancellationToken);
        public Task CreateTopicAsync(string topic, int partitions, short replicationFactor, CancellationToken cancellationToken = default) =>
            inner.CreateTopicAsync(topic, partitions, replicationFactor, cancellationToken);
        public Task<ProduceReceipt> ProduceAsync(ProduceRequest request, CancellationToken cancellationToken = default) => inner.ProduceAsync(request, cancellationToken);
#pragma warning disable CS1998
        public async IAsyncEnumerable<KafkaMessage> ConsumeAsync(ConsumeOptions options, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            options.OnReady?.Invoke();
            yield break;
        }
#pragma warning restore CS1998
        public Task<bool> IsTopicCompactedAsync(string topic, CancellationToken cancellationToken = default) => inner.IsTopicCompactedAsync(topic, cancellationToken);
        public Task AcknowledgeAsync(KafkaMessage message, CancellationToken cancellationToken = default) => inner.AcknowledgeAsync(message, cancellationToken);
        public Task<IReadOnlyList<ConsumerGroupSummary>> ListConsumerGroupsAsync(CancellationToken cancellationToken = default) => inner.ListConsumerGroupsAsync(cancellationToken);
        public Task<ConsumerGroupDetail> DescribeConsumerGroupAsync(string groupId, CancellationToken cancellationToken = default) => inner.DescribeConsumerGroupAsync(groupId, cancellationToken);
        public Task<IReadOnlyList<OffsetChange>> PlanOffsetResetAsync(OffsetResetRequest request, CancellationToken cancellationToken = default) => inner.PlanOffsetResetAsync(request, cancellationToken);
        public Task ApplyOffsetResetAsync(string groupId, IReadOnlyList<OffsetChange> changes, CancellationToken cancellationToken = default) => inner.ApplyOffsetResetAsync(groupId, changes, cancellationToken);
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}

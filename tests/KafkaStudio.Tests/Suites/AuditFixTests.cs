using KafkaStudio.Automation.Rethrow;
using KafkaStudio.Automation.Scheduling;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Connections;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Testing;
using KafkaStudio.Core.Validation;
using KafkaStudio.Scripting;
using KafkaStudio.Scripting.Parsing;
using KafkaStudio.Scripting.Runtime;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

/// <summary>Regression tests for the issues found in the deep code audit (gateway, scripting, automation, core).</summary>
public static class AuditFixTests
{
    /// <summary>A gateway whose produce fails a given number of times before succeeding.</summary>
    private sealed class FlakyProducer(IKafkaGateway inner, int failures) : IKafkaGateway
    {
        public int Attempts;

        public ConnectionProfile Profile => inner.Profile;
        public Task ConnectAsync(CancellationToken ct = default) => inner.ConnectAsync(ct);
        public Task<IReadOnlyList<string>> ListTopicsAsync(CancellationToken ct = default) => inner.ListTopicsAsync(ct);
        public Task<TopicMetadata> DescribeTopicAsync(string topic, CancellationToken ct = default) => inner.DescribeTopicAsync(topic, ct);
        public Task<ClusterInfo> DescribeClusterAsync(CancellationToken ct = default) => inner.DescribeClusterAsync(ct);
        public Task<IReadOnlyList<BrokerConfigEntry>> GetBrokerConfigAsync(int brokerId, CancellationToken ct = default) => inner.GetBrokerConfigAsync(brokerId, ct);
        public Task CreateTopicAsync(string topic, int partitions, short replicationFactor, CancellationToken ct = default) => inner.CreateTopicAsync(topic, partitions, replicationFactor, ct);
        public Task<ProduceReceipt> ProduceAsync(ProduceRequest request, CancellationToken ct = default)
        {
            if (Interlocked.Increment(ref Attempts) <= failures) throw new InvalidOperationException("destination down");
            return inner.ProduceAsync(request, ct);
        }
        public IAsyncEnumerable<KafkaMessage> ConsumeAsync(ConsumeOptions options, CancellationToken ct = default) => inner.ConsumeAsync(options, ct);
        public Task<bool> IsTopicCompactedAsync(string topic, CancellationToken ct = default) => inner.IsTopicCompactedAsync(topic, ct);
        public Task AcknowledgeAsync(KafkaMessage message, CancellationToken ct = default) => inner.AcknowledgeAsync(message, ct);
        public Task<IReadOnlyList<ConsumerGroupSummary>> ListConsumerGroupsAsync(CancellationToken ct = default) => inner.ListConsumerGroupsAsync(ct);
        public Task<ConsumerGroupDetail> DescribeConsumerGroupAsync(string groupId, CancellationToken ct = default) => inner.DescribeConsumerGroupAsync(groupId, ct);
        public Task<IReadOnlyList<OffsetChange>> PlanOffsetResetAsync(OffsetResetRequest request, CancellationToken ct = default) => inner.PlanOffsetResetAsync(request, ct);
        public Task ApplyOffsetResetAsync(string groupId, IReadOnlyList<OffsetChange> changes, CancellationToken ct = default) => inner.ApplyOffsetResetAsync(groupId, changes, ct);
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition() && DateTime.UtcNow < deadline) await Task.Delay(50);
    }

    private static RethrowRule Rule(string name, string from, string to) => new()
    {
        Name = name, SourceConnection = "local", SourceTopic = from, DestinationConnection = "local", DestinationTopic = to
    };

    public static void Register(TestRunner runner)
    {
        // ------------------------------------------------------------------ rethrow ----

        runner.Add("Audit: rethrow", "a failed produce is retried in place, so the first message is not lost", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var source = TestKafka.NewGateway(broker);
            var destination = new FlakyProducer(TestKafka.NewGateway(broker), failures: 2);
            var connections = new Dictionary<string, IKafkaGateway> { ["src"] = source, ["dst"] = destination };
            var rule = new RethrowRule
            {
                Name = "retry", SourceConnection = "src", SourceTopic = "in", DestinationConnection = "dst", DestinationTopic = "out"
            };

            var engine = new RethrowEngine();
            var relayed = 0;
            var failures = 0;
            engine.MessageRelayed += (_, _, _) => Interlocked.Increment(ref relayed);
            engine.RelayFailed += (_, _) => Interlocked.Increment(ref failures);

            using var cts = new CancellationTokenSource();
            var run = engine.RunAsync(rule, connections, cts.Token);
            await Task.Delay(100);
            await TestKafka.NewGateway(broker).ProduceAsync(new ProduceRequest { Topic = "in", Value = "first" });

            await WaitUntilAsync(() => relayed >= 1, TimeSpan.FromSeconds(10));
            cts.Cancel();
            try { await run; } catch { /* expected */ }

            Assert.Equal(1, relayed, "the message produced before any commit must be relayed after the destination recovers");
            Assert.Equal(2, failures);
            Assert.Equal(3, destination.Attempts);
        });

        runner.Add("Audit: rethrow", "a rule that would close a relay loop with running rules is rejected", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(broker) };
            await using var manager = new RethrowManager();

            manager.Start(Rule("ab", "a", "b"), connections);
            manager.Start(Rule("bc", "b", "c"), connections);
            Assert.Throws<ArgumentException>(() => manager.Start(Rule("ca", "c", "a"), connections));
            Assert.Throws<ArgumentException>(() => manager.Start(Rule("ba", "b", "a"), connections));
            manager.Start(Rule("cd", "c", "d"), connections); // no loop

            await manager.StopAsync("ab");
            manager.Start(Rule("ca", "c", "a"), connections); // a→b is gone, so c→a no longer loops
        });

        runner.Add("Audit: gateway", "resuming from committed falls back to the tail when asked to (per-partition relay resume)", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var producer = TestKafka.NewGateway(broker);
            for (var i = 0; i < 5; i++) await producer.ProduceAsync(new ProduceRequest { Topic = "t", Value = $"old-{i}" });

            var seen = new List<string?>();
            await foreach (var m in TestKafka.NewGateway(broker).ConsumeAsync(new ConsumeOptions
            {
                Topic = "t", ConsumerGroup = "never-committed", StartPosition = ConsumeStartPosition.Committed,
                UncommittedStart = ConsumeStartPosition.Latest, StopAtPartitionEnd = true
            }))
            {
                seen.Add(m.Value);
            }
            Assert.Equal(0, seen.Count, "nothing committed + UncommittedStart=Latest must not replay history");

            await foreach (var m in TestKafka.NewGateway(broker).ConsumeAsync(new ConsumeOptions
            {
                Topic = "t", ConsumerGroup = "never-committed", StartPosition = ConsumeStartPosition.Committed, StopAtPartitionEnd = true
            }))
            {
                seen.Add(m.Value);
            }
            Assert.Equal(5, seen.Count, "the default still reads from the beginning");
        });

        // ------------------------------------------------------------------ scripting ----

        runner.Add("Audit: runner", "with several connections and no 'use connection', a produce is an error, not a guess", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var connections = new Dictionary<string, IKafkaGateway>
            {
                ["a"] = TestKafka.NewGateway(broker, "a"),
                ["b"] = TestKafka.NewGateway(broker, "b")
            };
            var block = Parser.Parse("Scenario: s\nWhen produce message to topic \"t\" value \"x\"").Blocks[0];
            var result = await new ScriptRunner(connections).RunAsync(block);
            Assert.False(result.Success);
            Assert.Contains("no Kafka connection selected", result.Steps[0].Message);
            Assert.False(broker.TopicExists("t"), "nothing may be produced to an unchosen connection");

            var single = new Dictionary<string, IKafkaGateway> { ["only"] = TestKafka.NewGateway(broker) };
            Assert.True((await new ScriptRunner(single).RunAsync(block)).Success, "a single connection is still the implicit default");
        });

        runner.Add("Audit: runner", "a throwing StepCompleted subscriber doesn't fail or duplicate the step", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var runner2 = new ScriptRunner(new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(broker) });
            runner2.StepCompleted += _ => throw new InvalidOperationException("ui bug");
            var block = Parser.Parse("Scenario: s\nWhen produce message to topic \"t\" value \"x\"\nThen wait for 10 ms").Blocks[0];
            var result = await runner2.RunAsync(block);
            Assert.True(result.Success);
            Assert.Equal(2, result.Steps.Count);
        });

        runner.Add("Audit: parser", "durations that can't be waited on and unusable variable names are parse errors", () =>
        {
            var bad = new[]
            {
                "Task: t\nschedule every 99999999999999999999 hours\nGiven use connection \"x\"",
                "Scenario: s\nThen wait for 1000 hours",
                "Scenario: s\nThen expect message on topic \"t\" within 100 days",
                "Scenario: s\nGiven set variable order-id to \"x\"",
                "Scenario: s\nGiven set variable 1st to \"x\"",
                "Scenario: s\nThen capture json \"$.id\" as order.id",
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
            Parser.Parse("Scenario: s\nGiven set variable order_id to \"x\"\nThen wait for 2 hours");
        });

        runner.Add("Audit: lexer", "a comment after a table row is allowed; a backslash can't hide a newline in a string", () =>
        {
            var doc = Parser.Parse("Scenario Outline: s\nWhen produce message to topic \"t\" value \"<v>\"\nExamples:\n| v |  # header\n| 1 | # first\n| 2 |\n");
            Assert.Equal(2, doc.Blocks.Count);

            try
            {
                Parser.Parse("Scenario: s\nWhen produce message to topic \"t\" value \"abc\\\ndef\"\nThen wait for 1 ms");
                throw new AssertionFailedException("expected an unterminated-string error");
            }
            catch (KafScriptException ex)
            {
                Assert.Contains("unterminated", ex.Message);
            }
        });

        runner.Add("Audit: watch", "a watch whose subscription fails before becoming live fails the watch step", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var failing = new FlakyProducer(TestKafka.NewGateway(broker), failures: 0);
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = new BrokenConsumer(failing) };
            var block = Parser.Parse("Scenario: s\nGiven watch topic \"t\" from now\nThen wait for 10 ms").Blocks[0];
            var result = await new ScriptRunner(connections).RunAsync(block);
            Assert.False(result.Success);
            Assert.Contains("watch on topic 't' failed", result.Steps[0].Message);
        });

        runner.Add("Audit: scheduler", "a manual run is cancelled by DisposeAsync even when the schedule loop was never started", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(broker) };
            var scheduler = new AutomationScheduler();
            var block = Parser.Parse("Task: slow\nschedule run once\nGiven use connection \"local\"\nThen wait for 1 hours").Blocks[0];
            scheduler.Register("slow", block, connections);
            ScriptRunResult? completed = null;
            scheduler.RunCompleted += (_, r) => completed = r;

            var run = scheduler.RunNowAsync("slow");
            await Task.Delay(100);
            await scheduler.DisposeAsync();
            await run.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(completed);
            Assert.True(completed!.Cancelled);
        });

        // ------------------------------------------------------------------ core ----

        runner.Add("Audit: schema", "patternProperties, prefixItems, contains, draft-4 exclusive bounds, time offsets and big integers", () =>
        {
            var patterned = JsonSchema.Parse("""{ "type": "object", "patternProperties": { "^x-": { "type": "string" } }, "additionalProperties": false }""");
            Assert.Equal(0, patterned.Validate("""{ "x-trace": "abc" }""").Count);
            Assert.Equal(1, patterned.Validate("""{ "x-trace": 1 }""").Count);
            Assert.Equal(1, patterned.Validate("""{ "other": "abc" }""").Count);

            var tuple = JsonSchema.Parse("""{ "type": "array", "prefixItems": [ { "type": "string" }, { "type": "integer" } ], "items": false }""");
            Assert.Equal(0, tuple.Validate("""["a", 1]""").Count);
            Assert.Equal(1, tuple.Validate("""["a", 1, true]""").Count);
            Assert.Equal(1, tuple.Validate("""[1, 1]""").Count);

            var contains = JsonSchema.Parse("""{ "type": "array", "contains": { "const": "ok" } }""");
            Assert.Equal(0, contains.Validate("""["x", "ok"]""").Count);
            Assert.Equal(1, contains.Validate("""["x"]""").Count);

            var draft4 = JsonSchema.Parse("""{ "type": "number", "minimum": 0, "exclusiveMinimum": true }""");
            Assert.Equal(1, draft4.Validate("0").Count);
            Assert.Equal(0, draft4.Validate("0.5").Count);

            var time = JsonSchema.Parse("""{ "type": "string", "format": "time" }""");
            Assert.Equal(0, time.Validate("\"10:00:00-05:00\"").Count);
            Assert.Equal(0, time.Validate("\"10:00:00Z\"").Count);
            Assert.Equal(1, time.Validate("\"25:00:00Z\"").Count);

            var big = JsonSchema.Parse("""{ "type": "integer", "maximum": 9007199254740992 }""");
            Assert.Equal(1, big.Validate("9007199254740993").Count);
            Assert.Equal(0, big.Validate("9007199254740992").Count);

            Assert.False(JsonSchema.TryParse("""{ "if": { "type": "string" }, "then": {} }""", out _, out var problem));
            Assert.Contains("isn't supported", problem!);
        });

        runner.Add("Audit: stores", "a profile missing a required field is skipped instead of discarding the whole file", () =>
        {
            var dir = Path.Combine(Path.GetTempPath(), "kafkastudio-tests-" + Guid.NewGuid().ToString("N"));
            var previous = DataDirectoryScope.Swap(dir);
            try
            {
                Directory.CreateDirectory(dir);
                File.WriteAllText(Path.Combine(dir, "connections.json"),
                    """[ { "Name": "good", "BootstrapServers": "h:9092" }, { "Name": "broken" } ]""");
                var loaded = ConnectionProfileStore.Load();
                Assert.Equal(1, loaded.Count);
                Assert.Equal("good", loaded[0].Name);
                Assert.False(File.Exists(Path.Combine(dir, "connections.json.corrupt")), "a readable file must not be quarantined");

                var imported = ConnectionProfileTransfer.Import("""[ { "Name": "a", "BootstrapServers": "h:1" }, { "Name": "b" } ]""");
                Assert.Equal(1, imported.Count);
            }
            finally
            {
                DataDirectoryScope.Swap(previous);
                try { Directory.Delete(dir, recursive: true); } catch { /* best effort */ }
            }
        });
    }

    /// <summary>A gateway whose subscriptions fail immediately (e.g. an auth error on the consumer).</summary>
    private sealed class BrokenConsumer(IKafkaGateway inner) : IKafkaGateway
    {
        public ConnectionProfile Profile => inner.Profile;
        public Task ConnectAsync(CancellationToken ct = default) => inner.ConnectAsync(ct);
        public Task<IReadOnlyList<string>> ListTopicsAsync(CancellationToken ct = default) => inner.ListTopicsAsync(ct);
        public Task<TopicMetadata> DescribeTopicAsync(string topic, CancellationToken ct = default) => inner.DescribeTopicAsync(topic, ct);
        public Task<ClusterInfo> DescribeClusterAsync(CancellationToken ct = default) => inner.DescribeClusterAsync(ct);
        public Task<IReadOnlyList<BrokerConfigEntry>> GetBrokerConfigAsync(int brokerId, CancellationToken ct = default) => inner.GetBrokerConfigAsync(brokerId, ct);
        public Task CreateTopicAsync(string topic, int partitions, short replicationFactor, CancellationToken ct = default) => inner.CreateTopicAsync(topic, partitions, replicationFactor, ct);
        public Task<ProduceReceipt> ProduceAsync(ProduceRequest request, CancellationToken ct = default) => inner.ProduceAsync(request, ct);
        public async IAsyncEnumerable<KafkaMessage> ConsumeAsync(ConsumeOptions options, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.Yield();
            throw new InvalidOperationException("consumer rejected: not authorized");
#pragma warning disable CS0162
            yield break;
#pragma warning restore CS0162
        }
        public Task<bool> IsTopicCompactedAsync(string topic, CancellationToken ct = default) => inner.IsTopicCompactedAsync(topic, ct);
        public Task AcknowledgeAsync(KafkaMessage message, CancellationToken ct = default) => inner.AcknowledgeAsync(message, ct);
        public Task<IReadOnlyList<ConsumerGroupSummary>> ListConsumerGroupsAsync(CancellationToken ct = default) => inner.ListConsumerGroupsAsync(ct);
        public Task<ConsumerGroupDetail> DescribeConsumerGroupAsync(string groupId, CancellationToken ct = default) => inner.DescribeConsumerGroupAsync(groupId, ct);
        public Task<IReadOnlyList<OffsetChange>> PlanOffsetResetAsync(OffsetResetRequest request, CancellationToken ct = default) => inner.PlanOffsetResetAsync(request, ct);
        public Task ApplyOffsetResetAsync(string groupId, IReadOnlyList<OffsetChange> changes, CancellationToken ct = default) => inner.ApplyOffsetResetAsync(groupId, changes, ct);
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    private static class DataDirectoryScope
    {
        public static string? Swap(string? directory)
        {
            var previous = Core.Persistence.JsonFileStore.DataDirectory;
            Core.Persistence.JsonFileStore.DataDirectory = directory!;
            return previous;
        }
    }
}

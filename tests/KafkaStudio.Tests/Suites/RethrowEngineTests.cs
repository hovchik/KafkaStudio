using KafkaStudio.Automation.Rethrow;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Testing;
using KafkaStudio.Scripting.Ast;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

public static class RethrowEngineTests
{
    public static void Register(TestRunner runner)
    {
        runner.Add("RethrowEngine", "relays matching messages and skips non-matching ones", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var connections = new Dictionary<string, IKafkaGateway>
            {
                ["local"] = TestKafka.NewGateway(broker)
            };

            var rule = new RethrowRule
            {
                Name = "confirmed-only",
                SourceConnection = "local",
                SourceTopic = "orders",
                DestinationConnection = "local",
                DestinationTopic = "orders-relay",
                Filters = new[] { new Condition(ConditionField.Json, "$.status", Comparator.Equals, "CONFIRMED") }
            };

            var engine = new RethrowEngine();
            var relayedCount = 0;
            var skippedCount = 0;
            engine.MessageRelayed += (_, _, _) => Interlocked.Increment(ref relayedCount);
            engine.MessageSkipped += (_, _) => Interlocked.Increment(ref skippedCount);

            using var cts = new CancellationTokenSource();
            var engineTask = engine.RunAsync(rule, connections, cts.Token);

            await Task.Delay(150);

            var producer = TestKafka.NewGateway(broker);
            await producer.ProduceAsync(new ProduceRequest { Topic = "orders", Value = "{ \"status\": \"PENDING\" }" });
            await producer.ProduceAsync(new ProduceRequest { Topic = "orders", Key = "k1", Value = "{ \"status\": \"CONFIRMED\" }" });

            await WaitUntilAsync(() => relayedCount >= 1, TimeSpan.FromSeconds(3));

            cts.Cancel();
            try { await engineTask; } catch { /* expected */ }

            Assert.Equal(1, relayedCount);
            Assert.Equal(1, skippedCount);

            var checker = TestKafka.NewGateway(broker);
            var relayed = new List<KafkaMessage>();
            await foreach (var m in checker.ConsumeAsync(new ConsumeOptions
            {
                Topic = "orders-relay",
                ConsumerGroup = "checker",
                StartPosition = ConsumeStartPosition.Earliest,
                MaxMessages = 1
            }))
            {
                relayed.Add(m);
            }

            Assert.Equal(1, relayed.Count);
            Assert.Equal("k1", relayed[0].Key);
        });

        runner.Add("RethrowManager", "start/stop lifecycle and duplicate-start guard", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(broker) };
            var rule = new RethrowRule
            {
                Name = "r1",
                SourceConnection = "local",
                SourceTopic = "a",
                DestinationConnection = "local",
                DestinationTopic = "b"
            };

            await using var manager = new RethrowManager();
            manager.Start(rule, connections);
            Assert.True(manager.IsRunning("r1"));

            Assert.Throws<InvalidOperationException>(() => manager.Start(rule, connections));

            await manager.StopAsync("r1");
            Assert.False(manager.IsRunning("r1"));
        });
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
        }
    }
}

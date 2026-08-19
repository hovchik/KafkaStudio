using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Testing;
using KafkaStudio.Scripting.Parsing;
using KafkaStudio.Scripting.Runtime;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

public static class InterpreterTests
{
    public static void Register(TestRunner runner)
    {
        runner.Add("Interpreter: produce / scan / acknowledge", "scans a backlog and acknowledges every message", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gateway = TestKafka.NewGateway(broker);
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = gateway };

            var doc = Parser.Parse("""
                Scenario: Scan and acknowledge
                Given use connection "local"
                When produce message to topic "dlq" value "m1"
                And produce message to topic "dlq" value "m2"
                And produce message to topic "dlq" value "m3"
                Then scan topic "dlq" from beginning limit 3
                And acknowledge each scanned message
                """);

            var result = await new ScriptRunner(connections).RunAsync(doc.Blocks[0]);

            Assert.True(result.Success, string.Join("; ", result.Steps.Select(s => s.Message)));
            Assert.Contains("scanned 3 message(s)", result.Steps[4].Message);
            Assert.Contains("acknowledged 3 scanned message(s)", result.Steps[5].Message);
        });

        runner.Add("Interpreter: cross-topic timing check", "passes when the correlated message arrives in time", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gateway = TestKafka.NewGateway(broker);
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = gateway };

            var doc = Parser.Parse("""
                Scenario: Order confirmation triggers shipment notice
                Given use connection "local"
                Given watch topic "shipments" from now
                When produce message to topic "orders" value "{ \"status\": \"CONFIRMED\" }"
                Then expect message on topic "shipments" within 3 seconds where json "$.status" equals "NOTIFIED"
                """);

            using var cts = new CancellationTokenSource();
            var reactor = TestKafka.NewGateway(broker);
            var reactorTask = Task.Run(async () =>
            {
                await foreach (var message in reactor.ConsumeAsync(new ConsumeOptions
                {
                    Topic = "orders",
                    ConsumerGroup = "reactor",
                    StartPosition = ConsumeStartPosition.Latest
                }, cts.Token))
                {
                    _ = message;
                    await Task.Delay(150, cts.Token);
                    await reactor.ProduceAsync(new ProduceRequest
                    {
                        Topic = "shipments",
                        Value = "{ \"status\": \"NOTIFIED\" }"
                    }, cts.Token);
                    break;
                }
            }, cts.Token);

            await Task.Delay(150); // let the reactor's subscription register before we produce

            var result = await new ScriptRunner(connections).RunAsync(doc.Blocks[0]);

            cts.Cancel();
            try { await reactorTask; } catch { /* cancellation */ }

            Assert.True(result.Success, string.Join("; ", result.Steps.Select(s => $"{s.Status}:{s.Message}")));
        });

        runner.Add("Interpreter: cross-topic timing check", "fails when nothing arrives within the deadline", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gateway = TestKafka.NewGateway(broker);
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = gateway };

            var doc = Parser.Parse("""
                Scenario: No reaction
                Given use connection "local"
                Given watch topic "shipments" from now
                When produce message to topic "orders" value "noop"
                Then expect message on topic "shipments" within 1 seconds
                """);

            var result = await new ScriptRunner(connections).RunAsync(doc.Blocks[0]);

            Assert.False(result.Success, "expected the scenario to fail");
            var failedStep = result.Steps.Last(s => s.Status == StepStatus.Failed);
            Assert.Contains("no message arrived", failedStep.Message);
        });

        runner.Add("Interpreter: rethrow", "relays a message from one topic to another", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gateway = TestKafka.NewGateway(broker);
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = gateway };

            var doc = Parser.Parse("""
                Scenario: Relay orders
                Given use connection "local"
                Given watch topic "orders" from now
                When a message arrives within 3 seconds
                Then rethrow last message to topic "orders-relay" with key same
                """);

            var runTask = new ScriptRunner(connections).RunAsync(doc.Blocks[0]);
            await Task.Delay(150);

            var producer = TestKafka.NewGateway(broker);
            await producer.ProduceAsync(new ProduceRequest { Topic = "orders", Key = "abc", Value = "hello" });

            var result = await runTask;
            Assert.True(result.Success, string.Join("; ", result.Steps.Select(s => $"{s.Status}:{s.Message}")));

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
            Assert.Equal("abc", relayed[0].Key);
            Assert.Equal("hello", relayed[0].Value);
        });

        runner.Add("Interpreter: templates", "substitutes {{variables}} in produced messages", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gateway = TestKafka.NewGateway(broker);
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = gateway };

            var doc = Parser.Parse("""
                Scenario: Templated produce
                Given use connection "local"
                Given set variable orderId to "ORD-42"
                When produce message to topic "orders" key "{{orderId}}" value "hello {{orderId}}"
                """);

            var result = await new ScriptRunner(connections).RunAsync(doc.Blocks[0]);
            Assert.True(result.Success, string.Join("; ", result.Steps.Select(s => s.Message)));

            var checker = TestKafka.NewGateway(broker);
            var received = new List<KafkaMessage>();
            await foreach (var m in checker.ConsumeAsync(new ConsumeOptions
            {
                Topic = "orders",
                ConsumerGroup = "checker",
                StartPosition = ConsumeStartPosition.Earliest,
                MaxMessages = 1
            }))
            {
                received.Add(m);
            }

            Assert.Equal("ORD-42", received[0].Key);
            Assert.Equal("hello ORD-42", received[0].Value);
        });

        runner.Add("Interpreter: capture / assert", "captures a json field and a passing assertion succeeds", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gateway = TestKafka.NewGateway(broker);
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = gateway };

            var doc = Parser.Parse("""
                Scenario: Capture status
                Given use connection "local"
                Given watch topic "orders" from now
                When a message arrives within 3 seconds
                Then capture json "$.status" as status
                And assert status equals "CONFIRMED"
                """);

            var runTask = new ScriptRunner(connections).RunAsync(doc.Blocks[0]);
            await Task.Delay(150);
            var producer = TestKafka.NewGateway(broker);
            await producer.ProduceAsync(new ProduceRequest { Topic = "orders", Value = "{ \"status\": \"CONFIRMED\" }" });

            var result = await runTask;
            Assert.True(result.Success, string.Join("; ", result.Steps.Select(s => $"{s.Status}:{s.Message}")));
        });

        runner.Add("Interpreter: capture / assert", "a failing assertion fails the scenario with a clear message", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gateway = TestKafka.NewGateway(broker);
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = gateway };

            var doc = Parser.Parse("""
                Scenario: Wrong status
                Given use connection "local"
                Given watch topic "orders" from now
                When a message arrives within 3 seconds
                Then capture json "$.status" as status
                And assert status equals "SHIPPED"
                """);

            var runTask = new ScriptRunner(connections).RunAsync(doc.Blocks[0]);
            await Task.Delay(150);
            var producer = TestKafka.NewGateway(broker);
            await producer.ProduceAsync(new ProduceRequest { Topic = "orders", Value = "{ \"status\": \"CONFIRMED\" }" });

            var result = await runTask;
            Assert.False(result.Success, "expected the assertion to fail");
            Assert.Contains("assertion failed", result.Steps.Last(s => s.Status == StepStatus.Failed).Message);
        });

        runner.Add("Interpreter: error handling", "fails clearly when an unknown connection is used", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(broker) };

            var doc = Parser.Parse("""
                Scenario: Bad connection
                Given use connection "does-not-exist"
                """);

            var result = await new ScriptRunner(connections).RunAsync(doc.Blocks[0]);
            Assert.False(result.Success);
            Assert.Contains("unknown connection", result.Steps[0].Message);
        });

        runner.Add("Interpreter: error handling", "fails clearly when acknowledging a produced (not consumed) message", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(broker) };

            var doc = Parser.Parse("""
                Scenario: Bad acknowledge
                Given use connection "local"
                When produce message to topic "orders" value "hi"
                Then acknowledge last message
                """);

            var result = await new ScriptRunner(connections).RunAsync(doc.Blocks[0]);
            Assert.False(result.Success);
            Assert.Contains("consumer group", result.Steps.Last().Message);
        });
    }
}

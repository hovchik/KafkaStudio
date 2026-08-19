using KafkaStudio.Scripting;
using KafkaStudio.Scripting.Ast;
using KafkaStudio.Scripting.Parsing;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

public static class LexerParserTests
{
    public static void Register(TestRunner runner)
    {
        runner.Add("Lexer & Parser", "parses a basic scenario with produce/expect steps", () =>
        {
            const string source = """
                Scenario: Basic round trip
                Given use connection "local"
                When produce message to topic "orders" key "1" value "hello"
                Then expect message on topic "orders" within 5 seconds where key equals "1"
                """;

            var doc = Parser.Parse(source);

            Assert.Equal(1, doc.Blocks.Count);
            var block = doc.Blocks[0];
            Assert.Equal(BlockKind.Scenario, block.Kind);
            Assert.Equal("Basic round trip", block.Name);
            Assert.Equal(3, block.Steps.Count);
            Assert.True(block.Steps[0].Action is UseConnectionAction, "step 0 should be UseConnectionAction");
            Assert.True(block.Steps[1].Action is ProduceMessageAction, "step 1 should be ProduceMessageAction");
            Assert.True(block.Steps[2].Action is AwaitMessageAction, "step 2 should be AwaitMessageAction");

            var produce = (ProduceMessageAction)block.Steps[1].Action;
            Assert.Equal("orders", produce.Topic);
            Assert.Equal("1", produce.Key);
            Assert.Equal("hello", produce.Value);

            var expect = (AwaitMessageAction)block.Steps[2].Action;
            Assert.Equal(5.0, expect.Duration.Value);
            Assert.Equal(TimeUnit.Seconds, expect.Duration.Unit);
            Assert.Equal(1, expect.Conditions.Count);
            Assert.Equal(Comparator.Equals, expect.Conditions[0].Comparator);
        });

        runner.Add("Lexer & Parser", "parses a multi-line docstring value", () =>
        {
            const string source = """"
                Scenario: Docstring value
                When produce message to topic "orders" value """
                { "orderId": "{{orderId}}", "status": "CONFIRMED" }
                """
                """";

            var doc = Parser.Parse(source);
            var produce = (ProduceMessageAction)doc.Blocks[0].Steps[0].Action;
            Assert.Equal("{ \"orderId\": \"{{orderId}}\", \"status\": \"CONFIRMED\" }", produce.Value);
        });

        runner.Add("Lexer & Parser", "parses every duration unit", () =>
        {
            const string source = """
                Scenario: Durations
                Given wait for 500 ms
                And wait for 30 seconds
                And wait for 2 minutes
                And wait for 1 hour
                """;

            var doc = Parser.Parse(source);
            var steps = doc.Blocks[0].Steps;
            Assert.Equal(TimeUnit.Milliseconds, ((WaitAction)steps[0].Action).Duration.Unit);
            Assert.Equal(TimeUnit.Seconds, ((WaitAction)steps[1].Action).Duration.Unit);
            Assert.Equal(TimeUnit.Minutes, ((WaitAction)steps[2].Action).Duration.Unit);
            Assert.Equal(TimeUnit.Hours, ((WaitAction)steps[3].Action).Duration.Unit);
        });

        runner.Add("Lexer & Parser", "parses a Task block with an 'every' schedule", () =>
        {
            const string source = """
                Task: Heartbeat producer
                schedule every 10 minutes
                When produce message to topic "heartbeats" value "ping"
                """;

            var doc = Parser.Parse(source);
            var block = doc.Blocks[0];
            Assert.Equal(BlockKind.Task, block.Kind);
            Assert.NotNull(block.Schedule);
            Assert.Equal(ScheduleKind.Every, block.Schedule!.Kind);
            Assert.Equal(10.0, block.Schedule.Every!.Value);
            Assert.Equal(TimeUnit.Minutes, block.Schedule.Every.Unit);
        });

        runner.Add("Lexer & Parser", "parses a Task block with an 'at' schedule", () =>
        {
            const string source = """
                Task: Nightly reconciliation
                schedule at 9:30
                When produce message to topic "jobs" value "run"
                """;

            var doc = Parser.Parse(source);
            var block = doc.Blocks[0];
            Assert.Equal(ScheduleKind.At, block.Schedule!.Kind);
            Assert.Equal(9, block.Schedule.At!.Value.Hour);
            Assert.Equal(30, block.Schedule.At.Value.Minute);
        });

        runner.Add("Lexer & Parser", "supports multiple blocks and comments in one document", () =>
        {
            const string source = """
                # first scenario
                Scenario: One
                When produce message to topic "a" value "x"

                # second scenario
                Scenario: Two
                When produce message to topic "b" value "y"
                """;

            var doc = Parser.Parse(source);
            Assert.Equal(2, doc.Blocks.Count);
            Assert.Equal("One", doc.Blocks[0].Name);
            Assert.Equal("Two", doc.Blocks[1].Name);
        });

        runner.Add("Lexer & Parser", "parses rethrow, scan, acknowledge, capture, assert steps", () =>
        {
            const string source = """
                Scenario: Full vocabulary
                Given watch topic "orders" from now
                When a message arrives where value equals "x"
                Then rethrow last message to topic "orders-relay" with key same
                And scan topic "orders-dlq" from beginning limit 10
                And acknowledge each scanned message
                And capture json "$.status" as status
                And assert status equals "CONFIRMED"
                """;

            var doc = Parser.Parse(source);
            var steps = doc.Blocks[0].Steps;
            Assert.True(steps[0].Action is WatchTopicAction);
            Assert.True(steps[1].Action is AwaitMessageAction { IsAssertion: false });
            Assert.True(steps[2].Action is RethrowAction { KeepSourceKey: true });
            Assert.True(steps[3].Action is ScanTopicAction { Limit: 10 });
            Assert.True(steps[4].Action is AcknowledgeAction { EachScanned: true });
            Assert.True(steps[5].Action is CaptureAction { VariableName: "status" });
            Assert.True(steps[6].Action is AssertVariableAction { VariableName: "status" });
        });

        runner.Add("Lexer & Parser", "reports a line-numbered error for an unterminated string", () =>
        {
            const string source = "Scenario: Broken\nWhen produce message to topic \"orders\n";
            Assert.Throws<KafScriptException>(() => Parser.Parse(source));
        });

        runner.Add("Lexer & Parser", "reports a helpful error for an unrecognized step", () =>
        {
            const string source = "Scenario: Broken\nWhen teleport message to topic \"orders\"\n";
            try
            {
                Parser.Parse(source);
                throw new AssertionFailedException("expected KafScriptException");
            }
            catch (KafScriptException ex)
            {
                Assert.Contains("unrecognized step", ex.Message);
            }
        });
    }
}

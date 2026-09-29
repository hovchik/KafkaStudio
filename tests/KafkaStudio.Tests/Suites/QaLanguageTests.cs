using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Testing;
using KafkaStudio.Scripting;
using KafkaStudio.Scripting.Ast;
using KafkaStudio.Scripting.Parsing;
using KafkaStudio.Scripting.Runtime;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

/// <summary>The KafScript additions for QA work: tags, Feature/Background, Scenario Outlines with
/// Examples, negative and counting checks, message assertions, schema validation and test-data steps.</summary>
public static class QaLanguageTests
{
    private static (InMemoryKafkaBroker Broker, Dictionary<string, IKafkaGateway> Connections) NewCluster()
    {
        var broker = new InMemoryKafkaBroker();
        return (broker, new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(broker) });
    }

    private static string Describe(ScriptRunResult result) =>
        string.Join("; ", result.Steps.Select(s => $"{s.Status}:{s.Message}"));

    public static void Register(TestRunner runner)
    {
        // ------------------------------------------------------------------ structure ----

        runner.Add("QA language: structure", "tags, Feature tags and Background steps are applied to every block", () =>
        {
            var doc = Parser.Parse("""
                @orders
                Feature: Order pipeline

                Background:
                Given use connection "local"

                @smoke @jira:QA-12
                Scenario: First
                When log "one"

                @slow
                Scenario: Second
                When log "two"
                """);

            Assert.Equal("Order pipeline", doc.FeatureName);
            Assert.Equal(1, doc.Background.Count);
            Assert.Equal(2, doc.Blocks.Count);
            Assert.Equal("orders,smoke,jira:QA-12", string.Join(",", doc.Blocks[0].Tags));
            Assert.Equal("orders,slow", string.Join(",", doc.Blocks[1].Tags));
            Assert.True(doc.Blocks[0].Steps[0].Action is UseConnectionAction, "Background step comes first");
            Assert.Equal(2, doc.Blocks[1].Steps.Count);
            Assert.True(doc.Blocks[0].HasTag("@SMOKE"));
            return Task.CompletedTask;
        });

        runner.Add("QA language: structure", "misplaced Background / Examples / tags give clear errors", () =>
        {
            AssertParseError("Scenario: A\nGiven log \"x\"\nBackground:\nGiven log \"y\"", "Background must come before");
            AssertParseError("Scenario: A\nGiven log \"x\"\nExamples:\n| a |\n| 1 |", "only belongs under a 'Scenario Outline:'");
            AssertParseError("Scenario: A\nGiven log \"x\"\n@dangling", "tags must be followed by");
            AssertParseError("Scenario: A\nGiven log \"<x>\"\nGiven wait for <x> seconds", "placeholders only work inside a Scenario Outline");
            return Task.CompletedTask;
        });

        runner.Add("QA language: Scenario Outline", "each Examples row becomes a scenario with placeholders substituted", () =>
        {
            var doc = Parser.Parse("""
                @regression
                Scenario Outline: Order <orderId> is <status>
                Given use connection "local"
                When produce message to topic "orders" key "<orderId>" value "{ \"status\": \"<status>\" }"
                Then expect message on topic "orders" within <timeout> seconds where json "$.status" equals "<status>"

                Examples: happy path
                | orderId | status    | timeout |
                | ORD-1   | CONFIRMED | 5       |
                | ORD-2   | SHIPPED   | 2.5     |

                Examples:
                | orderId | status | timeout |
                | ORD-3   | a \| b | 1       |
                """);

            Assert.Equal(3, doc.Blocks.Count);
            Assert.Equal("Order ORD-1 is CONFIRMED", doc.Blocks[0].Name);
            Assert.Equal("Order ORD-3 is a | b", doc.Blocks[2].Name);
            Assert.Equal("Order <orderId> is <status>", doc.Blocks[1].OutlineName);
            Assert.Equal(2, doc.Blocks[1].ExampleIndex);
            Assert.Equal("regression", doc.Blocks[2].Tags.Single());

            var produce = (ProduceMessageAction)doc.Blocks[1].Steps[1].Action;
            Assert.Equal("ORD-2", produce.Key);
            Assert.Equal("{ \"status\": \"SHIPPED\" }", produce.Value);
            var expect = (AwaitMessageAction)doc.Blocks[1].Steps[2].Action;
            Assert.Equal(TimeSpan.FromSeconds(2.5), expect.Duration.ToTimeSpan());
            Assert.Equal("SHIPPED", expect.Conditions[0].Expected);
            Assert.Equal(5, doc.Blocks[0].Steps[2].Line, "template steps keep their source line");
            return Task.CompletedTask;
        });

        runner.Add("QA language: Scenario Outline", "an outline name without placeholders gets an example number", () =>
        {
            var doc = Parser.Parse("Scenario Outline: Same name\nGiven log \"<v>\"\nExamples:\n| v |\n| a |\n| b |");
            Assert.Equal("Same name (example 1)", doc.Blocks[0].Name);
            Assert.Equal("Same name (example 2)", doc.Blocks[1].Name);
            return Task.CompletedTask;
        });

        runner.Add("QA language: Scenario Outline", "bad tables and cells are reported with their row", () =>
        {
            AssertParseError("Scenario Outline: X\nGiven log \"<a>\"", "needs an 'Examples:' table");
            AssertParseError("Scenario Outline: X\nGiven log \"<a>\"\nExamples:\n| a | b |\n| 1 |", "has 1 cell(s) but the header has 2");
            AssertParseError("Scenario Outline: X\nGiven log \"<a>\"\nExamples:\n| a |", "no data rows");
            AssertParseError("Scenario Outline: X\nGiven wait for <b> seconds\nExamples:\n| a |\n| 1 |", "unknown placeholder <b>");
            AssertParseError("Scenario Outline: X\nGiven wait for <a> seconds\nExamples:\n| a |\n| soon |", "example 1 (line 5)");
            AssertParseError("Scenario Outline: X\nGiven log \"<a>\"\nExamples:\n| a | a |\n| 1 | 2 |", "appears twice");
            return Task.CompletedTask;
        });

        runner.Add("QA language: Scenario Outline", "expanded rows run and pass independently", async () =>
        {
            var (_, connections) = NewCluster();
            var doc = Parser.Parse("""
                Scenario Outline: Status <status>
                Given use connection "local"
                When produce message to topic "o" value "{ \"status\": \"<status>\" }"
                Then assert last message where json "$.status" equals "<status>"
                Examples:
                | status |
                | NEW    |
                | PAID   |
                """);
            foreach (var block in doc.Blocks)
            {
                var result = await new ScriptRunner(connections).RunAsync(block);
                Assert.True(result.Success, Describe(result));
            }
        });

        // ------------------------------------------------------------------ checks ----

        runner.Add("QA language: checks", "'expect no message' passes on silence and fails when a match arrives", async () =>
        {
            var (_, connections) = NewCluster();
            var quiet = Parser.Parse("""
                Scenario: Nothing leaks to the DLQ
                Given use connection "local"
                Given watch topic "dlq" from now
                When produce message to topic "dlq" value "{ \"orderId\": \"OTHER\" }"
                Then expect no message on topic "dlq" within 300 ms where json "$.orderId" equals "ORD-1"
                """);
            var ok = await new ScriptRunner(connections).RunAsync(quiet.Blocks[0]);
            Assert.True(ok.Success, Describe(ok));
            Assert.Contains("1 other message(s) ignored", ok.Steps[3].Message);

            var noisy = Parser.Parse("""
                Scenario: Something leaks
                Given use connection "local"
                Given watch topic "dlq" from now
                When produce message to topic "dlq" value "{ \"orderId\": \"ORD-1\" }"
                Then expect no message on topic "dlq" within 2 seconds where json "$.orderId" equals "ORD-1"
                """);
            var bad = await new ScriptRunner(connections).RunAsync(noisy.Blocks[0]);
            Assert.False(bad.Success);
            Assert.False(bad.Steps[3].IsError, "an unmet check is a failure, not an error");
            Assert.Contains("but one arrived", bad.Steps[3].Message);
        });

        runner.Add("QA language: checks", "'expect N messages' counts exactly / at least / at most", async () =>
        {
            var (_, connections) = NewCluster();
            var doc = Parser.Parse("""
                Scenario: Counts
                Given use connection "local"
                Given watch topic "c" from now
                When produce 3 messages to topic "c" key "K-{{$index}}" value "{ \"n\": {{$index}} }"
                Then expect at least 2 messages on topic "c" within 2 seconds
                And expect exactly 1 message on topic "c" within 300 ms where json "$.n" equals "3"

                Scenario: Too many
                Given use connection "local"
                Given watch topic "d" from now
                When produce 3 messages to topic "d" value "x"
                Then expect at most 2 messages on topic "d" within 500 ms
                """);
            var counts = await new ScriptRunner(connections).RunAsync(doc.Blocks[0]);
            Assert.True(counts.Success, Describe(counts));
            Assert.Contains("produced 3 messages", counts.Steps[2].Message);

            var tooMany = await new ScriptRunner(connections).RunAsync(doc.Blocks[1]);
            Assert.False(tooMany.Success);
            Assert.Contains("but more than 2 arrived", tooMany.Steps[3].Message);
        });

        runner.Add("QA language: checks", "'assert last message' checks headers, existence and numeric order", async () =>
        {
            var (_, connections) = NewCluster();
            var doc = Parser.Parse("""
                Scenario: Payment event
                Given use connection "local"
                When produce message to topic "p" key "PAY-1" value "{ \"amount\": 120.5, \"at\": \"2026-01-10T10:00:00Z\" }" header "source" to "checkout"
                Then assert last message where header "source" equals "checkout" and header "trace-id" not exists
                And assert last message where json "$.amount" greater than "100" and json "$.amount" less than "1000"
                And assert last message where json "$.at" greater than "2025-12-31T00:00:00Z" and json "$.refund" not exists
                And assert last message where key exists and value not contains "error"
                And assert last message where json "$.amount" greater than "500"
                """);
            var result = await new ScriptRunner(connections).RunAsync(doc.Blocks[0]);
            Assert.False(result.Success);
            Assert.Equal(StepStatus.Passed, result.Steps[5].Status, Describe(result));
            Assert.Contains("expected json \"$.amount\" greater than \"500\", but it was \"120.5\"", result.Steps[6].Message);
        });

        runner.Add("QA language: checks", "'assert VAR exists' / 'not exists' work without a value", async () =>
        {
            var (_, connections) = NewCluster();
            var doc = Parser.Parse("""
                Scenario: Vars
                Given set variable a to "1"
                Then assert a exists
                And assert b not exists
                And assert b exists
                """);
            var result = await new ScriptRunner(connections).RunAsync(doc.Blocks[0]);
            Assert.Equal(StepStatus.Passed, result.Steps[2].Status);
            Assert.Contains("was never set", result.Steps[3].Message);
        });

        // ------------------------------------------------------------------ contracts ----

        runner.Add("QA language: contracts", "'validate last message' reports every schema violation", async () =>
        {
            var (_, connections) = NewCluster();
            var doc = Parser.Parse(""""
                Scenario: Contract
                Given use connection "local"
                When produce message to topic "o" value "{ \"orderId\": \"ORD-1\", \"status\": \"LOST\", \"amount\": -5 }"
                Then validate last message against schema """
                { "type": "object", "required": ["orderId", "status", "currency"],
                  "properties": { "status": { "enum": ["NEW", "PAID"] }, "amount": { "type": "number", "minimum": 0 } } }
                """
                """");
            var result = await new ScriptRunner(connections).RunAsync(doc.Blocks[0]);
            Assert.False(result.Success);
            var message = result.Steps[2].Message;
            Assert.Contains("$.currency: required field is missing", message);
            Assert.Contains("$.status: \"LOST\" is not one of the allowed values", message);
            Assert.Contains("$.amount: -5 is less than the minimum 0", message);
            Assert.False(result.Steps[2].IsError);
        });

        runner.Add("QA language: contracts", "'validate each scanned message' uses a schema file relative to the script", async () =>
        {
            var (_, connections) = NewCluster();
            var dir = Path.Combine(Path.GetTempPath(), $"ks-schema-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Path.Combine(dir, "contracts"));
            try
            {
                await File.WriteAllTextAsync(Path.Combine(dir, "contracts", "order.json"),
                    """{ "type": "object", "required": ["id"], "properties": { "id": { "type": "string", "pattern": "^ORD-" } } }""");
                var doc = Parser.Parse("""
                    Scenario: Topic contract
                    Given use connection "local"
                    When produce message to topic "t" value "{ \"id\": \"ORD-1\" }"
                    And produce message to topic "t" value "{ \"id\": \"X-2\" }"
                    And produce message to topic "t" value "not json"
                    Then scan topic "t" from beginning
                    And validate each scanned message against schema file "contracts/order.json"
                    """);
                var result = await new ScriptRunner(connections) { BaseDirectory = dir }.RunAsync(doc.Blocks[0]);
                Assert.False(result.Success);
                Assert.Contains("2 of 3 scanned message(s) break schema file 'order.json'", result.Steps[5].Message);
                Assert.Contains("doesn't match pattern ^ORD-", result.Steps[5].Message);

                var missing = Parser.Parse("Scenario: M\nGiven use connection \"local\"\nWhen produce message to topic \"t\" value \"{}\"\nThen validate last message against schema file \"nope.json\"");
                var missingResult = await new ScriptRunner(connections) { BaseDirectory = dir }.RunAsync(missing.Blocks[0]);
                Assert.True(missingResult.Steps[2].IsError, "a missing schema file is an error, not a failed check");
            }
            finally
            {
                Directory.Delete(dir, recursive: true);
            }
        });

        runner.Add("QA language: contracts", "an invalid inline schema is a parse error", () =>
        {
            AssertParseError("Scenario: A\nThen validate last message against schema \"{ \\\"type\\\": \\\"strng\\\" }\"", "unknown type 'strng'");
            AssertParseError("Scenario: A\nThen validate last message against schema \"not json\"", "not valid JSON");
            return Task.CompletedTask;
        });

        // ------------------------------------------------------------------ test data ----

        runner.Add("QA language: test data", "'produce N messages' renders each copy with its own $index and generators", async () =>
        {
            var (broker, connections) = NewCluster();
            var doc = Parser.Parse("""
                Scenario: Seed
                Given use connection "local"
                When produce 5 messages to topic "seed" key "C-{{$index}}" value "{ \"n\": {{$randomInt(1,3)}}, \"cur\": \"{{$pick(EUR,USD)}}\", \"code\": \"{{$randomString(6)}}\" }"
                """);
            var result = await new ScriptRunner(connections).RunAsync(doc.Blocks[0]);
            Assert.True(result.Success, Describe(result));

            var gateway = TestKafka.NewGateway(broker);
            var messages = new List<KafkaMessage>();
            await foreach (var m in gateway.ConsumeAsync(new ConsumeOptions
            {
                Topic = "seed", ConsumerGroup = "check", StartPosition = ConsumeStartPosition.Earliest, StopAtPartitionEnd = true
            })) messages.Add(m);

            Assert.Equal("C-1,C-2,C-3,C-4,C-5", string.Join(",", messages.Select(m => m.Key)));
            foreach (var m in messages)
            {
                var n = int.Parse(JsonPathEvaluator.Evaluate(m.Value!, "$.n")!);
                Assert.True(n is >= 1 and <= 3, $"randomInt out of range: {n}");
                Assert.True(JsonPathEvaluator.Evaluate(m.Value!, "$.cur") is "EUR" or "USD");
                Assert.Equal(6, JsonPathEvaluator.Evaluate(m.Value!, "$.code")!.Length);
            }
        });

        runner.Add("QA language: test data", "time offsets and malformed generator arguments", () =>
        {
            var empty = new Dictionary<string, string>();
            var past = DateTimeOffset.Parse(TemplateEngine.Render("{{$now(-2h)}}", empty));
            Assert.True(Math.Abs((DateTimeOffset.UtcNow.AddHours(-2) - past).TotalSeconds) < 5, "now(-2h)");
            var tomorrow = TemplateEngine.Render("{{$date(+1d)}}", empty);
            Assert.Equal(DateTimeOffset.UtcNow.AddDays(1).ToString("yyyy-MM-dd"), tomorrow);
            Assert.Equal("{{$randomInt(5,1)}}", TemplateEngine.Render("{{$randomInt(5,1)}}", empty), "min > max is left as-is");
            Assert.Equal("{{$now(soon)}}", TemplateEngine.Render("{{$now(soon)}}", empty));
            Assert.Equal("7", TemplateEngine.RenderBuiltIns("{{$index}}", 7));
            return Task.CompletedTask;
        });
    }

    private static void AssertParseError(string source, string expectedFragment)
    {
        try
        {
            Parser.Parse(source);
        }
        catch (KafScriptException ex)
        {
            Assert.Contains(expectedFragment, ex.Message);
            return;
        }
        throw new AssertionFailedException($"expected a parse error containing \"{expectedFragment}\" for:\n{source}");
    }
}

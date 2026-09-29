using System.Xml.Linq;
using KafkaStudio.Automation.Testing;
using KafkaStudio.Automation.Testing.Reports;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Testing;
using KafkaStudio.Core.Validation;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

/// <summary>JSON Schema validation/inference, tag filters, the test suite runner and its reports.</summary>
public static class QaEngineTests
{
    public static void Register(TestRunner runner)
    {
        // ------------------------------------------------------------------ JSON Schema ----

        runner.Add("QA engine: JSON Schema", "validates types, required, enums, ranges, strings, arrays and formats", () =>
        {
            var schema = JsonSchema.Parse("""
                {
                  "$defs": { "line": { "type": "object", "required": ["sku"], "properties": { "sku": { "type": "string", "minLength": 3 }, "qty": { "type": "integer", "minimum": 1 } } } },
                  "type": "object",
                  "required": ["id", "status", "lines"],
                  "additionalProperties": false,
                  "properties": {
                    "id": { "type": "string", "format": "uuid" },
                    "status": { "enum": ["NEW", "PAID"] },
                    "total": { "type": ["number", "null"], "exclusiveMinimum": 0, "multipleOf": 0.01 },
                    "email": { "type": "string", "format": "email" },
                    "at": { "type": "string", "format": "date-time" },
                    "lines": { "type": "array", "minItems": 1, "uniqueItems": true, "items": { "$ref": "#/$defs/line" } }
                  }
                }
                """);

            var valid = schema.Validate("""
                { "id": "3f2504e0-4f89-11d3-9a0c-0305e82c3301", "status": "PAID", "total": 12.5, "email": "qa@example.com",
                  "at": "2026-02-01T10:00:00Z", "lines": [ { "sku": "ABC", "qty": 2 } ] }
                """);
            Assert.Equal(0, valid.Count, string.Join("; ", valid));
            Assert.Equal(0, schema.Validate("""{ "id": "3f2504e0-4f89-11d3-9a0c-0305e82c3301", "status": "NEW", "total": null, "lines": [ { "sku": "ABC" } ] }""").Count);

            var broken = schema.Validate("""
                { "id": "not-a-uuid", "status": "LOST", "total": 0, "email": "nope", "at": "yesterday",
                  "lines": [ { "sku": "AB", "qty": 1.5 }, { "sku": "AB", "qty": 1.5 } ], "extra": 1 }
                """).Select(v => v.ToString()).ToList();
            string Find(string fragment) => broken.FirstOrDefault(v => v.Contains(fragment, StringComparison.Ordinal))
                ?? throw new AssertionFailedException($"no violation containing \"{fragment}\" in: {string.Join(" | ", broken)}");
            Find("$.id: \"not-a-uuid\" is not a valid uuid");
            Find("$.status: \"LOST\" is not one of the allowed values: \"NEW\", \"PAID\"");
            Find("$.total: 0 must be greater than 0");
            Find("$.email: \"nope\" is not a valid email");
            Find("$.at: \"yesterday\" is not a valid date-time");
            Find("$.lines[0].sku: is 2 character(s) long, expected at least 3");
            Find("$.lines[0].qty: expected integer but was number 1.5");
            Find("$.lines[1]: duplicate of item [0]");
            Find("$.extra: field is not allowed");

            Assert.Contains("expected object but was an array", schema.Validate("[]")[0].ToString());
            Assert.Contains("not valid JSON", schema.Validate("{ nope")[0].Message);
            Assert.Contains("no text value", schema.Validate((string?)null)[0].Message);
            return Task.CompletedTask;
        });

        runner.Add("QA engine: JSON Schema", "anyOf / oneOf / not, and schema problems are caught up front", () =>
        {
            var schema = JsonSchema.Parse("""{ "oneOf": [ { "type": "string" }, { "type": "integer" } ], "not": { "const": "forbidden" } }""");
            Assert.Equal(0, schema.Validate("\"ok\"").Count);
            Assert.Contains("oneOf", schema.Validate("true")[0].Message);
            Assert.Contains("(not)", schema.Validate("\"forbidden\"")[0].Message);
            Assert.Contains("anyOf", JsonSchema.Parse("""{ "anyOf": [ { "minimum": 5 }, { "maximum": 1 } ] }""").Validate("3")[0].Message);

            Assert.False(JsonSchema.TryParse("""{ "type": "strin" }""", out _, out var p1));
            Assert.Contains("unknown type 'strin'", p1!);
            Assert.False(JsonSchema.TryParse("""{ "properties": { "a": { "pattern": "(" } } }""", out _, out var p2));
            Assert.Contains("#/properties/a/pattern: invalid regular expression", p2!);
            Assert.False(JsonSchema.TryParse("""{ "$ref": "#/$defs/missing" }""", out _, out var p3));
            Assert.Contains("can't resolve", p3!);
            Assert.False(JsonSchema.TryParse("""{ "required": "id" }""", out _, out _));
            Assert.True(JsonSchema.TryParse("true", out _, out _));
            return Task.CompletedTask;
        });

        runner.Add("QA engine: JSON Schema", "inference produces a schema every sample passes", () =>
        {
            var samples = new[]
            {
                """{ "id": "3f2504e0-4f89-11d3-9a0c-0305e82c3301", "amount": 5, "at": "2026-01-01T00:00:00Z", "tags": ["a"], "note": "x" }""",
                """{ "id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8", "amount": 7.25, "at": "2026-01-02T00:00:00Z", "tags": [] }""",
                "not json"
            };
            var inferred = JsonSchemaInference.Infer(samples);
            Assert.Equal(2, inferred.SamplesUsed);
            Assert.Equal(1, inferred.SamplesSkipped);

            var schema = JsonSchema.Parse(inferred.SchemaText);
            foreach (var sample in samples.Take(2)) Assert.Equal(0, schema.Validate(sample).Count, inferred.SchemaText);
            Assert.Contains("\"format\": \"uuid\"", inferred.SchemaText);
            Assert.Contains("\"format\": \"date-time\"", inferred.SchemaText);
            Assert.Contains("\"type\": \"number\"", inferred.SchemaText);
            Assert.False(inferred.SchemaText.Contains("\"note\"\n", StringComparison.Ordinal));
            Assert.Contains("required field is missing", schema.Validate("""{ "amount": 1 }""")[0].Message);
            return Task.CompletedTask;
        });

        // ------------------------------------------------------------------ tag filters ----

        runner.Add("QA engine: tag filters", "and / or / not / parentheses / comma shorthand", () =>
        {
            var smoke = new[] { "smoke", "orders" };
            var wip = new[] { "smoke", "wip" };
            Assert.True(TagExpression.Parse("@smoke and not @wip").Matches(smoke));
            Assert.False(TagExpression.Parse("@smoke and not @wip").Matches(wip));
            Assert.True(TagExpression.Parse("(@payments or @orders) and not @slow").Matches(smoke));
            Assert.True(TagExpression.Parse("@payments, @ORDERS").Matches(smoke));
            Assert.True(TagExpression.Parse("not @payments or @x and @y").Matches(smoke), "and binds tighter than or");
            Assert.True(TagExpression.Parse("").Matches(Array.Empty<string>()));

            Assert.False(TagExpression.TryParse("@a and", out _, out var e1));
            Assert.Contains("ends too early", e1!);
            Assert.False(TagExpression.TryParse("(@a", out _, out var e2));
            Assert.Contains("missing ')'", e2!);
            Assert.False(TagExpression.TryParse("@a @b", out _, out var e3));
            Assert.Contains("unexpected '@b'", e3!);
            return Task.CompletedTask;
        });

        // ------------------------------------------------------------------ suite runner ----

        runner.Add("QA engine: suite runner", "discovers, filters by tag and name, and tells failures from errors", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(broker) };
            var cases = TestDiscovery.FromSource("""
                @smoke
                Scenario: Passes
                Given use connection "local"
                When produce message to topic "t" value "{ \"ok\": true }"
                Then assert last message where json "$.ok" equals "true"

                @smoke
                Scenario: Fails
                Given use connection "local"
                When produce message to topic "t" value "{ \"ok\": false }"
                Then assert last message where json "$.ok" equals "true"

                @smoke
                Scenario: Errors
                Given use connection "nowhere"

                @wip
                Scenario: Not selected
                Given log "x"

                Task: Also not selected
                schedule every 5 minutes
                Given log "x"
                """, "/tests/orders.kafscript");
            Assert.Equal(5, cases.Count);

            var options = new TestRunOptions { Tags = TagExpression.Parse("@smoke") };
            var selected = TestDiscovery.Select(cases, options);
            Assert.Equal(3, selected.Count);
            Assert.Equal(1, TestDiscovery.Select(cases, new TestRunOptions { NameFilter = "fails" }).Count);

            var completed = new List<string>();
            var suite = new TestSuiteRunner(connections);
            suite.CaseCompleted += r => completed.Add(r.Case.Name);
            var report = await suite.RunAsync(selected, options);

            Assert.Equal("Passes,Fails,Errors", string.Join(",", completed));
            Assert.Equal(1, report.Passed);
            Assert.Equal(1, report.Failed);
            Assert.Equal(1, report.Errors);
            Assert.False(report.Success);
            Assert.Equal(11, report.Results[1].FailedLine);
            Assert.Contains("unknown connection 'nowhere'", report.Results[2].Message!);
            Assert.Equal("orders", report.Results[0].Case.SuiteName);
        });

        runner.Add("QA engine: suite runner", "retries mark a test that passes later as flaky", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gateway = TestKafka.NewGateway(broker);
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = gateway };
            await gateway.ProduceAsync(new ProduceRequest { Topic = "flaky", Value = "first" });
            await gateway.ProduceAsync(new ProduceRequest { Topic = "flaky", Value = "second" });

            // Each attempt consumes (and acknowledges) one more message, so the 2nd attempt sees "second".
            var cases = TestDiscovery.FromSource("""
                Scenario: Eventually consistent
                Given use connection "local"
                When scan topic "flaky" from committed group "flaky-reader" limit 1
                And acknowledge each scanned message
                Then assert last message where value equals "second"
                """);
            var report = await new TestSuiteRunner(connections).RunAsync(cases, new TestRunOptions { Retries = 2 });

            var result = report.Results.Single();
            Assert.Equal(TestOutcome.Passed, result.Outcome, result.Message);
            Assert.Equal(2, result.Attempts);
            Assert.True(result.IsFlaky);
            Assert.Equal(1, report.Flaky);
            Assert.Contains("attempt 1:", result.EarlierFailures.Single());
        });

        runner.Add("QA engine: suite runner", "fail-fast skips the rest; a timeout is an error; load errors are kept", async () =>
        {
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(new InMemoryKafkaBroker()) };
            var cases = TestDiscovery.FromSource("""
                Scenario: Slow
                Given wait for 10 seconds

                Scenario: Never runs
                Given log "x"
                """).Concat(TestDiscovery.FromSource("Scenario: Broken\nGiven frobnicate", "/tests/broken.kafscript")).ToList();

            var report = await new TestSuiteRunner(connections).RunAsync(cases,
                new TestRunOptions { FailFast = true, Timeout = TimeSpan.FromMilliseconds(200) });
            Assert.Equal(TestOutcome.Error, report.Results[0].Outcome);
            Assert.Contains("timed out after", report.Results[0].Message!);
            Assert.Equal(TestOutcome.Skipped, report.Results[1].Outcome);
            Assert.Contains("fail-fast", report.Results[1].Message!);
            Assert.Equal(TestOutcome.Skipped, report.Results[2].Outcome);

            var loadError = await new TestSuiteRunner(connections).RunAsync(cases.Skip(2).ToList(), new TestRunOptions());
            Assert.Equal(TestOutcome.Error, loadError.Results[0].Outcome);
            Assert.Contains("unrecognized step", loadError.Results[0].Message!);

            var missing = TestDiscovery.Discover(new[] { "/definitely/not/here.kafscript" });
            Assert.Contains("not found", missing.Single().LoadError!);
        });

        runner.Add("QA engine: suite runner", "starting variables are visible to every test", async () =>
        {
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(new InMemoryKafkaBroker()) };
            var cases = TestDiscovery.FromSource("Scenario: Env\nThen assert env equals \"staging\"");
            var report = await new TestSuiteRunner(connections).RunAsync(cases,
                new TestRunOptions { Variables = new Dictionary<string, string> { ["env"] = "staging" } });
            Assert.True(report.Success, report.Results[0].Message ?? "");
        });

        // ------------------------------------------------------------------ reports ----

        runner.Add("QA engine: reports", "JUnit XML, Markdown and HTML reports describe the run", async () =>
        {
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(new InMemoryKafkaBroker()) };
            var cases = TestDiscovery.FromSource("""
                @smoke
                Scenario: Good <one>
                Given log "fine"

                Scenario: Bad & broken
                Given set variable x to "1"
                Then assert x equals "2"

                Scenario: Oops
                Given use connection "nope"
                """, "/tests/pipeline.kafscript");
            var report = await new TestSuiteRunner(connections).RunAsync(cases, new TestRunOptions(),
                environment: new Dictionary<string, string> { ["connection"] = "local" });

            var xml = XDocument.Parse(JUnitReportWriter.Write(report));
            var root = xml.Root!;
            Assert.Equal("3", root.Attribute("tests")!.Value);
            Assert.Equal("1", root.Attribute("failures")!.Value);
            Assert.Equal("1", root.Attribute("errors")!.Value);
            var testcases = root.Descendants("testcase").ToList();
            Assert.Equal("pipeline", testcases[0].Attribute("classname")!.Value);
            Assert.NotNull(testcases[1].Element("failure"));
            Assert.Contains("expected equals \"2\"", testcases[1].Element("failure")!.Attribute("message")!.Value);
            Assert.NotNull(testcases[2].Element("error"));
            Assert.Contains("[PASS]", testcases[0].Element("system-out")!.Value);

            var markdown = MarkdownReportWriter.Write(report);
            Assert.Contains("❌ FAILED", markdown);
            Assert.Contains("## Failures", markdown);
            Assert.Contains("`@smoke`", markdown);
            Assert.Contains("- **connection:** local", markdown);

            var bug = MarkdownReportWriter.BugReport(report.Results[1]);
            Assert.Contains("**Expected vs. actual**", bug);
            Assert.Contains("line 7", bug);

            var html = HtmlReportWriter.Write(report);
            Assert.Contains("Bad &amp; broken", html);
            Assert.Contains("Good &lt;one&gt;", html);
            Assert.False(html.Contains("Good <one>", StringComparison.Ordinal), "names are HTML-encoded");
            Assert.Contains("prefers-color-scheme: dark", html);
        });
    }
}

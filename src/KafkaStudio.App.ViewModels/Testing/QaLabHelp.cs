using KafkaStudio.App.ViewModels.Shared;

namespace KafkaStudio.App.ViewModels.Testing;

/// <summary>
/// The QA Lab screen's "Help &amp; examples" content: how-tos for the Test Runner and Contract check, with
/// tag filters and a starter schema that "Try it" fills in. Content mirrors <c>docs/qa-testing.md</c>; a test
/// checks every tag-filter example parses and every schema example is a valid JSON Schema.
/// </summary>
public static class QaLabHelp
{
    private const int Tests = QaLabViewModel.TestsTab, Contracts = QaLabViewModel.ContractsTab;

    /// <summary>A starter order-event contract ("Try it" on the Contract check how-to puts it in the schema box).</summary>
    public const string SampleSchema = """
        {
          "type": "object",
          "required": ["orderId", "status", "amount", "currency", "createdAt"],
          "properties": {
            "orderId":   { "type": "string", "pattern": "^ORD-[0-9]+$" },
            "status":    { "enum": ["NEW", "CONFIRMED", "SHIPPED", "CANCELLED"] },
            "amount":    { "type": "number", "minimum": 0 },
            "currency":  { "type": "string", "enum": ["EUR", "USD", "GBP"] },
            "createdAt": { "type": "string", "format": "date-time" }
          }
        }
        """;

    public static IReadOnlyList<HowToTopicViewModel> BuildTopics() => new[]
    {
        // ------------------------------------------------------------------ both tabs ----
        new HowToTopicViewModel
        {
            Title = "What QA Lab is for",
            TabIndex = HowToTopicViewModel.AllTabs, TabName = "All tabs",
            Steps = "• Test Runner - run .kafscript regression packs by tag, with retries, live results, reports and one-click bug reports.\n" +
                    "• Contract check - validate a topic's messages against a JSON Schema, or infer one from real traffic.\n" +
                    "Write tests in the Script Editor (its Help & examples lists every QA step). docs/qa-testing.md has the full guide."
        },

        // ------------------------------------------------------------------ Test Runner ----
        new HowToTopicViewModel
        {
            Title = "Run a regression pack",
            TabIndex = Tests, TabName = "Test Runner",
            Steps = "1. Test sources: Add file… or Add folder… (searched recursively for .kafscript files).\n" +
                    "2. Keep 'Include the Script Editor's scenarios' ticked to run what's in the editor too.\n" +
                    "3. Press ▶ Run all (F5 or Ctrl+Enter). Results and each test's steps stream in live.\n" +
                    "Sources, filters and options are remembered. ⟳ Reload (Ctrl+R) re-reads changed files."
        },
        new HowToTopicViewModel
        {
            Title = "Run only smoke tests",
            TabIndex = Tests, TabName = "Test Runner",
            Steps = "Tag scenarios on the line above them (@smoke, @regression, @jira:QA-123), then type a tag expression in Filter. " +
                    "Tags above Feature: apply to every block in the file.",
            Example = "@smoke\nScenario: New order is accepted\n…",
            TryText = "@smoke"
        },
        new HowToTopicViewModel
        {
            Title = "Exclude work-in-progress and slow tests",
            TabIndex = Tests, TabName = "Test Runner",
            Steps = "Tag expressions support and, or, not and parentheses. A comma is shorthand for 'or'.",
            Example = "@smoke and not @wip\n(@orders or @payments) and not @slow\n@smoke, @api",
            TryText = "(@orders or @payments) and not @slow"
        },
        new HowToTopicViewModel
        {
            Title = "Run one test by name",
            TabIndex = Tests, TabName = "Test Runner",
            Steps = "Type part of its name in 'name contains…' under Filter (it combines with the tag filter), or select tests in the list and press Run selected."
        },
        new HowToTopicViewModel
        {
            Title = "Point the same pack at dev or staging",
            TabIndex = Tests, TabName = "Test Runner",
            Steps = "1. Leave 'use connection' out of the tests (or put it only in a Background).\n" +
                    "2. Pick the Default connection under Run options - tests without 'use connection' run against it.\n" +
                    "3. Put per-environment test data in Variables (name=value per line); every test starts with them as {{name}}.",
            Example = "customerId=CUST-42\nenv=staging"
        },
        new HowToTopicViewModel
        {
            Title = "Detect flaky tests",
            TabIndex = Tests, TabName = "Test Runner",
            Steps = "Set 'Retries on failure' to 1 or 2. A test that passes on a retry is marked flaky - it passed, but deserves a look. " +
                    "Earlier attempts are kept in the test's details and in bug reports."
        },
        new HowToTopicViewModel
        {
            Title = "Keep a hung test from blocking the run",
            TabIndex = Tests, TabName = "Test Runner",
            Steps = "Set 'Timeout per test (s)' (0 = no limit): a test over the limit is stopped and reported as an error. " +
                    "Tick 'Stop at the first failure' for a fast fail. ■ Stop (Shift+F5) stops after the current step."
        },
        new HowToTopicViewModel
        {
            Title = "Re-run just what failed",
            TabIndex = Tests, TabName = "Test Runner",
            Steps = "After fixing the system (or the test), press Run failed - only tests that failed or errored run again."
        },
        new HowToTopicViewModel
        {
            Title = "Read the results: failed vs. error",
            TabIndex = Tests, TabName = "Test Runner",
            Steps = "• Failed - a check wasn't met: the system under test is wrong.\n" +
                    "• Error - the test couldn't run: unknown connection, broker unreachable, missing schema file, broken script, missing topic.\n" +
                    "Select a test to see every step, expected vs. actual, and the messages involved."
        },
        new HowToTopicViewModel
        {
            Title = "File a bug from a failed test",
            TabIndex = Tests, TabName = "Test Runner",
            Steps = "Select the failed test and press Copy bug report. It copies Markdown with the test's location and tags, expected vs. actual, " +
                    "every step, earlier attempts and the environment - paste it into Jira, Azure Boards or GitHub."
        },
        new HowToTopicViewModel
        {
            Title = "Share a run's results",
            TabIndex = Tests, TabName = "Test Runner",
            Steps = "Toolbar, after a run:\n" +
                    "• JUnit XML - for Jenkins, GitLab, Azure DevOps, TeamCity.\n" +
                    "• HTML - one self-contained file with passed/failed filters, light/dark aware.\n" +
                    "• Markdown / Copy summary - for a PR, ticket or chat."
        },
        new HowToTopicViewModel
        {
            Title = "Run the same tests in CI",
            TabIndex = Tests, TabName = "Test Runner",
            Steps = "The kafkastudio CLI uses the same engine, tags and variables. Exit codes: 0 passed · 1 failures/errors · 2 bad config or unreachable cluster · 3 no tests selected.",
            Example = "kafkastudio test tests/kafka -c local=localhost:9092 \\\n  --tags \"@regression and not @wip\" --retries 1 \\\n  --junit reports/junit.xml --html reports/kafka-tests.html"
        },
        new HowToTopicViewModel
        {
            Title = "Write a data-driven test",
            TabIndex = Tests, TabName = "Test Runner",
            Steps = "Use a Scenario Outline with an Examples table in the Script Editor: every row runs and is reported as its own test.",
            Example = "@orders\nScenario Outline: Status <status> is relayed\nGiven watch topic \"orders-audit\" from now\n" +
                      "When produce message to topic \"orders\" key \"<id>\" value \"{ \\\"status\\\": \\\"<status>\\\" }\"\n" +
                      "Then expect message on topic \"orders-audit\" within 5 seconds where key equals \"<id>\"\n\n" +
                      "Examples:\n| id    | status    |\n| ORD-1 | NEW       |\n| ORD-2 | CONFIRMED |"
        },

        // ------------------------------------------------------------------ Contract check ----
        new HowToTopicViewModel
        {
            Title = "Check a topic against its contract",
            TabIndex = Contracts, TabName = "Contract check",
            Steps = "1. Pick a connection and a topic.\n" +
                    "2. Tick Newest and set N per partition, or untick it to check the whole topic (up to 200,000 messages).\n" +
                    "3. Paste the JSON Schema, or Open… a schema file.\n" +
                    "4. Press ✓ Validate. You get checked/valid/invalid counts, the most common problems by field, and each offending message.",
            Example = SampleSchema,
            TryText = SampleSchema
        },
        new HowToTopicViewModel
        {
            Title = "Don't have a schema? Infer one",
            TabIndex = Contracts, TabName = "Contract check",
            Steps = "Press Infer schema: it reads up to 5,000 messages, marks fields present in every message as required, lists every type seen " +
                    "and detects uuid and date-time strings. Treat it as a starting point - add enums, patterns and ranges samples can't reveal - then Save… it."
        },
        new HowToTopicViewModel
        {
            Title = "Find out what's wrong with bad messages",
            TabIndex = Contracts, TabName = "Contract check",
            Steps = "MOST COMMON PROBLEMS groups violations by field, e.g. '312× $.currency required field is missing' - usually one producer bug. " +
                    "Select an offending message to see every violation next to the message itself.",
            Example = "$.currency: required field is missing\n$.status: \"LOST\" is not one of the allowed values: \"NEW\", \"CONFIRMED\", …"
        },
        new HowToTopicViewModel
        {
            Title = "Keep the contract in the regression pack",
            TabIndex = Contracts, TabName = "Contract check",
            Steps = "Press Turn into a check: a @contract Scenario that scans the topic and validates it against this schema is added to the Script Editor. " +
                    "Save it next to your tests so the Test Runner and CI enforce it.",
            Example = "@contract\nScenario: orders matches its contract\nGiven use connection \"local\"\n" +
                      "Then scan topic \"orders\" from beginning limit 1000\nAnd validate each scanned message against schema file \"contracts/order.schema.json\""
        },
        new HowToTopicViewModel
        {
            Title = "Supported schema keywords",
            TabIndex = Contracts, TabName = "Contract check",
            Steps = "type (incl. lists and nullable), properties, required, additionalProperties, enum, const, minimum/maximum (+ exclusive), multipleOf, " +
                    "minLength/maxLength, pattern, format (date-time, date, time, uuid, email, uri, ipv4), items, minItems/maxItems/uniqueItems, " +
                    "min/maxProperties, allOf/anyOf/oneOf/not, local $ref (#/$defs/x). Other keywords (title, description, examples) are ignored. " +
                    "A mistake in the schema itself is reported, not treated as 'anything goes'."
        },
    };
}

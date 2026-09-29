using System.Collections.ObjectModel;

namespace KafkaStudio.App.ViewModels.Shared;

/// <summary>A single entry in an in-app KafScript help/reference panel: a step's syntax, a short
/// description, and a runnable example that can be inserted straight into an editor. Content mirrors
/// <c>docs/kafscript-language.md</c>, kept short enough to scan while writing a script.</summary>
public sealed class HelpTopicViewModel
{
    public required string Title { get; init; }
    public required string Syntax { get; init; }
    public required string Description { get; init; }
    public required string Example { get; init; }
}

/// <summary>
/// Shared KafScript help/examples content, used by both the Script Editor (Scenario/Task on-demand
/// runs) and the Tasks &amp; Checks screen (scheduled Task registration) so the two "help &amp; examples"
/// panels never drift out of sync with each other.
/// </summary>
public static class KafScriptHelp
{
    public static ObservableCollection<HelpTopicViewModel> BuildTopics() => new()
    {
        new HelpTopicViewModel
        {
            Title = "Use connection",
            Syntax = "use connection \"name\"",
            Description = "Selects which registered Kafka connection subsequent steps run against. Required before any step that talks to Kafka.",
            Example = "Given use connection \"local\""
        },
        new HelpTopicViewModel
        {
            Title = "Produce message",
            Syntax = "produce message to topic \"T\" [key \"K\"] [value V] [header \"H\" to \"V\"]...",
            Description = "Sends a message. key, value, and any number of header clauses are optional and can appear in any order after the topic.",
            Example = "When produce message to topic \"orders\" key \"{{orderId}}\" value \"{ \\\"status\\\": \\\"CONFIRMED\\\" }\""
        },
        new HelpTopicViewModel
        {
            Title = "Watch topic",
            Syntax = "watch topic \"T\" from beginning|end|now",
            Description = "Opens a live subscription on a topic immediately, so a following produce step can't race past it.",
            Example = "Given watch topic \"shipment-notices\" from now"
        },
        new HelpTopicViewModel
        {
            Title = "Expect message",
            Syntax = "expect message on topic \"T\" within DURATION [where COND [and COND]...]",
            Description = "Waits up to DURATION for a message on T matching every condition, and fails the scenario if none arrives in time.",
            Example = "Then expect message on topic \"shipment-notices\" within 30 seconds where json \"$.status\" equals \"NOTIFIED\""
        },
        new HelpTopicViewModel
        {
            Title = "Message arrives",
            Syntax = "[a] message arrives [on topic \"T\"] [within DURATION] [where COND [and COND]...]",
            Description = "The triggering form (typically a When step), used ahead of a rethrow/capture step. Sets the matching message as the \"last message\".",
            Example = "When a message arrives on topic \"orders\" within 10 seconds where json \"$.status\" equals \"CONFIRMED\""
        },
        new HelpTopicViewModel
        {
            Title = "Rethrow last message",
            Syntax = "rethrow last message to topic \"T\" [with key same|\"K\"] [header \"H\" to \"V\"]...",
            Description = "Republishes the most recently seen message (byte-for-byte, with its headers) to a different topic.",
            Example = "Then rethrow last message to topic \"orders-fulfillment\" with key same header \"relayed-by\" to \"kafka-studio\""
        },
        new HelpTopicViewModel
        {
            Title = "Scan topic",
            Syntax = "scan topic \"T\" from beginning|end|committed [group \"G\"] [limit N]",
            Description = "Bulk-reads a topic's current backlog into the scenario's \"scanned messages\" list, stopping at limit or once caught up. " +
                          "Pin a consumer group to make acknowledgements stick: a later 'from committed' scan resumes after the last acknowledged message.",
            Example = "Then scan topic \"orders-dlq\" from committed group \"dlq-sweeper\" limit 500"
        },
        new HelpTopicViewModel
        {
            Title = "Acknowledge messages",
            Syntax = "acknowledge last message / acknowledge each scanned message",
            Description = "Commits the consumer offset for the last consumed message, or for every message collected by the most recent scan.",
            Example = "Then acknowledge each scanned message"
        },
        new HelpTopicViewModel
        {
            Title = "Templates & built-ins",
            Syntax = "{{NAME}}  {{$uuid}}  {{$now}}  {{$timestamp}}  {{$date}}  {{$random}}",
            Description = "Any quoted value can use {{variables}} (from set/capture) and dynamic built-ins, evaluated fresh each time - handy for unique keys and ids.",
            Example = "When produce message to topic \"orders\" key \"{{$uuid}}\" value \"{ \\\"createdAt\\\": \\\"{{$now}}\\\" }\""
        },
        new HelpTopicViewModel
        {
            Title = "Log",
            Syntax = "log key | log value | log message | log \"text with {{vars}}\"",
            Description = "Writes the last message (or a literal) into the step results - useful while developing a check.",
            Example = "And log message"
        },
        new HelpTopicViewModel
        {
            Title = "Wait",
            Syntax = "wait for DURATION",
            Description = "Pauses the scenario. Durations: ms, seconds, minutes, hours (e.g. 500 ms, 2 seconds).",
            Example = "And wait for 2 seconds"
        },
        new HelpTopicViewModel
        {
            Title = "Set / capture variables",
            Syntax = "set variable NAME to \"value\" / capture json \"$.path\" as NAME / capture key|value as NAME",
            Description = "Sets a variable, or pulls a field off the last message into a variable, for use in {{NAME}} substitutions or asserts.",
            Example = "Given set variable orderId to \"ORD-1042\"\nThen capture json \"$.orderId\" as orderId\nAnd assert orderId equals \"ORD-1042\""
        },
        new HelpTopicViewModel
        {
            Title = "Assert",
            Syntax = "assert NAME equals|contains|matches|not equals \"value\"",
            Description = "Checks a previously set/captured variable and fails the scenario if it doesn't hold.",
            Example = "Then assert orderId equals \"ORD-1042\""
        },
        new HelpTopicViewModel
        {
            Title = "Scheduled task",
            Syntax = "Task: <name> / schedule run once|every N unit|at HH:MM",
            Description = "A Task block automates a script on a schedule instead of running it on demand like a Scenario.",
            Example = "Task: Nightly DLQ sweep\nschedule every 5 minutes\nGiven use connection \"local\"\nThen scan topic \"orders-dlq\" from beginning limit 500\nThen acknowledge each scanned message"
        },
        new HelpTopicViewModel
        {
            Title = "QA: tags, Feature, Background",
            Syntax = "@tag ... (line above a block) / Feature: <name> / Background: + steps",
            Description = "Tags let the QA Lab and the kafkastudio CLI pick tests (\"@smoke and not @wip\"). Tags above Feature: apply to every block; Background steps run before every Scenario in the file.",
            Example = "@orders\nFeature: Order events\n\nBackground:\nGiven use connection \"local\"\n\n@smoke\nScenario: New order is accepted\nWhen produce message to topic \"orders\" value \"{ \\\"status\\\": \\\"NEW\\\" }\"\nThen assert last message where json \"$.status\" equals \"NEW\""
        },
        new HelpTopicViewModel
        {
            Title = "QA: Scenario Outline + Examples",
            Syntax = "Scenario Outline: <name with <column>> ... Examples: | col | ... |",
            Description = "Data-driven tests: every Examples row runs as its own scenario. <column> is replaced inside quotes, and can stand alone for numbers (within <wait> seconds).",
            Example = "Scenario Outline: Status <status> is accepted\nGiven use connection \"local\"\nWhen produce message to topic \"orders\" key \"<id>\" value \"{ \\\"status\\\": \\\"<status>\\\" }\"\nThen assert last message where json \"$.status\" equals \"<status>\"\n\nExamples:\n| id    | status    |\n| ORD-1 | NEW       |\n| ORD-2 | CONFIRMED |"
        },
        new HelpTopicViewModel
        {
            Title = "QA: expect no message (negative test)",
            Syntax = "expect no message on topic \"T\" within DURATION [where COND [and COND]...]",
            Description = "Passes if no matching message arrives during the whole window; fails as soon as one does. Put a 'watch' step before the trigger.",
            Example = "Given watch topic \"shipments\" from now\nWhen produce message to topic \"orders\" key \"ORD-9\" value \"{ \\\"status\\\": \\\"CANCELLED\\\" }\"\nThen expect no message on topic \"shipments\" within 5 seconds where key equals \"ORD-9\""
        },
        new HelpTopicViewModel
        {
            Title = "QA: expect N messages",
            Syntax = "expect [exactly|at least|at most] N messages on topic \"T\" within DURATION [where ...]",
            Description = "Counts matching messages. 'at least' passes as soon as N arrive; 'exactly' and 'at most' watch the whole window so extra messages are caught.",
            Example = "Given watch topic \"invoices\" from now\nWhen produce 3 messages to topic \"invoices\" value \"x\"\nThen expect exactly 3 messages on topic \"invoices\" within 5 seconds"
        },
        new HelpTopicViewModel
        {
            Title = "QA: assert last message",
            Syntax = "assert last message where COND [and COND]...  (key | value | json \"$.p\" | header \"H\")",
            Description = "Checks the last produced/received message. Comparators: equals, not equals, contains, not contains, matches, exists, not exists, greater than, less than (numbers and ISO dates).",
            Example = "Then assert last message where header \"trace-id\" exists and json \"$.amount\" greater than \"0\""
        },
        new HelpTopicViewModel
        {
            Title = "QA: validate against a JSON Schema",
            Syntax = "validate last message | each scanned message against schema \"\"\"{...}\"\"\" | schema file \"path\"",
            Description = "Contract testing: checks JSON payloads against a JSON Schema (inline, or a file relative to the script). Lists each violation, e.g. '$.currency: required field is missing'.",
            Example = "Then scan topic \"orders\" from beginning limit 1000\nAnd validate each scanned message against schema file \"contracts/order.schema.json\""
        },
        new HelpTopicViewModel
        {
            Title = "QA: test data",
            Syntax = "produce N messages to topic \"T\" ... with {{$index}}, {{$randomInt(a,b)}}, {{$pick(a,b)}}, {{$randomString(n)}}, {{$now(-1h)}}",
            Description = "Seeds a topic with N generated messages. Also: {{$randomDecimal(a,b)}}, {{$date(+1d)}}, {{$timestamp(-5m)}}, {{$uuid}}. The Producer's 'send N times' fills {{$index}} too.",
            Example = "When produce 100 messages to topic \"customers\" key \"CUST-{{$index}}\" value \"{ \\\"tier\\\": \\\"{{$pick(bronze,silver,gold)}}\\\", \\\"credit\\\": {{$randomInt(0,5000)}} }\""
        },
    };
}

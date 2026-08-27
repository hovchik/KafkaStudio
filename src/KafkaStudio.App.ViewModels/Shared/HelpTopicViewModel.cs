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
            Description = "Republishes the most recently seen message to a different topic.",
            Example = "Then rethrow last message to topic \"orders-fulfillment\" with key same header \"relayed-by\" to \"kafka-studio\""
        },
        new HelpTopicViewModel
        {
            Title = "Scan topic",
            Syntax = "scan topic \"T\" from beginning|end [limit N]",
            Description = "Bulk-reads a topic's backlog into the scenario's \"scanned messages\" list, stopping at limit or once caught up.",
            Example = "Then scan topic \"orders-dlq\" from beginning limit 500"
        },
        new HelpTopicViewModel
        {
            Title = "Acknowledge messages",
            Syntax = "acknowledge last message / acknowledge each scanned message",
            Description = "Commits the consumer offset for the last message, or for every message collected by the most recent scan.",
            Example = "Then acknowledge each scanned message"
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
    };
}

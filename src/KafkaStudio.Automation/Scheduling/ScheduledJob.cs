using KafkaStudio.Core.Abstractions;
using KafkaStudio.Scripting.Ast;

namespace KafkaStudio.Automation.Scheduling;

/// <summary>A KafScript Scenario/Task block registered with the <see cref="AutomationScheduler"/>,
/// bound to the connections it should run against.</summary>
public sealed class ScheduledJob
{
    public required string Id { get; init; }
    public required ScriptBlock Block { get; init; }
    public required IReadOnlyDictionary<string, IKafkaGateway> Connections { get; init; }
    public bool Enabled { get; set; } = true;

    public DateTimeOffset? LastRunAt { get; internal set; }
    public DateTimeOffset? NextRunAt { get; internal set; }
    public int RunCount { get; internal set; }
}

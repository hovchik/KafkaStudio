namespace KafkaStudio.Scripting.Ast;

/// <summary>Base type for everything a single step line can do.</summary>
public abstract record ScriptAction;

/// <summary>use connection "NAME" - selects which registered Kafka connection subsequent steps use.</summary>
public sealed record UseConnectionAction(string ConnectionName) : ScriptAction;

/// <summary>produce message to topic "T" [key "K"] [value V] [header "H" to "V"]*</summary>
public sealed record ProduceMessageAction(
    string Topic,
    string? Key,
    string? Value,
    IReadOnlyList<HeaderAssignment> Headers) : ScriptAction;

/// <summary>watch topic "T" [from beginning|end|now] - opens a live subscription for later steps.</summary>
public sealed record WatchTopicAction(string Topic, TopicPosition Position) : ScriptAction;

/// <summary>
/// expect message on topic "T" within DURATION [where ...]   (Then/And - asserting form)
/// a message arrives [on topic "T"] [within DURATION] [where ...]   (When - triggering form)
/// Both compile to this node; <see cref="IsAssertion"/> only affects how failures are reported.
/// </summary>
public sealed record AwaitMessageAction(
    string? Topic,
    Duration Duration,
    IReadOnlyList<Condition> Conditions,
    bool IsAssertion) : ScriptAction;

/// <summary>rethrow last message to topic "T" [with key same|"K"] [header "H" to "V"]*</summary>
public sealed record RethrowAction(
    string Topic,
    bool KeepSourceKey,
    string? KeyOverride,
    IReadOnlyList<HeaderAssignment> Headers) : ScriptAction;

/// <summary>scan topic "T" [from beginning|end] [limit N] - bulk-reads without committing per message.</summary>
public sealed record ScanTopicAction(string Topic, TopicPosition Position, int? Limit) : ScriptAction;

/// <summary>acknowledge last message | acknowledge each scanned message</summary>
public sealed record AcknowledgeAction(bool EachScanned) : ScriptAction;

public enum LogTarget { Key, Value, Message, Literal }

/// <summary>log key | log value | log message | log "literal text"</summary>
public sealed record LogAction(LogTarget Target, string? Literal) : ScriptAction;

/// <summary>set variable NAME to V</summary>
public sealed record SetVariableAction(string Name, string Value) : ScriptAction;

/// <summary>capture json "$.path" | key | value as NAME</summary>
public sealed record CaptureAction(ConditionField Source, string? JsonPath, string VariableName) : ScriptAction;

/// <summary>wait for DURATION</summary>
public sealed record WaitAction(Duration Duration) : ScriptAction;

/// <summary>assert VAR equals|contains "X" - checks a previously captured/set variable.</summary>
public sealed record AssertVariableAction(string VariableName, Comparator Comparator, string Expected) : ScriptAction;

namespace KafkaStudio.Scripting.Ast;

public enum StepKeyword { Given, When, Then, And, But }

public enum BlockKind { Scenario, Task }

/// <summary>How <c>expect N messages</c> compares the number of matching messages to N.</summary>
public enum CountMode { Exactly, AtLeast, AtMost }

public enum TopicPosition { Beginning, End, Now, Committed }

public enum TimeUnit { Milliseconds, Seconds, Minutes, Hours }

public sealed record Duration(double Value, TimeUnit Unit)
{
    public TimeSpan ToTimeSpan() => Unit switch
    {
        TimeUnit.Milliseconds => TimeSpan.FromMilliseconds(Value),
        TimeUnit.Seconds => TimeSpan.FromSeconds(Value),
        TimeUnit.Minutes => TimeSpan.FromMinutes(Value),
        TimeUnit.Hours => TimeSpan.FromHours(Value),
        _ => throw new ArgumentOutOfRangeException()
    };

    public override string ToString() => $"{Value} {Unit.ToString().ToLowerInvariant()}";
}

public enum ConditionField { Key, Value, Json, Header }

/// <summary>
/// How a condition compares a field to its expected value. <see cref="Exists"/>/<see cref="NotExists"/>
/// take no expected value; <see cref="GreaterThan"/>/<see cref="LessThan"/> compare numerically when
/// both sides are numbers, and as ISO dates/ordinal strings otherwise. New members are appended so
/// persisted Rethrow Rule filters keep their meaning.
/// </summary>
public enum Comparator { Equals, Contains, Matches, NotEquals, NotContains, GreaterThan, LessThan, Exists, NotExists }

/// <summary>
/// One "field comparator expected" check. <see cref="JsonPath"/> is set for <see cref="ConditionField.Json"/>,
/// <see cref="HeaderName"/> for <see cref="ConditionField.Header"/>.
/// </summary>
public sealed record Condition(ConditionField Field, string? JsonPath, Comparator Comparator, string Expected, string? HeaderName = null);

/// <summary>A header assignment used by produce/rethrow steps: header "H" to "V".</summary>
public sealed record HeaderAssignment(string Name, string Value);

public enum ScheduleKind { RunOnce, Every, At }

public sealed record ScheduleSpec(ScheduleKind Kind, Duration? Every = null, TimeOnly? At = null);

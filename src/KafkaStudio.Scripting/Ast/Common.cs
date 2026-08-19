namespace KafkaStudio.Scripting.Ast;

public enum StepKeyword { Given, When, Then, And, But }

public enum BlockKind { Scenario, Task }

public enum TopicPosition { Beginning, End, Now }

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

public enum ConditionField { Key, Value, Json }

public enum Comparator { Equals, Contains, Matches, NotEquals }

public sealed record Condition(ConditionField Field, string? JsonPath, Comparator Comparator, string Expected);

/// <summary>A header assignment used by produce/rethrow steps: header "H" to "V".</summary>
public sealed record HeaderAssignment(string Name, string Value);

public enum ScheduleKind { RunOnce, Every, At }

public sealed record ScheduleSpec(ScheduleKind Kind, Duration? Every = null, TimeOnly? At = null);

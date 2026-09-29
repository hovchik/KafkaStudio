using System.Collections.Concurrent;
using System.Globalization;
using System.Text.RegularExpressions;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Scripting.Ast;

namespace KafkaStudio.Scripting.Runtime;

/// <summary>
/// Evaluates KafScript <see cref="Condition"/>s ("key equals ...", "json "$.x" matches ...") against a
/// message. Shared by the script interpreter and the Rethrow engine so both always agree on semantics.
/// Regexes are cached and run with a timeout, so a pathological pattern evaluated against every
/// message of a busy topic can't hang a relay or a check.
/// </summary>
public static class ConditionEvaluator
{
    public static readonly TimeSpan RegexTimeout = TimeSpan.FromSeconds(1);

    private static readonly ConcurrentDictionary<string, Regex> RegexCache = new();

    /// <summary>Reads a field off a message. <paramref name="argument"/> is the JSON path for
    /// <see cref="ConditionField.Json"/> and the header name for <see cref="ConditionField.Header"/>.</summary>
    public static string? ReadField(KafkaMessage message, ConditionField field, string? argument) => field switch
    {
        ConditionField.Key => message.Key,
        ConditionField.Value => message.Value,
        ConditionField.Json => message.Value is null || argument is null
            ? null
            : JsonPathEvaluator.Evaluate(message.Value, argument),
        ConditionField.Header => argument is not null && message.Headers.TryGetValue(argument, out var header) ? header : null,
        _ => null
    };

    public static string? ReadField(KafkaMessage message, Condition condition) =>
        ReadField(message, condition.Field, condition.Field == ConditionField.Header ? condition.HeaderName : condition.JsonPath);

    /// <summary>The first condition <paramref name="message"/> fails, with the value it actually had -
    /// used to explain an assertion failure ("json "$.status" was "NEW", expected equals "PAID"").</summary>
    public static (Condition Condition, string? Actual)? FirstFailure(KafkaMessage message, IReadOnlyList<Condition> conditions, Func<string, string>? render = null)
    {
        foreach (var condition in conditions)
        {
            var actual = ReadField(message, condition);
            var expected = render is null ? condition.Expected : render(condition.Expected);
            if (!Compare(actual, condition.Comparator, expected)) return (condition with { Expected = expected }, actual);
        }
        return null;
    }

    /// <summary>True when <paramref name="message"/> satisfies every condition. <paramref name="render"/>
    /// optionally expands {{variables}} in the expected values first.</summary>
    public static bool Matches(KafkaMessage message, IReadOnlyList<Condition> conditions, Func<string, string>? render = null)
    {
        foreach (var condition in conditions)
        {
            var actual = ReadField(message, condition);
            var expected = render is null ? condition.Expected : render(condition.Expected);
            if (!Compare(actual, condition.Comparator, expected)) return false;
        }
        return true;
    }

    /// <summary>
    /// Compares <paramref name="actual"/> to <paramref name="expected"/>. An invalid or runaway regex
    /// raises a <see cref="KafScriptException"/> with a readable message (never a raw
    /// ArgumentException/RegexMatchTimeoutException).
    /// </summary>
    public static bool Compare(string? actual, Comparator comparator, string expected) => comparator switch
    {
        Comparator.Equals => actual == expected,
        Comparator.NotEquals => actual != expected,
        Comparator.Contains => actual is not null && actual.Contains(expected, StringComparison.Ordinal),
        Comparator.NotContains => actual is null || !actual.Contains(expected, StringComparison.Ordinal),
        Comparator.Matches => actual is not null && IsMatch(actual, expected),
        Comparator.Exists => actual is not null,
        Comparator.NotExists => actual is null,
        Comparator.GreaterThan => actual is not null && CompareOrdered(actual, expected) > 0,
        Comparator.LessThan => actual is not null && CompareOrdered(actual, expected) < 0,
        _ => false
    };

    /// <summary>Numbers compare numerically, ISO dates chronologically, anything else ordinally.</summary>
    public static int CompareOrdered(string actual, string expected)
    {
        if (double.TryParse(actual, NumberStyles.Float, CultureInfo.InvariantCulture, out var a) &&
            double.TryParse(expected, NumberStyles.Float, CultureInfo.InvariantCulture, out var b))
        {
            return a.CompareTo(b);
        }
        if (DateTimeOffset.TryParse(actual, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var da) &&
            DateTimeOffset.TryParse(expected, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var db))
        {
            return da.CompareTo(db);
        }
        return string.CompareOrdinal(actual, expected);
    }

    public static string Describe(Comparator comparator) => comparator switch
    {
        Comparator.Equals => "equals",
        Comparator.NotEquals => "not equals",
        Comparator.Contains => "contains",
        Comparator.NotContains => "not contains",
        Comparator.Matches => "matches",
        Comparator.Exists => "exists",
        Comparator.NotExists => "not exists",
        Comparator.GreaterThan => "greater than",
        Comparator.LessThan => "less than",
        _ => comparator.ToString()
    };

    /// <summary>A condition as KafScript source, e.g. <c>json "$.status" equals "PAID"</c>.</summary>
    public static string Describe(Condition condition)
    {
        var field = condition.Field switch
        {
            ConditionField.Json => $"json \"{condition.JsonPath}\"",
            ConditionField.Header => $"header \"{condition.HeaderName}\"",
            _ => condition.Field.ToString().ToLowerInvariant()
        };
        return condition.Comparator is Comparator.Exists or Comparator.NotExists
            ? $"{field} {Describe(condition.Comparator)}"
            : $"{field} {Describe(condition.Comparator)} \"{condition.Expected}\"";
    }

    /// <summary>Returns a problem description for an invalid regex, or null when it compiles.</summary>
    public static string? ValidatePattern(string pattern)
    {
        try
        {
            GetRegex(pattern);
            return null;
        }
        catch (ArgumentException ex)
        {
            return $"invalid regular expression \"{pattern}\": {ex.Message}";
        }
    }

    private static bool IsMatch(string input, string pattern)
    {
        Regex regex;
        try
        {
            regex = GetRegex(pattern);
        }
        catch (ArgumentException ex)
        {
            throw new KafScriptException($"invalid regular expression \"{pattern}\": {ex.Message}");
        }

        try
        {
            return regex.IsMatch(input);
        }
        catch (RegexMatchTimeoutException)
        {
            throw new KafScriptException(
                $"regular expression \"{pattern}\" took longer than {RegexTimeout.TotalSeconds:0.#}s to evaluate - simplify it");
        }
    }

    private static Regex GetRegex(string pattern)
    {
        if (RegexCache.TryGetValue(pattern, out var cached)) return cached;
        var regex = new Regex(pattern, RegexOptions.CultureInvariant, RegexTimeout);
        if (RegexCache.Count < 512) RegexCache[pattern] = regex;
        return regex;
    }
}

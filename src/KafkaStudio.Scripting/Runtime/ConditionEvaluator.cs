using System.Collections.Concurrent;
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

    public static string? ReadField(KafkaMessage message, ConditionField field, string? jsonPath) => field switch
    {
        ConditionField.Key => message.Key,
        ConditionField.Value => message.Value,
        ConditionField.Json => message.Value is null || jsonPath is null
            ? null
            : JsonPathEvaluator.Evaluate(message.Value, jsonPath),
        _ => null
    };

    /// <summary>True when <paramref name="message"/> satisfies every condition. <paramref name="render"/>
    /// optionally expands {{variables}} in the expected values first.</summary>
    public static bool Matches(KafkaMessage message, IReadOnlyList<Condition> conditions, Func<string, string>? render = null)
    {
        foreach (var condition in conditions)
        {
            var actual = ReadField(message, condition.Field, condition.JsonPath);
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
        Comparator.Matches => actual is not null && IsMatch(actual, expected),
        _ => false
    };

    public static string Describe(Comparator comparator) => comparator switch
    {
        Comparator.Equals => "equals",
        Comparator.NotEquals => "not equals",
        Comparator.Contains => "contains",
        Comparator.Matches => "matches",
        _ => comparator.ToString()
    };

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

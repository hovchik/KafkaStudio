using System.Globalization;
using System.Text.RegularExpressions;

namespace KafkaStudio.Scripting.Runtime;

/// <summary>
/// Substitutes "{{variableName}}" placeholders inside any KafScript string literal, plus a few dynamic
/// built-ins (prefixed with <c>$</c> so they can never collide with user variables), each evaluated
/// fresh per occurrence:
/// <list type="bullet">
/// <item><c>{{$uuid}}</c> - a new random GUID</item>
/// <item><c>{{$now}}</c> - current UTC time, ISO 8601</item>
/// <item><c>{{$timestamp}}</c> - current Unix time in milliseconds</item>
/// <item><c>{{$date}}</c> - current UTC date, yyyy-MM-dd</item>
/// <item><c>{{$random}}</c> - a random non-negative integer</item>
/// </list>
/// Unknown placeholders are left untouched so a typo is visible in the output instead of silently empty.
/// </summary>
public static partial class TemplateEngine
{
    [GeneratedRegex(@"\{\{\s*(\$?[A-Za-z_][A-Za-z0-9_]*)\s*\}\}")]
    private static partial Regex Placeholder();

    public static IReadOnlyList<string> BuiltInNames { get; } = new[] { "$uuid", "$now", "$timestamp", "$date", "$random" };

    public static string Render(string template, IReadOnlyDictionary<string, string> variables)
    {
        if (!template.Contains("{{", StringComparison.Ordinal)) return template;
        return Placeholder().Replace(template, m =>
        {
            var name = m.Groups[1].Value;
            if (name.StartsWith('$')) return BuiltIn(name) ?? m.Value;
            return variables.TryGetValue(name, out var value) ? value : m.Value;
        });
    }

    /// <summary>Renders only the built-ins (no user variables) - used by the ad-hoc Producer screen.</summary>
    public static string RenderBuiltIns(string template) =>
        Render(template, new Dictionary<string, string>());

    private static string? BuiltIn(string name) => name.ToLowerInvariant() switch
    {
        "$uuid" => Guid.NewGuid().ToString(),
        "$now" => DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture),
        "$timestamp" => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture),
        "$date" => DateTimeOffset.UtcNow.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        "$random" => Random.Shared.Next().ToString(CultureInfo.InvariantCulture),
        _ => null
    };
}

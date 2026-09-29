using System.Globalization;
using System.Text.RegularExpressions;

namespace KafkaStudio.Scripting.Runtime;

/// <summary>
/// Substitutes "{{variableName}}" placeholders inside any KafScript string literal, plus dynamic
/// built-ins (prefixed with <c>$</c> so they can never collide with user variables), each evaluated
/// fresh per occurrence:
/// <list type="bullet">
/// <item><c>{{$uuid}}</c> - a new random GUID</item>
/// <item><c>{{$now}}</c> - current UTC time, ISO 8601; <c>{{$now(-15m)}}</c> shifts it (ms/s/m/h/d)</item>
/// <item><c>{{$timestamp}}</c> - current Unix time in milliseconds; <c>{{$timestamp(+1h)}}</c> shifts it</item>
/// <item><c>{{$date}}</c> - current UTC date, yyyy-MM-dd; <c>{{$date(-1d)}}</c> shifts it</item>
/// <item><c>{{$random}}</c> - a random non-negative integer</item>
/// <item><c>{{$randomInt(1,100)}}</c> - a random integer in [min, max]</item>
/// <item><c>{{$randomDecimal(1,100)}}</c> - a random number in [min, max] with 2 decimals</item>
/// <item><c>{{$randomString(8)}}</c> - N random letters/digits</item>
/// <item><c>{{$pick(EUR,USD,GBP)}}</c> - one of the given values</item>
/// <item><c>{{$index}}</c> - the 1-based copy number inside "produce N messages" (and Producer's "send N times")</item>
/// </list>
/// Unknown placeholders are left untouched so a typo is visible in the output instead of silently empty.
/// </summary>
public static partial class TemplateEngine
{
    [GeneratedRegex(@"\{\{\s*(\$?[A-Za-z_][A-Za-z0-9_]*)(?:\(([^(){}]*)\))?\s*\}\}")]
    private static partial Regex Placeholder();

    [GeneratedRegex(@"^\s*([+-])\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)\s*$")]
    private static partial Regex Offset();

    private const string Alphanumerics = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public static IReadOnlyList<string> BuiltInNames { get; } = new[]
    {
        "$uuid", "$now", "$timestamp", "$date", "$random", "$randomInt", "$randomDecimal", "$randomString", "$pick", "$index"
    };

    public static string Render(string template, IReadOnlyDictionary<string, string> variables)
    {
        if (!template.Contains("{{", StringComparison.Ordinal)) return template;
        return Placeholder().Replace(template, m =>
        {
            var name = m.Groups[1].Value;
            var args = m.Groups[2].Success ? m.Groups[2].Value : null;
            if (name.StartsWith('$'))
            {
                // Context-provided values (e.g. $index) win over the generated ones.
                if (args is null && variables.TryGetValue(name, out var provided)) return provided;
                return BuiltIn(name, args) ?? m.Value;
            }
            return args is null && variables.TryGetValue(name, out var value) ? value : m.Value;
        });
    }

    /// <summary>Renders only the built-ins (no user variables) - used by the ad-hoc Producer screen.</summary>
    public static string RenderBuiltIns(string template) =>
        Render(template, new Dictionary<string, string>());

    /// <summary>Renders the built-ins with <c>{{$index}}</c> set - used by "send N times".</summary>
    public static string RenderBuiltIns(string template, int index) =>
        Render(template, new Dictionary<string, string> { ["$index"] = index.ToString(CultureInfo.InvariantCulture) });

    private static string? BuiltIn(string name, string? args)
    {
        var lower = name.ToLowerInvariant();
        if (args is null)
        {
            return lower switch
            {
                "$uuid" => Guid.NewGuid().ToString(),
                "$now" => DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture),
                "$timestamp" => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture),
                "$date" => DateTimeOffset.UtcNow.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                "$random" => Random.Shared.Next().ToString(CultureInfo.InvariantCulture),
                _ => null
            };
        }

        var parts = args.Split(',', StringSplitOptions.TrimEntries);
        switch (lower)
        {
            case "$now" or "$timestamp" or "$date":
                if (ParseOffset(args) is not { } offset) return null;
                var at = DateTimeOffset.UtcNow + offset;
                return lower switch
                {
                    "$now" => at.ToString("O", CultureInfo.InvariantCulture),
                    "$timestamp" => at.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture),
                    _ => at.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
                };
            case "$randomint":
                if (parts.Length != 2 || !long.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var min) ||
                    !long.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var max) || max < min || max == long.MaxValue)
                {
                    return null;
                }
                return Random.Shared.NextInt64(min, max + 1).ToString(CultureInfo.InvariantCulture);
            case "$randomdecimal":
                if (parts.Length != 2 || !double.TryParse(parts[0], NumberStyles.Float, CultureInfo.InvariantCulture, out var dmin) ||
                    !double.TryParse(parts[1], NumberStyles.Float, CultureInfo.InvariantCulture, out var dmax) || dmax < dmin)
                {
                    return null;
                }
                return Math.Round(dmin + Random.Shared.NextDouble() * (dmax - dmin), 2).ToString("0.00", CultureInfo.InvariantCulture);
            case "$randomstring":
                if (parts.Length != 1 || !int.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var length) ||
                    length < 1 || length > 10_000)
                {
                    return null;
                }
                return string.Create(length, 0, (span, _) =>
                {
                    for (var i = 0; i < span.Length; i++) span[i] = Alphanumerics[Random.Shared.Next(Alphanumerics.Length)];
                });
            case "$pick":
                return parts.Length == 0 || args.Trim().Length == 0 ? null : parts[Random.Shared.Next(parts.Length)];
            default:
                return null;
        }
    }

    private static TimeSpan? ParseOffset(string text)
    {
        var m = Offset().Match(text);
        if (!m.Success) return null;
        var amount = double.Parse(m.Groups[2].Value, CultureInfo.InvariantCulture) * (m.Groups[1].Value == "-" ? -1 : 1);
        return m.Groups[3].Value switch
        {
            "ms" => TimeSpan.FromMilliseconds(amount),
            "s" => TimeSpan.FromSeconds(amount),
            "m" => TimeSpan.FromMinutes(amount),
            "h" => TimeSpan.FromHours(amount),
            _ => TimeSpan.FromDays(amount)
        };
    }
}

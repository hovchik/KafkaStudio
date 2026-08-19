using System.Text.RegularExpressions;

namespace KafkaStudio.Scripting.Runtime;

/// <summary>Substitutes "{{variableName}}" placeholders inside any KafScript string literal.</summary>
public static partial class TemplateEngine
{
    [GeneratedRegex(@"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")]
    private static partial Regex Placeholder();

    public static string Render(string template, IReadOnlyDictionary<string, string> variables) =>
        Placeholder().Replace(template, m =>
            variables.TryGetValue(m.Groups[1].Value, out var value) ? value : m.Value);
}

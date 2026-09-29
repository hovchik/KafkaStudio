namespace KafkaStudio.Automation.Testing;

/// <summary>
/// A Cucumber-style tag filter such as <c>@smoke and not @wip</c> or <c>(@orders or @payments) and not @slow</c>.
/// Operators: <c>and</c>, <c>or</c>, <c>not</c>, parentheses; <c>and</c> binds tighter than <c>or</c>.
/// Tags match case-insensitively and the leading <c>@</c> is optional. A comma-separated list
/// (<c>@smoke, @api</c>) is accepted as shorthand for <c>or</c>.
/// </summary>
public sealed class TagExpression
{
    private readonly Func<ISet<string>, bool> _predicate;

    public string Text { get; }

    private TagExpression(string text, Func<ISet<string>, bool> predicate)
    {
        Text = text;
        _predicate = predicate;
    }

    /// <summary>Matches everything.</summary>
    public static TagExpression Any { get; } = new("", _ => true);

    public bool Matches(IEnumerable<string> tags) =>
        _predicate(new HashSet<string>(tags.Select(Normalize), StringComparer.OrdinalIgnoreCase));

    /// <summary>Parses <paramref name="text"/>; empty text gives <see cref="Any"/>.</summary>
    /// <exception cref="FormatException">The expression is malformed.</exception>
    public static TagExpression Parse(string? text)
    {
        if (string.IsNullOrWhiteSpace(text)) return Any;
        var tokens = Tokenize(text);
        var pos = 0;
        var predicate = ParseOr(tokens, ref pos);
        if (pos < tokens.Count) throw new FormatException($"unexpected '{tokens[pos]}' in tag filter");
        return new TagExpression(text.Trim(), predicate);
    }

    public static bool TryParse(string? text, out TagExpression expression, out string? error)
    {
        try
        {
            expression = Parse(text);
            error = null;
            return true;
        }
        catch (FormatException ex)
        {
            expression = Any;
            error = ex.Message;
            return false;
        }
    }

    public override string ToString() => Text;

    private static string Normalize(string tag) => tag.Trim().TrimStart('@');

    private static List<string> Tokenize(string text)
    {
        var tokens = new List<string>();
        var i = 0;
        while (i < text.Length)
        {
            var c = text[i];
            if (char.IsWhiteSpace(c)) { i++; continue; }
            if (c is '(' or ')')
            {
                tokens.Add(c.ToString());
                i++;
                continue;
            }
            if (c == ',')
            {
                tokens.Add("or");
                i++;
                continue;
            }
            var start = i;
            while (i < text.Length && !char.IsWhiteSpace(text[i]) && text[i] is not ('(' or ')' or ',')) i++;
            tokens.Add(text[start..i]);
        }
        return tokens;
    }

    private static bool IsOperator(string token, string op) => string.Equals(token, op, StringComparison.OrdinalIgnoreCase);

    private static Func<ISet<string>, bool> ParseOr(List<string> tokens, ref int pos)
    {
        var left = ParseAnd(tokens, ref pos);
        while (pos < tokens.Count && IsOperator(tokens[pos], "or"))
        {
            pos++;
            var right = ParseAnd(tokens, ref pos);
            var l = left;
            left = tags => l(tags) || right(tags);
        }
        return left;
    }

    private static Func<ISet<string>, bool> ParseAnd(List<string> tokens, ref int pos)
    {
        var left = ParseNot(tokens, ref pos);
        while (pos < tokens.Count && IsOperator(tokens[pos], "and"))
        {
            pos++;
            var right = ParseNot(tokens, ref pos);
            var l = left;
            left = tags => l(tags) && right(tags);
        }
        return left;
    }

    private static Func<ISet<string>, bool> ParseNot(List<string> tokens, ref int pos)
    {
        if (pos >= tokens.Count) throw new FormatException("tag filter ends too early - expected a tag");
        var token = tokens[pos];
        if (IsOperator(token, "not"))
        {
            pos++;
            var inner = ParseNot(tokens, ref pos);
            return tags => !inner(tags);
        }
        if (token == "(")
        {
            pos++;
            var inner = ParseOr(tokens, ref pos);
            if (pos >= tokens.Count || tokens[pos] != ")") throw new FormatException("missing ')' in tag filter");
            pos++;
            return inner;
        }
        if (token == ")" || IsOperator(token, "and") || IsOperator(token, "or"))
        {
            throw new FormatException($"expected a tag but found '{token}'");
        }
        pos++;
        var tag = Normalize(token);
        if (tag.Length == 0) throw new FormatException("empty tag in filter");
        return tags => tags.Contains(tag);
    }
}

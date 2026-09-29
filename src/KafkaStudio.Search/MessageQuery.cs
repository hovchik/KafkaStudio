using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Search;

/// <summary>How a plain (non-structured) search term is matched.</summary>
public enum TextMatchMode
{
    /// <summary>The field contains the term anywhere.</summary>
    Contains,
    /// <summary>The whole field equals the term (after trimming).</summary>
    Exact,
    /// <summary>The term appears as a whole word/token (not as part of a longer id).</summary>
    WholeWord,
    /// <summary>The term is a regular expression.</summary>
    Regex,
    /// <summary>The field, or a token in it, is similar to the term (typos, case, dashes, spaces).</summary>
    Fuzzy
}

/// <summary>Which parts of a message a plain search term is matched against.</summary>
[Flags]
public enum SearchFields
{
    None = 0,
    Key = 1,
    Value = 2,
    Headers = 4,
    All = Key | Value | Headers
}

/// <summary>Options that shape how a <see cref="MessageQuery"/> matches.</summary>
public sealed record QueryOptions
{
    public TextMatchMode Mode { get; init; } = TextMatchMode.Contains;
    public bool CaseSensitive { get; init; }
    public SearchFields Fields { get; init; } = SearchFields.All;

    /// <summary>Similarity threshold for <see cref="TextMatchMode.Fuzzy"/> and <c>similar to</c>.</summary>
    public double FuzzyThreshold { get; init; } = TextSimilarity.DefaultThreshold;

    public static QueryOptions Default { get; } = new();
}

public enum QueryComparator
{
    Equals, NotEquals, Contains, NotContains, Matches, StartsWith, EndsWith,
    Exists, Missing, SimilarTo, GreaterThan, GreaterOrEqual, LessThan, LessOrEqual
}

/// <summary>A node of a parsed structured query.</summary>
public abstract record QueryNode;
public sealed record QueryCondition(FieldSelector Field, QueryComparator Comparator, string? Expected) : QueryNode;
public sealed record QueryAnd(QueryNode Left, QueryNode Right) : QueryNode;
public sealed record QueryOr(QueryNode Left, QueryNode Right) : QueryNode;
public sealed record QueryNot(QueryNode Inner) : QueryNode;

/// <summary>
/// A message search: either a plain text term (matched per <see cref="QueryOptions"/>) or a structured
/// query written in the same condition language KafScript's <c>where</c> clauses use, extended for
/// searching:
/// <code>
/// json "$.order.id" equals "ORD-42" and header "source" equals "billing"
/// key starts with "ORD-" and not value contains "test"
/// $.amount &gt; 100 or ($.status = "FAILED" and $.retries >= 3)
/// any similar to "ORD-0042"
/// </code>
/// Fields: <c>key</c>, <c>value</c>, <c>json "$.path"</c> (or a bare <c>$.path</c>), <c>header "name"</c>,
/// <c>topic</c>, <c>partition</c>, <c>offset</c>, <c>any</c> (key, value or any header).
/// Comparators: <c>equals</c>/<c>=</c>, <c>not equals</c>/<c>!=</c>, <c>contains</c>, <c>not contains</c>,
/// <c>matches</c>/<c>~</c> (regex), <c>starts with</c>, <c>ends with</c>, <c>exists</c>, <c>missing</c>,
/// <c>similar to</c>, <c>&gt;</c>, <c>&gt;=</c>, <c>&lt;</c>, <c>&lt;=</c> (numeric when both sides are numbers).
/// Combine with <c>and</c>, <c>or</c>, <c>not</c> and parentheses (<c>and</c> binds tighter than <c>or</c>).
/// Values are quoted (<c>"..."</c> or <c>'...'</c>) or a single bare word.
/// </summary>
public sealed class MessageQuery
{
    private static readonly ConcurrentDictionary<(string, bool), Regex> RegexCache = new();
    private static readonly TimeSpan RegexTimeout = TimeSpan.FromSeconds(1);

    public string Text { get; }
    public QueryOptions Options { get; }

    /// <summary>The parsed structured query, or null for a plain-text term.</summary>
    public QueryNode? Root { get; }

    public bool IsStructured => Root is not null;

    /// <summary>Plain term (trimmed) when not structured.</summary>
    public string? Term { get; }

    private readonly Regex? _termRegex;

    private MessageQuery(string text, QueryOptions options, QueryNode? root, string? term)
    {
        Text = text;
        Options = options;
        Root = root;
        Term = term;
        if (root is null && term is not null)
        {
            _termRegex = options.Mode switch
            {
                TextMatchMode.Regex => GetRegex(term, options.CaseSensitive),
                TextMatchMode.WholeWord => GetRegex($@"(?<![\w-]){Regex.Escape(term)}(?![\w-])", options.CaseSensitive),
                _ => null
            };
        }
    }

    /// <summary>Parses a query; throws <see cref="QueryParseException"/> with a readable message when the
    /// text is clearly meant as a structured query but is malformed, or the regex is invalid.</summary>
    public static MessageQuery Parse(string text, QueryOptions? options = null)
    {
        options ??= QueryOptions.Default;
        var trimmed = (text ?? "").Trim();
        if (trimmed.Length == 0) throw new QueryParseException("the search is empty");

        List<Token> tokens;
        try
        {
            tokens = Tokenize(trimmed);
        }
        catch (QueryParseException) when (!LooksStructured(trimmed))
        {
            return Plain(trimmed, options);
        }

        if (LooksStructured(tokens))
        {
            var parser = new Parser(tokens, options);
            var root = parser.ParseAll();
            return new MessageQuery(trimmed, options, root, null);
        }

        // A single quoted value searches for the text literally (a way to search for "key equals" itself).
        if (tokens.Count == 1 && tokens[0].Kind == TokenKind.String && tokens[0].Text.Length > 0)
        {
            return Plain(tokens[0].Text, options);
        }
        return Plain(trimmed, options);
    }

    public static bool TryParse(string text, QueryOptions? options, out MessageQuery? query, out string? error)
    {
        try
        {
            query = Parse(text, options);
            error = null;
            return true;
        }
        catch (QueryParseException ex)
        {
            query = null;
            error = ex.Message;
            return false;
        }
    }

    /// <summary>Builds a structured query from nodes (used by "find similar", field stats and traces).</summary>
    public static MessageQuery FromNode(QueryNode root, QueryOptions? options = null) =>
        new(Describe(root), options ?? QueryOptions.Default, root, null);

    /// <summary>ANDs several conditions together (null when the list is empty).</summary>
    public static QueryNode? AllOf(IEnumerable<QueryNode> nodes)
    {
        QueryNode? result = null;
        foreach (var node in nodes) result = result is null ? node : new QueryAnd(result, node);
        return result;
    }

    private static MessageQuery Plain(string term, QueryOptions options)
    {
        if (options.Mode == TextMatchMode.Regex) ValidateRegex(term, options.CaseSensitive);
        return new MessageQuery(term, options, null, term);
    }

    // ------------------------------------------------------------------ matching ----

    public bool Matches(KafkaMessage message) =>
        Root is not null ? Evaluate(Root, message) : MatchesPlain(message);

    private bool MatchesPlain(KafkaMessage message)
    {
        var fields = Options.Fields == SearchFields.None ? SearchFields.All : Options.Fields;
        if (fields.HasFlag(SearchFields.Key) && message.Key is not null && MatchesTerm(message.Key)) return true;
        if (fields.HasFlag(SearchFields.Value) && message.Value is not null && MatchesTerm(message.Value)) return true;
        if (fields.HasFlag(SearchFields.Headers))
        {
            foreach (var (name, value) in message.Headers)
            {
                if (MatchesTerm(value)) return true;
                // Header names only count for "contains"-style searches, where finding the header itself is useful.
                if (Options.Mode is TextMatchMode.Contains && MatchesTerm(name)) return true;
            }
        }
        return false;
    }

    private bool MatchesTerm(string text)
    {
        var term = Term!;
        var comparison = Options.CaseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;
        return Options.Mode switch
        {
            TextMatchMode.Contains => text.Contains(term, comparison),
            TextMatchMode.Exact => string.Equals(text.Trim(), term, comparison),
            TextMatchMode.WholeWord or TextMatchMode.Regex => SafeIsMatch(_termRegex!, text),
            TextMatchMode.Fuzzy => IsFuzzyMatch(text, term, Options.FuzzyThreshold),
            _ => false
        };
    }

    /// <summary>True when the whole text, or any token in it, is similar to <paramref name="term"/>.</summary>
    public static bool IsFuzzyMatch(string text, string term, double threshold)
    {
        if (TextSimilarity.IsSimilar(text, term, threshold)) return true;
        if (text.Length <= term.Length) return false;
        foreach (var token in TextSimilarity.Tokenize(text))
        {
            if (Math.Abs(token.Length - term.Length) > term.Length) continue;
            if (TextSimilarity.IsSimilar(token, term, threshold)) return true;
        }
        return false;
    }

    private bool Evaluate(QueryNode node, KafkaMessage message) => node switch
    {
        QueryAnd and => Evaluate(and.Left, message) && Evaluate(and.Right, message),
        QueryOr or => Evaluate(or.Left, message) || Evaluate(or.Right, message),
        QueryNot not => !Evaluate(not.Inner, message),
        QueryCondition condition => EvaluateCondition(condition, message),
        _ => false
    };

    private bool EvaluateCondition(QueryCondition condition, KafkaMessage message)
    {
        if (condition.Field.Kind == FieldKind.Any)
        {
            // Negative comparators on "any" mean "none of the fields ...".
            if (condition.Comparator is QueryComparator.NotEquals or QueryComparator.NotContains or QueryComparator.Missing)
            {
                var positive = condition with { Comparator = Positive(condition.Comparator) };
                return !condition.Field.ReadAll(message).Any(v => Compare(v, positive.Comparator, positive.Expected, true));
            }
            return condition.Field.ReadAll(message).Any(v => Compare(v, condition.Comparator, condition.Expected, true));
        }

        return Compare(condition.Field.Read(message), condition.Comparator, condition.Expected, false);
    }

    private static QueryComparator Positive(QueryComparator c) => c switch
    {
        QueryComparator.NotEquals => QueryComparator.Equals,
        QueryComparator.NotContains => QueryComparator.Contains,
        QueryComparator.Missing => QueryComparator.Exists,
        _ => c
    };

    private bool Compare(string? actual, QueryComparator comparator, string? expected, bool tokenizeForSimilarity)
    {
        var comparison = Options.CaseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;
        expected ??= "";
        switch (comparator)
        {
            case QueryComparator.Exists: return actual is not null;
            case QueryComparator.Missing: return actual is null;
            case QueryComparator.NotEquals: return actual is null || !string.Equals(actual, expected, comparison);
            case QueryComparator.NotContains: return actual is null || !actual.Contains(expected, comparison);
        }

        if (actual is null) return false;
        return comparator switch
        {
            QueryComparator.Equals => string.Equals(actual, expected, comparison),
            QueryComparator.Contains => actual.Contains(expected, comparison),
            QueryComparator.StartsWith => actual.StartsWith(expected, comparison),
            QueryComparator.EndsWith => actual.EndsWith(expected, comparison),
            QueryComparator.Matches => SafeIsMatch(GetRegex(expected, Options.CaseSensitive), actual),
            QueryComparator.SimilarTo => tokenizeForSimilarity
                ? IsFuzzyMatch(actual, expected, Options.FuzzyThreshold)
                : TextSimilarity.IsSimilar(actual, expected, Options.FuzzyThreshold),
            QueryComparator.GreaterThan => CompareOrdered(actual, expected, comparison) > 0,
            QueryComparator.GreaterOrEqual => CompareOrdered(actual, expected, comparison) >= 0,
            QueryComparator.LessThan => CompareOrdered(actual, expected, comparison) < 0,
            QueryComparator.LessOrEqual => CompareOrdered(actual, expected, comparison) <= 0,
            _ => false
        };
    }

    /// <summary>Numeric comparison when both sides are numbers, otherwise ordinal text comparison
    /// (which also orders ISO-8601 timestamps correctly).</summary>
    private static int CompareOrdered(string actual, string expected, StringComparison comparison)
    {
        if (double.TryParse(actual, NumberStyles.Float, CultureInfo.InvariantCulture, out var a) &&
            double.TryParse(expected, NumberStyles.Float, CultureInfo.InvariantCulture, out var b))
        {
            return a.CompareTo(b);
        }
        return string.Compare(actual, expected, comparison);
    }

    private static bool SafeIsMatch(Regex regex, string input)
    {
        try
        {
            return regex.IsMatch(input);
        }
        catch (RegexMatchTimeoutException)
        {
            return false;
        }
    }

    private static Regex GetRegex(string pattern, bool caseSensitive)
    {
        if (RegexCache.TryGetValue((pattern, caseSensitive), out var cached)) return cached;
        var options = RegexOptions.CultureInvariant | (caseSensitive ? RegexOptions.None : RegexOptions.IgnoreCase);
        Regex regex;
        try
        {
            regex = new Regex(pattern, options, RegexTimeout);
        }
        catch (ArgumentException ex)
        {
            throw new QueryParseException($"invalid regular expression \"{pattern}\": {ex.Message}");
        }
        if (RegexCache.Count < 512) RegexCache[(pattern, caseSensitive)] = regex;
        return regex;
    }

    private static void ValidateRegex(string pattern, bool caseSensitive) => GetRegex(pattern, caseSensitive);

    // ------------------------------------------------------------------ describe ----

    public override string ToString() => Text;

    /// <summary>Renders a node back to query text (round-trips through <see cref="Parse"/>).</summary>
    public static string Describe(QueryNode node) => node switch
    {
        QueryAnd and => $"{DescribeOperand(and.Left, node)} and {DescribeOperand(and.Right, node)}",
        QueryOr or => $"{Describe(or.Left)} or {Describe(or.Right)}",
        QueryNot not => $"not {DescribeOperand(not.Inner, node)}",
        QueryCondition c => c.Comparator is QueryComparator.Exists or QueryComparator.Missing
            ? $"{c.Field} {ComparatorText(c.Comparator)}"
            : $"{c.Field} {ComparatorText(c.Comparator)} {Quote(c.Expected ?? "")}",
        _ => ""
    };

    private static string DescribeOperand(QueryNode child, QueryNode parent) =>
        child is QueryOr || (parent is QueryNot && child is QueryAnd) ? $"({Describe(child)})" : Describe(child);

    public static string ComparatorText(QueryComparator c) => c switch
    {
        QueryComparator.Equals => "equals",
        QueryComparator.NotEquals => "not equals",
        QueryComparator.Contains => "contains",
        QueryComparator.NotContains => "not contains",
        QueryComparator.Matches => "matches",
        QueryComparator.StartsWith => "starts with",
        QueryComparator.EndsWith => "ends with",
        QueryComparator.Exists => "exists",
        QueryComparator.Missing => "missing",
        QueryComparator.SimilarTo => "similar to",
        QueryComparator.GreaterThan => ">",
        QueryComparator.GreaterOrEqual => ">=",
        QueryComparator.LessThan => "<",
        QueryComparator.LessOrEqual => "<=",
        _ => c.ToString()
    };

    /// <summary>Double-quotes a value for query text, escaping backslashes and quotes.</summary>
    public static string Quote(string value) => "\"" + value.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";

    // ------------------------------------------------------------------ tokenizer ----

    private enum TokenKind { Word, String, Symbol, LParen, RParen, End }

    private readonly record struct Token(TokenKind Kind, string Text, int Position)
    {
        public bool IsWord(string word) => Kind == TokenKind.Word && string.Equals(Text, word, StringComparison.OrdinalIgnoreCase);
    }

    private static readonly HashSet<string> FieldWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "key", "value", "json", "header", "topic", "partition", "offset", "any"
    };

    private static readonly HashSet<string> ComparatorStartWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "equals", "not", "contains", "matches", "starts", "ends", "exists", "missing", "similar", "is"
    };

    private static bool LooksStructured(string text)
    {
        var first = text.TrimStart();
        if (first.StartsWith('$') || first.StartsWith('(')) return true;
        var word = new string(first.TakeWhile(c => char.IsLetter(c)).ToArray());
        return FieldWords.Contains(word) && first.Length > word.Length && char.IsWhiteSpace(first[word.Length]);
    }

    /// <summary>A query is structured when it starts with "(", "not (", a $.path, or a field word followed
    /// by something that can only be a comparator (or a name, for json/header).</summary>
    private static bool LooksStructured(List<Token> tokens)
    {
        var i = 0;
        while (i < tokens.Count && (tokens[i].Kind == TokenKind.LParen || tokens[i].IsWord("not"))) i++;
        if (i >= tokens.Count) return false;
        if (i > 0 && tokens[0].Kind == TokenKind.LParen) return true;

        var first = tokens[i];
        if (first.Kind == TokenKind.Word && first.Text.StartsWith('$')) return true;
        if (first.Kind != TokenKind.Word || !FieldWords.Contains(first.Text)) return false;

        static bool IsComparatorStart(Token t) =>
            t.Kind == TokenKind.Symbol || (t.Kind == TokenKind.Word && ComparatorStartWords.Contains(t.Text));

        var next = i + 1 < tokens.Count ? tokens[i + 1] : default;
        if (first.IsWord("json") || first.IsWord("header"))
        {
            if (next.Kind == TokenKind.String || (next.Kind == TokenKind.Word && next.Text.StartsWith('$'))) return true;
            // header source = billing (unquoted name followed by a comparison)
            return next.Kind == TokenKind.Word && i + 2 < tokens.Count && IsComparatorStart(tokens[i + 2]);
        }
        return IsComparatorStart(next);
    }

    private static List<Token> Tokenize(string text)
    {
        var tokens = new List<Token>();
        var i = 0;
        while (i < text.Length)
        {
            var c = text[i];
            if (char.IsWhiteSpace(c)) { i++; continue; }

            if (c == '(') { tokens.Add(new Token(TokenKind.LParen, "(", i)); i++; continue; }
            if (c == ')') { tokens.Add(new Token(TokenKind.RParen, ")", i)); i++; continue; }

            if (c is '"' or '\'')
            {
                var start = i;
                var sb = new StringBuilder();
                i++;
                var closed = false;
                while (i < text.Length)
                {
                    if (text[i] == '\\' && i + 1 < text.Length && (text[i + 1] == c || text[i + 1] == '\\'))
                    {
                        sb.Append(text[i + 1]);
                        i += 2;
                        continue;
                    }
                    if (text[i] == c) { closed = true; i++; break; }
                    sb.Append(text[i]);
                    i++;
                }
                if (!closed) throw new QueryParseException($"unterminated quoted value starting at position {start + 1}");
                tokens.Add(new Token(TokenKind.String, sb.ToString(), start));
                continue;
            }

            if (c is '=' or '!' or '<' or '>' or '~')
            {
                var start = i;
                var symbol = c.ToString();
                if (i + 1 < text.Length && text[i + 1] == '=' && c is '=' or '!' or '<' or '>')
                {
                    symbol += "=";
                    i++;
                }
                i++;
                tokens.Add(new Token(TokenKind.Symbol, symbol, start));
                continue;
            }

            {
                var start = i;
                if (c == '$')
                {
                    // A json path: read until whitespace/operator, but keep bracket-quoted names intact.
                    while (i < text.Length && !char.IsWhiteSpace(text[i]) && text[i] is not ('=' or '!' or '<' or '>' or '~' or ')'))
                    {
                        if (text[i] == '[')
                        {
                            var close = text.IndexOf(']', i);
                            i = close < 0 ? text.Length : close + 1;
                            continue;
                        }
                        i++;
                    }
                }
                else
                {
                    while (i < text.Length && !char.IsWhiteSpace(text[i]) && text[i] is not ('(' or ')' or '=' or '!' or '<' or '>' or '~' or '"'))
                    {
                        i++;
                    }
                }
                tokens.Add(new Token(TokenKind.Word, text[start..i], start));
            }
        }
        return tokens;
    }

    // ------------------------------------------------------------------ parser ----

    private sealed class Parser
    {
        private readonly List<Token> _tokens;
        private readonly QueryOptions _options;
        private int _pos;

        public Parser(List<Token> tokens, QueryOptions options)
        {
            _tokens = tokens;
            _options = options;
        }

        private Token Current => _pos < _tokens.Count ? _tokens[_pos] : new Token(TokenKind.End, "", -1);

        private QueryParseException Error(string message)
        {
            var at = Current.Kind == TokenKind.End ? "at the end" : $"at '{Current.Text}'";
            return new QueryParseException($"{message} ({at})");
        }

        private bool AcceptWord(string word)
        {
            if (!Current.IsWord(word)) return false;
            _pos++;
            return true;
        }

        public QueryNode ParseAll()
        {
            var node = ParseOr();
            if (Current.Kind != TokenKind.End) throw Error("expected 'and', 'or' or the end of the query");
            return node;
        }

        private QueryNode ParseOr()
        {
            var node = ParseAnd();
            while (AcceptWord("or")) node = new QueryOr(node, ParseAnd());
            return node;
        }

        private QueryNode ParseAnd()
        {
            var node = ParseUnary();
            while (AcceptWord("and")) node = new QueryAnd(node, ParseUnary());
            return node;
        }

        private QueryNode ParseUnary()
        {
            if (AcceptWord("not")) return new QueryNot(ParseUnary());
            if (Current.Kind == TokenKind.LParen)
            {
                _pos++;
                var inner = ParseOr();
                if (Current.Kind != TokenKind.RParen) throw Error("expected ')'");
                _pos++;
                return inner;
            }
            return ParseCondition();
        }

        private QueryNode ParseCondition()
        {
            var field = ParseField();
            var comparator = ParseComparator();
            string? expected = null;
            if (comparator is not (QueryComparator.Exists or QueryComparator.Missing))
            {
                expected = ParseValue();
                if (comparator == QueryComparator.Matches) ValidateRegex(expected, _options.CaseSensitive);
            }
            return new QueryCondition(field, comparator, expected);
        }

        private FieldSelector ParseField()
        {
            var token = Current;
            if (token.Kind == TokenKind.Word && token.Text.StartsWith('$'))
            {
                _pos++;
                return Json(token.Text);
            }
            if (AcceptWord("key")) return FieldSelector.Key;
            if (AcceptWord("value")) return FieldSelector.Value;
            if (AcceptWord("topic")) return new FieldSelector(FieldKind.Topic);
            if (AcceptWord("partition")) return new FieldSelector(FieldKind.Partition);
            if (AcceptWord("offset")) return new FieldSelector(FieldKind.Offset);
            if (AcceptWord("any")) return FieldSelector.Any;
            if (AcceptWord("json"))
            {
                var path = Current;
                if (path.Kind is TokenKind.String or TokenKind.Word)
                {
                    _pos++;
                    return Json(path.Text);
                }
                throw Error("expected a json path after 'json', e.g. json \"$.order.id\"");
            }
            if (AcceptWord("header"))
            {
                var name = Current;
                if (name.Kind is TokenKind.String or TokenKind.Word)
                {
                    _pos++;
                    return FieldSelector.Header(name.Text);
                }
                throw Error("expected a header name after 'header', e.g. header \"trace-id\"");
            }
            throw Error("expected a field: key, value, json \"$.path\", $.path, header \"name\", topic, partition, offset or any");
        }

        private FieldSelector Json(string path)
        {
            if (!FieldSelector.TryParse(path.StartsWith('$') ? path : "$." + path, out var field, out var error))
            {
                throw new QueryParseException(error!);
            }
            return field;
        }

        private QueryComparator ParseComparator()
        {
            var token = Current;
            if (token.Kind == TokenKind.Symbol)
            {
                _pos++;
                return token.Text switch
                {
                    "=" or "==" => QueryComparator.Equals,
                    "!=" => QueryComparator.NotEquals,
                    "~" => QueryComparator.Matches,
                    ">" => QueryComparator.GreaterThan,
                    ">=" => QueryComparator.GreaterOrEqual,
                    "<" => QueryComparator.LessThan,
                    "<=" => QueryComparator.LessOrEqual,
                    _ => throw new QueryParseException($"unknown operator '{token.Text}'")
                };
            }

            if (AcceptWord("equals")) return QueryComparator.Equals;
            if (AcceptWord("contains")) return QueryComparator.Contains;
            if (AcceptWord("matches")) return QueryComparator.Matches;
            if (AcceptWord("exists")) return QueryComparator.Exists;
            if (AcceptWord("missing")) return QueryComparator.Missing;
            if (AcceptWord("starts")) { ExpectWord("with"); return QueryComparator.StartsWith; }
            if (AcceptWord("ends")) { ExpectWord("with"); return QueryComparator.EndsWith; }
            if (AcceptWord("similar")) { ExpectWord("to"); return QueryComparator.SimilarTo; }
            if (AcceptWord("is"))
            {
                if (AcceptWord("missing")) return QueryComparator.Missing;
                if (AcceptWord("not")) { ExpectWord("missing"); return QueryComparator.Exists; }
                throw Error("expected 'is missing' or 'is not missing'");
            }
            if (AcceptWord("not"))
            {
                if (AcceptWord("equals")) return QueryComparator.NotEquals;
                if (AcceptWord("contains")) return QueryComparator.NotContains;
                throw Error("expected 'not equals' or 'not contains'");
            }
            throw Error("expected a comparison: equals, not equals, contains, not contains, matches, starts with, ends with, similar to, exists, missing, =, !=, ~, >, <");
        }

        private void ExpectWord(string word)
        {
            if (!AcceptWord(word)) throw Error($"expected '{word}'");
        }

        private string ParseValue()
        {
            var token = Current;
            if (token.Kind is TokenKind.String or TokenKind.Word &&
                !(token.Kind == TokenKind.Word && (token.IsWord("and") || token.IsWord("or"))))
            {
                _pos++;
                return token.Text;
            }
            throw Error("expected a value (quote it if it contains spaces)");
        }
    }
}

public sealed class QueryParseException : Exception
{
    public QueryParseException(string message) : base(message)
    {
    }
}

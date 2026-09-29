using System.Globalization;
using System.Text;
using KafkaStudio.Scripting.Ast;
using KafkaStudio.Scripting.Lexing;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.Scripting.Parsing;

/// <summary>
/// Recursive-descent parser for KafScript. The grammar is intentionally fixed-order per action (not
/// free word-order English) so it stays unambiguous and cheap to parse, while still reading like plain
/// sentences - see docs/kafscript-language.md for the full grammar reference with examples of every
/// step form (produce, watch, expect/arrives, rethrow, scan, acknowledge, log, set, capture, wait,
/// assert, use connection).
/// </summary>
public sealed class Parser
{
    private static readonly Duration DefaultArrivalTimeout = new(30, TimeUnit.Seconds);

    private readonly List<Token> _tokens;
    private int _pos;

    private Parser(List<Token> tokens)
    {
        _tokens = tokens;
    }

    public static ScriptDocument Parse(string source)
    {
        var tokens = Lexer.Tokenize(source);
        return new Parser(tokens).ParseDocument();
    }

    private Token Current => _tokens[_pos];

    private bool AtEof => Current.Type == TokenType.Eof;

    private void Advance() => _pos = Math.Min(_pos + 1, _tokens.Count - 1);

    private bool IsWord(string text) =>
        Current.Type == TokenType.Word && string.Equals(Current.Text, text, StringComparison.OrdinalIgnoreCase);

    private bool AcceptWord(string text)
    {
        if (!IsWord(text)) return false;
        Advance();
        return true;
    }

    private void ExpectWord(string text)
    {
        if (!AcceptWord(text))
        {
            throw Error($"expected '{text}' but found {Describe(Current)}");
        }
    }

    private string ExpectWordText()
    {
        if (Current.Type != TokenType.Word)
        {
            throw Error($"expected an identifier but found {Describe(Current)}");
        }
        var text = Current.Text;
        Advance();
        return text;
    }

    private string ExpectString()
    {
        if (Current.Type is not (TokenType.String or TokenType.DocString))
        {
            throw Error($"expected a quoted value but found {Describe(Current)}");
        }
        var text = Current.Text;
        Advance();
        return text;
    }

    private double ExpectNumber()
    {
        if (Current.Type != TokenType.Number)
        {
            throw Error($"expected a number but found {Describe(Current)}");
        }
        if (!double.TryParse(Current.Text, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out var value) ||
            double.IsInfinity(value))
        {
            throw Error($"'{Current.Text}' is not a valid number");
        }
        Advance();
        return value;
    }

    private int ExpectWholeNumber(string what, int min, int max)
    {
        var line = Current;
        var value = ExpectNumber();
        if (value != Math.Floor(value) || value < min || value > max)
        {
            throw new KafScriptException($"{what} must be a whole number between {min} and {max}, got '{line.Text}'", line.Line);
        }
        return (int)value;
    }

    private void ExpectEndOfLine()
    {
        if (Current.Type is TokenType.Newline or TokenType.Eof)
        {
            if (Current.Type == TokenType.Newline) Advance();
            return;
        }
        throw Error($"expected end of line but found {Describe(Current)}");
    }

    private void SkipNewlines()
    {
        while (Current.Type == TokenType.Newline) Advance();
    }

    private KafScriptException Error(string message) => new(message, Current.Line);

    private static string Describe(Token token) => token.Type switch
    {
        TokenType.Eof => "end of file",
        TokenType.Newline => "end of line",
        TokenType.String or TokenType.DocString => $"\"{Truncate(token.Text)}\"",
        TokenType.Placeholder => $"placeholder <{token.Text}> (placeholders only work inside a Scenario Outline)",
        TokenType.Tag => $"tag @{token.Text} (tags go on the line above a Scenario/Task)",
        TokenType.TableRow => "a table row",
        _ => $"'{token.Text}'"
    };

    private static string Truncate(string s) => s.Length > 30 ? s[..30] + "…" : s;

    // ------------------------------------------------------------------ document / block ----

    private ScriptDocument ParseDocument()
    {
        SkipNewlines();
        var blocks = new List<ScriptBlock>();
        IReadOnlyList<string> featureTags = Array.Empty<string>();
        IReadOnlyList<Step> background = Array.Empty<Step>();
        string? featureName = null;

        var tags = ParseTags();
        if (IsWord("feature"))
        {
            Advance();
            if (Current.Type == TokenType.Colon) Advance();
            featureName = ParseFreeTextToEndOfLine();
            featureTags = tags;
            SkipNewlines();
            tags = ParseTags();
        }

        if (IsWord("background"))
        {
            if (tags.Count > 0) throw Error("tags can't be put on a Background - put them on the Feature or the Scenarios");
            background = ParseBackground();
            SkipNewlines();
            tags = ParseTags();
        }

        while (!AtEof)
        {
            var allTags = featureTags.Concat(tags).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            if (IsWord("background"))
            {
                throw Error("a Background must come before the first Scenario/Task, and a file can only have one");
            }
            if (IsWord("feature"))
            {
                throw Error("'Feature:' must be the first line of the file (and there can only be one)");
            }
            if (IsWord("scenario") && IsNextWord("outline", "template"))
            {
                blocks.AddRange(ParseOutline(allTags, background));
            }
            else
            {
                var block = ParseBlock();
                blocks.Add(block with { Steps = background.Concat(block.Steps).ToList(), Tags = allTags });
            }
            SkipNewlines();
            tags = ParseTags();
            if (tags.Count > 0 && AtEof) throw Error("tags must be followed by a Scenario or Task");
        }
        return new ScriptDocument(blocks) { Background = background, FeatureName = featureName };
    }

    /// <summary>Reads any run of "@tag" tokens (possibly over several lines) in front of a block.</summary>
    private List<string> ParseTags()
    {
        var tags = new List<string>();
        while (Current.Type == TokenType.Tag)
        {
            tags.Add(Current.Text);
            Advance();
            SkipNewlines();
        }
        return tags;
    }

    private bool IsNextWord(params string[] words)
    {
        var next = _pos + 1 < _tokens.Count ? _tokens[_pos + 1] : null;
        return next is { Type: TokenType.Word } &&
               words.Any(w => string.Equals(next.Text, w, StringComparison.OrdinalIgnoreCase));
    }

    private IReadOnlyList<Step> ParseBackground()
    {
        ExpectWord("background");
        if (Current.Type == TokenType.Colon) Advance();
        ParseFreeTextToEndOfLine(); // an optional description, like Gherkin's
        SkipNewlines();

        var steps = new List<Step>();
        while (IsStepKeyword())
        {
            steps.Add(ParseStep());
            SkipNewlines();
        }
        if (steps.Count == 0) throw Error("a Background needs at least one Given/When/Then step");
        ThrowUnlessBlockBoundary();
        return steps;
    }

    private ScriptBlock ParseBlock()
    {
        var line = Current.Line;
        BlockKind kind;
        if (AcceptWord("scenario")) kind = BlockKind.Scenario;
        else if (AcceptWord("task")) kind = BlockKind.Task;
        else if (IsWord("examples")) throw Error("'Examples:' only belongs under a 'Scenario Outline:'");
        else throw Error($"expected 'Scenario' or 'Task' but found {Describe(Current)}");

        if (Current.Type == TokenType.Colon) Advance();

        var name = ParseFreeTextToEndOfLine();
        SkipNewlines();

        ScheduleSpec? schedule = null;
        if (IsWord("schedule"))
        {
            schedule = ParseSchedule();
            ExpectEndOfLine();
            SkipNewlines();
        }

        if (string.IsNullOrEmpty(name))
        {
            throw new KafScriptException($"{kind} needs a name, e.g. '{kind}: My check'", line);
        }

        var steps = new List<Step>();
        while (IsStepKeyword())
        {
            steps.Add(ParseStep());
            SkipNewlines();
        }

        ThrowUnlessBlockBoundary();
        return new ScriptBlock(kind, name, schedule, steps, line);
    }

    /// <summary>
    /// Anything that's neither another step nor the start of the next block is almost always a
    /// mistyped step keyword - say so, instead of the confusing "expected 'Scenario' or 'Task'".
    /// </summary>
    private void ThrowUnlessBlockBoundary()
    {
        if (AtEof || IsWord("scenario") || IsWord("task") || Current.Type == TokenType.Tag || IsWord("background") || IsWord("feature")) return;
        if (IsWord("schedule"))
        {
            throw Error("'schedule' must come directly after the Task/Scenario name line, before any steps");
        }
        if (IsWord("examples"))
        {
            throw Error("'Examples:' only belongs under a 'Scenario Outline:' (this block is a plain Scenario)");
        }
        if (Current.Type == TokenType.TableRow)
        {
            throw Error("a table row must follow an 'Examples:' line of a Scenario Outline");
        }
        throw Error($"expected a step starting with Given/When/Then/And/But (or a new Scenario/Task) but found {Describe(Current)}");
    }

    // ------------------------------------------------------------------ scenario outlines ----

    /// <summary>
    /// Scenario Outline: NAME, its template steps, then one or more "Examples:" tables. Every data row
    /// becomes its own Scenario: "&lt;column&gt;" inside quoted values is replaced with the cell text, and a
    /// bare &lt;column&gt; (e.g. "within &lt;timeout&gt; seconds") is replaced with the cell's tokens. The
    /// template is re-parsed per row, so a bad cell value is reported with the row it came from.
    /// </summary>
    private IEnumerable<ScriptBlock> ParseOutline(IReadOnlyList<string> tags, IReadOnlyList<Step> background)
    {
        var line = Current.Line;
        ExpectWord("scenario");
        Advance(); // outline / template
        if (Current.Type == TokenType.Colon) Advance();
        var name = ParseFreeTextToEndOfLine();
        if (string.IsNullOrEmpty(name))
        {
            throw new KafScriptException("Scenario Outline needs a name, e.g. 'Scenario Outline: Order with status <status>'", line);
        }
        SkipNewlines();
        if (IsWord("schedule")) throw Error("a Scenario Outline can't have a schedule");

        // Template steps: every token up to the "Examples" line.
        var templateStart = _pos;
        while (!AtEof && !(IsWord("examples") && _tokens[_pos - 1].Type == TokenType.Newline))
        {
            if (_tokens[_pos - 1].Type == TokenType.Newline && (IsWord("scenario") || IsWord("task") || Current.Type == TokenType.Tag))
            {
                break;
            }
            Advance();
        }
        if (!IsWord("examples"))
        {
            throw new KafScriptException($"Scenario Outline '{name}' needs an 'Examples:' table after its steps", line);
        }
        var template = _tokens.GetRange(templateStart, _pos - templateStart);
        if (!template.Any(t => t.Type == TokenType.Word && IsStepKeywordText(t.Text)))
        {
            throw new KafScriptException($"Scenario Outline '{name}' has no steps", line);
        }

        var blocks = new List<ScriptBlock>();
        var exampleIndex = 0;
        while (IsWord("examples"))
        {
            var examplesLine = Current.Line;
            Advance();
            if (Current.Type == TokenType.Colon) Advance();
            ParseFreeTextToEndOfLine();
            SkipNewlines();

            if (Current.Type != TokenType.TableRow)
            {
                throw new KafScriptException("'Examples:' needs a header row like '| orderId | status |'", examplesLine);
            }
            var header = Current.Cells!;
            if (header.Any(string.IsNullOrWhiteSpace)) throw Error("every Examples column needs a name");
            var duplicate = header.GroupBy(h => h, StringComparer.Ordinal).FirstOrDefault(g => g.Count() > 1);
            if (duplicate is not null) throw Error($"Examples column '{duplicate.Key}' appears twice");
            Advance();
            SkipNewlines();

            var rows = 0;
            while (Current.Type == TokenType.TableRow)
            {
                var row = Current;
                var cells = row.Cells!;
                if (cells.Count != header.Count)
                {
                    throw Error($"this Examples row has {cells.Count} cell(s) but the header has {header.Count}");
                }
                var values = new Dictionary<string, string>(StringComparer.Ordinal);
                for (var c = 0; c < header.Count; c++) values[header[c]] = cells[c];

                exampleIndex++;
                rows++;
                var steps = ParseTemplateSteps(template, values, exampleIndex, row.Line);
                var rowName = name.Contains('<') ? SubstitutePlaceholders(name, values) : $"{name} (example {exampleIndex})";
                blocks.Add(new ScriptBlock(BlockKind.Scenario, rowName, null, background.Concat(steps).ToList(), row.Line)
                {
                    Tags = tags,
                    OutlineName = name,
                    ExampleIndex = exampleIndex
                });
                Advance();
                SkipNewlines();
            }
            if (rows == 0) throw new KafScriptException("'Examples:' table has a header but no data rows", examplesLine);
        }

        ThrowUnlessBlockBoundary();
        return blocks;
    }

    private static List<Step> ParseTemplateSteps(List<Token> template, Dictionary<string, string> values, int exampleIndex, int rowLine)
    {
        var tokens = new List<Token>();
        foreach (var token in template)
        {
            switch (token.Type)
            {
                case TokenType.String or TokenType.DocString:
                    tokens.Add(token with { Text = SubstitutePlaceholders(token.Text, values) });
                    break;
                case TokenType.Placeholder:
                    if (!values.TryGetValue(token.Text, out var cell))
                    {
                        throw new KafScriptException(
                            $"unknown placeholder <{token.Text}> (Examples columns: {string.Join(", ", values.Keys)})", token.Line);
                    }
                    List<Token> cellTokens;
                    try
                    {
                        cellTokens = Lexer.Tokenize(cell);
                    }
                    catch (KafScriptException ex)
                    {
                        throw new KafScriptException($"example {exampleIndex} (line {rowLine}): value \"{cell}\" of <{token.Text}>: {ex.Message}", token.Line);
                    }
                    tokens.AddRange(cellTokens
                        .Where(t => t.Type is not (TokenType.Newline or TokenType.Eof))
                        .Select(t => t with { Line = token.Line }));
                    break;
                default:
                    tokens.Add(token);
                    break;
            }
        }
        var lastLine = template.Count == 0 ? rowLine : template[^1].Line;
        tokens.Add(new Token(TokenType.Newline, "\n", lastLine));
        tokens.Add(new Token(TokenType.Eof, string.Empty, lastLine));

        var parser = new Parser(tokens);
        try
        {
            parser.SkipNewlines();
            var steps = new List<Step>();
            while (parser.IsStepKeyword())
            {
                steps.Add(parser.ParseStep());
                parser.SkipNewlines();
            }
            if (!parser.AtEof) parser.ThrowUnlessBlockBoundary();
            return steps;
        }
        catch (KafScriptException ex) when (values.Count > 0)
        {
            throw new KafScriptException($"example {exampleIndex} (line {rowLine}): {StripLinePrefix(ex.Message)}", ex.Line);
        }
    }

    private static string StripLinePrefix(string message) =>
        message.StartsWith("line ", StringComparison.Ordinal) && message.IndexOf(": ", StringComparison.Ordinal) is var i and > 0
            ? message[(i + 2)..]
            : message;

    private static string SubstitutePlaceholders(string text, IReadOnlyDictionary<string, string> values)
    {
        if (!text.Contains('<')) return text;
        foreach (var (column, value) in values)
        {
            text = text.Replace($"<{column}>", value, StringComparison.Ordinal);
        }
        return text;
    }

    private string ParseFreeTextToEndOfLine()
    {
        var sb = new StringBuilder();
        while (Current.Type is not (TokenType.Newline or TokenType.Eof))
        {
            if (sb.Length > 0) sb.Append(' ');
            sb.Append(Current.Type switch
            {
                TokenType.Placeholder => $"<{Current.Text}>",
                TokenType.Tag => "@" + Current.Text,
                TokenType.TableRow => Current.ToString(),
                _ => Current.Text
            });
            Advance();
        }
        return sb.ToString().Trim();
    }

    private static bool IsStepKeywordText(string text) =>
        text.ToLowerInvariant() is "given" or "when" or "then" or "and" or "but";

    private bool IsStepKeyword() => Current.Type == TokenType.Word && IsStepKeywordText(Current.Text);

    private ScheduleSpec ParseSchedule()
    {
        ExpectWord("schedule");
        if (AcceptWord("run"))
        {
            ExpectWord("once");
            return new ScheduleSpec(ScheduleKind.RunOnce);
        }
        if (AcceptWord("every"))
        {
            var durationLine = Current.Line;
            var duration = ParseDuration();
            if (duration.ToTimeSpan() < TimeSpan.FromSeconds(1))
            {
                throw new KafScriptException("'schedule every' needs an interval of at least 1 second", durationLine);
            }
            return new ScheduleSpec(ScheduleKind.Every, Every: duration);
        }
        if (AcceptWord("at"))
        {
            var hour = ExpectWholeNumber("hour", 0, 23);
            if (Current.Type != TokenType.Colon) throw Error("expected ':' in time, e.g. 'at 9:30'");
            Advance();
            var minute = ExpectWholeNumber("minute", 0, 59);
            return new ScheduleSpec(ScheduleKind.At, At: new TimeOnly(hour, minute));
        }
        throw Error("expected 'run once', 'every <duration>', or 'at <hh:mm>' after 'schedule'");
    }

    // ------------------------------------------------------------------ steps ----

    private Step ParseStep()
    {
        var line = Current.Line;
        var keyword = ExpectWordText().ToLowerInvariant() switch
        {
            "given" => StepKeyword.Given,
            "when" => StepKeyword.When,
            "then" => StepKeyword.Then,
            "and" => StepKeyword.And,
            "but" => StepKeyword.But,
            var other => throw Error($"unknown step keyword '{other}'")
        };

        var action = ParseAction();
        ExpectEndOfLine();
        return new Step(keyword, action, line);
    }

    private ScriptAction ParseAction()
    {
        if (IsWord("use")) return ParseUseConnection();
        if (IsWord("produce")) return ParseProduce();
        if (IsWord("watch")) return ParseWatch();
        if (IsWord("expect")) return ParseExpect();
        if (IsWord("message") || IsWord("a")) return ParseArrives();
        if (IsWord("rethrow")) return ParseRethrow();
        if (IsWord("scan")) return ParseScan();
        if (IsWord("acknowledge")) return ParseAcknowledge();
        if (IsWord("log")) return ParseLog();
        if (IsWord("set")) return ParseSet();
        if (IsWord("capture")) return ParseCapture();
        if (IsWord("wait")) return ParseWait();
        if (IsWord("assert")) return ParseAssert();
        if (IsWord("validate")) return ParseValidate();

        throw Error($"unrecognized step starting with {Describe(Current)}. " +
                     "Expected one of: use, produce, watch, expect, a/message, rethrow, scan, " +
                     "acknowledge, log, set, capture, wait, assert, validate.");
    }

    private ScriptAction ParseUseConnection()
    {
        ExpectWord("use");
        ExpectWord("connection");
        return new UseConnectionAction(ExpectString());
    }

    private ScriptAction ParseProduce()
    {
        ExpectWord("produce");
        var count = 1;
        if (Current.Type == TokenType.Number)
        {
            count = ExpectWholeNumber("message count", 1, 1_000_000);
            if (!AcceptWord("messages")) ExpectWord("message");
        }
        else
        {
            ExpectWord("message");
        }
        ExpectWord("to");
        ExpectWord("topic");
        var topic = ExpectString();

        string? key = null;
        string? value = null;
        var headers = new List<HeaderAssignment>();

        while (true)
        {
            if (AcceptWord("key")) key = ExpectString();
            else if (AcceptWord("value")) value = ExpectString();
            else if (AcceptWord("header"))
            {
                var name = ExpectString();
                ExpectWord("to");
                headers.Add(new HeaderAssignment(name, ExpectString()));
            }
            else break;
        }

        return new ProduceMessageAction(topic, key, value, headers, count);
    }

    private ScriptAction ParseWatch()
    {
        ExpectWord("watch");
        ExpectWord("topic");
        var topic = ExpectString();
        var position = ParsePosition(allowNow: true);
        return new WatchTopicAction(topic, position);
    }

    private ScriptAction ParseExpect()
    {
        ExpectWord("expect");
        if (AcceptWord("no"))
        {
            if (!AcceptWord("messages")) ExpectWord("message");
            ExpectWord("on");
            ExpectWord("topic");
            var noTopic = ExpectString();
            ExpectWord("within");
            var noDuration = ParseDuration();
            var noConditions = IsWord("where") ? ParseConditions() : Array.Empty<Condition>();
            return new ExpectNoMessageAction(noTopic, noDuration, noConditions);
        }

        CountMode? mode = null;
        if (AcceptWord("exactly")) mode = CountMode.Exactly;
        else if (AcceptWord("at"))
        {
            if (AcceptWord("least")) mode = CountMode.AtLeast;
            else if (AcceptWord("most")) mode = CountMode.AtMost;
            else throw Error("expected 'at least N' or 'at most N'");
        }
        if (mode is not null || Current.Type == TokenType.Number)
        {
            var count = ExpectWholeNumber("message count", 0, 1_000_000);
            if (!AcceptWord("messages")) ExpectWord("message");
            ExpectWord("on");
            ExpectWord("topic");
            var countTopic = ExpectString();
            ExpectWord("within");
            var countDuration = ParseDuration();
            var countConditions = IsWord("where") ? ParseConditions() : Array.Empty<Condition>();
            return new ExpectMessageCountAction(countTopic, mode ?? CountMode.Exactly, count, countDuration, countConditions);
        }

        ExpectWord("message");
        ExpectWord("on");
        ExpectWord("topic");
        var topic = ExpectString();
        ExpectWord("within");
        var duration = ParseDuration();
        var conditions = IsWord("where") ? ParseConditions() : Array.Empty<Condition>();
        return new AwaitMessageAction(topic, duration, conditions, IsAssertion: true);
    }

    private ScriptAction ParseArrives()
    {
        AcceptWord("a");
        ExpectWord("message");
        ExpectWord("arrives");

        string? topic = null;
        if (AcceptWord("on"))
        {
            ExpectWord("topic");
            topic = ExpectString();
        }

        var duration = DefaultArrivalTimeout;
        if (AcceptWord("within")) duration = ParseDuration();

        var conditions = IsWord("where") ? ParseConditions() : Array.Empty<Condition>();
        return new AwaitMessageAction(topic, duration, conditions, IsAssertion: false);
    }

    private ScriptAction ParseRethrow()
    {
        ExpectWord("rethrow");
        ExpectWord("last");
        ExpectWord("message");
        ExpectWord("to");
        ExpectWord("topic");
        var topic = ExpectString();

        var keepSourceKey = false;
        string? keyOverride = null;
        if (AcceptWord("with"))
        {
            ExpectWord("key");
            if (AcceptWord("same")) keepSourceKey = true;
            else keyOverride = ExpectString();
        }

        var headers = new List<HeaderAssignment>();
        while (AcceptWord("header"))
        {
            var name = ExpectString();
            ExpectWord("to");
            headers.Add(new HeaderAssignment(name, ExpectString()));
        }

        return new RethrowAction(topic, keepSourceKey, keyOverride, headers);
    }

    private ScriptAction ParseScan()
    {
        ExpectWord("scan");
        ExpectWord("topic");
        var topic = ExpectString();
        var position = ParsePosition(allowNow: false, allowCommitted: true);

        int? limit = null;
        string? group = null;
        while (true)
        {
            if (limit is null && AcceptWord("limit")) limit = ExpectWholeNumber("limit", 1, 10_000_000);
            else if (group is null && AcceptWord("group"))
            {
                group = ExpectString().Trim();
                if (group.Length == 0) throw Error("consumer group name can't be empty");
            }
            else break;
        }

        if (position == TopicPosition.Committed && group is null)
        {
            throw Error("'from committed' needs a pinned consumer group, e.g. 'scan topic \"T\" from committed group \"my-sweeper\"'");
        }

        return new ScanTopicAction(topic, position, limit, group);
    }

    private ScriptAction ParseAcknowledge()
    {
        ExpectWord("acknowledge");
        if (AcceptWord("last"))
        {
            ExpectWord("message");
            return new AcknowledgeAction(EachScanned: false);
        }
        if (AcceptWord("each"))
        {
            ExpectWord("scanned");
            ExpectWord("message");
            return new AcknowledgeAction(EachScanned: true);
        }
        throw Error("expected 'acknowledge last message' or 'acknowledge each scanned message'");
    }

    private ScriptAction ParseLog()
    {
        ExpectWord("log");
        if (AcceptWord("key")) return new LogAction(LogTarget.Key, null);
        if (AcceptWord("value")) return new LogAction(LogTarget.Value, null);
        if (AcceptWord("message")) return new LogAction(LogTarget.Message, null);
        if (Current.Type is TokenType.String or TokenType.DocString)
        {
            return new LogAction(LogTarget.Literal, ExpectString());
        }
        throw Error("expected 'log key', 'log value', 'log message', or 'log \"text\"'");
    }

    private ScriptAction ParseSet()
    {
        ExpectWord("set");
        ExpectWord("variable");
        var name = ExpectWordText();
        ExpectWord("to");
        var value = ExpectString();
        return new SetVariableAction(name, value);
    }

    private ScriptAction ParseCapture()
    {
        ExpectWord("capture");
        ConditionField source;
        string? path = null;
        if (AcceptWord("json"))
        {
            source = ConditionField.Json;
            path = ExpectJsonPath();
        }
        else if (AcceptWord("key")) source = ConditionField.Key;
        else if (AcceptWord("value")) source = ConditionField.Value;
        else throw Error("expected 'capture json \"$.path\"', 'capture key', or 'capture value'");

        ExpectWord("as");
        var name = ExpectWordText();
        return new CaptureAction(source, path, name);
    }

    private ScriptAction ParseWait()
    {
        ExpectWord("wait");
        ExpectWord("for");
        return new WaitAction(ParseDuration());
    }

    private ScriptAction ParseAssert()
    {
        ExpectWord("assert");
        if (IsWord("last") && IsNextWord("message"))
        {
            Advance();
            Advance();
            return new AssertMessageAction(ParseConditions());
        }
        var name = ExpectWordText();
        var comparator = ParseComparator();
        var expected = ParseExpectedValue(comparator);
        return new AssertVariableAction(name, comparator, expected);
    }

    private ScriptAction ParseValidate()
    {
        ExpectWord("validate");
        bool each;
        if (AcceptWord("last"))
        {
            ExpectWord("message");
            each = false;
        }
        else if (AcceptWord("each"))
        {
            ExpectWord("scanned");
            ExpectWord("message");
            each = true;
        }
        else throw Error("expected 'validate last message' or 'validate each scanned message'");

        ExpectWord("against");
        ExpectWord("schema");
        if (AcceptWord("file"))
        {
            var file = ExpectString().Trim();
            if (file.Length == 0) throw Error("schema file path can't be empty");
            return new ValidateSchemaAction(each, null, file);
        }

        var schemaToken = Current;
        var schema = ExpectString();
        if (!schema.Contains("{{", StringComparison.Ordinal) &&
            KafkaStudio.Core.Validation.JsonSchema.TryParse(schema, out _, out var problem) == false)
        {
            throw new KafScriptException($"invalid JSON schema: {problem}", schemaToken.Line);
        }
        return new ValidateSchemaAction(each, schema, null);
    }

    // ------------------------------------------------------------------ shared fragments ----

    private IReadOnlyList<Condition> ParseConditions()
    {
        ExpectWord("where");
        var conditions = new List<Condition> { ParseCondition() };
        while (AcceptWord("and")) conditions.Add(ParseCondition());
        return conditions;
    }

    private Condition ParseCondition()
    {
        ConditionField field;
        string? path = null;
        string? header = null;
        if (AcceptWord("key")) field = ConditionField.Key;
        else if (AcceptWord("value")) field = ConditionField.Value;
        else if (AcceptWord("json"))
        {
            field = ConditionField.Json;
            path = ExpectJsonPath();
        }
        else if (AcceptWord("header"))
        {
            field = ConditionField.Header;
            header = ExpectString();
            if (header.Trim().Length == 0) throw Error("header name can't be empty");
        }
        else throw Error("expected 'key', 'value', 'json \"$.path\"' or 'header \"name\"' in condition");

        var comparator = ParseComparator();
        var expected = ParseExpectedValue(comparator);
        return new Condition(field, path, comparator, expected, header);
    }

    /// <summary>The quoted value after a comparator; "exists"/"not exists" take none.</summary>
    private string ParseExpectedValue(Comparator comparator)
    {
        if (comparator is Comparator.Exists or Comparator.NotExists) return string.Empty;
        var expectedToken = Current;
        var expected = ExpectString();
        if (comparator == Comparator.Matches) ValidateRegex(expected, expectedToken.Line);
        return expected;
    }

    private string ExpectJsonPath()
    {
        var token = Current;
        var path = ExpectString();
        if (JsonPathEvaluator.Validate(path) is { } problem)
        {
            throw new KafScriptException(problem, token.Line);
        }
        return path;
    }

    private static void ValidateRegex(string pattern, int line)
    {
        // Templated patterns ({{var}}) can only be checked once rendered, at run time.
        if (pattern.Contains("{{", StringComparison.Ordinal)) return;
        if (ConditionEvaluator.ValidatePattern(pattern) is { } problem)
        {
            throw new KafScriptException(problem, line);
        }
    }

    private Comparator ParseComparator()
    {
        if (AcceptWord("equals")) return Comparator.Equals;
        if (AcceptWord("contains")) return Comparator.Contains;
        if (AcceptWord("matches")) return Comparator.Matches;
        if (AcceptWord("exists")) return Comparator.Exists;
        if (AcceptWord("greater"))
        {
            ExpectWord("than");
            return Comparator.GreaterThan;
        }
        if (AcceptWord("less"))
        {
            ExpectWord("than");
            return Comparator.LessThan;
        }
        if (AcceptWord("not"))
        {
            if (AcceptWord("contains")) return Comparator.NotContains;
            if (AcceptWord("exists")) return Comparator.NotExists;
            ExpectWord("equals");
            return Comparator.NotEquals;
        }
        throw Error("expected 'equals', 'contains', 'matches', 'exists', 'greater than', 'less than', " +
                    "'not equals', 'not contains' or 'not exists'");
    }

    private TopicPosition ParsePosition(bool allowNow, bool allowCommitted = false)
    {
        ExpectWord("from");
        if (AcceptWord("beginning")) return TopicPosition.Beginning;
        if (AcceptWord("end")) return TopicPosition.End;
        if (allowNow && AcceptWord("now")) return TopicPosition.Now;
        if (allowCommitted && AcceptWord("committed")) return TopicPosition.Committed;
        throw Error(allowNow
            ? "expected 'beginning', 'end', or 'now' after 'from'"
            : "expected 'beginning', 'end' or 'committed' after 'from'");
    }

    private Duration ParseDuration()
    {
        var value = ExpectNumber();
        var unitWord = ExpectWordText().ToLowerInvariant();
        var unit = unitWord switch
        {
            "ms" or "millisecond" or "milliseconds" => TimeUnit.Milliseconds,
            "s" or "sec" or "secs" or "second" or "seconds" => TimeUnit.Seconds,
            "m" or "min" or "mins" or "minute" or "minutes" => TimeUnit.Minutes,
            "h" or "hour" or "hours" => TimeUnit.Hours,
            _ => throw Error($"unknown time unit '{unitWord}' (expected seconds/minutes/hours/ms)")
        };
        return new Duration(value, unit);
    }
}

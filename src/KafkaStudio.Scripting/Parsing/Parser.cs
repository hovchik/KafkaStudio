using System.Globalization;
using System.Text;
using KafkaStudio.Scripting.Ast;
using KafkaStudio.Scripting.Lexing;

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
        var value = double.Parse(Current.Text, CultureInfo.InvariantCulture);
        Advance();
        return value;
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
        _ => $"'{token.Text}'"
    };

    private static string Truncate(string s) => s.Length > 30 ? s[..30] + "…" : s;

    // ------------------------------------------------------------------ document / block ----

    private ScriptDocument ParseDocument()
    {
        SkipNewlines();
        var blocks = new List<ScriptBlock>();
        while (!AtEof)
        {
            blocks.Add(ParseBlock());
            SkipNewlines();
        }
        return new ScriptDocument(blocks);
    }

    private ScriptBlock ParseBlock()
    {
        var line = Current.Line;
        BlockKind kind;
        if (AcceptWord("scenario")) kind = BlockKind.Scenario;
        else if (AcceptWord("task")) kind = BlockKind.Task;
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

        var steps = new List<Step>();
        while (IsStepKeyword())
        {
            steps.Add(ParseStep());
            SkipNewlines();
        }

        return new ScriptBlock(kind, name, schedule, steps, line);
    }

    private string ParseFreeTextToEndOfLine()
    {
        var sb = new StringBuilder();
        while (Current.Type is not (TokenType.Newline or TokenType.Eof))
        {
            if (sb.Length > 0) sb.Append(' ');
            sb.Append(Current.Text);
            Advance();
        }
        return sb.ToString().Trim();
    }

    private bool IsStepKeyword() =>
        Current.Type == TokenType.Word &&
        (IsWord("given") || IsWord("when") || IsWord("then") || IsWord("and") || IsWord("but"));

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
            var duration = ParseDuration();
            return new ScheduleSpec(ScheduleKind.Every, Every: duration);
        }
        if (AcceptWord("at"))
        {
            var hour = (int)ExpectNumber();
            if (Current.Type != TokenType.Colon) throw Error("expected ':' in time, e.g. 'at 9:30'");
            Advance();
            var minute = (int)ExpectNumber();
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

        throw Error($"unrecognized step starting with {Describe(Current)}. " +
                     "Expected one of: use, produce, watch, expect, a/message, rethrow, scan, " +
                     "acknowledge, log, set, capture, wait, assert.");
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
        ExpectWord("message");
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

        return new ProduceMessageAction(topic, key, value, headers);
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
        var position = ParsePosition(allowNow: false);

        int? limit = null;
        if (AcceptWord("limit")) limit = (int)ExpectNumber();

        return new ScanTopicAction(topic, position, limit);
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
            path = ExpectString();
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
        var name = ExpectWordText();
        var comparator = ParseComparator();
        var expected = ExpectString();
        return new AssertVariableAction(name, comparator, expected);
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
        if (AcceptWord("key")) field = ConditionField.Key;
        else if (AcceptWord("value")) field = ConditionField.Value;
        else if (AcceptWord("json"))
        {
            field = ConditionField.Json;
            path = ExpectString();
        }
        else throw Error("expected 'key', 'value', or 'json \"$.path\"' in condition");

        var comparator = ParseComparator();
        var expected = ExpectString();
        return new Condition(field, path, comparator, expected);
    }

    private Comparator ParseComparator()
    {
        if (AcceptWord("equals")) return Comparator.Equals;
        if (AcceptWord("contains")) return Comparator.Contains;
        if (AcceptWord("matches")) return Comparator.Matches;
        if (AcceptWord("not"))
        {
            ExpectWord("equals");
            return Comparator.NotEquals;
        }
        throw Error("expected 'equals', 'contains', 'matches', or 'not equals'");
    }

    private TopicPosition ParsePosition(bool allowNow)
    {
        ExpectWord("from");
        if (AcceptWord("beginning")) return TopicPosition.Beginning;
        if (AcceptWord("end")) return TopicPosition.End;
        if (allowNow && AcceptWord("now")) return TopicPosition.Now;
        throw Error(allowNow
            ? "expected 'beginning', 'end', or 'now' after 'from'"
            : "expected 'beginning' or 'end' after 'from'");
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

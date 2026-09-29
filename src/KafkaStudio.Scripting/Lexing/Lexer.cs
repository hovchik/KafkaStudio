using System.Text;
using System.Text.RegularExpressions;

namespace KafkaStudio.Scripting.Lexing;

/// <summary>
/// Hand-written scanner for KafScript. Deliberately simple and line-aware: most of KafScript's
/// "human sentence" feel comes from the parser accepting bare <see cref="TokenType.Word"/> tokens in
/// flexible positions, so the lexer's job is just to split source text into words, quoted strings,
/// triple-quoted doc-strings (for JSON payload bodies), numbers, colons, comments and newlines.
/// </summary>
public static partial class Lexer
{
    /// <summary>"Scenario:", "Scenario Outline:", "Task:", "Feature:", "Background:", "Examples:" at the
    /// start of a line. Everything after the colon is the block's name, taken verbatim.</summary>
    [GeneratedRegex(@"\G(?<kw>(?:scenario(?:[ \t]+(?:outline|template))?|task|feature|background|examples))[ \t]*:", RegexOptions.IgnoreCase)]
    private static partial Regex HeaderLine();

    public static List<Token> Tokenize(string source)
    {
        var tokens = new List<Token>();
        var line = 1;
        var i = 0;
        var n = source.Length;

        while (i < n)
        {
            var c = source[i];

            if (char.IsLetter(c) && (tokens.Count == 0 || tokens[^1].Type is TokenType.Newline or TokenType.Tag) &&
                HeaderLine().Match(source, i) is { Success: true } header)
            {
                // Header names may contain any punctuation ("Refund & cancel (EU) - 50%"), which the word
                // rules below would reject, so the rest of the line is one Text token.
                foreach (var word in header.Groups["kw"].Value.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    tokens.Add(new Token(TokenType.Word, word, line));
                }
                tokens.Add(new Token(TokenType.Colon, ":", line));
                i += header.Length;
                var lineEnd = source.IndexOf('\n', i);
                if (lineEnd < 0) lineEnd = n;
                var text = source[i..lineEnd].TrimEnd('\r').Trim();
                if (text.Length > 0) tokens.Add(new Token(TokenType.Text, text, line));
                i = lineEnd;
                continue;
            }

            if (c == '\r')
            {
                i++;
                continue;
            }

            if (c == '\n')
            {
                tokens.Add(new Token(TokenType.Newline, "\n", line));
                line++;
                i++;
                continue;
            }

            if (c == ' ' || c == '\t')
            {
                i++;
                continue;
            }

            if (c == '#')
            {
                while (i < n && source[i] != '\n') i++;
                continue;
            }

            if (c == '@' && i + 1 < n && IsWordChar(source[i + 1]))
            {
                var start = ++i;
                while (i < n && IsWordChar(source[i])) i++;
                tokens.Add(new Token(TokenType.Tag, source[start..i], line));
                continue;
            }

            if (c == '|')
            {
                // An Examples table row: the whole line, split into cells. "\|" is a literal pipe.
                var cells = new List<string>();
                var cell = new StringBuilder();
                i++;
                var closed = false;
                while (i < n && source[i] != '\n')
                {
                    var ch = source[i];
                    if (ch == '\\' && i + 1 < n && source[i + 1] == '|')
                    {
                        cell.Append('|');
                        i += 2;
                        continue;
                    }
                    if (ch == '|')
                    {
                        cells.Add(cell.ToString().Trim());
                        cell.Clear();
                        closed = true;
                        i++;
                        continue;
                    }
                    if (ch != '\r') cell.Append(ch);
                    closed = closed && char.IsWhiteSpace(ch);
                    i++;
                }
                if (!closed)
                {
                    throw new KafScriptException("table row must end with '|'", line);
                }
                tokens.Add(new Token(TokenType.TableRow, string.Join("|", cells), line) { Cells = cells });
                continue;
            }

            if (c == '<' && TryReadPlaceholder(source, i, out var placeholder, out var end))
            {
                tokens.Add(new Token(TokenType.Placeholder, placeholder, line));
                i = end;
                continue;
            }

            if (c == ':')
            {
                tokens.Add(new Token(TokenType.Colon, ":", line));
                i++;
                continue;
            }

            if (c == '"')
            {
                if (i + 2 < n && source[i + 1] == '"' && source[i + 2] == '"')
                {
                    var startLine = line;
                    i += 3;
                    var sb = new StringBuilder();
                    while (true)
                    {
                        if (i + 2 < n && source[i] == '"' && source[i + 1] == '"' && source[i + 2] == '"')
                        {
                            i += 3;
                            break;
                        }
                        if (i >= n)
                        {
                            throw new KafScriptException("unterminated triple-quoted string (\"\"\")", startLine);
                        }
                        if (source[i] == '\n') line++;
                        sb.Append(source[i]);
                        i++;
                    }
                    // Trim a single leading/trailing newline for readability, mirroring common
                    // multi-line string conventions (so """\n{...}\n""" doesn't carry stray blank lines).
                    // CRLF-saved files: normalize so payloads don't carry stray '\r' characters.
                    var text = sb.ToString().Replace("\r\n", "\n");
                    if (text.StartsWith('\n')) text = text[1..];
                    if (text.EndsWith('\n')) text = text[..^1];
                    tokens.Add(new Token(TokenType.DocString, text, startLine));
                    continue;
                }
                else
                {
                    var startLine = line;
                    i++;
                    var sb = new StringBuilder();
                    while (i < n && source[i] != '"')
                    {
                        if (source[i] == '\\' && i + 1 < n)
                        {
                            var next = source[i + 1];
                            sb.Append(next switch
                            {
                                'n' => '\n',
                                't' => '\t',
                                '"' => '"',
                                '\\' => '\\',
                                _ => next
                            });
                            i += 2;
                            continue;
                        }
                        if (source[i] == '\n')
                        {
                            throw new KafScriptException("unterminated string literal (missing closing \")", startLine);
                        }
                        sb.Append(source[i]);
                        i++;
                    }
                    if (i >= n)
                    {
                        throw new KafScriptException("unterminated string literal (missing closing \")", startLine);
                    }
                    i++; // closing quote
                    tokens.Add(new Token(TokenType.String, sb.ToString(), startLine));
                    continue;
                }
            }

            if (char.IsAsciiDigit(c))
            {
                var start = i;
                var seenDot = false;
                while (i < n && (char.IsAsciiDigit(source[i]) ||
                                 (source[i] == '.' && !seenDot && i + 1 < n && char.IsAsciiDigit(source[i + 1]))))
                {
                    if (source[i] == '.') seenDot = true;
                    i++;
                }
                tokens.Add(new Token(TokenType.Number, source[start..i], line));
                continue;
            }

            if (char.IsLetter(c) || c == '_' || c == '{')
            {
                var start = i;
                // Words can contain letters, digits, '_', '-', '.', and template markers {{ }}
                // so things like "message-id", "orders.dlq" or "{{orderId}}" lex as one Word token.
                while (i < n && (char.IsLetterOrDigit(source[i]) || source[i] is '_' or '-' or '.' or '{' or '}'))
                {
                    i++;
                }
                tokens.Add(new Token(TokenType.Word, source[start..i], line));
                continue;
            }

            throw new KafScriptException($"unexpected character '{c}'", line);
        }

        tokens.Add(new Token(TokenType.Newline, "\n", line));
        tokens.Add(new Token(TokenType.Eof, string.Empty, line));
        return tokens;
    }

    private static bool IsWordChar(char c) => char.IsLetterOrDigit(c) || c is '_' or '-' or '.' or ':';

    /// <summary>Reads "&lt;name&gt;" (letters, digits, '_', '-', spaces) starting at <paramref name="start"/>.</summary>
    private static bool TryReadPlaceholder(string source, int start, out string name, out int end)
    {
        var i = start + 1;
        while (i < source.Length && (char.IsLetterOrDigit(source[i]) || source[i] is '_' or '-' or ' ')) i++;
        if (i < source.Length && source[i] == '>' && i > start + 1 && char.IsLetter(source[start + 1]))
        {
            name = source[(start + 1)..i].Trim();
            end = i + 1;
            return true;
        }
        name = string.Empty;
        end = start;
        return false;
    }
}

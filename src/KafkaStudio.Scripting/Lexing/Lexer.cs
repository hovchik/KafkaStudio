using System.Text;

namespace KafkaStudio.Scripting.Lexing;

/// <summary>
/// Hand-written scanner for KafScript. Deliberately simple and line-aware: most of KafScript's
/// "human sentence" feel comes from the parser accepting bare <see cref="TokenType.Word"/> tokens in
/// flexible positions, so the lexer's job is just to split source text into words, quoted strings,
/// triple-quoted doc-strings (for JSON payload bodies), numbers, colons, comments and newlines.
/// </summary>
public static class Lexer
{
    public static List<Token> Tokenize(string source)
    {
        var tokens = new List<Token>();
        var line = 1;
        var i = 0;
        var n = source.Length;

        while (i < n)
        {
            var c = source[i];

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
                    var text = sb.ToString();
                    if (text.StartsWith('\n')) text = text[1..];
                    else if (text.StartsWith("\r\n")) text = text[2..];
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

            if (char.IsDigit(c))
            {
                var start = i;
                while (i < n && (char.IsDigit(source[i]) || source[i] == '.')) i++;
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
}

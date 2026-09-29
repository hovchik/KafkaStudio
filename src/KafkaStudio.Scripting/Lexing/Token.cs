namespace KafkaStudio.Scripting.Lexing;

public enum TokenType
{
    Word,       // bare word, e.g. produce, message, topic, orders (unquoted identifiers/keywords)
    String,     // "quoted text", single line, backslash-escaped
    DocString,  // """ ... """ possibly multi-line, used for JSON payloads etc.
    Number,     // 123 or 12.5
    Colon,      // :
    Tag,        // @smoke - Text is the tag without '@'
    TableRow,   // | a | b | - a whole Examples table line; see Token.Cells
    Placeholder,// <column> - a Scenario Outline placeholder outside quotes; Text is the column name
    Text,       // the free-text rest of a "Scenario: ..." / "Feature: ..." header line (any characters)
    Newline,
    Eof
}

/// <summary>A single lexical token, with source line number for readable error messages.</summary>
public sealed record Token(TokenType Type, string Text, int Line)
{
    /// <summary>For <see cref="TokenType.TableRow"/>: the trimmed cell values, left to right.</summary>
    public IReadOnlyList<string>? Cells { get; init; }

    public override string ToString() => Type switch
    {
        TokenType.String => $"\"{Text}\"",
        TokenType.DocString => "\"\"\"...\"\"\"",
        TokenType.Tag => "@" + Text,
        TokenType.TableRow => "| " + string.Join(" | ", Cells ?? Array.Empty<string>()) + " |",
        TokenType.Placeholder => $"<{Text}>",
        TokenType.Newline => "<newline>",
        TokenType.Eof => "<eof>",
        _ => Text
    };
}

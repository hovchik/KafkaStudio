namespace KafkaStudio.Scripting.Lexing;

public enum TokenType
{
    Word,       // bare word, e.g. produce, message, topic, orders (unquoted identifiers/keywords)
    String,     // "quoted text", single line, backslash-escaped
    DocString,  // """ ... """ possibly multi-line, used for JSON payloads etc.
    Number,     // 123 or 12.5
    Colon,      // :
    Newline,
    Eof
}

/// <summary>A single lexical token, with source line number for readable error messages.</summary>
public sealed record Token(TokenType Type, string Text, int Line)
{
    public override string ToString() => Type switch
    {
        TokenType.String => $"\"{Text}\"",
        TokenType.DocString => "\"\"\"...\"\"\"",
        TokenType.Newline => "<newline>",
        TokenType.Eof => "<eof>",
        _ => Text
    };
}

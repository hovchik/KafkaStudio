namespace KafkaStudio.Scripting;

/// <summary>Raised for any lexing, parsing, or interpretation error in a KafScript document.</summary>
public class KafScriptException : Exception
{
    public int? Line { get; }

    public KafScriptException(string message, int? line = null) : base(FormatMessage(message, line))
    {
        Line = line;
    }

    private static string FormatMessage(string message, int? line) =>
        line is null ? message : $"line {line}: {message}";
}

using System;
using System.Globalization;
using Avalonia.Controls.Documents;
using Avalonia.Data.Converters;
using Avalonia.Media;

namespace KafkaStudio.App.Converters;

/// <summary>
/// Converts a message body string (as produced by <see cref="KafkaStudio.Core.Messaging.KafkaMessage.PrettyValue"/>)
/// into an <see cref="InlineCollection"/> with basic JSON syntax coloring, for binding to
/// <see cref="Avalonia.Controls.TextBlock.Inlines"/> on a plain (native) <c>SelectableTextBlock</c>. Using the
/// built-in <c>Inlines</c> property - rather than a custom control - keeps all of Avalonia's native
/// selection/copy behavior working correctly. Falls back to a single plain run for anything that isn't JSON.
/// </summary>
public sealed class JsonInlinesConverter : IValueConverter
{
    public static readonly JsonInlinesConverter Instance = new();

    private static readonly IBrush PunctuationBrush = Brushes.Gray;
    private static readonly IBrush KeyBrush = new SolidColorBrush(Color.FromRgb(0x9C, 0xDC, 0xFE));
    private static readonly IBrush StringBrush = new SolidColorBrush(Color.FromRgb(0xCE, 0x91, 0x78));
    private static readonly IBrush NumberBrush = new SolidColorBrush(Color.FromRgb(0xB5, 0xCE, 0xA8));
    private static readonly IBrush KeywordBrush = new SolidColorBrush(Color.FromRgb(0x56, 0x9C, 0xD6));
    private static readonly IBrush DefaultBrush = Brushes.White;

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        var text = value as string;
        var inlines = new InlineCollection();

        if (string.IsNullOrEmpty(text))
        {
            return inlines;
        }

        if (!TryTokenize(text, inlines))
        {
            inlines.Clear();
            inlines.Add(new Run(text) { Foreground = DefaultBrush });
        }

        return inlines;
    }

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
        => throw new NotSupportedException();

    /// <summary>Very small hand-rolled JSON tokenizer just for coloring purposes (not a validator - the
    /// text has already been formatted by <c>JsonSerializer</c>, so we only need to recognize tokens, not
    /// reject malformed input).</summary>
    private static bool TryTokenize(string text, InlineCollection inlines)
    {
        var trimmed = text.TrimStart();
        if (trimmed.Length == 0 || (trimmed[0] != '{' && trimmed[0] != '[')) return false;

        var i = 0;
        var length = text.Length;

        while (i < length)
        {
            var c = text[i];

            if (char.IsWhiteSpace(c))
            {
                var start = i;
                while (i < length && char.IsWhiteSpace(text[i])) i++;
                inlines.Add(new Run(text.Substring(start, i - start)));
                continue;
            }

            if (c == '"')
            {
                var start = i;
                i++;
                while (i < length)
                {
                    if (text[i] == '\\' && i + 1 < length) { i += 2; continue; }
                    if (text[i] == '"') { i++; break; }
                    i++;
                }
                var token = text[start..i];

                var lookAheadIsColon = false;
                var j = i;
                while (j < length && char.IsWhiteSpace(text[j])) j++;
                if (j < length && text[j] == ':') lookAheadIsColon = true;

                inlines.Add(new Run(token) { Foreground = lookAheadIsColon ? KeyBrush : StringBrush });
                continue;
            }

            if (c is '{' or '}' or '[' or ']' or ',' or ':')
            {
                inlines.Add(new Run(c.ToString()) { Foreground = PunctuationBrush });
                i++;
                continue;
            }

            if (c == '-' || char.IsDigit(c))
            {
                var start = i;
                i++;
                while (i < length && (char.IsDigit(text[i]) || text[i] is '.' or 'e' or 'E' or '+' or '-')) i++;
                inlines.Add(new Run(text[start..i]) { Foreground = NumberBrush });
                continue;
            }

            if (text.AsSpan(i).StartsWith("true") || text.AsSpan(i).StartsWith("false") || text.AsSpan(i).StartsWith("null"))
            {
                var word = text.AsSpan(i).StartsWith("null") ? "null" : (text.AsSpan(i).StartsWith("true") ? "true" : "false");
                inlines.Add(new Run(word) { Foreground = KeywordBrush });
                i += word.Length;
                continue;
            }

            // Unrecognized character - emit as plain text and move on rather than failing outright.
            var plainStart = i;
            i++;
            inlines.Add(new Run(text[plainStart..i]) { Foreground = DefaultBrush });
        }

        return true;
    }
}

using System.Globalization;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Interactivity;
using Avalonia.VisualTree;

namespace KafkaStudio.App.Behaviors;

/// <summary>
/// Blocks non-numeric characters from being typed or pasted into numeric inputs. Applies
/// automatically to the text box inside every <see cref="NumericUpDown"/>, and to any plain
/// <see cref="TextBox"/> marked with <c>NumericInput.IsEnabled="True"</c>.
/// </summary>
public static class NumericInput
{
    public static readonly AttachedProperty<bool> IsEnabledProperty =
        AvaloniaProperty.RegisterAttached<TextBox, bool>("IsEnabled", typeof(NumericInput));

    public static bool GetIsEnabled(TextBox element) => element.GetValue(IsEnabledProperty);
    public static void SetIsEnabled(TextBox element, bool value) => element.SetValue(IsEnabledProperty, value);

    /// <summary>Registers the class handlers once at startup.</summary>
    public static void Register()
    {
        InputElement.TextInputEvent.AddClassHandler<TextBox>(OnTextInput, RoutingStrategies.Tunnel);
        TextBox.PastingFromClipboardEvent.AddClassHandler<TextBox>(OnPasting, RoutingStrategies.Bubble);
    }

    private static void OnTextInput(TextBox box, TextInputEventArgs e)
    {
        if (e.Text is null || !TryGetRules(box, out var allowDecimal, out var allowNegative)) return;
        var filtered = Filter(e.Text, allowDecimal, allowNegative);
        if (filtered.Length == e.Text.Length) return;
        if (filtered.Length == 0) e.Handled = true;
        else e.Text = filtered;
    }

    private static async void OnPasting(TextBox box, RoutedEventArgs e)
    {
        if (!TryGetRules(box, out var allowDecimal, out var allowNegative)) return;
        e.Handled = true;
        try
        {
            if (TopLevel.GetTopLevel(box)?.Clipboard is not { } clipboard) return;
            var text = await clipboard.GetTextAsync();
            if (string.IsNullOrEmpty(text)) return;
            var filtered = Filter(text.Trim(), allowDecimal, allowNegative);
            if (filtered.Length > 0) box.SelectedText = filtered;
        }
        catch
        {
            // async void: an unreadable clipboard must not crash the app - the paste just doesn't happen.
        }
    }

    private static bool TryGetRules(TextBox box, out bool allowDecimal, out bool allowNegative)
    {
        allowDecimal = allowNegative = false;
        if (box.FindAncestorOfType<NumericUpDown>() is { } spinner)
        {
            allowDecimal = spinner.FormatString?.Contains('.') == true;
            allowNegative = spinner.Minimum < 0;
            return true;
        }
        return GetIsEnabled(box);
    }

    private static string Filter(string text, bool allowDecimal, bool allowNegative)
    {
        var separator = CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator;
        var negative = CultureInfo.CurrentCulture.NumberFormat.NegativeSign;
        var chars = text.Where(c => char.IsAsciiDigit(c)
                                    || (allowDecimal && (c == '.' || separator.Contains(c)))
                                    || (allowNegative && negative.Contains(c)));
        return new string(chars.ToArray());
    }
}

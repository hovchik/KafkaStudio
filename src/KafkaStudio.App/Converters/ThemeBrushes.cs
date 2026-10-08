using Avalonia;
using Avalonia.Media;
using Avalonia.Styling;

namespace KafkaStudio.App.Converters;

/// <summary>
/// Looks brushes up from the design tokens (Styles/Tokens.axaml) for the current theme variant, so
/// converters that hand brushes to the UI follow a Dark/Light switch instead of baking in colors.
/// Falls back to a neutral brush when a key is missing so a typo can never crash a template.
/// </summary>
internal static class ThemeBrushes
{
    private static readonly IBrush Fallback = new SolidColorBrush(Color.Parse("#808A94A8"));

    public static IBrush Get(string key)
    {
        if (Application.Current is { } app)
        {
            var variant = app.ActualThemeVariant ?? ThemeVariant.Dark;
            if (app.TryGetResource(key, variant, out var value) && value is IBrush brush) return brush;
        }

        return Fallback;
    }
}

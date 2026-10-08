using System.Globalization;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Data.Converters;
using Avalonia.Media;

namespace KafkaStudio.App.Converters;

/// <summary>Resolves a navigation key ("topics", "producer", ...) to the matching
/// <c>Icon.{key}</c> geometry from Styles/Icons.axaml, so the sidebar draws crisp vector icons
/// while the view-model keeps describing items with plain strings.</summary>
public sealed class NavIconConverter : IValueConverter
{
    public static readonly NavIconConverter Instance = new();

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        if (value is string key && Application.Current is { } app &&
            app.TryFindResource($"Icon.{key}", out var resource) && resource is Geometry geometry)
        {
            return geometry;
        }

        return null;
    }

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

using System.Globalization;
using Avalonia.Data.Converters;

namespace KafkaStudio.App.Converters;

/// <summary>Renders a chevron glyph for a panel's expanded/collapsed toggle button in the Topics tab
/// (down-arrow when expanded - clicking collapses it - up-arrow when collapsed).</summary>
public sealed class BoolToChevronConverter : IValueConverter
{
    public static readonly BoolToChevronConverter Instance = new();

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        value is true ? "\u25bc" : "\u25b2";

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

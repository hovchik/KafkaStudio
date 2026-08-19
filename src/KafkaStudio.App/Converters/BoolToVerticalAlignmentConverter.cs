using System.Globalization;
using Avalonia.Data.Converters;
using Avalonia.Layout;

namespace KafkaStudio.App.Converters;

/// <summary>Switches a panel's <see cref="Layout.VerticalAlignment"/> between Stretch (expanded, fills
/// the available row/column) and Top (collapsed, shrinks to its header's height) so collapsing a panel
/// in the Topics tab actually reclaims the space instead of leaving an empty stretched border.</summary>
public sealed class BoolToVerticalAlignmentConverter : IValueConverter
{
    public static readonly BoolToVerticalAlignmentConverter Instance = new();

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        value is true ? VerticalAlignment.Stretch : VerticalAlignment.Top;

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

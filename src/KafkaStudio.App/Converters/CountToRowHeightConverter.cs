using System.Globalization;
using Avalonia.Controls;
using Avalonia.Data.Converters;

namespace KafkaStudio.App.Converters;

/// <summary>Produces the <see cref="GridLength"/> for a grid row that should collapse to zero height when
/// its content is empty, and otherwise start at a fixed pixel height the user can then drag with a
/// <see cref="GridSplitter"/>. <c>ConverterParameter</c> is the default height in pixels (defaults to 220).
/// The value may be a <see cref="bool"/> or an item count.</summary>
public sealed class CountToRowHeightConverter : IValueConverter
{
    public static readonly CountToRowHeightConverter Instance = new();

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        var hasContent = value switch
        {
            bool b => b,
            int i => i > 0,
            _ => value is not null,
        };

        if (!hasContent)
        {
            return new GridLength(0, GridUnitType.Pixel);
        }

        var height = 220d;
        if (parameter is string text && double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
        {
            height = parsed;
        }

        return new GridLength(height, GridUnitType.Pixel);
    }

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

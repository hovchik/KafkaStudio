using System.Globalization;
using Avalonia.Data.Converters;

namespace KafkaStudio.App.Converters;

/// <summary>Flips a Grid column index between 0 and 1 based on a bool flag, used to let the "Topics"
/// and "Messages" panels in the Topics tab be reorganized (swapped left/right) at runtime. The
/// converter parameter is the panel's normal (unswapped) column index, e.g. "0" or "1".</summary>
public sealed class SwapColumnConverter : IValueConverter
{
    public static readonly SwapColumnConverter Instance = new();

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        var normalColumn = parameter is string s ? int.Parse(s, CultureInfo.InvariantCulture) : 0;
        var swapped = value is true;
        return swapped ? 1 - normalColumn : normalColumn;
    }

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

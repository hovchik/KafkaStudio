using System.Globalization;
using Avalonia.Data.Converters;

namespace KafkaStudio.App.Converters;

/// <summary>Lifts the "Search results (all topics)" panel's height cap when the Topics/Messages panels
/// above it are both collapsed, so it can grow to fill the freed-up space instead of staying capped to
/// its small default height.</summary>
public sealed class BoolToMaxHeightConverter : IValueConverter
{
    public static readonly BoolToMaxHeightConverter Instance = new();

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        value is true ? double.PositiveInfinity : 260d;

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

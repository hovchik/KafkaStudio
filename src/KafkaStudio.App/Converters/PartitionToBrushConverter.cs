using System.Globalization;
using Avalonia.Data.Converters;
using Avalonia.Media;

namespace KafkaStudio.App.Converters;

/// <summary>Maps a Kafka partition number to a stable accent color from a small palette, purely so
/// messages from different partitions are visually distinguishable at a glance in the Consume (live)
/// view - the same partition always gets the same color for a given run.</summary>
public sealed class PartitionToBrushConverter : IValueConverter
{
    public static readonly PartitionToBrushConverter Instance = new();

    private const int PaletteSize = 8;

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        if (value is int partition)
        {
            var index = ((partition % PaletteSize) + PaletteSize) % PaletteSize;
            return ThemeBrushes.Get($"Partition{index}Brush");
        }

        return ThemeBrushes.Get("Partition0Brush");
    }

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

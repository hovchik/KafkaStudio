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

    private static readonly IBrush[] Palette =
    [
        new SolidColorBrush(Color.FromRgb(0x4C, 0xD9, 0x7B)),
        new SolidColorBrush(Color.FromRgb(0x56, 0x9C, 0xD6)),
        new SolidColorBrush(Color.FromRgb(0xE0, 0xAF, 0x68)),
        new SolidColorBrush(Color.FromRgb(0xC5, 0x86, 0xE8)),
        new SolidColorBrush(Color.FromRgb(0xFF, 0x6B, 0x6B)),
        new SolidColorBrush(Color.FromRgb(0x4F, 0xC1, 0xC1)),
        new SolidColorBrush(Color.FromRgb(0xDC, 0xDC, 0xAA)),
        new SolidColorBrush(Color.FromRgb(0x9C, 0xDC, 0xFE)),
    ];

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        if (value is int partition)
        {
            var index = ((partition % Palette.Length) + Palette.Length) % Palette.Length;
            return Palette[index];
        }

        return Palette[0];
    }

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

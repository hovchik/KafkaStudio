using System.Globalization;
using Avalonia.Data.Converters;
using Avalonia.Media;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.App.Converters;

/// <summary>Colors a step result's message green/red/gray by <see cref="StepStatus"/>, used by the
/// Script Editor's step results panel.</summary>
public sealed class StepStatusToBrushConverter : IValueConverter
{
    public static readonly StepStatusToBrushConverter Instance = new();

    private static readonly IBrush Passed = new SolidColorBrush(Color.Parse("#4CD97B"));
    private static readonly IBrush Failed = new SolidColorBrush(Color.Parse("#FF6B6B"));
    private static readonly IBrush Skipped = new SolidColorBrush(Color.Parse("#9AA0AC"));

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture) => value switch
    {
        StepStatus.Passed => Passed,
        StepStatus.Failed => Failed,
        StepStatus.Skipped => Skipped,
        _ => Skipped
    };

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

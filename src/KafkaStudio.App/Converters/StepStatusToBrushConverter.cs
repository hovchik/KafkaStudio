using System.Globalization;
using Avalonia.Data.Converters;
using Avalonia.Media;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.App.Converters;

/// <summary>Colors a step result by <see cref="StepStatus"/> (green/red/amber/gray), used by the
/// Script Editor's step results panel.</summary>
public sealed class StepStatusToBrushConverter : IValueConverter
{
    public static readonly StepStatusToBrushConverter Instance = new();

    private static readonly IBrush Passed = new SolidColorBrush(Color.Parse("#4CD97B"));
    private static readonly IBrush Failed = new SolidColorBrush(Color.Parse("#FF6B6B"));
    private static readonly IBrush Cancelled = new SolidColorBrush(Color.Parse("#E0AF68"));
    private static readonly IBrush Skipped = new SolidColorBrush(Color.Parse("#9AA0AC"));

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture) => value switch
    {
        StepStatus.Passed => Passed,
        StepStatus.Failed => Failed,
        StepStatus.Cancelled => Cancelled,
        _ => Skipped
    };

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

/// <summary>Maps a nullable pass/fail flag (true / false / null = unknown) to green / red / muted.</summary>
public sealed class PassFailBrushConverter : IValueConverter
{
    public static readonly PassFailBrushConverter Instance = new();

    private static readonly IBrush Passed = new SolidColorBrush(Color.Parse("#4CD97B"));
    private static readonly IBrush Failed = new SolidColorBrush(Color.Parse("#FF6B6B"));
    private static readonly IBrush Unknown = new SolidColorBrush(Color.Parse("#9AA0AC"));

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture) => value switch
    {
        true => Passed,
        false => Failed,
        _ => Unknown
    };

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

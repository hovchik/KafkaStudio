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

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture) => value switch
    {
        StepStatus.Passed => ThemeBrushes.Get("SuccessBrush"),
        StepStatus.Failed => ThemeBrushes.Get("DangerBrush"),
        StepStatus.Cancelled => ThemeBrushes.Get("WarningBrush"),
        _ => ThemeBrushes.Get("TextMutedBrush")
    };

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

/// <summary>Maps a nullable pass/fail flag (true / false / null = unknown) to green / red / muted.</summary>
public sealed class PassFailBrushConverter : IValueConverter
{
    public static readonly PassFailBrushConverter Instance = new();

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture) => value switch
    {
        true => ThemeBrushes.Get("SuccessBrush"),
        false => ThemeBrushes.Get("DangerBrush"),
        _ => ThemeBrushes.Get("TextMutedBrush")
    };

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

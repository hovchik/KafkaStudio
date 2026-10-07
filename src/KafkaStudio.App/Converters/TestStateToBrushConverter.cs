using System.Globalization;
using Avalonia.Data.Converters;
using Avalonia.Media;
using KafkaStudio.App.ViewModels.Testing;

namespace KafkaStudio.App.Converters;

/// <summary>Colors a test in the QA Lab's Test Runner by its <see cref="TestItemState"/>.</summary>
public sealed class TestStateToBrushConverter : IValueConverter
{
    public static readonly TestStateToBrushConverter Instance = new();

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture) => value switch
    {
        TestItemState.Passed => ThemeBrushes.Get("SuccessBrush"),
        TestItemState.Failed => ThemeBrushes.Get("DangerBrush"),
        TestItemState.Error => ThemeBrushes.Get("WarningBrush"),
        TestItemState.Running => ThemeBrushes.Get("AccentBrush"),
        _ => ThemeBrushes.Get("TextMutedBrush")
    };

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

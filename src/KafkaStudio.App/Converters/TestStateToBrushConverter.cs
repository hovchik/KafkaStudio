using System.Globalization;
using Avalonia.Data.Converters;
using Avalonia.Media;
using KafkaStudio.App.ViewModels.Testing;

namespace KafkaStudio.App.Converters;

/// <summary>Colors a test in the QA Lab's Test Runner by its <see cref="TestItemState"/>.</summary>
public sealed class TestStateToBrushConverter : IValueConverter
{
    public static readonly TestStateToBrushConverter Instance = new();

    private static readonly IBrush Passed = new SolidColorBrush(Color.Parse("#4CD97B"));
    private static readonly IBrush Failed = new SolidColorBrush(Color.Parse("#FF6B6B"));
    private static readonly IBrush Error = new SolidColorBrush(Color.Parse("#E0AF68"));
    private static readonly IBrush Running = new SolidColorBrush(Color.Parse("#6E8BFF"));
    private static readonly IBrush Idle = new SolidColorBrush(Color.Parse("#9AA0AC"));

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture) => value switch
    {
        TestItemState.Passed => Passed,
        TestItemState.Failed => Failed,
        TestItemState.Error => Error,
        TestItemState.Running => Running,
        _ => Idle
    };

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

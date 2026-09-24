using System.Globalization;
using Avalonia.Controls;
using Avalonia.Data.Converters;

namespace KafkaStudio.App.Converters;

/// <summary>Produces the <see cref="GridLength"/> for one row of the main content grid in
/// TopicBrowserView, switching between "*,Auto" (Topics/Messages row takes the remaining space, Search
/// results row sizes to its content) and "Auto,*" (the reverse) so that when both the Topics and Messages
/// panels are collapsed to their headers, the "Search results (all topics)" panel expands to fill the
/// freed-up space instead of staying capped to its small default height.
/// <c>ConverterParameter</c> selects which row this instance is sizing: "TopicsAndMessages" or
/// "SearchResults".</summary>
public sealed class BoolToRowHeightConverter : IValueConverter
{
    public static readonly BoolToRowHeightConverter Instance = new();

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        var collapsed = value is true;
        var isSearchResultsRow = string.Equals(parameter as string, "SearchResults", StringComparison.Ordinal);
        var isStarRow = isSearchResultsRow ? collapsed : !collapsed;
        return isStarRow ? new GridLength(1, GridUnitType.Star) : GridLength.Auto;
    }

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}

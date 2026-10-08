using Avalonia.Styling;
using KafkaStudio.Core.Persistence;

namespace KafkaStudio.App;

/// <summary>
/// Small, view-only preferences (currently just the theme) persisted next to the other KafkaStudio
/// data files. Kept out of the ViewModels layer because nothing there depends on it.
/// </summary>
internal static class UiSettings
{
    private const string FileName = "ui-settings.json";

    private sealed record Model(string? Theme);

    public static ThemeVariant LoadTheme()
    {
        var saved = JsonFileStore.Load(FileName, new Model(null));
        return string.Equals(saved.Theme, "light", StringComparison.OrdinalIgnoreCase)
            ? ThemeVariant.Light
            : ThemeVariant.Dark;
    }

    public static void SaveTheme(ThemeVariant variant)
    {
        // Best effort: a preference that fails to save is not worth an error in the UI.
        JsonFileStore.TrySave(FileName, new Model(variant == ThemeVariant.Light ? "light" : "dark"));
    }
}

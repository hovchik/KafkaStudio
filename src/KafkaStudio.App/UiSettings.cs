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

    private sealed record Model(string? Theme, bool SidebarCollapsed = false);

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
        var saved = JsonFileStore.Load(FileName, new Model(null));
        JsonFileStore.TrySave(FileName, saved with { Theme = variant == ThemeVariant.Light ? "light" : "dark" });
    }

    public static bool LoadSidebarCollapsed() => JsonFileStore.Load(FileName, new Model(null)).SidebarCollapsed;

    public static void SaveSidebarCollapsed(bool collapsed)
    {
        var saved = JsonFileStore.Load(FileName, new Model(null));
        JsonFileStore.TrySave(FileName, saved with { SidebarCollapsed = collapsed });
    }
}

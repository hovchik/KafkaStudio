using Avalonia;

namespace KafkaStudio.App;

internal static class Program
{
    // Avalonia's designer/previewer and platform backend both look for this exact
    // BuildAvaloniaApp() method by convention - don't rename it.
    [STAThread]
    public static void Main(string[] args) => BuildAvaloniaApp()
        .StartWithClassicDesktopLifetime(args);

    public static AppBuilder BuildAvaloniaApp() =>
        AppBuilder.Configure<App>()
            .UsePlatformDetect()
            .WithInterFont()
            .LogToTrace();
}

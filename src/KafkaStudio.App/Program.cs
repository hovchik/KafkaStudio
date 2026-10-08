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
            .With(WindowsRenderingOptions())
            .WithInterFont()
            .LogToTrace();

    /// <summary>
    /// Set <c>KAFKASTUDIO_GPU_RENDERING=1</c> to go back to Avalonia's default GPU (ANGLE/DirectX + WinUI
    /// composition) rendering path on Windows, e.g. to compare behaviour when diagnosing a display issue.
    /// </summary>
    private const string GpuRenderingVariable = "KAFKASTUDIO_GPU_RENDERING";

    /// <summary>
    /// Windows rendering backend. Avalonia's default on Windows is GPU rendering through ANGLE plus
    /// WinUI composition. That path has a known failure mode on some machines: after the window has
    /// been minimized (or the machine locked / asleep) for a long time the graphics device or the
    /// composition surface is reset, and when the window is restored it shows only its frame with
    /// nothing painted inside - the app looks hung although it is still running. Software rendering
    /// through a plain redirection surface has no GPU device to lose, so it survives this. The cost
    /// (CPU rasterisation) is negligible for a tool like this.
    /// </summary>
    private static Win32PlatformOptions WindowsRenderingOptions()
    {
        var useGpu = string.Equals(System.Environment.GetEnvironmentVariable(GpuRenderingVariable), "1", StringComparison.Ordinal);
        if (useGpu) return new Win32PlatformOptions();

        return new Win32PlatformOptions
        {
            RenderingMode = new[] { Win32RenderingMode.Software },
            CompositionMode = new[] { Win32CompositionMode.RedirectionSurface }
        };
    }
}

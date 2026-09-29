using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using Avalonia.Platform.Storage;
using Avalonia.Threading;
using KafkaStudio.App.ViewModels;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Connections;
using KafkaStudio.Kafka;

namespace KafkaStudio.App;

public partial class App : Application
{
    private MainWindowViewModel? _mainViewModel;
    private bool _shutdownCleanupDone;

    public override void Initialize() => AvaloniaXamlLoader.Load(this);

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            if (OperatingSystem.IsWindows())
            {
                // Saved SASL passwords are encrypted for the current Windows user instead of plain text.
                ConnectionProfileStore.Protector = new DpapiSecretProtector();
            }

            var window = new MainWindow();

            // Composition root: this is the one place that knows the real Kafka gateway
            // implementation exists (see AppState.RealGatewayFactory's doc comment for why the
            // ViewModels layer doesn't reference KafkaStudio.Kafka directly).
            var state = new AppState
            {
                RealGatewayFactory = profile => new ConfluentKafkaGateway(profile),
                PostToUi = action =>
                {
                    if (Dispatcher.UIThread.CheckAccess()) action();
                    else Dispatcher.UIThread.Post(action);
                },
                FileDialogs = new StorageFileDialogService(window),
                SetClipboardText = async text =>
                {
                    if (window.Clipboard is { } clipboard) await clipboard.SetTextAsync(text);
                }
            };

            _mainViewModel = new MainWindowViewModel(state);
            window.DataContext = _mainViewModel;
            desktop.MainWindow = window;
            desktop.ShutdownRequested += OnShutdownRequested;

            // Last-resort safety net: report instead of crashing on an exception thrown on the UI thread.
            Dispatcher.UIThread.UnhandledException += (_, e) =>
            {
                e.Handled = true;
                _mainViewModel.Notification = $"Unexpected error: {e.Exception.Message}";
            };
            TaskScheduler.UnobservedTaskException += (_, e) => e.SetObserved();

            // Reconnect any connections saved by a previous session in the background so
            // slow/unreachable brokers don't block the UI from showing up.
            _ = state.LoadPersistedConnectionsAsync();
        }

        base.OnFrameworkInitializationCompleted();
    }

    /// <summary>
    /// Stops scheduled tasks, rethrow relays and live consumers, and flushes producers before the
    /// process exits. The first shutdown request is cancelled so this can finish (bounded by a
    /// timeout), then shutdown is requested again.
    /// </summary>
    private async void OnShutdownRequested(object? sender, ShutdownRequestedEventArgs e)
    {
        if (_shutdownCleanupDone || _mainViewModel is null) return;
        e.Cancel = true;
        _shutdownCleanupDone = true;

        try
        {
            await _mainViewModel.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        }
        catch
        {
            // Don't keep the app alive because cleanup failed or hung.
        }

        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            desktop.Shutdown();
        }
    }
}

/// <summary>Open/save dialogs via Avalonia's StorageProvider.</summary>
internal sealed class StorageFileDialogService(TopLevel topLevel) : IFileDialogService
{
    private static FilePickerFileType[] Filters(string extension, string filterName) =>
    [
        new FilePickerFileType(filterName) { Patterns = [$"*{extension}"] },
        FilePickerFileTypes.All
    ];

    public async Task<string?> PickOpenFileAsync(string title, string extension, string filterName)
    {
        var files = await topLevel.StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions
        {
            Title = title,
            AllowMultiple = false,
            FileTypeFilter = Filters(extension, filterName)
        });
        return files.Count == 0 ? null : files[0].TryGetLocalPath();
    }

    public async Task<string?> PickSaveFileAsync(string title, string extension, string filterName, string suggestedName)
    {
        var file = await topLevel.StorageProvider.SaveFilePickerAsync(new FilePickerSaveOptions
        {
            Title = title,
            SuggestedFileName = suggestedName,
            DefaultExtension = extension.TrimStart('.'),
            ShowOverwritePrompt = true,
            FileTypeChoices = Filters(extension, filterName)
        });
        return file?.TryGetLocalPath();
    }
}

/// <summary>Encrypts secrets with Windows DPAPI, scoped to the current user.</summary>
[System.Runtime.Versioning.SupportedOSPlatform("windows")]
internal sealed class DpapiSecretProtector : ISecretProtector
{
    private static readonly byte[] Entropy = "KafkaStudio.ConnectionProfile.v1"u8.ToArray();

    public string Scheme => "dpapi";

    public string Protect(string plaintext) =>
        Convert.ToBase64String(System.Security.Cryptography.ProtectedData.Protect(
            System.Text.Encoding.UTF8.GetBytes(plaintext), Entropy, System.Security.Cryptography.DataProtectionScope.CurrentUser));

    public string Unprotect(string protectedValue) =>
        System.Text.Encoding.UTF8.GetString(System.Security.Cryptography.ProtectedData.Unprotect(
            Convert.FromBase64String(protectedValue), Entropy, System.Security.Cryptography.DataProtectionScope.CurrentUser));
}

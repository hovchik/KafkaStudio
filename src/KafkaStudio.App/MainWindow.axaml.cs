using Avalonia;
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Interactivity;
using Avalonia.Media;
using Avalonia.Styling;
using Avalonia.Threading;
using KafkaStudio.App.ViewModels;
using KafkaStudio.App.ViewModels.Mvvm;

namespace KafkaStudio.App;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
        ToggleThemeCommand = new RelayCommand(ToggleTheme);
        ActualThemeVariantChanged += (_, _) => UpdateThemeButton();
        UpdateThemeButton();
    }

    /// <summary>Flips Dark/Light (Ctrl+Shift+T and the sidebar button).</summary>
    public RelayCommand ToggleThemeCommand { get; }

    protected override void OnKeyDown(KeyEventArgs e)
    {
        base.OnKeyDown(e);
        if (e.Handled) return;

        // Esc closes the Connections flyout / dismisses a notification.
        if (e.Key == Key.Escape && DataContext is MainWindowViewModel vm)
        {
            if (vm.IsConnectionsOpen)
            {
                vm.IsConnectionsOpen = false;
                e.Handled = true;
            }
            else if (vm.Notification is not null)
            {
                vm.Notification = null;
                e.Handled = true;
            }
        }
    }

    protected override void OnPropertyChanged(AvaloniaPropertyChangedEventArgs change)
    {
        base.OnPropertyChanged(change);

        // Coming back from minimized: Windows may have thrown away the window's surface while it was
        // hidden (long minimize, lock screen, sleep), and the renderer doesn't always notice on its own,
        // leaving an empty frame. Explicitly mark the whole window dirty so the next frame repaints it.
        if (change.Property == WindowStateProperty &&
            change.GetOldValue<WindowState>() == WindowState.Minimized &&
            change.GetNewValue<WindowState>() != WindowState.Minimized)
        {
            Dispatcher.UIThread.Post(() =>
            {
                InvalidateMeasure();
                InvalidateVisual();
            }, DispatcherPriority.Render);
        }
    }

    private void OnFlyoutBackdropPressed(object? sender, PointerPressedEventArgs e)
    {
        if (DataContext is MainWindowViewModel vm) vm.IsConnectionsOpen = false;
    }

    private void OnToggleThemeClick(object? sender, RoutedEventArgs e) => ToggleTheme();

    private void ToggleTheme()
    {
        if (Application.Current is not { } app) return;
        var next = app.ActualThemeVariant == ThemeVariant.Light ? ThemeVariant.Dark : ThemeVariant.Light;
        app.RequestedThemeVariant = next;
        UiSettings.SaveTheme(next);
    }

    private void UpdateThemeButton()
    {
        var isLight = ActualThemeVariant == ThemeVariant.Light;
        // The button offers the *other* theme.
        ThemeLabel.Text = isLight ? "Dark theme" : "Light theme";
        if (this.TryFindResource(isLight ? "Icon.moon" : "Icon.sun", out var icon) && icon is Geometry geometry)
        {
            ThemeIcon.Data = geometry;
        }
    }
}

using Avalonia.Controls;
using Avalonia.Input;
using KafkaStudio.App.ViewModels;

namespace KafkaStudio.App;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
    }

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

    private void OnFlyoutBackdropPressed(object? sender, PointerPressedEventArgs e)
    {
        if (DataContext is MainWindowViewModel vm) vm.IsConnectionsOpen = false;
    }
}

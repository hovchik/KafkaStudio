using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using KafkaStudio.App.ViewModels;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Kafka;

namespace KafkaStudio.App;

public partial class App : Application
{
    private MainWindowViewModel? _mainViewModel;

    public override void Initialize() => AvaloniaXamlLoader.Load(this);

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            // Composition root: this is the one place that knows the real Kafka gateway
            // implementation exists (see AppState.RealGatewayFactory's doc comment for why the
            // ViewModels layer doesn't reference KafkaStudio.Kafka directly).
            var state = new AppState
            {
                RealGatewayFactory = profile => new ConfluentKafkaGateway(profile)
            };

            _mainViewModel = new MainWindowViewModel(state);

            desktop.MainWindow = new MainWindow { DataContext = _mainViewModel };
            desktop.ShutdownRequested += OnShutdownRequested;
        }

        base.OnFrameworkInitializationCompleted();
    }

    private async void OnShutdownRequested(object? sender, ShutdownRequestedEventArgs e)
    {
        if (_mainViewModel is not null)
        {
            await _mainViewModel.DisposeAsync();
        }
    }
}

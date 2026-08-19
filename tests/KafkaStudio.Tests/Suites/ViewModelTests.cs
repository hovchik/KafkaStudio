using KafkaStudio.App.ViewModels;
using KafkaStudio.App.ViewModels.Scripts;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.App.ViewModels.Tasks;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

public static class ViewModelTests
{
    public static void Register(TestRunner runner)
    {
        runner.Add("ViewModels: Connections", "adding a demo connection makes it visible everywhere", async () =>
        {
            var state = new AppState();
            var main = new MainWindowViewModel(state);

            state.AddDemoConnection("local");

            Assert.Equal(1, main.Connections.Connections.Count);
            Assert.Equal(1, main.Producer.ConnectionNames.Count);
            Assert.Equal(1, main.Topics.ConnectionNames.Count);

            await main.DisposeAsync();
        });

        runner.Add("ViewModels: Producer", "SendCommand produces a message through the shared state", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var main = new MainWindowViewModel(state);

            main.Producer.SelectedConnection = "local";
            main.Producer.Topic = "orders";
            main.Producer.Value = "hello";

            Assert.True(main.Producer.SendCommand.CanExecute(null));
            await main.Producer.SendCommand.Execute2();

            Assert.Equal(1, main.Producer.History.Count);

            await main.DisposeAsync();
        });

        runner.Add("ViewModels: Scripts", "parses on construction and exposes a parse error for bad input", () =>
        {
            var state = new AppState();
            var vm = new ScriptEditorViewModel(state);

            Assert.NotNull(vm.Document);
            Assert.Null(vm.ParseError);

            vm.Source = "Scenario: Broken\nWhen teleport to topic \"x\"\n";
            Assert.NotNull(vm.ParseError);
            Assert.Null(vm.Document);
        });

        runner.Add("ViewModels: Scripts", "RunAllCommand executes the parsed scenario end to end", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var vm = new ScriptEditorViewModel(state);
            vm.Source = """
                Scenario: Simple produce
                Given use connection "local"
                When produce message to topic "orders" value "hi"
                """;

            await vm.RunAllCommand.Execute2();

            Assert.Contains("1 scenario(s)/task(s) passed", vm.RunSummary ?? "");
            Assert.Equal(2, vm.StepResults.Count);
        });

        runner.Add("ViewModels: Tasks", "registering a Task block schedules a job the UI can see", () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var vm = new TasksViewModel(state);

            vm.NewTaskSource = """
                Task: Heartbeat
                schedule every 10 minutes
                Given use connection "local"
                When produce message to topic "heartbeats" value "ping"
                """;
            vm.RegisterTaskCommand.Execute(null);

            Assert.Equal(1, vm.Jobs.Count);
            Assert.Equal("Heartbeat", vm.Jobs[0].Name);
            Assert.Contains("every", vm.Jobs[0].Schedule);
        });
    }
}

file static class AsyncRelayCommandTestExtensions
{
    // AsyncRelayCommand.Execute(object?) is "async void" (required by ICommand), which is awkward to
    // await directly from a test. This helper re-invokes the same underlying delegate in a way tests
    // can await, without changing the production ICommand surface.
    public static async Task Execute2(this KafkaStudio.App.ViewModels.Mvvm.AsyncRelayCommand command)
    {
        command.Execute(null);
        while (command.IsRunning) await Task.Delay(10);
    }
}

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

        runner.Add("ViewModels: Brokers", "lists the demo cluster's broker, marks the controller and loads its config", async () =>
        {
            var state = new AppState();
            var main = new MainWindowViewModel(state);
            state.AddDemoConnection("local");

            await main.Brokers.RefreshAsync();

            Assert.Equal(1, main.Brokers.Brokers.Count);
            Assert.True(main.Brokers.Brokers[0].IsController);
            Assert.True(main.Brokers.ClusterSummary!.Contains("controller 0"));
            await Task.Delay(50);
            Assert.True(main.Brokers.Config.Count > 0, "broker config was not loaded");

            await main.DisposeAsync();
        });

        runner.Add("ViewModels: Connections", "exported connections import back, passwords only when asked", async () =>
        {
            var state = new AppState();
            var main = new MainWindowViewModel(state);
            state.AddDemoConnection("demo-a");
            var secured = new KafkaStudio.Core.Connections.ConnectionProfile
            {
                Name = "secured", BootstrapServers = "broker:9092",
                SecurityProtocol = KafkaStudio.Core.Connections.SecurityProtocolKind.SaslSsl,
                SaslMechanism = KafkaStudio.Core.Connections.SaslMechanismKind.Plain,
                SaslUsername = "u", SaslPassword = "secret"
            };

            var withoutSecrets = KafkaStudio.Core.Connections.ConnectionProfileTransfer.Export(new[] { secured }, includePasswords: false);
            Assert.False(withoutSecrets.Contains("secret"));
            var json = KafkaStudio.Core.Connections.ConnectionProfileTransfer.Export(
                state.ConnectionProfiles.Values.Append(secured), includePasswords: true);

            var path = Path.Combine(Path.GetTempPath(), $"ks-conn-{Guid.NewGuid():N}.json");
            File.WriteAllText(path, json);
            try
            {
                var target = new AppState();
                var targetMain = new MainWindowViewModel(target);
                await targetMain.Connections.ImportConnectionsFromFileAsync(path);
                Assert.Equal(2, targetMain.Connections.Connections.Count);
                Assert.True(target.ConnectionProfiles["demo-a"].IsDemoConnection);
                Assert.Equal("secret", target.ConnectionProfiles["secured"].SaslPassword);
                Assert.Equal(KafkaStudio.Core.Connections.SecurityProtocolKind.SaslSsl, target.ConnectionProfiles["secured"].SecurityProtocol);
                await targetMain.DisposeAsync();
            }
            finally
            {
                File.Delete(path);
            }
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
            Assert.Equal(2, vm.StepResults.Count(r => !r.IsBlockHeader));
            Assert.Equal(1, vm.StepResults.Count(r => r.IsBlockHeader));
            Assert.Contains("produce message", vm.StepResults.Last().Description);
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

        runner.Add("ViewModels: Tasks", "scheduled tasks never run on their own - only via Run now", () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var vm = new TasksViewModel(state);

            vm.NewTaskSource = """
                Task: Heartbeat
                schedule run once
                Given use connection "local"
                When produce message to topic "heartbeats" value "ping"
                """;
            vm.RegisterTaskCommand.Execute(null);
            System.Threading.Thread.Sleep(2500); // longer than the scheduler's 1s tick

            Assert.Equal(0, vm.Jobs[0].Job.RunCount);
            Assert.Equal("manual only", vm.Jobs[0].NextRun);
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

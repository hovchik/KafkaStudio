using KafkaStudio.App.ViewModels;
using KafkaStudio.App.ViewModels.Scripts;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.App.ViewModels.Tasks;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Testing;
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

        runner.Add("ViewModels: Producer", "a repeated send carries the form as it was when Send was clicked", async () =>
        {
            var state = new AppState();
            var gateway = new SlowProduceGateway(new InMemoryKafkaGateway(
                new KafkaStudio.Core.Connections.ConnectionProfile { Name = "slow", BootstrapServers = "x:1" }, state.DemoBroker));
            state.AddConnection(gateway.Profile, gateway);
            var main = new MainWindowViewModel(state);

            main.Producer.SelectedConnection = "slow";
            main.Producer.Topic = "orders";
            main.Producer.Value = "hello";
            main.Producer.RepeatCount = 3;

            main.Producer.SendCommand.Execute(null);
            // The first message is in flight: edits made now must not leak into the remaining ones.
            main.Producer.Value = "changed";
            main.Producer.IsTombstone = true;
            while (main.Producer.SendCommand.IsRunning) await Task.Delay(10);

            Assert.Equal(3, gateway.ProducedValues.Count);
            Assert.True(gateway.ProducedValues.All(v => v == "hello"), "a mid-run edit changed what was sent");

            await main.DisposeAsync();
        });

        runner.Add("ViewModels: Consumer", "switching connection stops the watch and late messages are discarded", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("a");
            state.AddDemoConnection("b");
            var main = new MainWindowViewModel(state);
            var consumer = main.Consumer;
            var gateway = state.Connections["a"];

            consumer.SelectedConnection = "a";
            consumer.Topic = "orders";
            consumer.StartCommand.Execute(null);
            Assert.True(consumer.IsWatching);
            await WaitUntil(() => consumer.StatusMessage == "Watching 'orders'."); // Latest: only messages after subscribing
            await gateway.ProduceAsync(new ProduceRequest { Topic = "orders", Value = "first" });
            await WaitUntil(() => consumer.Messages.Count == 1);

            consumer.SelectedConnection = "b";
            Assert.False(consumer.IsWatching, "changing the connection must stop the watch");
            Assert.Equal("Stopped.", consumer.StatusMessage);

            await gateway.ProduceAsync(new ProduceRequest { Topic = "orders", Value = "late" });
            await Task.Delay(100);
            Assert.Equal(1, consumer.Messages.Count);
            Assert.Equal(1L, consumer.ReceivedCount);
            Assert.Equal("Stopped.", consumer.StatusMessage);

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
                Task: Pinger
                schedule every 10 minutes
                Given use connection "local"
                When produce message to topic "pings" value "ping"
                """;
            vm.RegisterTaskCommand.Execute(null);

            Assert.Equal(1, vm.Jobs.Count);
            Assert.Equal("Pinger", vm.Jobs[0].Name);
            Assert.Contains("every", vm.Jobs[0].Schedule);
        });

        runner.Add("ViewModels: Tasks", "scheduled tasks never run on their own - only via Run now", () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var vm = new TasksViewModel(state);

            vm.NewTaskSource = """
                Task: Pinger
                schedule run once
                Given use connection "local"
                When produce message to topic "pings" value "ping"
                """;
            vm.RegisterTaskCommand.Execute(null);
            System.Threading.Thread.Sleep(2500); // longer than the scheduler's 1s tick

            Assert.Equal(0, vm.Jobs[0].Job.RunCount);
            Assert.Equal("manual only", vm.Jobs[0].NextRun);
        });

        runner.Add("ViewModels: Tasks", "re-registering a task that is mid-run keeps the running job", async () =>
        {
            var state = new AppState();
            state.AddDemoConnection("local");
            var vm = new TasksViewModel(state);

            vm.NewTaskSource = """
                Task: Sleeper
                Given use connection "local"
                Given wait for 1 hour
                """;
            vm.RegisterTaskCommand.Execute(null);
            // Jobs also holds tasks persisted by earlier tests, so look ours up by name.
            var row = vm.Jobs.Single(j => j.Name == "Sleeper");
            var count = vm.Jobs.Count;
            vm.RunNowCommand.Execute(row);
            await WaitUntil(() => row.IsRunning);

            vm.RegisterTaskCommand.Execute(null);

            Assert.Equal(count, vm.Jobs.Count);
            Assert.True(ReferenceEquals(row, vm.Jobs.Single(j => j.Name == "Sleeper")), "the running job was replaced");
            Assert.Contains("still running", vm.StatusMessage ?? "");

            await state.DisposeAsync(); // cancels the hour-long run
        });
    }

    private static async Task WaitUntil(Func<bool> condition, int timeoutMs = 5000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new AssertionFailedException("timed out waiting for condition");
            await Task.Delay(10);
        }
    }

    /// <summary>Demo gateway whose produces take a moment, so a test can edit the form mid-run.</summary>
    private sealed class SlowProduceGateway : IKafkaGateway
    {
        private readonly InMemoryKafkaGateway _inner;
        public SlowProduceGateway(InMemoryKafkaGateway inner) => _inner = inner;

        public List<string?> ProducedValues { get; } = new();

        public async Task<ProduceReceipt> ProduceAsync(ProduceRequest request, CancellationToken cancellationToken = default)
        {
            await Task.Delay(30, cancellationToken);
            ProducedValues.Add(request.Value);
            return await _inner.ProduceAsync(request, cancellationToken);
        }

        public KafkaStudio.Core.Connections.ConnectionProfile Profile => _inner.Profile;
        public Task ConnectAsync(CancellationToken ct = default) => _inner.ConnectAsync(ct);
        public Task<IReadOnlyList<string>> ListTopicsAsync(CancellationToken ct = default) => _inner.ListTopicsAsync(ct);
        public Task<TopicMetadata> DescribeTopicAsync(string topic, CancellationToken ct = default) => _inner.DescribeTopicAsync(topic, ct);
        public Task<ClusterInfo> DescribeClusterAsync(CancellationToken ct = default) => _inner.DescribeClusterAsync(ct);
        public Task<IReadOnlyList<BrokerConfigEntry>> GetBrokerConfigAsync(int brokerId, CancellationToken ct = default) => _inner.GetBrokerConfigAsync(brokerId, ct);
        public Task CreateTopicAsync(string topic, int partitions, short replicationFactor, CancellationToken ct = default) => _inner.CreateTopicAsync(topic, partitions, replicationFactor, ct);
        public IAsyncEnumerable<KafkaMessage> ConsumeAsync(ConsumeOptions options, CancellationToken ct = default) => _inner.ConsumeAsync(options, ct);
        public Task<bool> IsTopicCompactedAsync(string topic, CancellationToken ct = default) => _inner.IsTopicCompactedAsync(topic, ct);
        public Task AcknowledgeAsync(KafkaMessage message, CancellationToken ct = default) => _inner.AcknowledgeAsync(message, ct);
        public Task<IReadOnlyList<ConsumerGroupSummary>> ListConsumerGroupsAsync(CancellationToken ct = default) => _inner.ListConsumerGroupsAsync(ct);
        public Task<ConsumerGroupDetail> DescribeConsumerGroupAsync(string groupId, CancellationToken ct = default) => _inner.DescribeConsumerGroupAsync(groupId, ct);
        public Task<IReadOnlyList<OffsetChange>> PlanOffsetResetAsync(OffsetResetRequest request, CancellationToken ct = default) => _inner.PlanOffsetResetAsync(request, ct);
        public Task ApplyOffsetResetAsync(string groupId, IReadOnlyList<OffsetChange> changes, CancellationToken ct = default) => _inner.ApplyOffsetResetAsync(groupId, changes, ct);
        public ValueTask DisposeAsync() => _inner.DisposeAsync();
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

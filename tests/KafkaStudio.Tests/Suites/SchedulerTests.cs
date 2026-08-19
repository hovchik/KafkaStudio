using KafkaStudio.Automation.Scheduling;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Testing;
using KafkaStudio.Scripting.Parsing;
using KafkaStudio.Scripting.Runtime;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

public static class SchedulerTests
{
    public static void Register(TestRunner runner)
    {
        runner.Add("AutomationScheduler", "'run once' fires exactly once", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(broker) };

            var doc = Parser.Parse("""
                Task: One shot
                schedule run once
                When produce message to topic "heartbeats" value "ping"
                """);

            await using var scheduler = new AutomationScheduler();
            var completions = 0;
            scheduler.RunCompleted += (_, _) => Interlocked.Increment(ref completions);

            var job = scheduler.Register("one-shot", doc.Blocks[0], connections);
            scheduler.Start();

            await WaitUntilAsync(() => job.RunCount >= 1, TimeSpan.FromSeconds(3));
            await Task.Delay(1500); // long enough that a buggy scheduler would have fired it again

            Assert.Equal(1, job.RunCount);
            Assert.Null(job.NextRunAt);
        });

        runner.Add("AutomationScheduler", "'every' schedule fires repeatedly", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(broker) };

            var doc = Parser.Parse("""
                Task: Heartbeat
                schedule every 1 seconds
                When produce message to topic "heartbeats" value "ping"
                """);

            await using var scheduler = new AutomationScheduler();
            var job = scheduler.Register("heartbeat", doc.Blocks[0], connections);
            scheduler.Start();

            await WaitUntilAsync(() => job.RunCount >= 2, TimeSpan.FromSeconds(5));

            Assert.True(job.RunCount >= 2, $"expected at least 2 runs, got {job.RunCount}");
        });

        runner.Add("AutomationScheduler", "RunNowAsync runs a job immediately regardless of schedule", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var connections = new Dictionary<string, IKafkaGateway> { ["local"] = TestKafka.NewGateway(broker) };

            var doc = Parser.Parse("""
                Scenario: Manual trigger
                Given use connection "local"
                When produce message to topic "orders" value "hi"
                """);

            await using var scheduler = new AutomationScheduler();
            ScriptRunResult? captured = null;
            scheduler.RunCompleted += (_, result) => captured = result;

            scheduler.Register("manual", doc.Blocks[0], connections);
            await scheduler.RunNowAsync("manual");

            Assert.NotNull(captured);
            Assert.True(captured!.Success);
        });
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
        }
    }
}

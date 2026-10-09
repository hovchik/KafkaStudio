using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Testing;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

/// <summary>The in-memory (demo) broker: partitions, partitioner, per-partition offsets, live group members, compaction.</summary>
public static class MockBrokerTests
{
    private const string Suite = "Mock broker";

    public static void Register(TestRunner runner)
    {
        runner.Add(Suite, "create-topic honours partitions and replication factor", async () =>
        {
            var broker = new InMemoryKafkaBroker(brokerCount: 3);
            var gw = TestKafka.NewGateway(broker);
            await gw.CreateTopicAsync("orders", 4, 2);

            var meta = await gw.DescribeTopicAsync("orders");
            Assert.Equal(4, meta.Partitions.Count);
            Assert.Equal(2, meta.ReplicationFactor);
            Assert.Equal("0,1,2,0", string.Join(",", meta.Partitions.Select(p => p.LeaderBrokerId)));
            Assert.Equal(3, (await gw.DescribeClusterAsync()).Brokers.Count);

            await Assert.ThrowsAsync<InvalidOperationException>(() => gw.CreateTopicAsync("too-safe", 1, 4));
            await Assert.ThrowsAsync<ArgumentException>(() => gw.CreateTopicAsync("empty", 0, 1));
            await Assert.ThrowsAsync<InvalidOperationException>(() => gw.CreateTopicAsync("orders", 1, 1));
        });

        runner.Add(Suite, "a key always lands on the same partition and offsets count per partition", async () =>
        {
            var gw = TestKafka.NewGateway(new InMemoryKafkaBroker());
            await gw.CreateTopicAsync("t", 3, 1);

            var first = await gw.ProduceAsync(new ProduceRequest { Topic = "t", Key = "ORD-7", Value = "a" });
            for (var i = 0; i < 5; i++)
            {
                var again = await gw.ProduceAsync(new ProduceRequest { Topic = "t", Key = "ORD-7", Value = "b" });
                Assert.Equal(first.Partition, again.Partition);
                Assert.Equal(first.Offset + i + 1, again.Offset);
            }

            var seen = new HashSet<int>();
            for (var i = 0; i < 40; i++)
            {
                seen.Add((await gw.ProduceAsync(new ProduceRequest { Topic = "t", Key = $"k{i}", Value = "x" })).Partition);
            }
            Assert.Equal(3, seen.Count);
        });

        runner.Add(Suite, "keyless messages rotate over the partitions", async () =>
        {
            var gw = TestKafka.NewGateway(new InMemoryKafkaBroker());
            await gw.CreateTopicAsync("t", 3, 1);
            var partitions = new List<int>();
            for (var i = 0; i < 6; i++) partitions.Add((await gw.ProduceAsync(new ProduceRequest { Topic = "t", Value = "x" })).Partition);
            Assert.Equal(3, partitions.Distinct().Count());
            Assert.Equal(2, partitions.Count(p => p == partitions[0]));
        });

        runner.Add(Suite, "an explicit partition is honoured and a missing one is rejected", async () =>
        {
            var gw = TestKafka.NewGateway(new InMemoryKafkaBroker());
            await gw.CreateTopicAsync("t", 3, 1);
            var receipt = await gw.ProduceAsync(new ProduceRequest { Topic = "t", Key = "any", Value = "x", Partition = 2 });
            Assert.Equal(2, receipt.Partition);
            Assert.Equal(0L, receipt.Offset);
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                () => gw.ProduceAsync(new ProduceRequest { Topic = "t", Value = "x", Partition = 3 }));
        });

        runner.Add(Suite, "consuming reads every partition; earliest/latest/tail apply per partition", async () =>
        {
            var gw = TestKafka.NewGateway(new InMemoryKafkaBroker());
            await gw.CreateTopicAsync("t", 2, 1);
            for (var i = 0; i < 6; i++) await gw.ProduceAsync(new ProduceRequest { Topic = "t", Value = $"v{i}", Partition = i % 2 });

            async Task<List<KafkaMessage>> Read(ConsumeOptions o)
            {
                var list = new List<KafkaMessage>();
                await foreach (var m in gw.ConsumeAsync(o)) list.Add(m);
                return list;
            }
            ConsumeOptions Opts(ConsumeStartPosition pos, int tail = 50) => new()
            {
                Topic = "t", ConsumerGroup = "g", StartPosition = pos, TailCount = tail, StopAtPartitionEnd = true
            };

            Assert.Equal(6, (await Read(Opts(ConsumeStartPosition.Earliest))).Count);
            Assert.Equal(0, (await Read(Opts(ConsumeStartPosition.Latest))).Count);
            var tail = await Read(Opts(ConsumeStartPosition.Tail, 1));
            Assert.Equal(2, tail.Count);
            Assert.Equal("0,1", string.Join(",", tail.Select(m => m.Partition).OrderBy(p => p)));
            Assert.True(tail.All(m => m.Offset == 2));
        });

        runner.Add(Suite, "commits and resets are tracked per partition", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gw = TestKafka.NewGateway(broker);
            await gw.CreateTopicAsync("t", 2, 1);
            for (var i = 0; i < 6; i++) await gw.ProduceAsync(new ProduceRequest { Topic = "t", Value = $"v{i}", Partition = i % 2 });

            // The group consumed all of partition 0 and nothing of partition 1.
            await foreach (var m in gw.ConsumeAsync(new ConsumeOptions
            {
                Topic = "t", ConsumerGroup = "g", StartPosition = ConsumeStartPosition.Earliest, StopAtPartitionEnd = true, AutoAcknowledge = true
            }))
            {
                if (m.Partition == 1) break;
            }

            var detail = await gw.DescribeConsumerGroupAsync("g");
            Assert.True(detail.Offsets.Count >= 1);
            Assert.Equal(0, detail.Offsets.Count(o => o.Partition > 1));

            var plan = await gw.PlanOffsetResetAsync(new OffsetResetRequest { GroupId = "g", Target = OffsetResetTarget.Earliest, Partition = 0 });
            Assert.True(plan.All(c => c.Partition == 0));
            await gw.ApplyOffsetResetAsync("g", plan);
            Assert.Equal(-1L, broker.GetCommittedOffset("t", "g", 0));

            // Resuming from committed now starts partition 0 from the beginning again.
            var resumed = new List<KafkaMessage>();
            await foreach (var m in gw.ConsumeAsync(new ConsumeOptions
            {
                Topic = "t", ConsumerGroup = "g", StartPosition = ConsumeStartPosition.Committed,
                UncommittedStart = ConsumeStartPosition.Earliest, StopAtPartitionEnd = true
            })) resumed.Add(m);
            Assert.Equal(3, resumed.Count(m => m.Partition == 0));
        });

        runner.Add(Suite, "a reading consumer is a live group member and blocks offset resets", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gw = TestKafka.NewGateway(broker);
            await gw.CreateTopicAsync("t", 2, 1);
            await gw.ProduceAsync(new ProduceRequest { Topic = "t", Value = "x", Partition = 0 });
            broker.Commit("t", "watchers", 0);

            using var cts = new CancellationTokenSource();
            var ready = new TaskCompletionSource();
            var reader = Task.Run(async () =>
            {
                try
                {
                    await foreach (var _ in gw.ConsumeAsync(new ConsumeOptions
                    {
                        Topic = "t", ConsumerGroup = "watchers", StartPosition = ConsumeStartPosition.Latest, OnReady = () => ready.TrySetResult()
                    }, cts.Token)) { }
                }
                catch (OperationCanceledException) { }
            });
            await ready.Task;

            var detail = await gw.DescribeConsumerGroupAsync("watchers");
            Assert.True(detail.IsActive);
            Assert.Equal(1, detail.Members.Count);
            Assert.Equal(2, detail.Members[0].Assignment.Count); // owns both partitions of the topic
            var plan = await gw.PlanOffsetResetAsync(new OffsetResetRequest { GroupId = "watchers", Target = OffsetResetTarget.Earliest });
            await Assert.ThrowsAsync<InvalidOperationException>(() => gw.ApplyOffsetResetAsync("watchers", plan));

            cts.Cancel();
            await reader;
            Assert.False((await gw.DescribeConsumerGroupAsync("watchers")).IsActive);
            await gw.ApplyOffsetResetAsync("watchers", plan);
        });

        runner.Add(Suite, "concurrent producers never make a live consumer skip a message", async () =>
        {
            var gw = TestKafka.NewGateway(new InMemoryKafkaBroker());
            await gw.CreateTopicAsync("t", 1, 1);
            const int total = 2000;

            var received = 0;
            var ready = new TaskCompletionSource();
            var reader = Task.Run(async () =>
            {
                await foreach (var _ in gw.ConsumeAsync(new ConsumeOptions
                {
                    Topic = "t", ConsumerGroup = "g", StartPosition = ConsumeStartPosition.Latest, MaxMessages = total,
                    OnReady = () => ready.TrySetResult()
                })) Interlocked.Increment(ref received);
            });
            await ready.Task;

            await Task.WhenAll(Enumerable.Range(0, 8).Select(w => Task.Run(async () =>
            {
                for (var i = 0; i < total / 8; i++) await gw.ProduceAsync(new ProduceRequest { Topic = "t", Value = $"{w}-{i}" });
            })));

            var finished = await Task.WhenAny(reader, Task.Delay(10_000));
            Assert.True(ReferenceEquals(finished, reader), $"consumer only saw {Volatile.Read(ref received)} of {total} messages");
        });

        runner.Add(Suite, "compaction keeps the newest message per key and drops tombstoned keys", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gw = TestKafka.NewGateway(broker);
            await gw.CreateTopicAsync("t", 1, 1);
            foreach (var (key, value) in new (string, string?)[] { ("a", "1"), ("b", "1"), ("a", "2"), ("b", null), ("c", "1") })
            {
                await gw.ProduceAsync(new ProduceRequest { Topic = "t", Key = key, Value = value });
            }

            Assert.Equal(3, broker.Compact("t"));

            var kept = new List<KafkaMessage>();
            await foreach (var m in gw.ConsumeAsync(new ConsumeOptions
            {
                Topic = "t", ConsumerGroup = "g", StartPosition = ConsumeStartPosition.Earliest, StopAtPartitionEnd = true
            })) kept.Add(m);
            Assert.Equal("a=2,c=1", string.Join(",", kept.Select(m => $"{m.Key}={m.Value}")));
            Assert.Equal("2,4", string.Join(",", kept.Select(m => m.Offset))); // offsets are never reassigned
            Assert.Equal(5L, (await gw.DescribeTopicAsync("t")).Partitions[0].LatestOffset);
        });
    }
}

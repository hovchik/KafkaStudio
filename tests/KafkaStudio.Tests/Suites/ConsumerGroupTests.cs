using KafkaStudio.App.ViewModels.ConsumerGroups;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Testing;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

/// <summary>Consumer group listing, lag and offset resets - the gateway contract (in-memory) and the Consumer Groups screen.</summary>
public static class ConsumerGroupTests
{
    private static async Task<(AppState state, InMemoryKafkaBroker broker)> Seeded()
    {
        var state = new AppState();
        state.AddDemoConnection("local");
        var gateway = state.Connections["local"];
        for (var i = 0; i < 10; i++)
        {
            await gateway.ProduceAsync(new ProduceRequest { Topic = "orders", Key = $"ORD-{i}", Value = $"{{\"n\":{i}}}" });
        }
        // The group has read (and committed) the first 4 messages.
        state.DemoBroker.Commit("orders", "billing", 3);
        return (state, state.DemoBroker);
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        for (var i = 0; i < 200 && !condition(); i++) await Task.Delay(10);
        Assert.True(condition(), "timed out waiting for the screen to load");
    }

    public static void Register(TestRunner runner)
    {
        runner.Add("Consumer groups", "lists groups with committed offset, end offset and lag", async () =>
        {
            var (state, _) = await Seeded();
            var gateway = state.Connections["local"];

            var groups = await gateway.ListConsumerGroupsAsync();
            Assert.Equal(1, groups.Count);
            Assert.Equal("billing", groups[0].GroupId);
            Assert.Equal("Empty", groups[0].State);

            var detail = await gateway.DescribeConsumerGroupAsync("billing");
            var row = detail.Offsets.Single();
            Assert.Equal(4L, row.CommittedOffset);
            Assert.Equal(10L, row.EndOffset);
            Assert.Equal(6L, row.Lag);
            Assert.Equal(6L, detail.TotalLag);
        });

        runner.Add("Consumer groups", "reset to earliest, latest and an explicit offset", async () =>
        {
            var (state, _) = await Seeded();
            var gateway = state.Connections["local"];

            async Task<long?> ResetTo(OffsetResetTarget target, long? offset = null)
            {
                var plan = await gateway.PlanOffsetResetAsync(new OffsetResetRequest { GroupId = "billing", Target = target, Offset = offset });
                await gateway.ApplyOffsetResetAsync("billing", plan);
                return (await gateway.DescribeConsumerGroupAsync("billing")).Offsets.Single().CommittedOffset;
            }

            Assert.Equal(10L, await ResetTo(OffsetResetTarget.Latest));
            Assert.Equal(7L, await ResetTo(OffsetResetTarget.Offset, 7));
            Assert.Equal(10L, await ResetTo(OffsetResetTarget.Offset, 999)); // clamped to the end
            Assert.Equal(0L, await ResetTo(OffsetResetTarget.Earliest));     // a reset to 0 is a real committed position, as on a real broker
            Assert.Equal(10L, (await gateway.DescribeConsumerGroupAsync("billing")).TotalLag);
        });

        runner.Add("Consumer groups", "planning a reset changes nothing", async () =>
        {
            var (state, _) = await Seeded();
            var gateway = state.Connections["local"];

            var plan = await gateway.PlanOffsetResetAsync(new OffsetResetRequest { GroupId = "billing", Target = OffsetResetTarget.Latest });

            Assert.Equal(4L, plan.Single().CurrentOffset);
            Assert.Equal(10L, plan.Single().NewOffset);
            Assert.Equal(4L, (await gateway.DescribeConsumerGroupAsync("billing")).Offsets.Single().CommittedOffset);
        });

        runner.Add("Consumer groups", "reset to a timestamp lands on the first message at or after it", async () =>
        {
            var (state, broker) = await Seeded();
            var gateway = state.Connections["local"];
            var timestamps = broker.GetTimestamps("orders");
            var target = timestamps[6].timestamp;

            var plan = await gateway.PlanOffsetResetAsync(new OffsetResetRequest
            {
                GroupId = "billing", Target = OffsetResetTarget.Timestamp, Timestamp = target
            });

            // Messages produced in the same instant share a timestamp, so the first one at/after it wins.
            var expected = timestamps.First(t => t.timestamp >= target).offset;
            Assert.Equal(expected, plan.Single().NewOffset);

            var future = await gateway.PlanOffsetResetAsync(new OffsetResetRequest
            {
                GroupId = "billing", Target = OffsetResetTarget.Timestamp, Timestamp = DateTimeOffset.UtcNow.AddYears(1)
            });
            Assert.Equal(10L, future.Single().NewOffset);
        });

        runner.Add("Consumer groups", "an active group is flagged and its offsets cannot be changed", async () =>
        {
            var (state, broker) = await Seeded();
            var gateway = state.Connections["local"];
            broker.SimulateActiveMembers("billing", 2);

            var detail = await gateway.DescribeConsumerGroupAsync("billing");
            Assert.True(detail.IsActive);
            Assert.Equal(2, detail.Members.Count);

            var plan = await gateway.PlanOffsetResetAsync(new OffsetResetRequest { GroupId = "billing", Target = OffsetResetTarget.Latest });
            await Assert.ThrowsAsync<InvalidOperationException>(() => gateway.ApplyOffsetResetAsync("billing", plan));
            Assert.Equal(4L, (await gateway.DescribeConsumerGroupAsync("billing")).Offsets.Single().CommittedOffset);
        });

        runner.Add("ViewModels: Consumer Groups", "shows lag for the selected group and warns when it is active", async () =>
        {
            var (state, broker) = await Seeded();
            broker.SimulateActiveMembers("billing", 1);
            var vm = new ConsumerGroupsViewModel(state);

            await WaitUntil(() => vm.Groups.Count == 1);
            vm.SelectedGroup = vm.Groups[0];
            await WaitUntil(() => vm.HasDetail);

            Assert.Equal(1, vm.Offsets.Count);
            Assert.Equal(6L, vm.Offsets[0].Lag);
            Assert.True(vm.IsGroupActive);
            Assert.NotNull(vm.ActiveGroupWarning);
            Assert.False(vm.Offsets.Single().CanEdit, "an active group's offsets must not be editable");
        });

        runner.Add("ViewModels: Consumer Groups", "a partition with lag is editable, prefilled with End, and applies directly", async () =>
        {
            var (state, _) = await Seeded();
            var vm = new ConsumerGroupsViewModel(state);
            await WaitUntil(() => vm.Groups.Count == 1);
            vm.SelectedGroup = vm.Groups[0];
            await WaitUntil(() => vm.HasDetail);

            var row = vm.Offsets.Single();
            Assert.True(row.HasLag);
            Assert.True(row.CanEdit);
            Assert.Equal(10L, row.NewOffset ?? -1);

            row.NewOffset = 8;
            await row.ApplyCommand.ExecuteAsync();

            Assert.Equal(8L, vm.Detail!.Offsets.Single().CommittedOffset);
            Assert.Equal(2L, vm.Detail.TotalLag);
        });

        runner.Add("ViewModels: Consumer Groups", "the list shows lag per group and can show only groups with lag", async () =>
        {
            var (state, broker) = await Seeded();
            broker.Commit("orders", "caught-up", 9);   // next offset 10 = end
            broker.Commit("orders", "far-behind", -1 + 0); // nothing read: lag 10
            broker.SetNextOffset("orders", "far-behind", 0);
            var vm = new ConsumerGroupsViewModel(state);

            await WaitUntil(() => vm.Groups.Count == 3 && vm.Groups.All(g => g.Lag is not null));
            Assert.Equal(6L, vm.Groups.Single(g => g.GroupId == "billing").Lag ?? -1);
            Assert.Equal(0L, vm.Groups.Single(g => g.GroupId == "caught-up").Lag ?? -1);

            vm.OnlyWithLag = true;

            Assert.Equal(2, vm.Groups.Count);
            Assert.Equal("far-behind", vm.Groups[0].GroupId); // most behind first
            Assert.Equal("billing", vm.Groups[1].GroupId);
        });

        runner.Add("ViewModels: Consumer Groups", "a partition without lag is not editable", async () =>
        {
            var (state, broker) = await Seeded();
            broker.Commit("orders", "billing", 9); // fully caught up
            var vm = new ConsumerGroupsViewModel(state);
            await WaitUntil(() => vm.Groups.Count == 1);
            vm.SelectedGroup = vm.Groups[0];
            await WaitUntil(() => vm.HasDetail);

            Assert.False(vm.Offsets.Single().HasLag);
            Assert.False(vm.Offsets.Single().CanEdit);
        });

        runner.Add("ViewModels: Consumer Groups", "an out-of-range offset is rejected and nothing changes", async () =>
        {
            var (state, _) = await Seeded();
            var vm = new ConsumerGroupsViewModel(state);
            await WaitUntil(() => vm.Groups.Count == 1);
            vm.SelectedGroup = vm.Groups[0];
            await WaitUntil(() => vm.HasDetail);

            var row = vm.Offsets.Single();
            row.NewOffset = 500;
            await row.ApplyCommand.ExecuteAsync();

            Assert.Equal(4L, vm.Detail!.Offsets.Single().CommittedOffset);
            Assert.Contains("between", vm.StatusMessage ?? "");
        });
    }
}

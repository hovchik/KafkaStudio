using KafkaStudio.App.ViewModels;
using KafkaStudio.App.ViewModels.KeyChains;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Search;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

/// <summary>The Key Chain screen: ordered topic chains, "does this key exist in each topic and when", saved chains.</summary>
public static class KeyChainTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    private static KafkaMessage Msg(string topic, string key, double seconds, long offset = 0) => new()
    {
        Topic = topic, Partition = 0, Offset = offset, Key = key, Value = "{}", Timestamp = T0.AddSeconds(seconds)
    };

    private static ScanSummary Scan(Dictionary<string, string>? failed = null) => new()
    {
        TopicsTotal = 3, TopicsScanned = 3, MessagesScanned = 0, FailedTopics = failed ?? new(), StoppedEarly = false, Cancelled = false
    };

    private static async Task<AppState> Seeded()
    {
        var state = new AppState();
        state.AddDemoConnection("local");
        var gateway = state.Connections["local"];
        async Task Produce(string topic, string key)
        {
            await gateway.ProduceAsync(new ProduceRequest { Topic = topic, Key = key, Value = "{\"k\":\"" + key + "\"}" });
            await Task.Delay(15);
        }
        await Produce("orders", "ORD-1");
        await Produce("orders", "ORD-2");
        await Produce("payments", "ORD-1");
        await Produce("payments", "ORD-2");
        await Produce("shipments", "ORD-1");
        await Produce("shipments", "ORD-1-other");
        return state;
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

    public static void Register(TestRunner runner)
    {
        runner.Add("Key chain", "Build computes per-step deltas, flags gaps and out-of-order topics", () =>
        {
            var topics = new[] { "orders", "payments", "shipments", "audit" };
            var matches = new Dictionary<string, IReadOnlyCollection<KafkaMessage>>
            {
                ["orders"] = new[] { Msg("orders", "K", 0), Msg("orders", "K", 9, 1) }, // retry later on
                ["payments"] = new[] { Msg("payments", "K", 2.5) },
                ["shipments"] = new[] { Msg("shipments", "K", 1) },                     // before payments
            };
            var result = KeyChainAnalyzer.Build("flow", "K", topics, matches, Scan());

            Assert.Equal(4, result.Steps.Count);
            Assert.Equal(3, result.FoundCount);
            Assert.False(result.Complete);
            Assert.Equal(2, result.Steps[0].Count);
            Assert.Equal(null, result.Steps[0].SincePrevious);
            Assert.Equal(TimeSpan.FromSeconds(2.5), result.Steps[1].SincePrevious!.Value);
            Assert.Equal(TimeSpan.FromSeconds(-1.5), result.Steps[2].SincePrevious!.Value);
            Assert.True(result.Steps[2].OutOfOrder);
            Assert.False(result.Steps[1].OutOfOrder);
            Assert.Equal(TimeSpan.FromSeconds(1), result.Steps[2].SinceStart!.Value);
            Assert.Equal(ChainStepStatus.Missing, result.Steps[3].Status);
            Assert.Equal("audit", result.FirstGap!.Topic);
            Assert.Contains("missing from audit", result.Summary);
            Assert.Contains("out of order", result.Summary);
        });

        runner.Add("Key chain", "the verdict says whether the topics saw the text in exactly the chain's order", () =>
        {
            ChainResult Run(params (string topic, double[] at)[] data) => KeyChainAnalyzer.Build("flow", "K",
                data.Select(d => d.topic).ToList(),
                data.ToDictionary(d => d.topic, d => (IReadOnlyCollection<KafkaMessage>)d.at.Select((t, i) => Msg(d.topic, "K", t, i)).ToList()), Scan());

            var ok = Run(("a", new[] { 0.0 }), ("b", new[] { 1.0 }), ("c", new[] { 3.0 }));
            Assert.Equal(SequenceVerdict.Ok, ok.Verdict);
            Assert.Null(ok.BrokenStep);
            Assert.Contains("Sequence OK", ok.VerdictText);

            var sameInstant = Run(("a", new[] { 1.0 }), ("b", new[] { 1.0 }));
            Assert.Equal(SequenceVerdict.Ok, sameInstant.Verdict, "equal timestamps keep the order");

            var wrong = Run(("a", new[] { 0.0 }), ("b", new[] { 5.0 }), ("c", new[] { 2.0 }), ("d", new[] { 9.0 }));
            Assert.Equal(SequenceVerdict.Broken, wrong.Verdict);
            Assert.Equal("c", wrong.BrokenStep!.Topic);
            Assert.Contains("Sequence broken at step 3 (c)", wrong.VerdictText);
            Assert.Contains("before it reached b", wrong.VerdictText);

            // an early duplicate upstream does not break the order: the later copy is the one that counts
            var retry = Run(("a", new[] { 0.0, 4.0 }), ("b", new[] { 5.0 }));
            Assert.Equal(SequenceVerdict.Ok, retry.Verdict);
            var late = Run(("a", new[] { 0.0, 6.0 }), ("b", new[] { 5.0 }), ("c", new[] { 7.0 }));
            Assert.Equal(SequenceVerdict.Ok, late.Verdict, "a (0) -> b (5) -> c (7) is a valid path despite a retry at 6");
            Assert.Equal(5.0, (late.Steps[1].First!.Timestamp - T0).TotalSeconds);

            var missing = Run(("a", new[] { 0.0 }), ("b", Array.Empty<double>()), ("c", new[] { 3.0 }));
            Assert.Equal(SequenceVerdict.Broken, missing.Verdict, "a missing step breaks the sequence");
            Assert.Equal("b", missing.BrokenStep!.Topic);
            Assert.Contains("Sequence broken at step 2 (b) - not found in b", missing.VerdictText);

            var none = Run(("a", Array.Empty<double>()), ("b", Array.Empty<double>()));
            Assert.Equal(SequenceVerdict.NotFound, none.Verdict);
            Assert.Contains("Not found", none.VerdictText);
            Assert.Null(none.Details, "nothing beyond the verdict to add");
            Assert.True(missing.Details!.StartsWith("Found in 2 of 3", StringComparison.Ordinal), missing.Details);
            Assert.False(missing.Details.Contains("Sequence broken"), "details do not repeat the verdict");
            Assert.Null(ok.Details);
        });

        runner.Add("Key chain", "an unreadable topic is reported as such, not as missing", () =>
        {
            var result = KeyChainAnalyzer.Build("c", "K", new[] { "a", "b" },
                new Dictionary<string, IReadOnlyCollection<KafkaMessage>> { ["a"] = new[] { Msg("a", "K", 0) } },
                Scan(new() { ["b"] = "unknown topic" }));
            Assert.Equal(ChainStepStatus.Unreadable, result.Steps[1].Status);
            Assert.Equal("unknown topic", result.Steps[1].Error);
        });

        runner.Add("Key chain", "analyzer finds the exact key in each topic of the chain, in chain order, and only reads", async () =>
        {
            var state = await Seeded();
            var gateway = state.Connections["local"];
            var chain = new KeyChain { Name = "flow", Topics = { "shipments", "orders", "payments" } };

            var result = await KeyChainAnalyzer.RunAsync(gateway, chain, "ORD-1", SearchRange.All);
            Assert.Equal("shipments,orders,payments", string.Join(",", result.Steps.Select(s => s.Topic)));
            Assert.True(result.Complete, result.Summary);
            Assert.Equal(2, result.Steps[0].Count, "matches any text: ORD-1-other contains ORD-1 too");
            var viaValue = await KeyChainAnalyzer.RunAsync(gateway, chain, "\"k\":\"ORD-2", SearchRange.All);
            Assert.Equal(2, viaValue.FoundCount, "text inside the value is found");
            var lower = await KeyChainAnalyzer.RunAsync(gateway, chain, "ord-2", SearchRange.All);
            Assert.Equal(2, lower.FoundCount, "case-insensitive by default");
            var exactCase = await KeyChainAnalyzer.RunAsync(gateway, chain, "ord-2", SearchRange.All, caseSensitive: true);
            Assert.Equal(0, exactCase.FoundCount);
            // chain order is shipments first, but it was produced last -> out of order
            Assert.True(result.Steps[1].SincePrevious!.Value < TimeSpan.Zero);
            Assert.True(result.Steps.Skip(1).Any(s => s.OutOfOrder));
            Assert.Equal(SequenceVerdict.Broken, result.Verdict, "shipments was produced last but is first in this chain");

            var partial = await KeyChainAnalyzer.RunAsync(gateway, new KeyChain { Name = "f", Topics = { "orders", "payments", "shipments" } }, "ORD-2", SearchRange.All);
            Assert.Equal(2, partial.FoundCount);
            Assert.Equal("shipments", partial.FirstGap!.Topic);
            Assert.Equal(true, partial.EndToEnd > TimeSpan.Zero);

            var none = await KeyChainAnalyzer.RunAsync(gateway, chain, "nope", SearchRange.All);
            Assert.Equal(0, none.FoundCount);
            Assert.Contains("Not found", none.Summary);

            // nothing was produced by the analysis
            var total = 0;
            await TopicScanner.ScanAsync(gateway, new[] { "orders", "payments", "shipments" }, SearchRange.All, (_, _) => { Interlocked.Increment(ref total); return ScanDecision.Continue; });
            Assert.Equal(6, total);
        });

        runner.Add("Key chain", "text matches the key, a header name or value, or the value", () =>
        {
            var m = new KafkaMessage { Topic = "t", Partition = 0, Offset = 0, Key = "ORD-9", Value = "shipped to Berlin", Timestamp = T0, Headers = new Dictionary<string, string> { ["source"] = "billing" } };
            Assert.True(KeyChainAnalyzer.Matches(m, "ord-9"));
            Assert.True(KeyChainAnalyzer.Matches(m, "berlin"));
            Assert.True(KeyChainAnalyzer.Matches(m, "billing"));
            Assert.True(KeyChainAnalyzer.Matches(m, "source"));
            Assert.False(KeyChainAnalyzer.Matches(m, "paris"));
            Assert.False(KeyChainAnalyzer.Matches(m, "berlin", StringComparison.Ordinal));
        });

        runner.Add("Key chain", "saved chains round-trip through the store", () =>
        {
            Assert.Null(KeyChainStore.Save(new[]
            {
                new KeyChain { Name = "a", Topics = { "t1", "t2" } },
                new KeyChain { Name = "A", Topics = { "dup-name" } },
                new KeyChain { Name = "  ", Topics = { "blank-name" } },
                new KeyChain { Name = "b", Topics = { "t3", " " } }
            }));
            var loaded = KeyChainStore.Load();
            Assert.Equal(2, loaded.Count, "blank names and case-duplicates are dropped");
            Assert.Equal("t1,t2", string.Join(",", loaded[0].Topics));
            Assert.Equal("t3", string.Join(",", loaded[1].Topics));
            KeyChainStore.Save(Array.Empty<KeyChain>());
        });

        runner.Add("Key chain", "view model: build, save, reorder, switch and delete multiple chains; run any key", async () =>
        {
            KeyChainStore.Save(Array.Empty<KeyChain>());
            var state = await Seeded();
            var vm = new KeyChainViewModel(state);
            await WaitUntil(() => vm.TopicNames.Count == 3);
            Assert.Equal("local", vm.SelectedConnection);
            Assert.False(vm.AnalyzeCommand.CanExecute(null), "no key, no topics");

            vm.ChainName = "Order flow";
            foreach (var t in new[] { "orders", "payments", "shipments" }) { vm.TopicToAdd = t; vm.AddTopicCommand.Execute(null); }
            vm.TopicToAdd = "orders"; vm.AddTopicCommand.Execute(null);
            Assert.Equal(3, vm.ChainTopics.Count, "no duplicate topics");
            vm.TopicToAdd = "ord"; vm.AddTopicCommand.Execute(null);
            Assert.Equal(3, vm.ChainTopics.Count, "a half-typed name is not a topic");
            Assert.Contains("No topic named 'ord'", vm.Status ?? "");
            vm.TopicToAdd = null;
            vm.MoveDownCommand.Execute("orders");
            Assert.Equal("payments,orders,shipments", string.Join(",", vm.ChainTopics));
            vm.MoveUpCommand.Execute("orders");
            vm.SaveChainCommand.Execute(null);
            Assert.Equal(1, vm.Chains.Count);

            vm.NewChainCommand.Execute(null);
            Assert.Equal(0, vm.ChainTopics.Count);
            vm.ChainName = "order flow"; // same name, any case
            vm.TopicToAdd = "orders"; vm.AddTopicCommand.Execute(null);
            vm.SaveChainCommand.Execute(null);
            Assert.Equal(1, vm.Chains.Count, "duplicate name refused");
            Assert.Contains("already exists", vm.Status ?? "");
            vm.ChainName = "Orders only";
            vm.SaveChainCommand.Execute(null);
            Assert.Equal(2, vm.Chains.Count);

            vm.SelectedChain = vm.Chains[0];
            Assert.Equal("Order flow", vm.ChainName);
            Assert.Equal(3, vm.ChainTopics.Count);

            vm.Key = "ORD-1";
            await vm.AnalyzeCommand.ExecuteAsync();
            Assert.Equal(3, vm.Steps.Count);
            Assert.Equal(true, vm.LastRunComplete, vm.Status ?? "");
            Assert.True(vm.Steps.All(s => s.Found));
            Assert.True(vm.IsSequenceOk, vm.Verdict ?? "");
            Assert.Equal(vm.Steps[0], vm.SelectedStep);
            Assert.Contains("ORD-1", vm.Detail ?? "");

            vm.SelectedChain = vm.Chains[1];
            Assert.Equal(0, vm.Steps.Count, "switching chains clears the previous result");
            vm.Key = "ORD-404";
            await vm.AnalyzeCommand.ExecuteAsync();
            Assert.Equal(false, vm.LastRunComplete);
            Assert.Contains("Not found", vm.Summary ?? "");
            Assert.True(vm.IsNotFound);
            Assert.Null(vm.Status, "the verdict is shown once, in the banner");

            // persisted: a new view model sees both chains
            Assert.Equal(2, new KeyChainViewModel(state).Chains.Count);
            vm.DeleteChainCommand.Execute(null);
            Assert.Equal(1, vm.Chains.Count);
            Assert.Equal(1, KeyChainStore.Load().Count);
            KeyChainStore.Save(Array.Empty<KeyChain>());
        });

        runner.Add("Key chain", "appears in the Data group of the sidebar", async () =>
        {
            var main = new MainWindowViewModel(await Seeded());
            var item = main.NavigationItems.Single(i => i.Key == "chain");
            Assert.Equal("DATA", item.Group);
            Assert.Equal(main.KeyChain, item.ViewModel);
        });
    }
}

using KafkaStudio.App.ViewModels;
using KafkaStudio.App.ViewModels.DataSearch;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.App.ViewModels.Topics;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Search;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

/// <summary>The Find Data screen and the Topic Browser's search/compare upgrades, end to end against the
/// demo (in-memory) broker.</summary>
public static class DataSearchViewModelTests
{
    private static async Task<AppState> SeededState()
    {
        var state = new AppState();
        state.AddDemoConnection("local");
        var gateway = state.Connections["local"];
        async Task Produce(string topic, string? key, string value, Dictionary<string, string>? headers = null) =>
            await gateway.ProduceAsync(new ProduceRequest { Topic = topic, Key = key, Value = value, Headers = headers });

        await Produce("orders", "ORD-1", """{"orderId":"ORD-1","amount":10,"status":"NEW","createdAt":"2026-01-01T00:00:00Z"}""");
        await Produce("orders", "ORD-2", """{"orderId":"ORD-2","amount":25,"status":"NEW","createdAt":"2026-01-01T00:00:05Z"}""");
        await Produce("orders", "ORD-2", """{"orderId":"ORD-2","amount":25,"status":"NEW","createdAt":"2026-01-01T00:00:07Z"}"""); // retry
        await Produce("payments", "P-1", """{"orderId":"ORD-1","amount":10,"status":"PAID"}""", new() { ["source"] = "billing" });
        await Produce("shipments", "S-1", "shipped ORD-1");
        return state;
    }

    private static async Task<DataSearchViewModel> Ready(AppState state)
    {
        var vm = new DataSearchViewModel(state);
        await WaitUntil(() => vm.Scope.TopicNames.Count == 3);
        return vm;
    }

    public static void Register(TestRunner runner)
    {
        runner.Add("ViewModels: Find Data", "search runs structured queries and reports per-topic outcomes", async () =>
        {
            var state = await SeededState();
            var vm = await Ready(state);
            Assert.Equal("local", vm.Scope.SelectedConnection);

            vm.Query.QueryText = "$.orderId = ORD-1";
            Assert.True(vm.Query.IsStructured);
            Assert.Contains("Structured query", vm.Query.QueryDescription ?? "");
            await vm.Query.SearchCommand.ExecuteAsync();

            Assert.Equal(2, vm.Query.Hits.Count, vm.Query.Status ?? "");
            Assert.Equal(2, vm.Query.TopicOutcomes.Count);
            Assert.Equal(true, vm.Query.LastRunFound);
            Assert.Contains("Found 2 message(s) on 2 of 3 topic(s)", vm.Query.Summary ?? "");
            Assert.Equal("$.orderId = ORD-1", vm.Query.History[0]);

            vm.Query.SelectedHit = vm.Query.Hits[0];
            Assert.Equal(vm.Query.Hits[0], vm.SelectedMessage);
        });

        runner.Add("ViewModels: Find Data", "exists check, errors, modes and single-topic scope", async () =>
        {
            var state = await SeededState();
            var vm = await Ready(state);

            vm.Query.QueryText = "key equals";
            Assert.NotNull(vm.Query.QueryError);
            Assert.False(vm.Query.SearchCommand.CanExecute(null));

            vm.Query.QueryText = "ORD-404";
            await vm.Query.ExistsCommand.ExecuteAsync();
            Assert.Equal(false, vm.Query.LastRunFound);
            Assert.Contains("Not found in 3 topic(s)", vm.Query.Summary ?? "");

            vm.Query.QueryText = "ORD-2";
            await vm.Query.ExistsCommand.ExecuteAsync();
            Assert.Equal(1, vm.Query.Hits.Count, "exists stops at the first hit per topic");
            Assert.Contains("Found on 1 of 3 topic(s): orders", vm.Query.Summary ?? "");

            vm.Query.Mode = vm.Query.Modes.Single(m => m.Value == TextMatchMode.Fuzzy);
            vm.Query.QueryText = "ORD-0002";
            await vm.Query.SearchCommand.ExecuteAsync();
            Assert.Equal(2, vm.Query.Hits.Count, "fuzzy finds ORD-2 under a different spelling");

            vm.Query.Mode = vm.Query.Modes[0];
            await vm.Query.RunQueryAsync("ORD-1", singleTopic: "shipments");
            Assert.Equal(1, vm.Query.Hits.Count);
            Assert.Equal("shipments", vm.Query.Hits[0].Topic);

            vm.Scope.TopicNameFilter = "pay";
            vm.Query.SingleTopic = null;
            await vm.Query.SearchCommand.ExecuteAsync();
            Assert.Equal(1, vm.Query.Hits.Count, "name filter narrows the scope to payments");

            vm.Scope.TopicNameFilter = null;
            vm.Scope.SelectedRangeKind = vm.Scope.RangeKinds.Single(k => k.Value == SearchRangeKind.Between);
            vm.Scope.FromText = "not a date";
            await vm.Query.SearchCommand.ExecuteAsync();
            Assert.Contains("can't read the start time", vm.Query.Status ?? "");
        });

        runner.Add("ViewModels: Find Data", "saved searches restore query, options and scope", async () =>
        {
            var state = await SeededState();
            var vm = await Ready(state);
            vm.Query.QueryText = "PAID";
            vm.Query.Mode = vm.Query.Modes.Single(m => m.Value == TextMatchMode.WholeWord);
            vm.Query.SearchKey = false;
            vm.Scope.SelectedRangeKind = vm.Scope.RangeKinds.Single(k => k.Value == SearchRangeKind.LastDuration);
            vm.Scope.LastMinutes = 30;
            vm.Query.NewSearchName = "paid";
            vm.Query.SaveSearchCommand.Execute(null);

            var fresh = await Ready(state);
            var saved = fresh.Query.SavedSearches.Single(s => s.Name == "paid");
            fresh.Query.SelectedSavedSearch = saved;
            Assert.Equal("PAID", fresh.Query.QueryText);
            Assert.Equal(TextMatchMode.WholeWord, fresh.Query.Mode.Value);
            Assert.False(fresh.Query.SearchKey);
            Assert.True(fresh.Scope.IsLastDuration);
            Assert.Equal(30.0, fresh.Scope.LastMinutes);

            fresh.Query.DeleteSavedSearchCommand.Execute(null);
            Assert.Equal(0, fresh.Query.SavedSearches.Count(s => s.Name == "paid"));
        });

        runner.Add("ViewModels: Find Data", "a search becomes a KafScript check in the Script Editor", async () =>
        {
            var state = await SeededState();
            var main = new MainWindowViewModel(state);
            await WaitUntil(() => main.DataSearch.Scope.TopicNames.Count == 3);
            var vm = main.DataSearch;

            vm.Query.CaseSensitive = true;
            vm.Query.QueryText = "$.status = PAID";
            await vm.Query.SearchCommand.ExecuteAsync();
            vm.Query.ToScriptCommand.Execute(null);

            Assert.Equal("scripts", main.SelectedItem.Key);
            Assert.Contains("Then expect message on topic \"payments\" within 30 seconds where json \"$.status\" equals \"PAID\"", main.Scripts.Source);
            Assert.Null(main.Scripts.ParseError, main.Scripts.ParseError + "\n" + main.Scripts.Source);
            Assert.False(main.Scripts.Source.Contains("\"orders\""), "only topics that had hits");

            vm.Query.ToTaskCommand.Execute(null);
            Assert.Contains("schedule every 15 minutes", main.Scripts.Source);
            Assert.Null(main.Scripts.ParseError, main.Scripts.ParseError + "\n" + main.Scripts.Source);
            await main.DisposeAsync();
        });

        runner.Add("ViewModels: Find Data", "bulk check, trace and reconcile tabs", async () =>
        {
            var state = await SeededState();
            var vm = await Ready(state);

            vm.Bulk.IdsText = "ORD-1\nORD-2\nORD-404";
            Assert.Equal(3, vm.Bulk.ParsedIdCount);
            await vm.Bulk.RunCommand.ExecuteAsync();
            Assert.Contains("2 of 3 id(s) found, 1 missing", vm.Bulk.Summary ?? vm.Bulk.Status ?? "");
            Assert.Equal("ORD-404", vm.Bulk.Rows[0].Id);
            vm.Bulk.OnlyMissing = true;
            Assert.Equal(1, vm.Bulk.Rows.Count);
            vm.Bulk.MatchField = "bogus";
            await vm.Bulk.RunCommand.ExecuteAsync();
            Assert.Contains("unknown field", vm.Bulk.Status ?? "");

            vm.Trace.Id = "ORD-1";
            await vm.Trace.RunCommand.ExecuteAsync();
            Assert.Equal(3, vm.Trace.Hops.Count, vm.Trace.Status ?? "");
            Assert.Equal("orders → payments → shipments", vm.Trace.PathText);

            vm.Reconcile.TopicA = "orders";
            vm.Reconcile.TopicB = "payments";
            vm.Reconcile.JoinB = "$.orderId";
            vm.Reconcile.CompareFields = "$.amount";
            await vm.Reconcile.RunCommand.ExecuteAsync();
            Assert.Contains("1 matched, 0 different, 1 only in A, 0 only in B", vm.Reconcile.Summary ?? vm.Reconcile.Status ?? "");
            vm.Reconcile.SelectedRow = vm.Reconcile.Rows.Single(r => r.Status == ReconcileStatus.Matched);
            Assert.True(vm.Reconcile.ShowBCommand.CanExecute(null));
            Assert.True(vm.Reconcile.RowDiff.Count > 0, "orders vs payments differ in status/createdAt");
            vm.Reconcile.StatusFilter = vm.Reconcile.StatusFilters.Single(f => f.Value == ReconcileStatus.OnlyInA);
            Assert.Equal("ORD-2", vm.Reconcile.Rows.Single().JoinValue);
        });

        runner.Add("ViewModels: Find Data", "'Find similar' and 'Trace key' from any message open Find Data", async () =>
        {
            var state = await SeededState();
            var main = new MainWindowViewModel(state);
            await WaitUntil(() => main.DataSearch.Scope.TopicNames.Count == 3);

            main.Topics.SelectedConnection = "local";
            await WaitUntil(() => main.Topics.Topics.Count == 3);
            await main.Topics.OpenTopicCommand.ExecuteAsync(main.Topics.Topics.Single(t => t.Name == "orders"));
            var retry = main.Topics.ScannedMessages[0]; // newest first: the ORD-2 retry

            main.Topics.Actions.FindSimilarCommand.Execute(retry);
            Assert.Equal("search", main.SelectedItem.Key);
            var similar = main.DataSearch.Similar;
            Assert.Equal(DataSearchViewModel.SimilarTab, main.DataSearch.SelectedTabIndex);
            Assert.Equal(retry, similar.Reference);
            Assert.True(similar.ReferenceFields.Any(f => f.Path == "$.createdAt" && f.IsVolatile));

            await similar.FindSimilarCommand.ExecuteAsync();
            Assert.Equal(1.0, similar.Matches[0].Score, similar.Summary ?? similar.Status ?? "");
            Assert.Equal("ORD-2", similar.Matches[0].Message.Key);
            similar.SelectedMatch = similar.Matches[0];
            Assert.Contains("No differences", similar.MatchDiffSummary ?? "");

            similar.ReferenceFields.Single(f => f.Path == "$.orderId").IsChecked = true;
            similar.ReferenceFields.Single(f => f.Path == "$.amount").IsChecked = true;
            Assert.Equal("json \"$.orderId\" equals \"ORD-2\" and json \"$.amount\" equals \"25\"", similar.BuildCheckedFieldsQuery());
            await similar.SearchCheckedFieldsCommand.ExecuteAsync();
            Assert.Equal(DataSearchViewModel.SearchTab, main.DataSearch.SelectedTabIndex);
            Assert.Equal(2, main.DataSearch.Query.Hits.Count);

            main.SelectedItem = main.NavigationItems[0];
            main.Topics.Actions.TraceKeyCommand.Execute(retry);
            Assert.Equal("search", main.SelectedItem.Key);
            await WaitUntil(() => main.DataSearch.Trace.Hops.Count == 2);
            Assert.Equal("ORD-2", main.DataSearch.Trace.Id);
            await main.DisposeAsync();
        });

        runner.Add("ViewModels: Find Data", "duplicates and field statistics tabs", async () =>
        {
            var state = await SeededState();
            var vm = await Ready(state);

            vm.Duplicates.Topic = "orders";
            await vm.Duplicates.RunCommand.ExecuteAsync();
            Assert.Equal("ORD-2", vm.Duplicates.Groups.Single().GroupKey, vm.Duplicates.Status ?? "");
            vm.Duplicates.GroupBy = vm.Duplicates.GroupByChoices.Single(c => c.Value == DuplicateGroupBy.Value);
            await vm.Duplicates.RunCommand.ExecuteAsync();
            Assert.Equal(0, vm.Duplicates.Groups.Count, "the retry has a different createdAt");
            vm.Duplicates.GroupBy = vm.Duplicates.GroupByChoices.Single(c => c.Value == DuplicateGroupBy.ValueIgnoringVolatile);
            await vm.Duplicates.RunCommand.ExecuteAsync();
            Assert.Equal(1, vm.Duplicates.Groups.Count);
            vm.Duplicates.SelectedGroup = vm.Duplicates.Groups[0];
            Assert.NotNull(vm.SelectedMessage);
            vm.Duplicates.GroupBy = vm.Duplicates.GroupByChoices.Single(c => c.Value == DuplicateGroupBy.Fields);
            Assert.True(vm.Duplicates.NeedsFields);
            vm.Duplicates.FieldsText = "$.amount";
            await vm.Duplicates.RunCommand.ExecuteAsync();
            Assert.Equal("$.amount=25", vm.Duplicates.Groups.Single().GroupKey);

            vm.FieldStats.Topic = "orders";
            await vm.FieldStats.RunCommand.ExecuteAsync();
            var amount = vm.FieldStats.Fields.Single(f => f.Path == "$.amount");
            Assert.Equal(2, amount.Distinct, vm.FieldStats.Status ?? "");
            vm.FieldStats.SelectedField = amount;
            var top = amount.TopValues[0];
            Assert.Equal("25", top.Value);
            await vm.FieldStats.SearchValueCommand.ExecuteAsync(top);
            Assert.Equal(DataSearchViewModel.SearchTab, vm.SelectedTabIndex);
            Assert.Equal("orders", vm.Query.SingleTopic);
            Assert.Equal(2, vm.Query.Hits.Count);
        });

        runner.Add("ViewModels: Topic Browser", "structured message filter, filter errors and structured global search", async () =>
        {
            var state = await SeededState();
            var vm = new TopicBrowserViewModel(state) { SelectedConnection = "local" };
            await WaitUntil(() => vm.Topics.Count == 3);
            await vm.OpenTopicCommand.ExecuteAsync(vm.Topics.Single(t => t.Name == "orders"));
            Assert.Equal(3, vm.ScannedMessages.Count);

            vm.MessageFilter = "$.amount > 20";
            Assert.Equal(2, vm.ScannedMessages.Count);
            Assert.Null(vm.MessageFilterError);
            vm.MessageFilter = "ord-1";
            Assert.Equal(1, vm.ScannedMessages.Count, "plain text still a case-insensitive contains");
            vm.MessageFilter = "$.amount >";
            Assert.NotNull(vm.MessageFilterError);
            Assert.Equal(3, vm.ScannedMessages.Count, "an invalid filter shows everything");
            vm.MessageFilter = null;

            vm.GlobalSearchTerm = "header source = billing";
            await vm.GlobalSearchCommand.ExecuteAsync();
            await WaitUntil(() => !vm.IsGlobalSearching);
            Assert.Equal("payments", vm.GlobalSearchResults.Single().Topic);
        });

        runner.Add("ViewModels: Find Data", "help & examples: every tab has how-tos, filters by tab, and 'Try it' fills in valid examples", async () =>
        {
            var vm = await Ready(await SeededState());
            var topics = FindDataHelp.BuildTopics();
            for (var tab = DataSearchViewModel.SearchTab; tab <= DataSearchViewModel.FieldStatsTab; tab++)
                Assert.True(topics.Any(t => t.TabIndex == tab), $"tab {tab} has no how-to");

            Assert.False(vm.Help.IsVisible);
            vm.Help.ToggleCommand.Execute(null);
            Assert.True(vm.Help.IsVisible);

            // Only the current tab's topics (plus the general ones) are listed, following tab changes.
            Assert.True(vm.Help.Topics.All(t => t.IsGeneral || t.TabIndex == DataSearchViewModel.SearchTab));
            vm.SelectedTabIndex = DataSearchViewModel.TraceTab;
            Assert.True(vm.Help.Topics.Any(t => t.TabIndex == DataSearchViewModel.TraceTab));
            Assert.False(vm.Help.Topics.Any(t => t.TabIndex == DataSearchViewModel.SearchTab));
            vm.Help.CurrentTabOnly = false;
            Assert.Equal(topics.Count, vm.Help.Topics.Count);

            foreach (var topic in topics.Where(t => t.HasTry))
            {
                vm.Help.TryCommand.Execute(topic);
                Assert.Equal(topic.TabIndex, vm.SelectedTabIndex, $"'Try it' on '{topic.Title}' opens its tab");
                if (topic.TabIndex == DataSearchViewModel.SearchTab)
                {
                    Assert.Equal(topic.TryText, vm.Query.QueryText);
                    Assert.Null(vm.Query.QueryError, $"example query of '{topic.Title}' is invalid: {vm.Query.QueryError}");
                }
            }
            Assert.Equal(DuplicateGroupBy.Fields, vm.Duplicates.GroupBy.Value);
            Assert.Equal("$.orderId, $.eventType", vm.Duplicates.FieldsText);
            Assert.Equal("ORD-1042", vm.Trace.Id);
            Assert.Contains("ORD-1002", vm.Bulk.IdsText!);

            vm.Help.OpenTabCommand.Execute(topics.First(t => t.TabIndex == DataSearchViewModel.FieldStatsTab));
            Assert.Equal(DataSearchViewModel.FieldStatsTab, vm.SelectedTabIndex);
        });

        runner.Add("ViewModels: Topic Browser", "compare strip shows a structural diff of two pinned messages", async () =>
        {
            var state = await SeededState();
            var vm = new TopicBrowserViewModel(state) { SelectedConnection = "local" };
            await WaitUntil(() => vm.Topics.Count == 3);
            await vm.OpenTopicCommand.ExecuteAsync(vm.Topics.Single(t => t.Name == "orders"));

            Assert.False(vm.ToggleDiffCommand.CanExecute(null));
            vm.SelectedMessage = vm.ScannedMessages[0];
            vm.AddSelectedToComparisonCommand.Execute(null);
            vm.SelectedMessage = vm.ScannedMessages[1];
            vm.AddSelectedToComparisonCommand.Execute(null);
            Assert.True(vm.ToggleDiffCommand.CanExecute(null));
            Assert.NotNull(vm.DiffLeft);
            Assert.NotNull(vm.DiffRight);

            vm.ToggleDiffCommand.Execute(null);
            Assert.True(vm.IsDiffOpen);
            Assert.Contains("No differences (ignoring 1 volatile field(s))", vm.DiffSummary ?? "");
            vm.DiffIgnoreVolatile = false;
            Assert.Equal("$.createdAt", vm.DiffRows.Single().Path);
            vm.DiffShowUnchanged = true;
            Assert.True(vm.DiffRows.Count > 3);

            vm.ClearComparisonCommand.Execute(null);
            Assert.False(vm.IsDiffOpen);
            Assert.Equal(0, vm.DiffRows.Count);
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
}

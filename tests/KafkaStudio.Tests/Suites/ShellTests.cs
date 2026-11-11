using KafkaStudio.App.ViewModels;
using KafkaStudio.App.ViewModels.ConsumerGroups;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

/// <summary>The window shell: command palette, collapsible sidebar and the active-cluster chip.</summary>
public static class ShellTests
{
    public static void Register(TestRunner runner)
    {
        runner.Add("Shell: palette", "filters screens by every typed word and runs the selected one", async () =>
        {
            var main = new MainWindowViewModel(new AppState());

            main.IsPaletteOpen = true;
            Assert.True(main.PaletteResults.Count >= main.NavigationItems.Count, "an empty query lists every screen and action");

            main.PaletteQuery = "go groups";
            Assert.Equal(1, main.PaletteResults.Count);
            Assert.Equal("Go to Consumer Groups", main.SelectedPaletteCommand!.Title);

            main.RunPaletteCommand();
            Assert.Equal("groups", main.SelectedItem.Key);
            Assert.False(main.IsPaletteOpen, "running an entry closes the palette");

            await main.DisposeAsync();
        });

        runner.Add("Shell: palette", "selection moves with the arrow keys and stays inside the list", async () =>
        {
            var main = new MainWindowViewModel(new AppState());
            main.IsPaletteOpen = true;

            main.MovePaletteSelection(-1);
            Assert.Equal(main.PaletteResults[0], main.SelectedPaletteCommand);
            main.MovePaletteSelection(1);
            Assert.Equal(main.PaletteResults[1], main.SelectedPaletteCommand);

            await main.DisposeAsync();
        });

        runner.Add("Shell: sidebar", "collapsing changes the width and notifies listeners", async () =>
        {
            var main = new MainWindowViewModel(new AppState());
            bool? saved = null;
            main.SidebarCollapsedChanged += value => saved = value;

            Assert.Equal(244d, main.SidebarWidth);
            main.ToggleSidebarCommand.Execute(null);
            Assert.Equal(56d, main.SidebarWidth);
            Assert.Equal(true, saved);

            await main.DisposeAsync();
        });

        runner.Add("Shell: sidebar", "every screen belongs to exactly one labelled group", async () =>
        {
            var main = new MainWindowViewModel(new AppState());

            Assert.True(main.NavigationItems.All(i => i.Group.Length > 0), "every item has a group");
            Assert.Equal(3, main.NavigationItems.Count(i => i.StartsGroup));
            Assert.Equal(11, main.NavigationItems.Select(i => i.Key).Distinct().Count());

            await main.DisposeAsync();
        });

        runner.Add("Shell: cluster chip", "shows the connection name, then a count, and flags an empty profile list", async () =>
        {
            var state = new AppState();
            var main = new MainWindowViewModel(state);
            Assert.Equal("No connection", main.ClusterStatusText);
            Assert.False(main.ClusterIsOk);

            state.AddDemoConnection("demo");
            Assert.Equal("demo", main.ClusterStatusText);
            Assert.True(main.ClusterIsOk);

            await main.DisposeAsync();
        });

        runner.Add("Shell: production marker", "the header flags a connected cluster the user marked as production", async () =>
        {
            var state = new AppState();
            var main = new MainWindowViewModel(state);

            state.AddDemoConnection("demo");
            Assert.False(main.ClusterIsProduction, "unmarked clusters are not flagged");

            state.ConnectionProfiles["demo"] = state.ConnectionProfiles["demo"] with { IsProduction = true };
            state.AddConnection(state.ConnectionProfiles["demo"], state.Connections["demo"]);
            Assert.True(main.ClusterIsProduction);

            await main.DisposeAsync();
        });

        runner.Add("Consumer groups: lag", "lag share is relative to what the partition retains and flags half or more as critical", () =>
        {
            GroupOffsetRowViewModel Row(long committed, long earliest, long end) => new(
                new KafkaStudio.Core.Messaging.ConsumerGroupOffset { Topic = "t", Partition = 0, CommittedOffset = committed, EarliestOffset = earliest, EndOffset = end },
                () => false, _ => Task.CompletedTask);

            Assert.Equal(0d, Row(100, 0, 100).LagShare);
            Assert.True(Row(90, 0, 100).IsLagWarning, "10 of 100 retained is a warning");
            Assert.True(Row(40, 0, 100).IsLagCritical, "60 of 100 retained is critical");
            Assert.Equal(1d, Row(0, 50, 100).LagShare);
            return Task.CompletedTask;
        });
    }
}

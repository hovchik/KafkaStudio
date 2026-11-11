using KafkaStudio.App.ViewModels;
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
            Assert.Equal(10, main.NavigationItems.Select(i => i.Key).Distinct().Count());

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
    }
}

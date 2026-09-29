using Avalonia.Controls;
using Avalonia.Input;
using KafkaStudio.App.ViewModels.Topics;

namespace KafkaStudio.App.Views.Topics;

public partial class TopicBrowserView : UserControl
{
    public TopicBrowserView()
    {
        InitializeComponent();
    }

    private void OnTopicDoubleTapped(object? sender, TappedEventArgs e) => OpenSelectedTopic(sender);

    private void OnTopicListKeyDown(object? sender, KeyEventArgs e)
    {
        if (e.Key != Key.Enter) return;
        OpenSelectedTopic(sender);
        e.Handled = true;
    }

    private void OpenSelectedTopic(object? sender)
    {
        if (DataContext is not TopicBrowserViewModel vm) return;
        if (sender is not ListBox { SelectedItem: TopicRowViewModel row }) return;

        if (vm.OpenTopicCommand.CanExecute(row))
        {
            vm.OpenTopicCommand.Execute(row);
        }
    }

    private void OnGlobalSearchHitDoubleTapped(object? sender, TappedEventArgs e) => OpenSelectedHit(sender);

    private void OnSearchResultsKeyDown(object? sender, KeyEventArgs e)
    {
        if (e.Key != Key.Enter) return;
        OpenSelectedHit(sender);
        e.Handled = true;
    }

    private void OpenSelectedHit(object? sender)
    {
        if (DataContext is not TopicBrowserViewModel vm) return;
        if (sender is not ListBox { SelectedItem: GlobalSearchHit hit }) return;

        if (vm.OpenGlobalSearchHitCommand.CanExecute(hit))
        {
            vm.OpenGlobalSearchHitCommand.Execute(hit);
        }
    }

    private void OnTopicHitCountTapped(object? sender, TappedEventArgs e)
    {
        if (DataContext is not TopicBrowserViewModel vm) return;
        if (sender is not Control { DataContext: TopicHitCount hit }) return;

        if (vm.ToggleGlobalSearchTopicFilterCommand.CanExecute(hit))
        {
            vm.ToggleGlobalSearchTopicFilterCommand.Execute(hit);
        }
    }
}

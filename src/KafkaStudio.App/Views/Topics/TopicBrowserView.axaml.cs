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

    private void OnTopicDoubleTapped(object? sender, TappedEventArgs e)
    {
        if (DataContext is not TopicBrowserViewModel vm) return;
        if (sender is not ListBox { SelectedItem: TopicRowViewModel row }) return;

        if (vm.OpenTopicCommand.CanExecute(row))
        {
            vm.OpenTopicCommand.Execute(row);
        }
    }
}

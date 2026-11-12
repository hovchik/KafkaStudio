using Avalonia.Controls;
using Avalonia.Input;

namespace KafkaStudio.App.Views.KeyChains;

public partial class KeyChainView : UserControl
{
    public KeyChainView()
    {
        InitializeComponent();
    }

    // Show the whole topic list as soon as the box is focused; typing then narrows it.
    private void OnTopicBoxGotFocus(object? sender, GotFocusEventArgs e)
    {
        if (sender is AutoCompleteBox box && string.IsNullOrEmpty(box.Text)) box.IsDropDownOpen = true;
    }
}

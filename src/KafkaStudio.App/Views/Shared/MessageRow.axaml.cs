using Avalonia.Controls;

namespace KafkaStudio.App.Views.Shared;

/// <summary>A single message as a table row (time, partition, offset, key, value preview).</summary>
public partial class MessageRow : UserControl
{
    public MessageRow() => InitializeComponent();
}

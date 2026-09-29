using Avalonia;
using Avalonia.Controls;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.App.Views.Shared;

/// <summary>Shows one message in full (metadata, key, headers, pretty value) with copy/edit actions.</summary>
public partial class MessageDetailView : UserControl
{
    public static readonly StyledProperty<KafkaMessage?> MessageProperty =
        AvaloniaProperty.Register<MessageDetailView, KafkaMessage?>(nameof(Message));

    public static readonly StyledProperty<MessageActions?> ActionsProperty =
        AvaloniaProperty.Register<MessageDetailView, MessageActions?>(nameof(Actions));

    public KafkaMessage? Message
    {
        get => GetValue(MessageProperty);
        set => SetValue(MessageProperty, value);
    }

    public MessageActions? Actions
    {
        get => GetValue(ActionsProperty);
        set => SetValue(ActionsProperty, value);
    }

    public MessageDetailView()
    {
        InitializeComponent();
    }
}

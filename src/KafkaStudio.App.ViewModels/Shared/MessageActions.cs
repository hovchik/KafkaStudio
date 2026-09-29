using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.App.ViewModels.Shared;

/// <summary>
/// The per-message actions every message list offers (copy value/key/JSON, open in Producer, export),
/// bundled so the Topics and Consume screens behave identically.
/// </summary>
public sealed class MessageActions
{
    private readonly AppState _state;
    private readonly Func<string?> _connection;
    private readonly Action<string> _status;

    public RelayCommand<KafkaMessage> CopyValueCommand { get; }
    public RelayCommand<KafkaMessage> CopyKeyCommand { get; }
    public RelayCommand<KafkaMessage> CopyAsJsonCommand { get; }
    public RelayCommand<KafkaMessage> EditInProducerCommand { get; }

    public MessageActions(AppState state, Func<string?> connection, Action<string> status)
    {
        _state = state;
        _connection = connection;
        _status = status;
        CopyValueCommand = new RelayCommand<KafkaMessage>(m => Copy(m?.Value ?? m?.PrettyValue, "value"), m => m is not null);
        CopyKeyCommand = new RelayCommand<KafkaMessage>(m => Copy(m?.Key, "key"), m => m?.Key is not null);
        CopyAsJsonCommand = new RelayCommand<KafkaMessage>(m => Copy(m is null ? null : MessageExport.ToJson(m), "message JSON"), m => m is not null);
        EditInProducerCommand = new RelayCommand<KafkaMessage>(m =>
        {
            if (m is not null) _state.RequestEditInProducer(_connection(), m);
        }, m => m is not null);
    }

    private async void Copy(string? text, string what)
    {
        if (text is null) return;
        if (_state.SetClipboardText is null)
        {
            _status("Clipboard isn't available.");
            return;
        }
        try
        {
            await _state.SetClipboardText(text).ConfigureAwait(true);
            _status($"Copied {what} to the clipboard.");
        }
        catch (Exception ex)
        {
            _status($"Copy failed: {ex.Message}");
        }
    }

    /// <summary>Asks for a file and writes <paramref name="messages"/> to it as a JSON array.</summary>
    public async Task ExportAsync(IReadOnlyCollection<KafkaMessage> messages, string suggestedName)
    {
        if (messages.Count == 0)
        {
            _status("Nothing to export.");
            return;
        }
        if (_state.FileDialogs is null)
        {
            _status("File dialogs aren't available.");
            return;
        }

        var path = await _state.FileDialogs.PickSaveFileAsync("Export messages", ".json", "JSON", suggestedName).ConfigureAwait(true);
        if (path is null) return;

        try
        {
            var json = MessageExport.ToJson(messages);
            await File.WriteAllTextAsync(path, json).ConfigureAwait(true);
            _status($"Exported {messages.Count:N0} message(s) to {Path.GetFileName(path)}.");
        }
        catch (Exception ex)
        {
            _status($"Export failed: {ex.Message}");
        }
    }

    /// <summary>File-name-safe version of a topic name.</summary>
    public static string SafeFileName(string name) =>
        string.Concat(name.Select(c => Path.GetInvalidFileNameChars().Contains(c) ? '_' : c));
}

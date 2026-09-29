using System.Collections.ObjectModel;
using System.Text.Encodings.Web;
using System.Text.Json;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.App.ViewModels.Producer;

public sealed class HeaderEntryViewModel : ObservableObject
{
    private string _name = "";
    public string Name { get => _name; set => SetProperty(ref _name, value); }

    private string _value = "";
    public string Value { get => _value; set => SetProperty(ref _value, value); }
}

/// <summary>One line of the "Recently sent" list.</summary>
public sealed class SentMessageRow
{
    public required string Summary { get; init; }
    public required string Detail { get; init; }
    public bool Failed { get; init; }
}

/// <summary>Ad-hoc "send a message" form - the point-and-click equivalent of a KafScript "produce
/// message" step, handy for quick manual testing without writing a script. Supports the same
/// {{$uuid}} / {{$now}} / {{$timestamp}} / {{$date}} / {{$random}} built-ins as scripts (evaluated
/// fresh for every message, so "send 100" produces 100 distinct ids), an explicit partition, and
/// tombstones (null value).</summary>
public sealed class ProducerViewModel : ObservableObject
{
    public const int MaxRepeat = 100_000;
    private const int MaxHistory = 200;

    private readonly AppState _state;
    private CancellationTokenSource? _topicNamesCts;
    private CancellationTokenSource? _sendCts;

    public ObservableCollection<string> ConnectionNames { get; } = new();
    public ObservableCollection<HeaderEntryViewModel> Headers { get; } = new();
    public ObservableCollection<SentMessageRow> History { get; } = new();

    /// <summary>All topic names for the selected connection - populated automatically whenever
    /// <see cref="SelectedConnection"/> changes, so the Topic field can offer them all while still
    /// letting the user filter by typing (the AutoCompleteBox in the view does the filtering).</summary>
    public ObservableCollection<string> TopicNames { get; } = new();

    private string? _selectedConnection;
    public string? SelectedConnection
    {
        get => _selectedConnection;
        set
        {
            if (SetProperty(ref _selectedConnection, value))
            {
                _ = RefreshTopicNamesAsync();
                SendCommand.RaiseCanExecuteChanged();
            }
        }
    }

    private string _topic = "";
    public string Topic
    {
        get => _topic;
        set
        {
            if (SetProperty(ref _topic, value ?? "")) SendCommand.RaiseCanExecuteChanged();
        }
    }

    private string _key = "";
    public string Key { get => _key; set => SetProperty(ref _key, value ?? ""); }

    private string _value = "";
    public string Value
    {
        get => _value;
        set
        {
            if (SetProperty(ref _value, value ?? "")) OnPropertyChanged(nameof(ValueInfo));
        }
    }

    /// <summary>Small hint under the editor: size and whether the value is valid JSON.</summary>
    public string ValueInfo
    {
        get
        {
            if (IsTombstone) return "tombstone - the value will be null";
            var bytes = System.Text.Encoding.UTF8.GetByteCount(Value);
            var trimmed = Value.TrimStart();
            if (trimmed.StartsWith('{') || trimmed.StartsWith('['))
            {
                try
                {
                    using var _ = JsonDocument.Parse(Value);
                    return $"{bytes:N0} bytes · valid JSON";
                }
                catch (JsonException ex)
                {
                    return $"{bytes:N0} bytes · invalid JSON: {ex.Message}";
                }
            }
            return $"{bytes:N0} bytes";
        }
    }

    private bool _isTombstone;
    /// <summary>Send a null value (a compaction "delete" marker for the key).</summary>
    public bool IsTombstone
    {
        get => _isTombstone;
        set { if (SetProperty(ref _isTombstone, value)) OnPropertyChanged(nameof(ValueInfo)); }
    }

    private string? _partition;
    /// <summary>Optional explicit partition; empty lets the partitioner choose.</summary>
    public string? Partition { get => _partition; set => SetProperty(ref _partition, value); }

    private int _repeatCount = 1;
    /// <summary>How many messages one click sends (templates are re-evaluated for each).</summary>
    public int RepeatCount
    {
        get => _repeatCount;
        set => SetProperty(ref _repeatCount, Math.Clamp(value, 1, MaxRepeat));
    }

    private string? _statusMessage;
    public string? StatusMessage { get => _statusMessage; set => SetProperty(ref _statusMessage, value); }

    private bool _isSending;
    public bool IsSending
    {
        get => _isSending;
        private set
        {
            if (SetProperty(ref _isSending, value))
            {
                SendCommand.RaiseCanExecuteChanged();
                CancelSendCommand.RaiseCanExecuteChanged();
            }
        }
    }

    public RelayCommand AddHeaderCommand { get; }
    public RelayCommand<HeaderEntryViewModel> RemoveHeaderCommand { get; }
    public AsyncRelayCommand SendCommand { get; }
    public RelayCommand CancelSendCommand { get; }
    public RelayCommand FormatJsonCommand { get; }
    public RelayCommand ClearHistoryCommand { get; }

    public IReadOnlyList<string> TemplateHelp { get; } = TemplateEngine.BuiltInNames.Select(n => $"{{{{{n}}}}}").ToList();

    public ProducerViewModel(AppState state)
    {
        _state = state;
        _state.ConnectionsChanged += RefreshConnectionNames;
        AddHeaderCommand = new RelayCommand(() => Headers.Add(new HeaderEntryViewModel()));
        RemoveHeaderCommand = new RelayCommand<HeaderEntryViewModel>(h => { if (h is not null) Headers.Remove(h); });
        SendCommand = new AsyncRelayCommand(SendAsync,
            () => !IsSending && SelectedConnection is not null && !string.IsNullOrWhiteSpace(Topic));
        CancelSendCommand = new RelayCommand(() => _sendCts?.Cancel(), () => IsSending);
        FormatJsonCommand = new RelayCommand(FormatJson);
        ClearHistoryCommand = new RelayCommand(History.Clear);
        RefreshConnectionNames();
    }

    /// <summary>Pre-fills the form from an existing message (e.g. "Edit in Producer" on a consumed message).</summary>
    public void LoadMessage(string? connection, KafkaMessage message)
    {
        if (connection is not null && ConnectionNames.Contains(connection)) SelectedConnection = connection;
        Topic = message.Topic;
        Key = message.Key ?? "";
        IsTombstone = message.IsTombstone;
        Value = message.IsTombstone ? "" : message.Value is not null ? message.PrettyValue : "";
        Partition = null;
        Headers.Clear();
        foreach (var (name, value) in message.Headers) Headers.Add(new HeaderEntryViewModel { Name = name, Value = value });
        StatusMessage = message.IsBinary
            ? $"Loaded {message.Topic}@{message.Offset} - its value is binary and can't be edited as text."
            : $"Loaded {message.Topic}#{message.Partition}@{message.Offset} - edit and Send.";
    }

    private void FormatJson()
    {
        try
        {
            using var doc = JsonDocument.Parse(Value);
            Value = JsonSerializer.Serialize(doc.RootElement, new JsonSerializerOptions
            {
                WriteIndented = true,
                Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            });
            StatusMessage = "Formatted.";
        }
        catch (JsonException ex)
        {
            StatusMessage = $"Not valid JSON: {ex.Message}";
        }
    }

    private void RefreshConnectionNames()
    {
        CollectionSync.SyncSorted(ConnectionNames, _state.Connections.Keys);
        if (SelectedConnection is not null && !ConnectionNames.Contains(SelectedConnection)) SelectedConnection = null;
        if (SelectedConnection is null && ConnectionNames.Count == 1) SelectedConnection = ConnectionNames[0];
    }

    private async Task RefreshTopicNamesAsync()
    {
        _topicNamesCts?.Cancel();
        var cts = new CancellationTokenSource();
        _topicNamesCts = cts;

        TopicNames.Clear();
        if (SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway)) return;

        try
        {
            var names = await gateway.ListTopicsAsync(cts.Token).ConfigureAwait(true);
            if (cts.IsCancellationRequested) return;
            foreach (var name in names) TopicNames.Add(name);
        }
        catch (OperationCanceledException)
        {
            // superseded by a newer connection selection - ignore.
        }
        catch (Exception ex)
        {
            if (!cts.IsCancellationRequested) StatusMessage = $"Failed to load topic names: {ex.Message}";
        }
    }

    private async Task SendAsync()
    {
        if (SelectedConnection is null || !_state.Connections.TryGetValue(SelectedConnection, out var gateway))
        {
            StatusMessage = "Pick a connection first.";
            return;
        }

        var topic = Topic.Trim();
        int? partition = null;
        if (!string.IsNullOrWhiteSpace(Partition))
        {
            if (!int.TryParse(Partition.Trim(), out var p) || p < 0)
            {
                StatusMessage = $"Partition '{Partition}' must be a non-negative whole number (or empty).";
                return;
            }
            partition = p;
        }

        var headerTemplates = new List<(string Name, string Value)>();
        foreach (var h in Headers.Where(h => !string.IsNullOrWhiteSpace(h.Name)))
        {
            headerTemplates.Add((h.Name.Trim(), h.Value));
        }

        var count = RepeatCount;
        var cts = new CancellationTokenSource();
        _sendCts = cts;
        IsSending = true;
        var sent = 0;
        ProduceReceipt? last = null;
        var started = DateTimeOffset.Now;
        try
        {
            for (var i = 0; i < count; i++)
            {
                cts.Token.ThrowIfCancellationRequested();

                // Last-wins on duplicate header names instead of an "item with the same key" error.
                Dictionary<string, string>? headers = null;
                if (headerTemplates.Count > 0)
                {
                    headers = new Dictionary<string, string>(StringComparer.Ordinal);
                    foreach (var (name, value) in headerTemplates) headers[name] = TemplateEngine.RenderBuiltIns(value);
                }

                last = await gateway.ProduceAsync(new ProduceRequest
                {
                    Topic = topic,
                    Key = string.IsNullOrEmpty(Key) ? null : TemplateEngine.RenderBuiltIns(Key),
                    Value = IsTombstone ? null : TemplateEngine.RenderBuiltIns(Value),
                    Headers = headers,
                    Partition = partition
                }, cts.Token).ConfigureAwait(true);
                sent++;

                if (count > 1 && (sent % 100 == 0)) StatusMessage = $"Sent {sent:N0} / {count:N0}...";
            }

            AddHistory(new SentMessageRow
            {
                Summary = count == 1
                    ? $"{started:HH:mm:ss}  {last!.Topic}#{last.Partition}@{last.Offset}"
                    : $"{started:HH:mm:ss}  {sent:N0} × {topic} (last #{last!.Partition}@{last.Offset})",
                Detail = IsTombstone ? "<tombstone>" : Truncate(Value, 300)
            });
            StatusMessage = count == 1 ? "Sent." : $"Sent {sent:N0} messages.";
        }
        catch (OperationCanceledException)
        {
            StatusMessage = $"Cancelled after {sent:N0} message(s).";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Failed to send{(sent > 0 ? $" (after {sent:N0} succeeded)" : "")}: {ex.Message}";
            AddHistory(new SentMessageRow { Summary = $"{started:HH:mm:ss}  FAILED → {topic}", Detail = ex.Message, Failed = true });
        }
        finally
        {
            IsSending = false;
            _sendCts = null;
            cts.Dispose();
        }
    }

    private void AddHistory(SentMessageRow row)
    {
        History.Insert(0, row);
        while (History.Count > MaxHistory) History.RemoveAt(History.Count - 1);
    }

    private static string Truncate(string s, int max) => s.Length <= max ? s : s[..max] + "…";
}

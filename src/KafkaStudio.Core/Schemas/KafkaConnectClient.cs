using System.Text.Json;

namespace KafkaStudio.Core.Schemas;

public sealed record ConnectorTask(int Id, string State, string? WorkerId, string? Trace);

public sealed record ConnectorInfo
{
    public required string Name { get; init; }
    public string Type { get; init; } = "";
    public string State { get; init; } = "";
    public string? WorkerId { get; init; }
    public IReadOnlyList<ConnectorTask> Tasks { get; init; } = Array.Empty<ConnectorTask>();
    public IReadOnlyDictionary<string, string> Config { get; init; } = new Dictionary<string, string>();

    /// <summary>A connector is unhealthy when it, or any of its tasks, is FAILED.</summary>
    public bool IsFailed => State == "FAILED" || Tasks.Any(t => t.State == "FAILED");
}

/// <summary>
/// Read-only client for the Kafka Connect REST API: connectors, their state, tasks and configuration.
/// Passwords and secrets in a connector config come back masked from Connect itself ("[hidden]").
/// </summary>
public sealed class KafkaConnectClient : IDisposable
{
    private readonly HttpClient _http;

    public KafkaConnectClient(string baseUrl, HttpMessageHandler? handler = null)
    {
        _http = handler is null ? new HttpClient() : new HttpClient(handler, disposeHandler: false);
        _http.BaseAddress = new Uri(baseUrl.TrimEnd('/') + "/");
        _http.Timeout = TimeSpan.FromSeconds(15);
    }

    public async Task<IReadOnlyList<ConnectorInfo>> ListAsync(CancellationToken ct = default)
    {
        // expand=status&expand=info returns everything in one request (Connect 2.3+).
        using var response = await _http.GetAsync("connectors?expand=status&expand=info", ct).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new HttpRequestException($"Kafka Connect {(int)response.StatusCode}: {(body.Length > 200 ? body[..200] + "…" : body)}");
        }
        return Parse(body);
    }

    public static IReadOnlyList<ConnectorInfo> Parse(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var list = new List<ConnectorInfo>();
        foreach (var entry in doc.RootElement.EnumerateObject())
        {
            var status = entry.Value.TryGetProperty("status", out var s) ? s : default;
            var info = entry.Value.TryGetProperty("info", out var i) ? i : default;

            var connector = status.ValueKind == JsonValueKind.Object && status.TryGetProperty("connector", out var c) ? c : default;
            var tasks = new List<ConnectorTask>();
            if (status.ValueKind == JsonValueKind.Object && status.TryGetProperty("tasks", out var t) && t.ValueKind == JsonValueKind.Array)
            {
                foreach (var task in t.EnumerateArray())
                {
                    tasks.Add(new ConnectorTask(
                        task.TryGetProperty("id", out var id) ? id.GetInt32() : 0,
                        Str(task, "state") ?? "",
                        Str(task, "worker_id"),
                        Str(task, "trace")));
                }
            }

            var config = new Dictionary<string, string>();
            if (info.ValueKind == JsonValueKind.Object && info.TryGetProperty("config", out var cfg) && cfg.ValueKind == JsonValueKind.Object)
            {
                foreach (var kv in cfg.EnumerateObject()) config[kv.Name] = kv.Value.ToString();
            }

            list.Add(new ConnectorInfo
            {
                Name = entry.Name,
                Type = Str(status, "type") ?? Str(info, "type") ?? "",
                State = connector.ValueKind == JsonValueKind.Object ? Str(connector, "state") ?? "" : "",
                WorkerId = connector.ValueKind == JsonValueKind.Object ? Str(connector, "worker_id") : null,
                Tasks = tasks,
                Config = config
            });
        }
        return list.OrderBy(c => c.Name, StringComparer.Ordinal).ToList();
    }

    private static string? Str(JsonElement e, string name) =>
        e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

    public void Dispose() => _http.Dispose();
}

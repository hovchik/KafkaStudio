using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace KafkaStudio.Core.Schemas;

public sealed record RegisteredSchema(int Id, string SchemaType, string Schema);

public sealed record SubjectVersion(string Subject, int Version, int Id, string SchemaType, string Schema);

/// <summary>
/// Read-only client for a Confluent-compatible Schema Registry (also Redpanda and Apicurio's
/// compatibility API). It only ever reads; the app never registers schemas.
/// </summary>
public sealed class SchemaRegistryClient : IDisposable
{
    private readonly HttpClient _http;
    private readonly bool _ownsClient;
    private readonly ConcurrentDictionary<int, RegisteredSchema> _byId = new();

    public SchemaRegistryClient(string baseUrl, string? username = null, string? password = null, HttpMessageHandler? handler = null)
    {
        _http = handler is null ? new HttpClient() : new HttpClient(handler, disposeHandler: false);
        _ownsClient = true;
        _http.BaseAddress = new Uri(baseUrl.TrimEnd('/') + "/");
        _http.Timeout = TimeSpan.FromSeconds(15);
        _http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/vnd.schemaregistry.v1+json"));
        if (!string.IsNullOrEmpty(username))
        {
            var token = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
            _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", token);
        }
    }

    public async Task<RegisteredSchema> GetByIdAsync(int id, CancellationToken ct = default)
    {
        if (_byId.TryGetValue(id, out var cached)) return cached;
        using var doc = await GetJsonAsync($"schemas/ids/{id}", ct).ConfigureAwait(false);
        var root = doc.RootElement;
        var schema = new RegisteredSchema(
            id,
            root.TryGetProperty("schemaType", out var st) ? st.GetString() ?? "AVRO" : "AVRO",
            root.GetProperty("schema").GetString() ?? "");
        _byId[id] = schema;
        return schema;
    }

    public async Task<IReadOnlyList<string>> ListSubjectsAsync(CancellationToken ct = default)
    {
        using var doc = await GetJsonAsync("subjects", ct).ConfigureAwait(false);
        return doc.RootElement.EnumerateArray().Select(e => e.GetString() ?? "").Where(s => s.Length > 0).Order().ToList();
    }

    public async Task<IReadOnlyList<int>> ListVersionsAsync(string subject, CancellationToken ct = default)
    {
        using var doc = await GetJsonAsync($"subjects/{Uri.EscapeDataString(subject)}/versions", ct).ConfigureAwait(false);
        return doc.RootElement.EnumerateArray().Select(e => e.GetInt32()).ToList();
    }

    public async Task<SubjectVersion> GetVersionAsync(string subject, int version, CancellationToken ct = default)
    {
        using var doc = await GetJsonAsync($"subjects/{Uri.EscapeDataString(subject)}/versions/{version}", ct).ConfigureAwait(false);
        var root = doc.RootElement;
        return new SubjectVersion(
            subject, version,
            root.GetProperty("id").GetInt32(),
            root.TryGetProperty("schemaType", out var st) ? st.GetString() ?? "AVRO" : "AVRO",
            root.GetProperty("schema").GetString() ?? "");
    }

    private async Task<JsonDocument> GetJsonAsync(string path, CancellationToken ct)
    {
        using var response = await _http.GetAsync(path, ct).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new HttpRequestException($"Schema Registry {(int)response.StatusCode} for {path}: {Trim(body)}");
        }
        return JsonDocument.Parse(body);
    }

    private static string Trim(string s) => s.Length <= 200 ? s : s[..200] + "…";

    public void Dispose()
    {
        if (_ownsClient) _http.Dispose();
    }
}

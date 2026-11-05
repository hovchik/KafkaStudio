using KafkaStudio.Core.Connections;
using KafkaStudio.Core.Display;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Schemas;

namespace KafkaStudio.App.ViewModels.Shared;

/// <summary>
/// What the read screens do to a message before showing it: decode it through the Schema Registry (or a
/// chosen value format), then hide masked fields. One per connection; display only.
/// </summary>
public sealed class DisplayPipeline : IDisposable
{
    public const string MaskedHeader = "x-kafkastudio-masked";

    private readonly AppState _state;
    private readonly SchemaRegistryClient? _registry;
    private readonly SchemaRegistryDecoder? _decoder;
    private readonly FieldMasker _masker;

    public DisplayPipeline(AppState state, ConnectionProfile profile)
    {
        _state = state;
        _masker = new FieldMasker(profile.MaskedFields);
        if (!string.IsNullOrWhiteSpace(profile.SchemaRegistryUrl) &&
            Uri.TryCreate(profile.SchemaRegistryUrl, UriKind.Absolute, out _))
        {
            _registry = new SchemaRegistryClient(profile.SchemaRegistryUrl, profile.SchemaRegistryUsername, profile.SchemaRegistryPassword);
            _decoder = new SchemaRegistryDecoder(_registry);
        }
    }

    public async ValueTask<KafkaMessage> TransformAsync(KafkaMessage message, CancellationToken ct)
    {
        var result = message;
        var format = _state.DisplayFormat;
        if (format != ValueFormat.Auto)
        {
            result = ValueFormatter.Apply(result, format);
        }
        else if (_decoder is not null)
        {
            result = await _decoder.DecodeAsync(result, ct).ConfigureAwait(false);
        }

        var masked = _masker.Apply(result);
        if (!ReferenceEquals(masked, result))
        {
            var headers = new Dictionary<string, string>(masked.Headers) { [MaskedHeader] = "true" };
            masked = masked with { Headers = headers };
        }
        return masked;
    }

    public void Dispose() => _registry?.Dispose();
}

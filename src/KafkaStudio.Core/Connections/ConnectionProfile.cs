namespace KafkaStudio.Core.Connections;

/// <summary>
/// Security transport used when talking to a Kafka cluster.
/// Mirrors librdkafka's security.protocol values so it maps 1:1 onto Confluent.Kafka config.
/// </summary>
public enum SecurityProtocolKind
{
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl
}

public enum SaslMechanismKind
{
    None,
    Plain,
    ScramSha256,
    ScramSha512,
    OAuthBearer,
    GssApi
}

/// <summary>
/// Everything needed to connect to a Kafka cluster: bootstrap servers, security settings,
/// and any advanced librdkafka key/value overrides the user wants to pass through untouched.
/// Instances are immutable value objects; use <see cref="With"/> style records/`with` expressions to tweak one.
/// </summary>
public sealed record ConnectionProfile
{
    /// <summary>Friendly name shown in the UI, e.g. "Local Docker" or "Staging EU".</summary>
    public required string Name { get; init; }

    /// <summary>Comma separated host:port list, e.g. "localhost:9092,localhost:9093".</summary>
    public required string BootstrapServers { get; init; }

    public SecurityProtocolKind SecurityProtocol { get; init; } = SecurityProtocolKind.Plaintext;

    public SaslMechanismKind SaslMechanism { get; init; } = SaslMechanismKind.None;

    public string? SaslUsername { get; init; }

    /// <summary>
    /// Stored only in memory / in the (optionally encrypted) local profile store - never logged, never
    /// included in ToString(). Consumers should treat this as sensitive at all times.
    /// </summary>
    public string? SaslPassword { get; init; }

    /// <summary>Path to a CA certificate / truststore, when using SSL with a private CA.</summary>
    public string? SslCaLocation { get; init; }

    public bool SslEnableVerification { get; init; } = true;

    /// <summary>Optional Confluent Schema Registry URL, used only by steps that need Avro/JSON-Schema decoding.</summary>
    public string? SchemaRegistryUrl { get; init; }

    /// <summary>Client id reported to the broker; also used as a default prefix for consumer group names.</summary>
    public string ClientId { get; init; } = "kafka-studio";

    /// <summary>
    /// Escape hatch: any additional librdkafka configuration keys (e.g. "socket.timeout.ms") that
    /// don't have a first-class property here yet. Applied last, after the typed properties above.
    /// </summary>
    public IReadOnlyDictionary<string, string> AdvancedProperties { get; init; } =
        new Dictionary<string, string>();

    public override string ToString() => $"{Name} ({BootstrapServers})";
}

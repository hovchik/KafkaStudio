using Confluent.Kafka;
using KafkaStudio.Core.Connections;

namespace KafkaStudio.Kafka;

/// <summary>Translates a broker-agnostic <see cref="ConnectionProfile"/> into the librdkafka-flavoured
/// config objects Confluent.Kafka expects.</summary>
internal static class ConfigMapper
{
    /// <summary>How long a produce may sit in librdkafka's queue before it is failed. The librdkafka
    /// default is five minutes, far too long for an interactive "Send" button against a broker that
    /// isn't answering. Overridable through the profile's advanced properties.</summary>
    private const int DefaultMessageTimeoutMs = 30_000;

    public static ProducerConfig ToProducerConfig(ConnectionProfile profile)
    {
        var config = new ProducerConfig { MessageTimeoutMs = DefaultMessageTimeoutMs };
        ApplyCommon(config, profile);
        return config;
    }

    /// <summary>
    /// Rejects security settings librdkafka would otherwise accept and fail on later with an opaque
    /// error (e.g. SASL_SSL with no mechanism falls back to GSSAPI and dies in the handshake).
    /// </summary>
    public static void ValidateSecurity(ConnectionProfile profile)
    {
        var usesSasl = profile.SecurityProtocol is SecurityProtocolKind.SaslPlaintext or SecurityProtocolKind.SaslSsl;
        if (usesSasl && profile.SaslMechanism == SaslMechanismKind.None)
        {
            throw new ArgumentException($"security protocol {profile.SecurityProtocol} needs a SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER or GSSAPI)");
        }
        if (usesSasl && profile.SaslMechanism is SaslMechanismKind.Plain or SaslMechanismKind.ScramSha256 or SaslMechanismKind.ScramSha512
            && string.IsNullOrEmpty(profile.SaslUsername))
        {
            throw new ArgumentException($"SASL mechanism {profile.SaslMechanism} needs a username");
        }
    }

    public static ConsumerConfig ToConsumerConfig(ConnectionProfile profile, string groupId, AutoOffsetReset autoOffsetReset)
    {
        var config = new ConsumerConfig
        {
            GroupId = groupId,
            AutoOffsetReset = autoOffsetReset,
            EnableAutoCommit = false, // KafkaStudio always commits explicitly (see ConfluentKafkaGateway)

            // Lets the pump loop detect "no more messages available right now" per partition (via
            // ConsumeResult.IsPartitionEOF) so bounded scans (e.g. Topic Browser / "scan and acknowledge")
            // can stop once they've drained everything currently on the topic, instead of blocking
            // forever waiting for MaxMessages that may never arrive.
            EnablePartitionEof = true
        };
        ApplyCommon(config, profile);
        return config;
    }

    public static AdminClientConfig ToAdminConfig(ConnectionProfile profile)
    {
        var config = new AdminClientConfig();
        ApplyCommon(config, profile);
        return config;
    }

    private static void ApplyCommon(ClientConfig config, ConnectionProfile profile)
    {
        config.BootstrapServers = profile.BootstrapServers;
        config.ClientId = profile.ClientId;

        // Brokers default to auto.create.topics.enable=true, and a plain metadata request for a topic
        // name (which librdkafka sends for producers/admin clients when describing or looking up a
        // topic) is enough to create it. KafkaStudio must never change a cluster without an explicit
        // user action - a typo in a topic name must not create a topic - so opt out everywhere.
        config.Set("allow.auto.create.topics", "false");
        config.SecurityProtocol = profile.SecurityProtocol switch
        {
            SecurityProtocolKind.Plaintext => Confluent.Kafka.SecurityProtocol.Plaintext,
            SecurityProtocolKind.Ssl => Confluent.Kafka.SecurityProtocol.Ssl,
            SecurityProtocolKind.SaslPlaintext => Confluent.Kafka.SecurityProtocol.SaslPlaintext,
            SecurityProtocolKind.SaslSsl => Confluent.Kafka.SecurityProtocol.SaslSsl,
            _ => Confluent.Kafka.SecurityProtocol.Plaintext
        };

        if (profile.SaslMechanism != SaslMechanismKind.None)
        {
            config.SaslMechanism = profile.SaslMechanism switch
            {
                SaslMechanismKind.Plain => Confluent.Kafka.SaslMechanism.Plain,
                SaslMechanismKind.ScramSha256 => Confluent.Kafka.SaslMechanism.ScramSha256,
                SaslMechanismKind.ScramSha512 => Confluent.Kafka.SaslMechanism.ScramSha512,
                SaslMechanismKind.OAuthBearer => Confluent.Kafka.SaslMechanism.OAuthBearer,
                SaslMechanismKind.GssApi => Confluent.Kafka.SaslMechanism.Gssapi,
                _ => Confluent.Kafka.SaslMechanism.Plain
            };
            config.SaslUsername = profile.SaslUsername;
            config.SaslPassword = profile.SaslPassword;

            // SASL handshakes fail/hang silently as a bare "Local: Timed out" with no other detail;
            // turning on librdkafka's broker+security debug logging lets the gateway's log handler
            // capture the actual low-level reason (DNS/connect failure, auth rejection, etc.).
            config.Debug = "broker,security";
        }

        if (!string.IsNullOrEmpty(profile.SslCaLocation))
        {
            config.SslCaLocation = profile.SslCaLocation;
        }
        config.EnableSslCertificateVerification = profile.SslEnableVerification;

        // Escape hatch: anything the typed properties above don't cover yet.
        foreach (var (key, value) in profile.AdvancedProperties)
        {
            config.Set(key, value);
        }
    }
}

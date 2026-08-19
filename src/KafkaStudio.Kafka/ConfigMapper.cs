using Confluent.Kafka;
using KafkaStudio.Core.Connections;

namespace KafkaStudio.Kafka;

/// <summary>Translates a broker-agnostic <see cref="ConnectionProfile"/> into the librdkafka-flavoured
/// config objects Confluent.Kafka expects.</summary>
internal static class ConfigMapper
{
    public static ProducerConfig ToProducerConfig(ConnectionProfile profile)
    {
        var config = new ProducerConfig();
        ApplyCommon(config, profile);
        return config;
    }

    public static ConsumerConfig ToConsumerConfig(ConnectionProfile profile, string groupId, AutoOffsetReset autoOffsetReset)
    {
        var config = new ConsumerConfig
        {
            GroupId = groupId,
            AutoOffsetReset = autoOffsetReset,
            EnableAutoCommit = false // KafkaStudio always commits explicitly (see ConfluentKafkaGateway)
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
        }

        if (!string.IsNullOrEmpty(profile.SslCaLocation))
        {
            config.SslCaLocation = profile.SslCaLocation;
        }
        config.EnableSslCertificateVerification = profile.SslEnableVerification;

        // Escape hatch: anything the typed properties above don't cover yet.
        foreach (var (key, value) in profile.AdvancedProperties)
        {
            config[key] = value;
        }
    }
}

using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Connections;
using KafkaStudio.Core.Testing;

namespace KafkaStudio.Tests.Harness;

/// <summary>Convenience factory for wiring up an <see cref="InMemoryKafkaBroker"/>-backed test cluster.</summary>
public static class TestKafka
{
    public static ConnectionProfile Profile(string name = "local") => new()
    {
        Name = name,
        BootstrapServers = "in-memory:0"
    };

    public static IKafkaGateway NewGateway(InMemoryKafkaBroker broker, string profileName = "local") =>
        new InMemoryKafkaGateway(Profile(profileName), broker);
}

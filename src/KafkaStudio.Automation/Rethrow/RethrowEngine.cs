using System.Text.RegularExpressions;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Scripting.Ast;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.Automation.Rethrow;

/// <summary>Executes a single <see cref="RethrowRule"/> until cancelled: subscribe to the source topic
/// and relay every matching message to the destination topic.</summary>
public sealed class RethrowEngine
{
    public event Action<RethrowRule, KafkaMessage, ProduceReceipt>? MessageRelayed;
    public event Action<RethrowRule, KafkaMessage>? MessageSkipped;
    public event Action<RethrowRule, Exception>? RelayFailed;

    public async Task RunAsync(
        RethrowRule rule,
        IReadOnlyDictionary<string, IKafkaGateway> connections,
        CancellationToken cancellationToken = default)
    {
        if (!connections.TryGetValue(rule.SourceConnection, out var source))
        {
            throw new KeyNotFoundException($"unknown source connection '{rule.SourceConnection}'");
        }
        if (!connections.TryGetValue(rule.DestinationConnection, out var destination))
        {
            throw new KeyNotFoundException($"unknown destination connection '{rule.DestinationConnection}'");
        }

        var options = new ConsumeOptions
        {
            Topic = rule.SourceTopic,
            ConsumerGroup = $"rethrow-{rule.Name}",
            StartPosition = ConsumeStartPosition.Latest,
            AutoAcknowledge = true
        };

        await foreach (var message in source.ConsumeAsync(options, cancellationToken).ConfigureAwait(false))
        {
            try
            {
                if (!Matches(message, rule.Filters))
                {
                    MessageSkipped?.Invoke(rule, message);
                    continue;
                }

                var key = rule.KeepSourceKey ? message.Key : rule.FixedKey;
                var headers = new Dictionary<string, string>(message.Headers);
                foreach (var (name, value) in rule.ExtraHeaders) headers[name] = value;

                var receipt = await destination.ProduceAsync(new ProduceRequest
                {
                    Topic = rule.DestinationTopic,
                    Key = key,
                    Value = message.Value ?? string.Empty,
                    Headers = headers
                }, cancellationToken).ConfigureAwait(false);

                MessageRelayed?.Invoke(rule, message, receipt);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                RelayFailed?.Invoke(rule, ex);
            }
        }
    }

    private static bool Matches(KafkaMessage message, IReadOnlyList<Condition> filters)
    {
        foreach (var filter in filters)
        {
            var actual = filter.Field switch
            {
                ConditionField.Key => message.Key,
                ConditionField.Value => message.Value,
                ConditionField.Json => message.Value is null ? null : JsonPathEvaluator.Evaluate(message.Value, filter.JsonPath!),
                _ => null
            };

            var ok = filter.Comparator switch
            {
                Comparator.Equals => actual == filter.Expected,
                Comparator.NotEquals => actual != filter.Expected,
                Comparator.Contains => actual is not null && actual.Contains(filter.Expected, StringComparison.Ordinal),
                Comparator.Matches => actual is not null && Regex.IsMatch(actual, filter.Expected),
                _ => false
            };

            if (!ok) return false;
        }
        return true;
    }
}

using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.Automation.Rethrow;

/// <summary>Executes a single <see cref="RethrowRule"/> until cancelled: subscribe to the source topic
/// and relay every matching message to the destination topic.
///
/// Delivery is at-least-once: a message's offset is committed only after it has been produced to the
/// destination (or deliberately skipped by the filters), so a crash or broker error mid-relay re-delivers
/// it instead of silently dropping it. A standing relay is also resilient: if the subscription itself
/// fails (broker restart, network blip), the engine reports it via <see cref="RelayFailed"/> and
/// resubscribes with exponential backoff instead of stopping for good.</summary>
public sealed class RethrowEngine
{
    public static readonly TimeSpan InitialRetryDelay = TimeSpan.FromSeconds(1);
    public static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);

    public event Action<RethrowRule, KafkaMessage, ProduceReceipt>? MessageRelayed;
    public event Action<RethrowRule, KafkaMessage>? MessageSkipped;
    public event Action<RethrowRule, Exception>? RelayFailed;

    /// <summary>Throws <see cref="ArgumentException"/> when the rule can't work at all.</summary>
    public static void Validate(RethrowRule rule, IReadOnlyDictionary<string, IKafkaGateway> connections)
    {
        if (string.IsNullOrWhiteSpace(rule.Name)) throw new ArgumentException("rule name is required");
        if (string.IsNullOrWhiteSpace(rule.SourceTopic)) throw new ArgumentException("source topic is required");
        if (string.IsNullOrWhiteSpace(rule.DestinationTopic)) throw new ArgumentException("destination topic is required");
        if (!connections.ContainsKey(rule.SourceConnection))
            throw new ArgumentException($"unknown source connection '{rule.SourceConnection}'");
        if (!connections.ContainsKey(rule.DestinationConnection))
            throw new ArgumentException($"unknown destination connection '{rule.DestinationConnection}'");
        if (rule.SourceConnection == rule.DestinationConnection && rule.SourceTopic == rule.DestinationTopic)
            throw new ArgumentException("source and destination are the same topic - the rule would relay its own output forever");
        foreach (var filter in rule.Filters)
        {
            if (filter.Comparator == Scripting.Ast.Comparator.Matches && ConditionEvaluator.ValidatePattern(filter.Expected) is { } problem)
                throw new ArgumentException(problem);
            if (filter.JsonPath is not null && JsonPathEvaluator.Validate(filter.JsonPath) is { } pathProblem)
                throw new ArgumentException(pathProblem);
        }
    }

    public async Task RunAsync(
        RethrowRule rule,
        IReadOnlyDictionary<string, IKafkaGateway> connections,
        CancellationToken cancellationToken = default)
    {
        Validate(rule, connections);
        var source = connections[rule.SourceConnection];
        var destination = connections[rule.DestinationConnection];

        var delay = InitialRetryDelay;
        var hasCommitted = false;

        while (!cancellationToken.IsCancellationRequested)
        {
            var options = new ConsumeOptions
            {
                Topic = rule.SourceTopic,
                ConsumerGroup = $"rethrow-{rule.Name}",
                // First subscription starts at the tail (a new rule relays new traffic, not history);
                // after a failure, resume from what was committed so nothing in between is lost.
                StartPosition = hasCommitted ? ConsumeStartPosition.Committed : ConsumeStartPosition.Latest,
                // Commits are per partition: a partition this rule hasn't relayed from yet must resume
                // at its tail too, not replay its whole history to the destination.
                UncommittedStart = ConsumeStartPosition.Latest,
                AutoAcknowledge = false
            };

            try
            {
                await foreach (var message in source.ConsumeAsync(options, cancellationToken).ConfigureAwait(false))
                {
                    await RelayAsync(rule, source, destination, message, cancellationToken).ConfigureAwait(false);
                    hasCommitted = true;
                    delay = InitialRetryDelay; // healthy again
                }

                return; // the stream ended normally (gateway closed)
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                RelayFailed?.Invoke(rule, new InvalidOperationException(
                    $"subscription to '{rule.SourceTopic}' failed, retrying in {delay.TotalSeconds:0}s: {ex.Message}", ex));
            }

            try
            {
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxRetryDelay.Ticks));
        }
    }

    private async Task RelayAsync(RethrowRule rule, IKafkaGateway source, IKafkaGateway destination,
        KafkaMessage message, CancellationToken cancellationToken)
    {
        bool matches;
        try
        {
            matches = ConditionEvaluator.Matches(message, rule.Filters);
        }
        catch (Exception ex)
        {
            // A filter that can't be evaluated (e.g. regex timeout) - report and skip this message.
            RelayFailed?.Invoke(rule, ex);
            matches = false;
        }

        if (!matches)
        {
            MessageSkipped?.Invoke(rule, message);
            await source.AcknowledgeAsync(message, cancellationToken).ConfigureAwait(false);
            return;
        }

        var key = rule.KeepSourceKey ? message.Key : rule.FixedKey;
        var headers = new Dictionary<string, string>(message.Headers);
        foreach (var (name, value) in rule.ExtraHeaders) headers[name] = value;

        var request = new ProduceRequest
        {
            Topic = rule.DestinationTopic,
            Key = key,
            Value = message.Value,
            RawValue = message.RawValue,
            Headers = headers
        };

        // Produce failures (destination down, auth, too large) are retried right here, with backoff,
        // while the source subscription stays alive at this message's position. Tearing the
        // subscription down instead would lose the message when nothing has been committed yet (a
        // resubscribe starts at the tail) and would skip whatever arrived during the backoff.
        var delay = InitialRetryDelay;
        ProduceReceipt receipt;
        while (true)
        {
            try
            {
                receipt = await destination.ProduceAsync(request, cancellationToken).ConfigureAwait(false);
                break;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                RelayFailed?.Invoke(rule, new InvalidOperationException(
                    $"could not produce to '{rule.DestinationTopic}', retrying in {delay.TotalSeconds:0}s: {ex.Message}", ex));
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxRetryDelay.Ticks));
            }
        }

        await source.AcknowledgeAsync(message, cancellationToken).ConfigureAwait(false);
        MessageRelayed?.Invoke(rule, message, receipt);
    }
}

using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Scripting.Ast;

namespace KafkaStudio.Scripting.Runtime;

/// <summary>
/// Interprets a parsed <see cref="ScriptBlock"/> (a Scenario or Task) against one or more
/// <see cref="IKafkaGateway"/> connections. This is the piece that turns KafScript source into actual
/// Kafka traffic and check results - everything else (Lexer/Parser) just gets the text into an AST.
/// </summary>
public sealed class ScriptRunner
{
    /// <summary>How long a "scan" step keeps reading after the last message before deciding the
    /// backlog is exhausted. Scans are meant to be bounded reads of existing history, not live tails,
    /// so this stops them from blocking forever when no explicit "limit" is given.</summary>
    public static readonly TimeSpan ScanIdleTimeout = TimeSpan.FromSeconds(3);

    private static readonly Duration DefaultArrivalTimeout = new(30, TimeUnit.Seconds);

    private readonly IReadOnlyDictionary<string, IKafkaGateway> _connections;
    private readonly IKafkaGateway? _defaultGateway;
    private readonly IClock _clock;
    private readonly Action<string>? _onLog;

    public ScriptRunner(
        IReadOnlyDictionary<string, IKafkaGateway> connections,
        IKafkaGateway? defaultGateway = null,
        IClock? clock = null,
        Action<string>? onLog = null)
    {
        _connections = connections;
        _defaultGateway = defaultGateway ?? connections.Values.FirstOrDefault();
        _clock = clock ?? SystemClock.Instance;
        _onLog = onLog;
    }

    public async Task<ScriptRunResult> RunAsync(ScriptBlock block, CancellationToken cancellationToken = default)
    {
        var context = new ScenarioContext { Gateway = _defaultGateway };
        var results = new List<StepResult>();
        var overall = Stopwatch.StartNew();
        var success = true;

        try
        {
            foreach (var step in block.Steps)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var stepTimer = Stopwatch.StartNew();
                try
                {
                    var message = await ExecuteAsync(step, context, cancellationToken).ConfigureAwait(false);
                    results.Add(new StepResult(step, StepStatus.Passed, message, stepTimer.Elapsed));
                    _onLog?.Invoke($"[{step.Keyword}] {message}");
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (KafScriptException ex)
                {
                    results.Add(new StepResult(step, StepStatus.Failed, ex.Message, stepTimer.Elapsed));
                    _onLog?.Invoke($"[{step.Keyword}] FAILED: {ex.Message}");
                    success = false;
                    break;
                }
                catch (Exception ex)
                {
                    results.Add(new StepResult(step, StepStatus.Failed, $"unexpected error: {ex.Message}", stepTimer.Elapsed));
                    _onLog?.Invoke($"[{step.Keyword}] ERROR: {ex.Message}");
                    success = false;
                    break;
                }
            }

            // Any step never reached is recorded as Skipped so the report shows the full script.
            for (var i = results.Count; i < block.Steps.Count; i++)
            {
                results.Add(new StepResult(block.Steps[i], StepStatus.Skipped, "not reached", TimeSpan.Zero));
            }
        }
        finally
        {
            await context.DisposeWatchesAsync().ConfigureAwait(false);
        }

        return new ScriptRunResult(block, success, results, overall.Elapsed);
    }

    private Task<string> ExecuteAsync(Step step, ScenarioContext ctx, CancellationToken ct) => step.Action switch
    {
        UseConnectionAction a => Task.FromResult(ExecuteUseConnection(a, ctx)),
        ProduceMessageAction a => ExecuteProduce(a, ctx, ct),
        WatchTopicAction a => ExecuteWatch(a, ctx, ct),
        AwaitMessageAction a => ExecuteAwait(a, ctx, ct),
        RethrowAction a => ExecuteRethrow(a, ctx, ct),
        ScanTopicAction a => ExecuteScan(a, ctx, ct),
        AcknowledgeAction a => ExecuteAcknowledge(a, ctx, ct),
        LogAction a => Task.FromResult(ExecuteLog(a, ctx)),
        SetVariableAction a => Task.FromResult(ExecuteSetVariable(a, ctx)),
        CaptureAction a => Task.FromResult(ExecuteCapture(a, ctx)),
        WaitAction a => ExecuteWait(a, ct),
        AssertVariableAction a => Task.FromResult(ExecuteAssertVariable(a, ctx)),
        _ => throw new KafScriptException($"unsupported action '{step.Action.GetType().Name}'", step.Line)
    };

    private static string Render(string text, ScenarioContext ctx) => TemplateEngine.Render(text, ctx.Variables);

    private static IKafkaGateway RequireGateway(ScenarioContext ctx) => ctx.Gateway
        ?? throw new KafScriptException("no Kafka connection selected - add a 'use connection \"name\"' step first");

    private string ExecuteUseConnection(UseConnectionAction a, ScenarioContext ctx)
    {
        if (!_connections.TryGetValue(a.ConnectionName, out var gateway))
        {
            throw new KafScriptException(
                $"unknown connection '{a.ConnectionName}' (known: {string.Join(", ", _connections.Keys)})");
        }
        ctx.Gateway = gateway;
        return $"using connection '{a.ConnectionName}'";
    }

    private async Task<string> ExecuteProduce(ProduceMessageAction a, ScenarioContext ctx, CancellationToken ct)
    {
        var gateway = RequireGateway(ctx);
        var topic = Render(a.Topic, ctx);
        var key = a.Key is null ? null : Render(a.Key, ctx);
        var value = a.Value is null ? string.Empty : Render(a.Value, ctx);
        var headers = a.Headers.Count == 0
            ? null
            : a.Headers.ToDictionary(h => h.Name, h => Render(h.Value, ctx));

        var receipt = await gateway.ProduceAsync(
            new ProduceRequest { Topic = topic, Key = key, Value = value, Headers = headers }, ct)
            .ConfigureAwait(false);

        ctx.LastMessage = new KafkaMessage
        {
            Topic = topic,
            Partition = receipt.Partition,
            Offset = receipt.Offset,
            Key = key,
            Value = value,
            Headers = headers ?? new Dictionary<string, string>(),
            Timestamp = receipt.Timestamp
        };

        return $"produced message to {topic}#{receipt.Partition}@{receipt.Offset}";
    }

    private Task<string> ExecuteWatch(WatchTopicAction a, ScenarioContext ctx, CancellationToken ct)
    {
        var gateway = RequireGateway(ctx);
        var topic = Render(a.Topic, ctx);
        var startPosition = a.Position == TopicPosition.Beginning
            ? ConsumeStartPosition.Earliest
            : ConsumeStartPosition.Latest; // "end" and "now" both mean "start from the current tail"

        var options = new ConsumeOptions
        {
            Topic = topic,
            ConsumerGroup = $"kafscript-watch-{Guid.NewGuid():N}",
            StartPosition = startPosition
        };

        if (ctx.Watches.TryGetValue(topic, out var existing))
        {
            // Fire-and-forget: dispose the old subscription without blocking this step. Deliberately
            // not awaited so re-watching a topic mid-scenario stays fast.
            _ = existing.DisposeAsync().AsTask();
        }
        ctx.Watches[topic] = WatchHandle.Start(gateway, options, ct);
        ctx.LastWatchedTopic = topic;

        return Task.FromResult($"watching topic '{topic}' from {a.Position.ToString().ToLowerInvariant()}");
    }

    private async Task<string> ExecuteAwait(AwaitMessageAction a, ScenarioContext ctx, CancellationToken ct)
    {
        var topic = a.Topic is not null
            ? Render(a.Topic, ctx)
            : ctx.LastWatchedTopic
              ?? throw new KafScriptException("no topic given and no prior 'watch topic' step to fall back to");

        if (!ctx.Watches.TryGetValue(topic, out var handle))
        {
            // Convenience: an explicit "watch" wasn't set up, so start one now from the current tail.
            // This is race-safe for scripts where the trigger happens after this step runs, but for the
            // classic "produce on A, expect on B" cross-topic check you should add an explicit
            // "Given watch topic B from now" step *before* producing to A.
            var gateway = RequireGateway(ctx);
            var options = new ConsumeOptions
            {
                Topic = topic,
                ConsumerGroup = $"kafscript-expect-{Guid.NewGuid():N}",
                StartPosition = ConsumeStartPosition.Latest
            };
            handle = WatchHandle.Start(gateway, options, ct);
            ctx.Watches[topic] = handle;
        }

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(a.Duration.ToTimeSpan());

        try
        {
            await foreach (var message in handle.Reader.ReadAllAsync(timeoutCts.Token).ConfigureAwait(false))
            {
                if (MatchesConditions(message, a.Conditions, ctx))
                {
                    ctx.LastMessage = message;
                    return $"received matching message on '{topic}' at offset {message.Offset}";
                }
            }
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            // fall through to the failure below
        }

        var conditionText = a.Conditions.Count == 0 ? "" : " matching the given conditions";
        throw new StepAssertionException(
            $"no message arrived on topic '{topic}'{conditionText} within {a.Duration}");
    }

    private async Task<string> ExecuteRethrow(RethrowAction a, ScenarioContext ctx, CancellationToken ct)
    {
        if (ctx.LastMessage is null)
        {
            throw new KafScriptException(
                "no message available to rethrow - precede this with a 'watch'/'expect'/'message arrives' step");
        }

        var gateway = RequireGateway(ctx);
        var topic = Render(a.Topic, ctx);
        var key = a.KeepSourceKey ? ctx.LastMessage.Key
            : a.KeyOverride is not null ? Render(a.KeyOverride, ctx)
            : null;
        var headers = a.Headers.Count == 0
            ? null
            : a.Headers.ToDictionary(h => h.Name, h => Render(h.Value, ctx));

        var receipt = await gateway.ProduceAsync(new ProduceRequest
        {
            Topic = topic,
            Key = key,
            Value = ctx.LastMessage.Value ?? string.Empty,
            Headers = headers
        }, ct).ConfigureAwait(false);

        return $"rethrew message to {topic}#{receipt.Partition}@{receipt.Offset}";
    }

    private async Task<string> ExecuteScan(ScanTopicAction a, ScenarioContext ctx, CancellationToken ct)
    {
        var gateway = RequireGateway(ctx);
        var topic = Render(a.Topic, ctx);
        var options = new ConsumeOptions
        {
            Topic = topic,
            ConsumerGroup = $"kafscript-scan-{Guid.NewGuid():N}",
            StartPosition = a.Position == TopicPosition.Beginning ? ConsumeStartPosition.Earliest : ConsumeStartPosition.Latest,
            AutoAcknowledge = false,
            MaxMessages = a.Limit
        };

        ctx.ScannedMessages.Clear();

        using var idleCts = new CancellationTokenSource();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, idleCts.Token);
        idleCts.CancelAfter(ScanIdleTimeout);

        try
        {
            await foreach (var message in gateway.ConsumeAsync(options, linked.Token).ConfigureAwait(false))
            {
                ctx.ScannedMessages.Add(message);
                ctx.LastMessage = message;
                idleCts.CancelAfter(ScanIdleTimeout);

                if (a.Limit is { } limit && ctx.ScannedMessages.Count >= limit) break;
            }
        }
        catch (OperationCanceledException) when (idleCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            // idle timeout: treated as "no more messages available right now", i.e. end of scan
        }

        return $"scanned {ctx.ScannedMessages.Count} message(s) from topic '{topic}'";
    }

    private async Task<string> ExecuteAcknowledge(AcknowledgeAction a, ScenarioContext ctx, CancellationToken ct)
    {
        var gateway = RequireGateway(ctx);

        if (a.EachScanned)
        {
            foreach (var message in ctx.ScannedMessages)
            {
                await gateway.AcknowledgeAsync(message, ct).ConfigureAwait(false);
            }
            return $"acknowledged {ctx.ScannedMessages.Count} scanned message(s)";
        }

        if (ctx.LastMessage is null)
        {
            throw new KafScriptException("no message available to acknowledge");
        }
        await gateway.AcknowledgeAsync(ctx.LastMessage, ct).ConfigureAwait(false);
        return $"acknowledged message at offset {ctx.LastMessage.Offset}";
    }

    private static string ExecuteLog(LogAction a, ScenarioContext ctx)
    {
        var text = a.Target switch
        {
            LogTarget.Key => ctx.LastMessage?.Key ?? "<null>",
            LogTarget.Value => ctx.LastMessage?.Value ?? "<null>",
            LogTarget.Message => ctx.LastMessage?.ToDisplayString() ?? "<no message>",
            LogTarget.Literal => Render(a.Literal ?? string.Empty, ctx),
            _ => string.Empty
        };
        return $"log: {text}";
    }

    private static string ExecuteSetVariable(SetVariableAction a, ScenarioContext ctx)
    {
        var value = Render(a.Value, ctx);
        ctx.Variables[a.Name] = value;
        return $"set variable {a.Name} = \"{value}\"";
    }

    private static string ExecuteCapture(CaptureAction a, ScenarioContext ctx)
    {
        if (ctx.LastMessage is null)
        {
            throw new KafScriptException("no message available to capture from");
        }

        string? value = a.Source switch
        {
            ConditionField.Key => ctx.LastMessage.Key,
            ConditionField.Value => ctx.LastMessage.Value,
            ConditionField.Json => ctx.LastMessage.Value is null
                ? null
                : JsonPathEvaluator.Evaluate(ctx.LastMessage.Value, a.JsonPath!),
            _ => null
        };

        if (value is null)
        {
            var what = a.Source == ConditionField.Json ? $"json path '{a.JsonPath}'" : a.Source.ToString().ToLowerInvariant();
            throw new StepAssertionException($"capture failed: {what} not found on the last message");
        }

        ctx.Variables[a.VariableName] = value;
        return $"captured {a.VariableName} = \"{value}\"";
    }

    private async Task<string> ExecuteWait(WaitAction a, CancellationToken ct)
    {
        await _clock.Delay(a.Duration.ToTimeSpan(), ct).ConfigureAwait(false);
        return $"waited {a.Duration}";
    }

    private static string ExecuteAssertVariable(AssertVariableAction a, ScenarioContext ctx)
    {
        if (!ctx.Variables.TryGetValue(a.VariableName, out var actual))
        {
            throw new KafScriptException($"unknown variable '{a.VariableName}' (was it captured/set earlier?)");
        }

        var expected = Render(a.Expected, ctx);
        var ok = Compare(actual, a.Comparator, expected);
        if (!ok)
        {
            throw new StepAssertionException(
                $"assertion failed: variable '{a.VariableName}' was \"{actual}\", expected {DescribeComparator(a.Comparator)} \"{expected}\"");
        }
        return $"assert {a.VariableName} {DescribeComparator(a.Comparator)} \"{expected}\" - passed";
    }

    private static bool MatchesConditions(KafkaMessage message, IReadOnlyList<Condition> conditions, ScenarioContext ctx)
    {
        foreach (var condition in conditions)
        {
            var actual = condition.Field switch
            {
                ConditionField.Key => message.Key,
                ConditionField.Value => message.Value,
                ConditionField.Json => message.Value is null
                    ? null
                    : JsonPathEvaluator.Evaluate(message.Value, condition.JsonPath!),
                _ => null
            };

            var expected = Render(condition.Expected, ctx);
            if (!Compare(actual, condition.Comparator, expected)) return false;
        }
        return true;
    }

    private static bool Compare(string? actual, Comparator comparator, string expected) => comparator switch
    {
        Comparator.Equals => actual == expected,
        Comparator.NotEquals => actual != expected,
        Comparator.Contains => actual is not null && actual.Contains(expected, StringComparison.Ordinal),
        Comparator.Matches => actual is not null && Regex.IsMatch(actual, expected),
        _ => false
    };

    private static string DescribeComparator(Comparator comparator) => comparator switch
    {
        Comparator.Equals => "equals",
        Comparator.NotEquals => "not equals",
        Comparator.Contains => "contains",
        Comparator.Matches => "matches",
        _ => comparator.ToString()
    };
}

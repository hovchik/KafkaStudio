using System.Diagnostics;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Validation;
using KafkaStudio.Scripting.Ast;

namespace KafkaStudio.Scripting.Runtime;

/// <summary>
/// Interprets a parsed <see cref="ScriptBlock"/> (a Scenario or Task) against one or more
/// <see cref="IKafkaGateway"/> connections. This is the piece that turns KafScript source into actual
/// Kafka traffic and check results - everything else (Lexer/Parser) just gets the text into an AST.
/// </summary>
public sealed class ScriptRunner
{
    /// <summary>How long a "scan" step keeps waiting after the last message before deciding the
    /// backlog is exhausted. Scans ask the gateway to stop at the end of the current backlog on their
    /// own; this is only a safety net for a gateway/broker that never reports partition end.</summary>
    public static readonly TimeSpan ScanIdleTimeout = TimeSpan.FromSeconds(10);

    /// <summary>How long a scan may take to receive its first message (joining/assigning partitions on
    /// a real cluster can take a few seconds) before it's treated as "topic is empty".</summary>
    public static readonly TimeSpan ScanStartTimeout = TimeSpan.FromSeconds(30);

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

    /// <summary>Variables every run starts with (e.g. environment data passed to a test run with
    /// <c>--var</c>); a script's own <c>set variable</c> steps override them.</summary>
    public IReadOnlyDictionary<string, string>? InitialVariables { get; init; }

    /// <summary>Directory that relative <c>schema file "..."</c> paths resolve against - usually the
    /// folder of the .kafscript file being run. Defaults to the current directory.</summary>
    public string? BaseDirectory { get; init; }

    /// <summary>At most this many schema violations are spelled out in a failed step's message.</summary>
    public const int MaxReportedViolations = 5;

    /// <summary>Invoked (on the runner's thread) after every step completes - lets a UI stream results
    /// in live instead of waiting for the whole block.</summary>
    public event Action<StepResult>? StepCompleted;

    /// <summary>
    /// Runs every step in order, stopping at the first failure. Never throws for script/Kafka errors -
    /// they become Failed steps. Cancellation also doesn't throw: the step in flight is reported as
    /// <see cref="StepStatus.Cancelled"/>, the rest as Skipped, and the result has
    /// <see cref="ScriptRunResult.Cancelled"/> set.
    /// </summary>
    public async Task<ScriptRunResult> RunAsync(ScriptBlock block, CancellationToken cancellationToken = default)
    {
        var context = new ScenarioContext { Gateway = _defaultGateway };
        if (InitialVariables is not null)
        {
            foreach (var (name, value) in InitialVariables) context.Variables[name] = value;
        }
        var results = new List<StepResult>();
        var overall = Stopwatch.StartNew();
        var success = true;
        var cancelled = false;

        void Record(StepResult result)
        {
            results.Add(result);
            StepCompleted?.Invoke(result);
        }

        try
        {
            foreach (var step in block.Steps)
            {
                var stepTimer = Stopwatch.StartNew();
                if (cancellationToken.IsCancellationRequested)
                {
                    Record(new StepResult(step, StepStatus.Cancelled, "cancelled", TimeSpan.Zero));
                    success = false;
                    cancelled = true;
                    break;
                }

                try
                {
                    var message = await ExecuteAsync(step, context, cancellationToken).ConfigureAwait(false);
                    Record(new StepResult(step, StepStatus.Passed, message, stepTimer.Elapsed));
                    _onLog?.Invoke($"[{step.Keyword}] {message}");
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    Record(new StepResult(step, StepStatus.Cancelled, "cancelled", stepTimer.Elapsed));
                    _onLog?.Invoke($"[{step.Keyword}] CANCELLED");
                    success = false;
                    cancelled = true;
                    break;
                }
                catch (KafScriptException ex)
                {
                    Record(new StepResult(step, StepStatus.Failed, ex.Message, stepTimer.Elapsed) { IsError = ex is not StepAssertionException });
                    _onLog?.Invoke($"[{step.Keyword}] FAILED: {ex.Message}");
                    success = false;
                    break;
                }
                catch (Exception ex)
                {
                    Record(new StepResult(step, StepStatus.Failed, $"unexpected error: {ex.Message}", stepTimer.Elapsed) { IsError = true });
                    _onLog?.Invoke($"[{step.Keyword}] ERROR: {ex.Message}");
                    success = false;
                    break;
                }
            }

            // Any step never reached is recorded as Skipped so the report shows the full script.
            for (var i = results.Count; i < block.Steps.Count; i++)
            {
                Record(new StepResult(block.Steps[i], StepStatus.Skipped, "not reached", TimeSpan.Zero));
            }
        }
        finally
        {
            await context.DisposeWatchesAsync().ConfigureAwait(false);
        }

        return new ScriptRunResult(block, success, results, overall.Elapsed, cancelled);
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
        ExpectNoMessageAction a => ExecuteExpectNoMessage(a, ctx, ct),
        ExpectMessageCountAction a => ExecuteExpectCount(a, ctx, ct),
        AssertMessageAction a => Task.FromResult(ExecuteAssertMessage(a, ctx)),
        ValidateSchemaAction a => ExecuteValidateSchema(a, ctx, ct),
        _ => throw new KafScriptException($"unsupported action '{step.Action.GetType().Name}'", step.Line)
    };

    private static string Render(string text, ScenarioContext ctx) => TemplateEngine.Render(text, ctx.Variables);

    private static IKafkaGateway RequireGateway(ScenarioContext ctx) => ctx.Gateway
        ?? throw new KafScriptException("no Kafka connection selected - add a 'use connection \"name\"' step first");

    private static string RenderTopic(string topic, ScenarioContext ctx)
    {
        var rendered = Render(topic, ctx).Trim();
        if (rendered.Length == 0) throw new KafScriptException("topic name is empty");
        return rendered;
    }

    /// <summary>Header assignments rendered into a dictionary. A repeated header name keeps the last value
    /// instead of throwing a raw "duplicate key" error.</summary>
    private static Dictionary<string, string>? RenderHeaders(IReadOnlyList<HeaderAssignment> headers, ScenarioContext ctx)
    {
        if (headers.Count == 0) return null;
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var h in headers) result[Render(h.Name, ctx)] = Render(h.Value, ctx);
        return result;
    }

    private string ExecuteUseConnection(UseConnectionAction a, ScenarioContext ctx)
    {
        if (!_connections.TryGetValue(a.ConnectionName, out var gateway))
        {
            var known = _connections.Count == 0 ? "none - add one on the Connections screen" : string.Join(", ", _connections.Keys);
            throw new KafScriptException($"unknown connection '{a.ConnectionName}' (known: {known})");
        }
        ctx.Gateway = gateway;
        return $"using connection '{a.ConnectionName}'";
    }

    private async Task<string> ExecuteProduce(ProduceMessageAction a, ScenarioContext ctx, CancellationToken ct)
    {
        var gateway = RequireGateway(ctx);
        var topic = RenderTopic(a.Topic, ctx);
        ProduceReceipt? receipt = null;

        for (var index = 1; index <= a.Count; index++)
        {
            ct.ThrowIfCancellationRequested();
            // {{$index}} is only meaningful for "produce N messages"; a single produce sees 1.
            ctx.Variables["$index"] = index.ToString(System.Globalization.CultureInfo.InvariantCulture);
            try
            {
                var key = a.Key is null ? null : Render(a.Key, ctx);
                var value = a.Value is null ? string.Empty : Render(a.Value, ctx);
                var headers = RenderHeaders(a.Headers, ctx);

                receipt = await gateway.ProduceAsync(
                    new ProduceRequest { Topic = topic, Key = key, Value = value, Headers = headers }, ct)
                    .ConfigureAwait(false);

                ctx.LastMessage = new KafkaMessage
                {
                    Topic = topic,
                    Partition = receipt.Partition,
                    Offset = receipt.Offset,
                    Key = key,
                    Value = value,
                    RawValue = System.Text.Encoding.UTF8.GetBytes(value),
                    Headers = headers ?? new Dictionary<string, string>(),
                    Timestamp = receipt.Timestamp
                };
            }
            finally
            {
                ctx.Variables.Remove("$index");
            }
        }

        return a.Count == 1
            ? $"produced message to {topic}#{receipt!.Partition}@{receipt.Offset}"
            : $"produced {a.Count} messages to {topic} (last at #{receipt!.Partition}@{receipt.Offset})";
    }

    private async Task<string> ExecuteWatch(WatchTopicAction a, ScenarioContext ctx, CancellationToken ct)
    {
        var gateway = RequireGateway(ctx);
        var topic = RenderTopic(a.Topic, ctx);
        var startPosition = a.Position == TopicPosition.Beginning
            ? ConsumeStartPosition.Earliest
            : ConsumeStartPosition.Latest; // "end" and "now" both mean "start from the current tail"

        var options = new ConsumeOptions
        {
            Topic = topic,
            ConsumerGroup = $"kafscript-watch-{Guid.NewGuid():N}",
            StartPosition = startPosition
        };

        if (ctx.Watches.Remove(topic, out var existing))
        {
            // Don't block this step on tearing down the old subscription (closing a real consumer can
            // take a while) - but do remember it, so the run doesn't finish with it still alive.
            ctx.PendingDisposals.Add(existing.DisposeAsync().AsTask());
        }
        ctx.Watches[topic] = await WatchHandle.StartAsync(gateway, options, ct).ConfigureAwait(false);
        ctx.LastWatchedTopic = topic;

        return $"watching topic '{topic}' from {a.Position.ToString().ToLowerInvariant()}";
    }

    private async Task<string> ExecuteAwait(AwaitMessageAction a, ScenarioContext ctx, CancellationToken ct)
    {
        var topic = a.Topic is not null
            ? RenderTopic(a.Topic, ctx)
            : ctx.LastWatchedTopic
              ?? throw new KafScriptException("no topic given and no prior 'watch topic' step to fall back to");

        var handle = await GetOrStartWatchAsync(topic, ctx, ct).ConfigureAwait(false);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(a.Duration.ToTimeSpan());

        var inspected = 0;
        try
        {
            await foreach (var message in handle.Reader.ReadAllAsync(timeoutCts.Token).ConfigureAwait(false))
            {
                inspected++;
                if (ConditionEvaluator.Matches(message, a.Conditions, s => Render(s, ctx)))
                {
                    ctx.LastMessage = message;
                    return $"received matching message on '{topic}' at partition {message.Partition}, offset {message.Offset}";
                }
            }

            // The subscription ended on its own (e.g. the topic's consumer was closed) - fall through.
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            // fall through to the failure below
        }

        var conditionText = a.Conditions.Count == 0 ? "" : " matching the given conditions";
        var seen = inspected == 0 ? "no messages arrived at all" : $"{inspected} message(s) arrived but none matched";
        throw new StepAssertionException(
            $"no message arrived on topic '{topic}'{conditionText} within {a.Duration} ({seen})");
    }

    /// <summary>
    /// The watch opened on <paramref name="topic"/> by an earlier step, or - as a convenience - a new one
    /// from the current tail. That's race-safe for scripts where the trigger happens after this step
    /// runs, but for the classic "produce on A, expect on B" cross-topic check you should add an
    /// explicit "Given watch topic B from now" step *before* producing to A.
    /// </summary>
    private static async Task<WatchHandle> GetOrStartWatchAsync(string topic, ScenarioContext ctx, CancellationToken ct)
    {
        if (ctx.Watches.TryGetValue(topic, out var handle)) return handle;
        var gateway = RequireGateway(ctx);
        var options = new ConsumeOptions
        {
            Topic = topic,
            ConsumerGroup = $"kafscript-expect-{Guid.NewGuid():N}",
            StartPosition = ConsumeStartPosition.Latest
        };
        handle = await WatchHandle.StartAsync(gateway, options, ct).ConfigureAwait(false);
        ctx.Watches[topic] = handle;
        return handle;
    }

    /// <summary>Reads messages from a watch until <paramref name="window"/> elapses or
    /// <paramref name="onMessage"/> returns false. Returns false when stopped early.</summary>
    private static async Task<bool> ReadWindowAsync(WatchHandle handle, TimeSpan window, Func<KafkaMessage, bool> onMessage, CancellationToken ct)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(window);
        try
        {
            await foreach (var message in handle.Reader.ReadAllAsync(timeoutCts.Token).ConfigureAwait(false))
            {
                if (!onMessage(message)) return false;
            }
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            // window over
        }
        return true;
    }

    private async Task<string> ExecuteExpectNoMessage(ExpectNoMessageAction a, ScenarioContext ctx, CancellationToken ct)
    {
        var topic = RenderTopic(a.Topic, ctx);
        var handle = await GetOrStartWatchAsync(topic, ctx, ct).ConfigureAwait(false);

        var inspected = 0;
        KafkaMessage? offending = null;
        await ReadWindowAsync(handle, a.Duration.ToTimeSpan(), message =>
        {
            inspected++;
            if (!ConditionEvaluator.Matches(message, a.Conditions, s => Render(s, ctx))) return true;
            offending = message;
            return false;
        }, ct).ConfigureAwait(false);

        var conditionText = a.Conditions.Count == 0 ? "" : " matching the given conditions";
        if (offending is not null)
        {
            ctx.LastMessage = offending;
            throw new StepAssertionException(
                $"expected no message on topic '{topic}'{conditionText} within {a.Duration}, but one arrived at " +
                $"partition {offending.Partition}, offset {offending.Offset}: {Preview(offending)}");
        }
        return inspected == 0
            ? $"no message arrived on '{topic}' within {a.Duration} - as expected"
            : $"no message{conditionText} arrived on '{topic}' within {a.Duration} ({inspected} other message(s) ignored) - as expected";
    }

    private async Task<string> ExecuteExpectCount(ExpectMessageCountAction a, ScenarioContext ctx, CancellationToken ct)
    {
        var topic = RenderTopic(a.Topic, ctx);
        var handle = await GetOrStartWatchAsync(topic, ctx, ct).ConfigureAwait(false);

        var matched = 0;
        var inspected = 0;
        await ReadWindowAsync(handle, a.Duration.ToTimeSpan(), message =>
        {
            inspected++;
            if (!ConditionEvaluator.Matches(message, a.Conditions, s => Render(s, ctx))) return true;
            matched++;
            ctx.LastMessage = message;
            return a.Mode switch
            {
                CountMode.AtLeast => matched < a.Count,
                _ => matched <= a.Count // exactly / at most: keep watching the whole window, stop once over
            };
        }, ct).ConfigureAwait(false);

        var ok = a.Mode switch
        {
            CountMode.AtLeast => matched >= a.Count,
            CountMode.AtMost => matched <= a.Count,
            _ => matched == a.Count
        };
        var modeText = a.Mode switch { CountMode.AtLeast => "at least ", CountMode.AtMost => "at most ", _ => "exactly " };
        var conditionText = a.Conditions.Count == 0 ? "" : " matching the given conditions";
        if (!ok)
        {
            var seen = matched > a.Count && a.Mode != CountMode.AtLeast ? $"more than {a.Count}" : matched.ToString(System.Globalization.CultureInfo.InvariantCulture);
            throw new StepAssertionException(
                $"expected {modeText}{a.Count} message(s){conditionText} on topic '{topic}' within {a.Duration}, " +
                $"but {seen} arrived ({inspected} inspected)");
        }
        return $"{matched} message(s){conditionText} arrived on '{topic}' (expected {modeText}{a.Count})";
    }

    private static string ExecuteAssertMessage(AssertMessageAction a, ScenarioContext ctx)
    {
        var message = ctx.LastMessage ?? throw new KafScriptException(
            "no message to check - 'assert last message' needs an earlier produce/expect/message arrives/scan step");

        if (ConditionEvaluator.FirstFailure(message, a.Conditions, s => Render(s, ctx)) is { } failure)
        {
            var actual = failure.Actual is null ? "missing" : $"\"{Truncate(failure.Actual)}\"";
            throw new StepAssertionException(
                $"assertion failed on message {message.Topic}#{message.Partition}@{message.Offset}: " +
                $"expected {ConditionEvaluator.Describe(failure.Condition)}, but it was {actual}");
        }
        return $"last message satisfies {string.Join(" and ", a.Conditions.Select(ConditionEvaluator.Describe))}";
    }

    private async Task<string> ExecuteValidateSchema(ValidateSchemaAction a, ScenarioContext ctx, CancellationToken ct)
    {
        string schemaText;
        string source;
        if (a.SchemaFile is not null)
        {
            var file = Render(a.SchemaFile, ctx);
            var path = Path.IsPathRooted(file) ? file : Path.GetFullPath(Path.Combine(BaseDirectory ?? Directory.GetCurrentDirectory(), file));
            try
            {
                schemaText = await File.ReadAllTextAsync(path, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                throw new KafScriptException($"can't read schema file '{path}': {ex.Message}");
            }
            source = $"schema file '{Path.GetFileName(path)}'";
        }
        else
        {
            schemaText = Render(a.SchemaText!, ctx);
            source = "the schema";
        }

        if (!JsonSchema.TryParse(schemaText, out var schema, out var problem))
        {
            throw new KafScriptException($"invalid JSON schema ({source}): {problem}");
        }

        if (!a.EachScanned)
        {
            var message = ctx.LastMessage ?? throw new KafScriptException(
                "no message to validate - 'validate last message' needs an earlier produce/expect/message arrives/scan step");
            var violations = schema!.Validate(message.Value);
            if (violations.Count > 0)
            {
                throw new StepAssertionException(
                    $"message {message.Topic}#{message.Partition}@{message.Offset} breaks {source}: {DescribeViolations(violations)}");
            }
            return $"message {message.Topic}#{message.Partition}@{message.Offset} matches {source}";
        }

        var invalid = new List<(KafkaMessage Message, IReadOnlyList<SchemaViolation> Violations)>();
        foreach (var message in ctx.ScannedMessages)
        {
            var violations = schema!.Validate(message.Value);
            if (violations.Count > 0) invalid.Add((message, violations));
        }
        if (invalid.Count > 0)
        {
            var first = invalid[0];
            throw new StepAssertionException(
                $"{invalid.Count} of {ctx.ScannedMessages.Count} scanned message(s) break {source}; first at " +
                $"{first.Message.Topic}#{first.Message.Partition}@{first.Message.Offset}: {DescribeViolations(first.Violations)}");
        }
        return $"all {ctx.ScannedMessages.Count} scanned message(s) match {source}";
    }

    private static string DescribeViolations(IReadOnlyList<SchemaViolation> violations)
    {
        var shown = string.Join("; ", violations.Take(MaxReportedViolations));
        return violations.Count > MaxReportedViolations ? $"{shown}; …and {violations.Count - MaxReportedViolations} more" : shown;
    }

    private static string Preview(KafkaMessage message)
    {
        var text = message.Value ?? message.ValuePreview;
        return $"key={message.Key ?? "<null>"} value={Truncate(text)}";
    }

    private static string Truncate(string text) => text.Length > 120 ? text[..120] + "…" : text;

    private async Task<string> ExecuteRethrow(RethrowAction a, ScenarioContext ctx, CancellationToken ct)
    {
        var source = ctx.LastMessage ?? throw new KafScriptException(
            "no message available to rethrow - precede this with a 'watch'/'expect'/'message arrives' step");

        var gateway = RequireGateway(ctx);
        var topic = RenderTopic(a.Topic, ctx);
        var key = a.KeepSourceKey ? source.Key
            : a.KeyOverride is not null ? Render(a.KeyOverride, ctx)
            : null;

        // Relay keeps the source's headers (overridable per header), like a Rethrow Rule does.
        var headers = new Dictionary<string, string>(source.Headers, StringComparer.Ordinal);
        foreach (var (name, value) in RenderHeaders(a.Headers, ctx) ?? new Dictionary<string, string>())
        {
            headers[name] = value;
        }

        var receipt = await gateway.ProduceAsync(new ProduceRequest
        {
            Topic = topic,
            Key = key,
            Value = source.Value,
            RawValue = source.RawValue, // byte-for-byte, so binary payloads survive the relay
            Headers = headers.Count == 0 ? null : headers
        }, ct).ConfigureAwait(false);

        return $"rethrew message to {topic}#{receipt.Partition}@{receipt.Offset}";
    }

    private async Task<string> ExecuteScan(ScanTopicAction a, ScenarioContext ctx, CancellationToken ct)
    {
        var gateway = RequireGateway(ctx);
        var topic = RenderTopic(a.Topic, ctx);
        var options = new ConsumeOptions
        {
            Topic = topic,
            ConsumerGroup = a.ConsumerGroup is null ? $"kafscript-scan-{Guid.NewGuid():N}" : Render(a.ConsumerGroup, ctx),
            StartPosition = a.Position switch
            {
                TopicPosition.Beginning => ConsumeStartPosition.Earliest,
                TopicPosition.Committed => ConsumeStartPosition.Committed,
                _ => ConsumeStartPosition.Latest
            },
            AutoAcknowledge = false,
            MaxMessages = a.Limit,
            // A scan is a bounded read of the current backlog: let the gateway stop at partition end
            // instead of relying on an idle timer (which, on a real cluster, could fire before
            // partition assignment even finished and report an empty topic).
            StopAtPartitionEnd = true
        };

        ctx.ScannedMessages.Clear();

        using var idleCts = new CancellationTokenSource();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, idleCts.Token);
        idleCts.CancelAfter(ScanStartTimeout);

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
            if (ctx.ScannedMessages.Count == 0)
            {
                return "acknowledged 0 scanned message(s) (nothing was scanned)";
            }

            // Committing the highest offset per partition acknowledges everything before it too, so
            // there's no need for one broker round trip per message.
            var highestPerPartition = ctx.ScannedMessages
                .GroupBy(m => (m.Topic, m.Partition))
                .Select(g => g.MaxBy(m => m.Offset)!);
            foreach (var message in highestPerPartition)
            {
                await gateway.AcknowledgeAsync(message, ct).ConfigureAwait(false);
            }
            return $"acknowledged {ctx.ScannedMessages.Count} scanned message(s)";
        }

        if (ctx.LastMessage is null)
        {
            throw new KafScriptException("no message available to acknowledge");
        }
        if (ctx.LastMessage.ConsumerGroup is null)
        {
            throw new KafScriptException(
                "the last message was produced by this script, not consumed by a consumer group - only consumed messages can be acknowledged");
        }
        await gateway.AcknowledgeAsync(ctx.LastMessage, ct).ConfigureAwait(false);
        return $"acknowledged message at offset {ctx.LastMessage.Offset}";
    }

    private static string ExecuteLog(LogAction a, ScenarioContext ctx)
    {
        var text = a.Target switch
        {
            LogTarget.Key => ctx.LastMessage?.Key ?? "<null>",
            LogTarget.Value => ctx.LastMessage is null ? "<no message>" : ctx.LastMessage.Value ?? ctx.LastMessage.ValuePreview,
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

        var value = ConditionEvaluator.ReadField(ctx.LastMessage, a.Source, a.JsonPath);
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
        var known = ctx.Variables.TryGetValue(a.VariableName, out var actual);
        if (a.Comparator is Comparator.Exists or Comparator.NotExists)
        {
            if (known != (a.Comparator == Comparator.Exists))
            {
                throw new StepAssertionException(known
                    ? $"assertion failed: variable '{a.VariableName}' is set (to \"{actual}\") but was expected not to exist"
                    : $"assertion failed: variable '{a.VariableName}' was never set/captured");
            }
            return $"assert {a.VariableName} {ConditionEvaluator.Describe(a.Comparator)} - passed";
        }
        if (!known)
        {
            throw new KafScriptException($"unknown variable '{a.VariableName}' (was it captured/set earlier?)");
        }

        var expected = Render(a.Expected, ctx);
        var ok = ConditionEvaluator.Compare(actual, a.Comparator, expected);
        if (!ok)
        {
            throw new StepAssertionException(
                $"assertion failed: variable '{a.VariableName}' was \"{actual}\", expected {ConditionEvaluator.Describe(a.Comparator)} \"{expected}\"");
        }
        return $"assert {a.VariableName} {ConditionEvaluator.Describe(a.Comparator)} \"{expected}\" - passed";
    }
}

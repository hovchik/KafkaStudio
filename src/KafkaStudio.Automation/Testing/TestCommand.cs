using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using KafkaStudio.Automation.Testing.Reports;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Connections;
using KafkaStudio.Core.Testing;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.Automation.Testing;

/// <summary>
/// The <c>kafkastudio test</c> command: discovers .kafscript tests, runs the selected ones and writes
/// reports - built for CI pipelines (exit code, JUnit XML) and for running a regression pack from a
/// terminal. Lives here rather than in the CLI project so it can be tested without a Kafka client: the
/// real gateway is injected (like <c>AppState.RealGatewayFactory</c> in the app).
/// Exit codes: <see cref="ExitPassed"/>, <see cref="ExitFailed"/>, <see cref="ExitUsage"/>, <see cref="ExitNoTests"/>.
/// </summary>
public sealed partial class TestCommand
{
    public const int ExitPassed = 0, ExitFailed = 1, ExitUsage = 2, ExitNoTests = 3;

    /// <summary>How long a cluster gets to answer the reachability check before the run is abandoned.</summary>
    public static TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public const string Usage = """
        Usage: kafkastudio test <file-or-folder>... [options]

        Runs the Scenarios in .kafscript files (folders are searched recursively) and reports the results.

        Connections (the names scripts use in 'use connection "name"'):
          -c, --connection NAME=SERVERS   a Kafka cluster, e.g. -c local=localhost:9092 (repeatable)
              --connections FILE          JSON list of connection profiles (same fields as the app;
                                          ${ENV_VAR} is replaced, so secrets can come from CI variables)
              --demo NAME                 an in-memory demo cluster (repeatable)

        Selection:
          -t, --tags EXPR                 tag filter, e.g. "@smoke and not @wip"
          -n, --name TEXT                 only tests whose name contains TEXT
              --include-tasks             also run Task blocks once
              --list                      list the selected tests and exit

        Running:
          -v, --var NAME=VALUE            starting variable for every test (repeatable)
              --retries N                 re-run a failed test up to N more times (flaky detection)
              --fail-fast                 stop at the first failure
              --timeout DURATION          per-test time limit, e.g. 90s, 5m, 1h

        Reports:
              --junit FILE                JUnit XML (Jenkins, GitLab, GitHub, Azure DevOps...)
              --html FILE                 self-contained HTML report
              --markdown FILE             Markdown summary with failure details
          -q, --quiet                     only print failures and the summary

        Exit codes: 0 all passed, 1 failures or errors, 2 bad arguments/config or a cluster that
        can't be reached, 3 no tests selected.
        """;

    private readonly TextWriter _out;
    private readonly TextWriter _err;
    private readonly Func<ConnectionProfile, IKafkaGateway> _realGatewayFactory;

    public TestCommand(TextWriter output, TextWriter error, Func<ConnectionProfile, IKafkaGateway> realGatewayFactory)
    {
        _out = output;
        _err = error;
        _realGatewayFactory = realGatewayFactory;
    }

    /// <summary>Parsed command line.</summary>
    public sealed record Arguments
    {
        public List<string> Paths { get; } = new();
        public Dictionary<string, string> Connections { get; } = new(StringComparer.Ordinal);
        public List<string> ConnectionFiles { get; } = new();
        public List<string> DemoConnections { get; } = new();
        public Dictionary<string, string> Variables { get; } = new(StringComparer.Ordinal);
        public string? Tags { get; set; }
        public string? Name { get; set; }
        public int Retries { get; set; }
        public bool FailFast { get; set; }
        public TimeSpan? Timeout { get; set; }
        public bool IncludeTasks { get; set; }
        public bool ListOnly { get; set; }
        public bool Quiet { get; set; }
        public string? JUnitPath { get; set; }
        public string? HtmlPath { get; set; }
        public string? MarkdownPath { get; set; }
        public bool Help { get; set; }
    }

    /// <summary>Parses the arguments after "test". Throws <see cref="FormatException"/> for bad input.</summary>
    public static Arguments Parse(IReadOnlyList<string> args)
    {
        var a = new Arguments();
        for (var i = 0; i < args.Count; i++)
        {
            var arg = args[i];
            string Value()
            {
                if (i + 1 >= args.Count || (args[i + 1].StartsWith('-') && args[i + 1].Length > 1))
                {
                    throw new FormatException($"{arg} needs a value");
                }
                return args[++i];
            }
            (string, string) Pair()
            {
                var value = Value();
                var eq = value.IndexOf('=');
                if (eq <= 0) throw new FormatException($"{arg} expects NAME=VALUE, got '{value}'");
                return (value[..eq].Trim(), value[(eq + 1)..]);
            }

            switch (arg)
            {
                case "-h" or "--help" or "/?":
                    a.Help = true;
                    break;
                case "-c" or "--connection":
                    var (connection, servers) = Pair();
                    if (servers.Trim().Length == 0) throw new FormatException($"connection '{connection}' has no bootstrap servers");
                    a.Connections[connection] = servers.Trim();
                    break;
                case "--connections":
                    a.ConnectionFiles.Add(Value());
                    break;
                case "--demo":
                    a.DemoConnections.Add(Value());
                    break;
                case "-t" or "--tags":
                    a.Tags = Value();
                    break;
                case "-n" or "--name":
                    a.Name = Value();
                    break;
                case "-v" or "--var":
                    var (name, value) = Pair();
                    a.Variables[name] = value;
                    break;
                case "--retries":
                    var retries = Value();
                    if (!int.TryParse(retries, NumberStyles.None, CultureInfo.InvariantCulture, out var r) || r > 10)
                    {
                        throw new FormatException($"--retries must be a whole number from 0 to 10, got '{retries}'");
                    }
                    a.Retries = r;
                    break;
                case "--fail-fast":
                    a.FailFast = true;
                    break;
                case "--timeout":
                    var timeout = Value();
                    a.Timeout = ParseDuration(timeout) ?? throw new FormatException($"--timeout expects a duration like 90s, 5m or 1h, got '{timeout}'");
                    break;
                case "--include-tasks":
                    a.IncludeTasks = true;
                    break;
                case "--list":
                    a.ListOnly = true;
                    break;
                case "-q" or "--quiet":
                    a.Quiet = true;
                    break;
                case "--junit":
                    a.JUnitPath = Value();
                    break;
                case "--html":
                    a.HtmlPath = Value();
                    break;
                case "--markdown" or "--md":
                    a.MarkdownPath = Value();
                    break;
                default:
                    if (arg.StartsWith('-') && arg.Length > 1) throw new FormatException($"unknown option '{arg}' (see --help)");
                    a.Paths.Add(arg);
                    break;
            }
        }
        if (!a.Help && a.Paths.Count == 0) throw new FormatException("give at least one .kafscript file or folder to run");
        return a;
    }

    [GeneratedRegex(@"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h)\s*$", RegexOptions.IgnoreCase)]
    private static partial Regex DurationPattern();

    private static TimeSpan? ParseDuration(string text)
    {
        var m = DurationPattern().Match(text);
        if (!m.Success) return null;
        var value = double.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture);
        var result = m.Groups[2].Value.ToLowerInvariant() switch
        {
            "ms" => TimeSpan.FromMilliseconds(value),
            "s" => TimeSpan.FromSeconds(value),
            "m" => TimeSpan.FromMinutes(value),
            _ => TimeSpan.FromHours(value)
        };
        return result > TimeSpan.Zero ? result : null;
    }

    public async Task<int> RunAsync(IReadOnlyList<string> args, CancellationToken cancellationToken = default)
    {
        Arguments a;
        TagExpression tags;
        try
        {
            a = Parse(args);
            if (a.Help)
            {
                await _out.WriteLineAsync(Usage).ConfigureAwait(false);
                return ExitPassed;
            }
            tags = TagExpression.Parse(a.Tags);
        }
        catch (FormatException ex)
        {
            await _err.WriteLineAsync($"error: {ex.Message}").ConfigureAwait(false);
            await _err.WriteLineAsync("Run 'kafkastudio test --help' for usage.").ConfigureAwait(false);
            return ExitUsage;
        }

        var options = new TestRunOptions
        {
            Tags = tags,
            NameFilter = a.Name,
            Retries = a.Retries,
            FailFast = a.FailFast,
            Timeout = a.Timeout,
            Variables = a.Variables,
            IncludeTasks = a.IncludeTasks
        };
        var selected = TestDiscovery.Select(TestDiscovery.Discover(a.Paths), options);

        if (a.ListOnly)
        {
            foreach (var testCase in selected)
            {
                var tagText = testCase.Tags.Count == 0 ? "" : "  " + string.Join(" ", testCase.Tags.Select(t => "@" + t));
                var problem = testCase.LoadError is null ? "" : $"  [load error: {testCase.LoadError}]";
                await _out.WriteLineAsync($"{testCase.FileName}:{testCase.Line}  {testCase.Name}{tagText}{problem}").ConfigureAwait(false);
            }
            await _out.WriteLineAsync($"{selected.Count} test(s) selected").ConfigureAwait(false);
            return selected.Count == 0 ? ExitNoTests : ExitPassed;
        }

        if (selected.Count == 0)
        {
            await _err.WriteLineAsync("no tests matched (check the paths, --tags and --name)").ConfigureAwait(false);
            return ExitNoTests;
        }

        Dictionary<string, IKafkaGateway> connections;
        try
        {
            connections = await OpenConnectionsAsync(a, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await _err.WriteLineAsync($"error: {ex.Message}").ConfigureAwait(false);
            return ExitUsage;
        }

        try
        {
            var environment = new Dictionary<string, string>
            {
                ["connections"] = connections.Count == 0 ? "(none)" : string.Join(", ", connections.Keys.OrderBy(k => k, StringComparer.Ordinal)),
                ["machine"] = Environment.MachineName
            };
            if (a.Variables.Count > 0) environment["variables"] = string.Join(", ", a.Variables.Keys.OrderBy(k => k, StringComparer.Ordinal));

            var suite = new TestSuiteRunner(connections);
            suite.CaseCompleted += result => PrintResult(result, a.Quiet);
            if (!a.Quiet) await _out.WriteLineAsync($"Running {selected.Count} test(s)…").ConfigureAwait(false);

            var report = await suite.RunAsync(selected, options, cancellationToken, environment: environment).ConfigureAwait(false);

            await _out.WriteLineAsync().ConfigureAwait(false);
            await _out.WriteLineAsync((report.Success ? "PASSED  " : report.WasCancelled ? "STOPPED  " : "FAILED  ") + report.Summary).ConfigureAwait(false);

            var writeFailed = false;
            writeFailed |= !await TryWriteAsync(a.JUnitPath, () => JUnitReportWriter.Write(report), "JUnit report").ConfigureAwait(false);
            writeFailed |= !await TryWriteAsync(a.HtmlPath, () => HtmlReportWriter.Write(report), "HTML report").ConfigureAwait(false);
            writeFailed |= !await TryWriteAsync(a.MarkdownPath, () => MarkdownReportWriter.Write(report), "Markdown report").ConfigureAwait(false);

            return report.Success && !writeFailed ? ExitPassed : ExitFailed;
        }
        finally
        {
            foreach (var gateway in connections.Values)
            {
                try { await gateway.DisposeAsync().ConfigureAwait(false); }
                catch { /* best effort */ }
            }
        }
    }

    private void PrintResult(TestCaseResult result, bool quiet)
    {
        var problem = result.Outcome is TestOutcome.Failed or TestOutcome.Error;
        if (quiet && !problem) return;
        var label = result.Outcome switch
        {
            TestOutcome.Passed => result.IsFlaky ? "FLAKY" : "PASS ",
            TestOutcome.Failed => "FAIL ",
            TestOutcome.Error => "ERROR",
            TestOutcome.Cancelled => "STOP ",
            _ => "SKIP "
        };
        var where = $"{result.Case.FileName}:{result.Case.FailedLineOr(result.FailedLine)}";
        lock (_out)
        {
            _out.WriteLine($"  {label} {result.Case.Name}  ({TestRunReport.FormatDuration(result.Duration)}, {where})");
            if (problem && result.Message is not null) _out.WriteLine($"         {result.Message}");
            if (result.IsFlaky) _out.WriteLine($"         passed on attempt {result.Attempts}; earlier: {result.EarlierFailures.LastOrDefault()}");
        }
    }

    private async Task<bool> TryWriteAsync(string? path, Func<string> content, string what)
    {
        if (path is null) return true;
        try
        {
            var full = Path.GetFullPath(path);
            if (Path.GetDirectoryName(full) is { } dir) Directory.CreateDirectory(dir);
            await File.WriteAllTextAsync(full, content()).ConfigureAwait(false);
            await _out.WriteLineAsync($"{what}: {full}").ConfigureAwait(false);
            return true;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            await _err.WriteLineAsync($"error: can't write {what} to '{path}': {ex.Message}").ConfigureAwait(false);
            return false;
        }
    }

    private async Task<Dictionary<string, IKafkaGateway>> OpenConnectionsAsync(Arguments a, CancellationToken ct)
    {
        var profiles = new List<ConnectionProfile>();
        foreach (var file in a.ConnectionFiles) profiles.AddRange(LoadConnectionsFile(file));
        profiles.AddRange(a.Connections.Select(c => new ConnectionProfile { Name = c.Key, BootstrapServers = c.Value }));

        var connections = new Dictionary<string, IKafkaGateway>(StringComparer.Ordinal);
        try
        {
            var broker = a.DemoConnections.Count > 0 ? new InMemoryKafkaBroker() : null;
            foreach (var name in a.DemoConnections)
            {
                var profile = new ConnectionProfile { Name = name, BootstrapServers = ConnectionProfile.DemoBootstrapServers, IsDemo = true };
                connections[name] = new InMemoryKafkaGateway(profile, broker!);
            }
            foreach (var profile in profiles)
            {
                if (connections.ContainsKey(profile.Name)) throw new InvalidOperationException($"connection '{profile.Name}' is defined twice");
                var gateway = profile.IsDemoConnection
                    ? new InMemoryKafkaGateway(profile, broker ??= new InMemoryKafkaBroker())
                    : _realGatewayFactory(profile);
                connections[profile.Name] = gateway;
                try
                {
                    await gateway.ConnectAsync(ct).ConfigureAwait(false);
                    // Creating a client doesn't contact the broker: ask for the topic list, so an
                    // unreachable cluster fails the run up front instead of hanging inside the first test.
                    var probe = gateway.ListTopicsAsync(ct);
                    if (await Task.WhenAny(probe, Task.Delay(ConnectTimeout, ct)).ConfigureAwait(false) != probe)
                    {
                        throw new TimeoutException($"no answer from the cluster within {ConnectTimeout.TotalSeconds:0}s");
                    }
                    await probe.ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    throw new InvalidOperationException($"can't connect '{profile.Name}' ({profile.BootstrapServers}): {ex.Message}", ex);
                }
            }
            return connections;
        }
        catch
        {
            foreach (var gateway in connections.Values)
            {
                try { await gateway.DisposeAsync().ConfigureAwait(false); }
                catch { /* best effort */ }
            }
            throw;
        }
    }

    private static readonly JsonSerializerOptions ProfileJson = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
        Converters = { new JsonStringEnumConverter() }
    };

    [GeneratedRegex(@"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")]
    private static partial Regex EnvironmentReference();

    /// <summary>
    /// Reads a JSON array of <see cref="ConnectionProfile"/>s. <c>${NAME}</c> is replaced with the
    /// environment variable's value (JSON-escaped), so passwords can come from CI secrets instead of the
    /// file; an unset variable is an error rather than an empty password.
    /// </summary>
    public static IReadOnlyList<ConnectionProfile> LoadConnectionsFile(string path)
    {
        string text;
        try
        {
            text = File.ReadAllText(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new InvalidOperationException($"can't read connections file '{path}': {ex.Message}", ex);
        }

        text = EnvironmentReference().Replace(text, m =>
        {
            var value = Environment.GetEnvironmentVariable(m.Groups[1].Value)
                ?? throw new InvalidOperationException($"connections file '{path}' uses ${{{m.Groups[1].Value}}}, but that environment variable isn't set");
            return JsonEncodedText.Encode(value).ToString();
        });

        try
        {
            var trimmed = text.TrimStart();
            var profiles = trimmed.StartsWith('{')
                ? new List<ConnectionProfile> { JsonSerializer.Deserialize<ConnectionProfile>(text, ProfileJson)! }
                : JsonSerializer.Deserialize<List<ConnectionProfile>>(text, ProfileJson) ?? new List<ConnectionProfile>();
            foreach (var profile in profiles)
            {
                if (string.IsNullOrWhiteSpace(profile.Name) || string.IsNullOrWhiteSpace(profile.BootstrapServers))
                {
                    throw new InvalidOperationException($"connections file '{path}': every connection needs a name and bootstrapServers");
                }
            }
            return profiles;
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"connections file '{path}' is not valid: {ex.Message}", ex);
        }
    }
}

internal static class TestCaseExtensions
{
    /// <summary>The failing step's line when there is one, else the test's own line.</summary>
    public static int FailedLineOr(this TestCase testCase, int? failedLine) => failedLine ?? testCase.Line;
}

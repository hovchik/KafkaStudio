using KafkaStudio.Automation.Testing;
using KafkaStudio.Kafka;

// kafkastudio test <paths> [options] - see TestCommand.Usage. All the logic lives in
// KafkaStudio.Automation.Testing.TestCommand; this entry point only supplies the real Kafka client and
// Ctrl+C handling.

if (args.Length == 0 || args[0] is "-h" or "--help" or "help")
{
    Console.WriteLine(TestCommand.Usage);
    return args.Length == 0 ? TestCommand.ExitUsage : TestCommand.ExitPassed;
}

if (args[0] != "test")
{
    Console.Error.WriteLine($"error: unknown command '{args[0]}' - the only command is 'test' (see --help)");
    return TestCommand.ExitUsage;
}

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    // First Ctrl+C stops the run gracefully (the current test is reported as stopped and reports are
    // still written); a second one kills the process.
    if (cts.IsCancellationRequested) return;
    e.Cancel = true;
    Console.Error.WriteLine("stopping after the current step… (press Ctrl+C again to abort)");
    cts.Cancel();
};

var command = new TestCommand(Console.Out, Console.Error, profile => new ConfluentKafkaGateway(profile));
return await command.RunAsync(args[1..], cts.Token);

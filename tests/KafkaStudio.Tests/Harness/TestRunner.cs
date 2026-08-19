namespace KafkaStudio.Tests.Harness;

public sealed class TestRunner
{
    private readonly List<(string Group, string Name, Func<Task> Action)> _tests = new();

    public void Add(string group, string name, Func<Task> action) => _tests.Add((group, name, action));

    public void Add(string group, string name, Action action) => _tests.Add((group, name, () =>
    {
        action();
        return Task.CompletedTask;
    }));

    public async Task<int> RunAllAsync()
    {
        var passed = 0;
        var failed = 0;
        string? currentGroup = null;

        foreach (var (group, name, action) in _tests)
        {
            if (group != currentGroup)
            {
                currentGroup = group;
                Console.WriteLine();
                Console.WriteLine(group);
                Console.WriteLine(new string('-', group.Length));
            }

            var sw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                await action().ConfigureAwait(false);
                Console.WriteLine($"  [PASS] {name} ({sw.ElapsedMilliseconds} ms)");
                passed++;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [FAIL] {name} ({sw.ElapsedMilliseconds} ms)");
                Console.WriteLine($"         {ex.GetType().Name}: {ex.Message}");
                failed++;
            }
        }

        Console.WriteLine();
        Console.WriteLine($"Total: {passed + failed}, Passed: {passed}, Failed: {failed}");
        return failed == 0 ? 0 : 1;
    }
}

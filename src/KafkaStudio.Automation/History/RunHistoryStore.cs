using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.Automation.History;

public sealed record RunHistoryEntry(string JobId, DateTimeOffset RunAt, ScriptRunResult Result);

/// <summary>Bounded in-memory history of scenario/task runs, newest first, for the UI's run log and for
/// simple pass/fail trend reporting. Thread-safe; capped so a long-lived app doesn't grow unbounded.</summary>
public sealed class RunHistoryStore
{
    private readonly int _capacity;
    private readonly object _gate = new();
    private readonly LinkedList<RunHistoryEntry> _entries = new();

    public RunHistoryStore(int capacity = 500)
    {
        _capacity = capacity;
    }

    public void Add(string jobId, DateTimeOffset runAt, ScriptRunResult result)
    {
        lock (_gate)
        {
            _entries.AddFirst(new RunHistoryEntry(jobId, runAt, result));
            while (_entries.Count > _capacity) _entries.RemoveLast();
        }
    }

    public IReadOnlyList<RunHistoryEntry> Recent(int count = 100)
    {
        lock (_gate)
        {
            return _entries.Take(count).ToList();
        }
    }

    public IReadOnlyList<RunHistoryEntry> ForJob(string jobId, int count = 100)
    {
        lock (_gate)
        {
            return _entries.Where(e => e.JobId == jobId).Take(count).ToList();
        }
    }

    public (int passed, int failed) Totals()
    {
        lock (_gate)
        {
            return (_entries.Count(e => e.Result.Success), _entries.Count(e => !e.Result.Success));
        }
    }
}

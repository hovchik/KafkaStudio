using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Search;

public enum DiffKind { Unchanged, Changed, Added, Removed }

/// <summary>One field in a structural diff. <see cref="Left"/>/<see cref="Right"/> are null when the field
/// is absent on that side (use <see cref="LeftText"/>/<see cref="RightText"/> for display).</summary>
public sealed record DiffEntry(string Path, DiffKind Kind, string? Left, string? Right, bool IsVolatile)
{
    public string LeftText => Kind == DiffKind.Added ? "—" : Left ?? "null";
    public string RightText => Kind == DiffKind.Removed ? "—" : Right ?? "null";

    public string Symbol => Kind switch
    {
        DiffKind.Changed => "≠",
        DiffKind.Added => "+",
        DiffKind.Removed => "−",
        _ => "="
    };
}

public sealed record DiffOptions
{
    /// <summary>Hide fields that are expected to differ (timestamps, generated ids, trace ids).</summary>
    public bool IgnoreVolatile { get; init; }

    /// <summary>Paths (or path prefixes) to leave out, e.g. <c>$.meta</c>, <c>headers.trace-id</c>.</summary>
    public IReadOnlyList<string> IgnorePaths { get; init; } = Array.Empty<string>();

    public bool IncludeUnchanged { get; init; }
}

public sealed record DiffResult(IReadOnlyList<DiffEntry> Entries, int Changed, int Added, int Removed, int Unchanged, int IgnoredVolatile)
{
    public bool IsIdentical => Changed + Added + Removed == 0;

    public string Summary => IsIdentical
        ? $"No differences{(IgnoredVolatile > 0 ? $" (ignoring {IgnoredVolatile} volatile field(s))" : "")} - {Unchanged} field(s) equal."
        : $"{Changed} changed, {Added} added, {Removed} removed, {Unchanged} equal" +
          (IgnoredVolatile > 0 ? $" · {IgnoredVolatile} volatile field(s) ignored" : "");
}

/// <summary>
/// Field-by-field comparison of two messages: key, headers (<c>headers.name</c>) and every JSON leaf of
/// the value (or the whole value when it isn't JSON). "Added" means present only on the right.
/// </summary>
public static class JsonDiff
{
    public static DiffResult Compare(KafkaMessage left, KafkaMessage right, DiffOptions? options = null)
    {
        options ??= new DiffOptions();
        var l = Fields(left);
        var r = Fields(right);

        var paths = l.Keys.Concat(r.Keys.Where(k => !l.ContainsKey(k))).ToList();
        var entries = new List<DiffEntry>();
        int changed = 0, added = 0, removed = 0, unchanged = 0, ignoredVolatile = 0;

        foreach (var path in paths)
        {
            if (IsIgnored(path, options.IgnorePaths)) continue;
            var inLeft = l.TryGetValue(path, out var lv);
            var inRight = r.TryGetValue(path, out var rv);
            var kind = !inLeft ? DiffKind.Added
                : !inRight ? DiffKind.Removed
                : string.Equals(lv, rv, StringComparison.Ordinal) ? DiffKind.Unchanged
                : DiffKind.Changed;

            var isVolatile = path != "key" && (VolatileFields.IsVolatileName(path) ||
                              (VolatileFields.IsVolatileValue(lv) && VolatileFields.IsVolatileValue(rv)) ||
                              (kind != DiffKind.Changed && VolatileFields.IsVolatileValue(lv ?? rv)));
            if (options.IgnoreVolatile && isVolatile && kind != DiffKind.Unchanged)
            {
                ignoredVolatile++;
                continue;
            }

            switch (kind)
            {
                case DiffKind.Changed: changed++; break;
                case DiffKind.Added: added++; break;
                case DiffKind.Removed: removed++; break;
                default: unchanged++; break;
            }
            if (kind != DiffKind.Unchanged || options.IncludeUnchanged)
            {
                entries.Add(new DiffEntry(path, kind, lv, rv, isVolatile));
            }
        }

        return new DiffResult(entries, changed, added, removed, unchanged, ignoredVolatile);
    }

    private static bool IsIgnored(string path, IReadOnlyList<string> ignore)
    {
        foreach (var raw in ignore)
        {
            var prefix = raw.Trim();
            if (prefix.Length == 0) continue;
            if (!prefix.StartsWith('$') && !prefix.StartsWith("headers", StringComparison.Ordinal) && prefix != "key" && prefix != "value")
            {
                prefix = "$." + prefix.TrimStart('.');
            }
            if (path == prefix || path.StartsWith(prefix + ".", StringComparison.Ordinal) || path.StartsWith(prefix + "[", StringComparison.Ordinal))
            {
                return true;
            }
        }
        return false;
    }

    /// <summary>Ordered path → value map (key, headers, then value leaves).</summary>
    private static Dictionary<string, string?> Fields(KafkaMessage message)
    {
        var fields = new Dictionary<string, string?>(StringComparer.Ordinal);
        if (message.Key is not null) fields["key"] = message.Key;
        foreach (var (name, value) in message.Headers.OrderBy(h => h.Key, StringComparer.Ordinal))
        {
            fields[JsonFlattener.Child("headers", name)] = value;
        }
        if (JsonFlattener.TryFlatten(message.Value, out var leaves))
        {
            foreach (var leaf in leaves) fields[leaf.Path] = leaf.Value;
        }
        else
        {
            fields["value"] = message.IsTombstone ? null : message.Value ?? $"<binary, {message.RawValue!.Length} bytes>";
        }
        return fields;
    }
}

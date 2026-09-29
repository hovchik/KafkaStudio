using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Search;

/// <summary>
/// Finds which of a (possibly large) set of ids a message carries, in O(fields + tokens) per message
/// instead of O(ids): ids go into a hash set, and the message's candidate values are looked up in it.
/// With a specific <see cref="FieldSelector"/> the field's value must equal an id; with
/// <see cref="FieldSelector.Any"/> the key, every header value, the whole value, and every id-like
/// token inside the value (see <see cref="TextSimilarity.Tokenize"/>) are checked - so an id embedded
/// anywhere in a JSON payload is found without knowing its path.
/// </summary>
public sealed class IdMatcher
{
    private readonly Dictionary<string, string> _ids;
    private readonly FieldSelector _field;

    public IdMatcher(IEnumerable<string> ids, FieldSelector field, bool caseSensitive = true)
    {
        _field = field;
        _ids = new Dictionary<string, string>(caseSensitive ? StringComparer.Ordinal : StringComparer.OrdinalIgnoreCase);
        foreach (var id in ids)
        {
            var trimmed = id.Trim();
            if (trimmed.Length > 0) _ids.TryAdd(trimmed, trimmed);
        }
    }

    public int Count => _ids.Count;
    public FieldSelector Field => _field;

    /// <summary>The ids (as originally given) carried by <paramref name="message"/>, each at most once.</summary>
    public IReadOnlyCollection<string> Match(KafkaMessage message)
    {
        HashSet<string>? found = null;
        void Check(string? candidate)
        {
            if (candidate is null) return;
            var trimmed = candidate.Trim();
            if (trimmed.Length > 0 && _ids.TryGetValue(trimmed, out var id)) (found ??= new HashSet<string>(StringComparer.Ordinal)).Add(id);
        }

        if (_field.Kind != FieldKind.Any)
        {
            Check(_field.Read(message));
        }
        else
        {
            Check(message.Key);
            foreach (var header in message.Headers.Values) Check(header);
            if (message.Value is { } value)
            {
                Check(value);
                if (value.Length <= JsonFlattener.MaxDocumentLength)
                {
                    foreach (var token in TextSimilarity.Tokenize(value)) Check(token);
                }
            }
        }

        return (IReadOnlyCollection<string>?)found ?? Array.Empty<string>();
    }

    /// <summary>
    /// Parses a pasted or imported list of ids: one per line, or separated by commas, semicolons or tabs
    /// (so a CSV works too). Surrounding quotes are stripped and duplicates removed (first occurrence
    /// wins). <paramref name="column"/> (1-based) picks a single CSV column; <paramref name="skipHeader"/>
    /// drops the first line.
    /// </summary>
    public static IReadOnlyList<string> ParseIdList(string text, int? column = null, bool skipHeader = false)
    {
        var result = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var lines = (text ?? "").Replace("\r\n", "\n").Split('\n');
        for (var i = skipHeader ? 1 : 0; i < lines.Length; i++)
        {
            var cells = lines[i].Split(new[] { ',', ';', '\t' });
            IEnumerable<string> picked = column is { } c
                ? (c >= 1 && c <= cells.Length ? new[] { cells[c - 1] } : Array.Empty<string>())
                : cells;
            foreach (var cell in picked)
            {
                var id = cell.Trim().Trim('"', '\'').Trim();
                if (id.Length > 0 && seen.Add(id)) result.Add(id);
            }
        }
        return result;
    }
}

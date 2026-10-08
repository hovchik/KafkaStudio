using System.Text;
using System.Text.RegularExpressions;

namespace KafkaStudio.Search;

/// <summary>
/// Cheap string-similarity helpers used for fuzzy ("similar to") matching and near-duplicate scoring.
/// Everything here is bounded so it stays fast when run against every message of a large topic.
/// </summary>
public static class TextSimilarity
{
    /// <summary>Default ratio at or above which two values count as "similar".</summary>
    public const double DefaultThreshold = 0.8;

    /// <summary>Strings longer than this are compared by token overlap instead of edit distance.</summary>
    public const int MaxEditDistanceLength = 128;

    internal static readonly char[] TokenDelimiters =
    {
        ' ', '\t', '\r', '\n', '"', '\'', ',', '{', '}', '[', ']', '(', ')', ':', ';', '=', '&', '?', '/', '\\', '<', '>', '|'
    };

    /// <summary>Lower-cases, drops whitespace, dashes and underscores, and strips leading zeros from
    /// numbers, so "ORD-0042 ", "ord_42" and "ORD 42" all normalize to the same thing.</summary>
    public static string Normalize(string s)
    {
        var sb = new StringBuilder(s.Length);
        var previousWasDigit = false;
        for (var i = 0; i < s.Length; i++)
        {
            var c = s[i];
            if (char.IsWhiteSpace(c) || c is '-' or '_')
            {
                // A separator starts a new number, so "10-05" keeps the zero rule per part (→ "105").
                previousWasDigit = false;
                continue;
            }
            var isDigit = char.IsAsciiDigit(c);
            // A leading zero of a number (not the number "0" itself) carries no identity.
            if (c == '0' && !previousWasDigit && i + 1 < s.Length && char.IsAsciiDigit(s[i + 1])) continue;
            previousWasDigit = isDigit;
            sb.Append(char.ToLowerInvariant(c));
        }
        return sb.ToString();
    }

    /// <summary>Splits text into id-like tokens: JSON/URL punctuation and whitespace separate tokens, while
    /// dashes, dots and underscores stay inside them (so UUIDs and "ORD-42" survive intact).</summary>
    public static IEnumerable<string> Tokenize(string text) =>
        text.Split(TokenDelimiters, StringSplitOptions.RemoveEmptyEntries);

    /// <summary>Levenshtein distance with an early exit once it exceeds <paramref name="max"/>
    /// (returns max + 1 in that case).</summary>
    public static int EditDistance(string a, string b, int max = int.MaxValue)
    {
        if (Math.Abs(a.Length - b.Length) > max) return max + 1;
        if (a.Length == 0) return b.Length;
        if (b.Length == 0) return a.Length;

        var previous = new int[b.Length + 1];
        var current = new int[b.Length + 1];
        for (var j = 0; j <= b.Length; j++) previous[j] = j;

        for (var i = 1; i <= a.Length; i++)
        {
            current[0] = i;
            var rowMin = current[0];
            for (var j = 1; j <= b.Length; j++)
            {
                var cost = a[i - 1] == b[j - 1] ? 0 : 1;
                current[j] = Math.Min(Math.Min(current[j - 1] + 1, previous[j] + 1), previous[j - 1] + cost);
                rowMin = Math.Min(rowMin, current[j]);
            }
            if (rowMin > max) return max + 1;
            (previous, current) = (current, previous);
        }
        return previous[b.Length];
    }

    /// <summary>0..1 similarity of two strings: 1 when they normalize to the same text, otherwise
    /// 1 - editDistance / length for short strings, and token (Jaccard) overlap for long ones.</summary>
    public static double Ratio(string? a, string? b)
    {
        if (a is null || b is null) return a == b ? 1 : 0;
        if (string.Equals(a, b, StringComparison.Ordinal)) return 1;
        var na = Normalize(a);
        var nb = Normalize(b);
        if (na == nb) return 1;
        if (na.Length == 0 || nb.Length == 0) return 0;

        if (na.Length <= MaxEditDistanceLength && nb.Length <= MaxEditDistanceLength)
        {
            var longest = Math.Max(na.Length, nb.Length);
            return 1.0 - (double)EditDistance(na, nb) / longest;
        }
        return TokenJaccard(a, b);
    }

    public static bool IsSimilar(string? a, string? b, double threshold = DefaultThreshold) => Ratio(a, b) >= threshold;

    /// <summary>Jaccard overlap of the two texts' lower-cased token sets.</summary>
    public static double TokenJaccard(string a, string b)
    {
        var ta = Tokenize(a).Select(t => t.ToLowerInvariant()).ToHashSet();
        var tb = Tokenize(b).Select(t => t.ToLowerInvariant()).ToHashSet();
        if (ta.Count == 0 && tb.Count == 0) return 1;
        var intersection = ta.Count(tb.Contains);
        var union = ta.Count + tb.Count - intersection;
        return union == 0 ? 0 : (double)intersection / union;
    }
}

/// <summary>
/// Recognizes fields whose values are expected to differ between otherwise-identical messages
/// (timestamps, generated ids, trace ids) so diffs and similarity scores can ignore them.
/// </summary>
public static class VolatileFields
{
    private static readonly Regex VolatileName = new(
        @"(^ts$|_at$|(?-i:[a-z]At$)|uuid|guid|trace[-_]?id|span[-_]?id|correlation[-_]?id|request[-_]?id|message[-_]?id|event[-_]?id|idempotency[-_]?key|nonce)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// <summary>Splits a field name into words at '_', '-', spaces and camelCase boundaries, so "time"/"date"
    /// are matched as whole words: "event_time" and "orderDate" are volatile, "timeout" and "update" are not.</summary>
    private static readonly Regex NameWordBoundary = new(
        @"[_\-\s]+|(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])",
        RegexOptions.CultureInvariant);

    private static readonly HashSet<string> TimeWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "time", "date", "timestamp", "datetime"
    };

    /// <summary>Names that identify something (orderId, accountNumber, sku code...) - their values may look
    /// generated (UUIDs, long digit runs) but are the identity of the message, not noise.</summary>
    private static readonly Regex IdLikeName = new(@"(id|number|no|code|key)$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex Uuid = new(
        @"^[{(]?[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}[)}]?$",
        RegexOptions.CultureInvariant);

    private static readonly Regex IsoDate = new(
        @"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?)?$",
        RegexOptions.CultureInvariant);

    private static readonly Regex EpochNumber = new(@"^1\d{9}(\d{3})?(\d{3})?$", RegexOptions.CultureInvariant);

    /// <summary>True when the last segment of <paramref name="path"/> has a volatile-sounding name
    /// (createdAt, timestamp, traceId...) or <paramref name="value"/> looks like a UUID, an ISO date or a
    /// Unix epoch (seconds/millis/micros).</summary>
    public static bool IsVolatile(string path, string? value) => IsVolatileName(path) || IsVolatileValue(value);

    /// <summary>Like <see cref="IsVolatile"/>, but a value-shape match alone doesn't count for an id-like
    /// field name (orderId, accountNumber...): used when grouping duplicates, where dropping the id would
    /// make two different orders look like the same one.</summary>
    public static bool IsVolatileForGrouping(string path, string? value) =>
        IsVolatileName(path) || (!IsIdLikeName(path) && IsVolatileValue(value));

    public static bool IsVolatileName(string path)
    {
        var name = LastSegment(path);
        if (name.Length == 0) return false;
        return VolatileName.IsMatch(name) || NameWordBoundary.Split(name).Any(TimeWords.Contains);
    }

    /// <summary>True when the last segment of <paramref name="path"/> ends in id/number/no/code/key.</summary>
    public static bool IsIdLikeName(string path)
    {
        var name = LastSegment(path);
        return name.Length > 0 && IdLikeName.IsMatch(name);
    }

    public static bool IsVolatileValue(string? value)
    {
        if (string.IsNullOrEmpty(value) || value.Length > 64) return false;
        return Uuid.IsMatch(value) || IsoDate.IsMatch(value) || EpochNumber.IsMatch(value);
    }

    private static string LastSegment(string path)
    {
        var p = path.TrimEnd(']');
        var cut = Math.Max(p.LastIndexOf('.'), p.LastIndexOf('['));
        var name = cut >= 0 ? p[(cut + 1)..] : p;
        return name.Trim('\'', '"', '*');
    }
}

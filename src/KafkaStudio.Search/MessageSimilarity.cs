using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Search;

public enum SimilarityMode
{
    /// <summary>Same fields with the same (or nearly the same) values - near-duplicates, retries, typo'd ids.</summary>
    Content,
    /// <summary>Same JSON structure (set of field paths), values ignored - schema variants.</summary>
    Shape
}

/// <summary>A message prepared for repeated similarity comparisons (flattened once).</summary>
public sealed class MessageFingerprint
{
    public KafkaMessage Message { get; }
    public bool IsJson { get; }

    /// <summary>Leaf path (array indexes kept) → value, including a "key" pseudo-field when the message has a key.</summary>
    public IReadOnlyDictionary<string, string?> Fields { get; }

    /// <summary>Distinct shape paths (<c>$.items[*].sku</c>).</summary>
    public IReadOnlySet<string> Shape { get; }

    public string Text { get; }

    public MessageFingerprint(KafkaMessage message, bool ignoreVolatile)
    {
        Message = message;
        Text = message.Value ?? "";
        var fields = new Dictionary<string, string?>(StringComparer.Ordinal);
        if (JsonFlattener.TryFlatten(message.Value, out var leaves))
        {
            IsJson = true;
            foreach (var leaf in leaves)
            {
                if (ignoreVolatile && VolatileFields.IsVolatile(leaf.Path, leaf.Value)) continue;
                fields[leaf.Path] = leaf.Value;
            }
            Shape = JsonFlattener.Shape(leaves);
        }
        else
        {
            Shape = new HashSet<string>();
        }
        if (message.Key is not null && !(ignoreVolatile && VolatileFields.IsVolatileValue(message.Key)))
        {
            fields["key"] = message.Key;
        }
        Fields = fields;
    }
}

/// <summary>A message found by <see cref="SimilarityFinder"/>, with its 0..1 score.</summary>
public sealed record SimilarMatch(KafkaMessage Message, double Score, string Explanation)
{
    public string ScoreText => $"{Score:P0}";
}

/// <summary>Scores how similar two messages are - see <see cref="SimilarityMode"/>.</summary>
public static class MessageSimilarity
{
    /// <summary>Value pairs at least this similar earn partial credit in content scoring.</summary>
    public const double PartialCreditThreshold = 0.75;

    public static double Score(MessageFingerprint reference, MessageFingerprint candidate, SimilarityMode mode) =>
        mode == SimilarityMode.Shape ? ShapeScore(reference, candidate) : ContentScore(reference, candidate, out _);

    public static double ShapeScore(MessageFingerprint a, MessageFingerprint b)
    {
        if (!a.IsJson || !b.IsJson) return 0;
        if (a.Shape.Count == 0 && b.Shape.Count == 0) return 1;
        var intersection = a.Shape.Count(b.Shape.Contains);
        var union = a.Shape.Count + b.Shape.Count - intersection;
        return union == 0 ? 0 : (double)intersection / union;
    }

    /// <summary>
    /// JSON: every field in either message counts once; equal values score 1, near-equal values (typos,
    /// case, spacing) score their similarity, missing or different values score 0 - averaged over the union.
    /// Text: token overlap of the values (plus key equality).
    /// </summary>
    public static double ContentScore(MessageFingerprint a, MessageFingerprint b, out string explanation)
    {
        if (a.IsJson && b.IsJson)
        {
            var union = a.Fields.Keys.Union(b.Fields.Keys, StringComparer.Ordinal).ToList();
            if (union.Count == 0)
            {
                explanation = "both empty";
                return 1;
            }
            double total = 0;
            var equal = 0;
            var near = 0;
            var different = new List<string>();
            foreach (var path in union)
            {
                var inA = a.Fields.TryGetValue(path, out var va);
                var inB = b.Fields.TryGetValue(path, out var vb);
                if (!inA || !inB)
                {
                    if (different.Count < 3) different.Add(path + (inA ? " (missing)" : " (extra)"));
                    continue;
                }
                if (string.Equals(va, vb, StringComparison.Ordinal))
                {
                    total += 1;
                    equal++;
                    continue;
                }
                var ratio = TextSimilarity.Ratio(va, vb);
                if (ratio >= PartialCreditThreshold)
                {
                    total += ratio;
                    near++;
                }
                else if (different.Count < 3)
                {
                    different.Add(path);
                }
            }
            var diffText = different.Count == 0 ? "" : $"; differs: {string.Join(", ", different)}";
            explanation = $"{equal} equal, {near} near-equal of {union.Count} field(s){diffText}";
            return total / union.Count;
        }

        if (a.IsJson != b.IsJson)
        {
            explanation = "one is JSON, the other isn't";
            return a.Text.Length > 0 && b.Text.Length > 0 ? TextSimilarity.TokenJaccard(a.Text, b.Text) * 0.5 : 0;
        }

        var textScore = a.Text.Length <= TextSimilarity.MaxEditDistanceLength && b.Text.Length <= TextSimilarity.MaxEditDistanceLength
            ? TextSimilarity.Ratio(a.Text, b.Text)
            : TextSimilarity.TokenJaccard(a.Text, b.Text);
        var keyA = a.Message.Key;
        var keyB = b.Message.Key;
        if (keyA is not null || keyB is not null)
        {
            var keyScore = TextSimilarity.Ratio(keyA, keyB);
            explanation = $"text {textScore:P0}, key {keyScore:P0}";
            return textScore * 0.8 + keyScore * 0.2;
        }
        explanation = $"text {textScore:P0}";
        return textScore;
    }
}

public sealed record SimilarityRequest
{
    public required KafkaMessage Reference { get; init; }
    public required IReadOnlyCollection<string> Topics { get; init; }
    public SimilarityMode Mode { get; init; } = SimilarityMode.Content;
    public double Threshold { get; init; } = 0.8;
    public bool IgnoreVolatileFields { get; init; } = true;
    public SearchRange Range { get; init; } = SearchRange.All;
    public int MaxResults { get; init; } = 500;
}

public sealed record SimilarityResult(IReadOnlyList<SimilarMatch> Matches, ScanSummary Scan, bool Truncated)
{
    public string Summary =>
        $"{Matches.Count:N0} similar message(s){(Truncated ? $" (best {Matches.Count:N0} kept)" : "")} on " +
        $"{Matches.Select(m => m.Message.Topic).Distinct().Count()} topic(s) - scanned {Scan.MessagesScanned:N0} message(s)" +
        $"{(Scan.Cancelled ? " (cancelled - incomplete)" : "")}.{Scan.FailedSuffix}";
}

/// <summary>Finds messages similar to a reference message across topics, best matches first.</summary>
public static class SimilarityFinder
{
    public static async Task<SimilarityResult> RunAsync(
        IKafkaGateway gateway,
        SimilarityRequest request,
        CancellationToken cancellationToken = default,
        ScanProgress? progress = null,
        DateTimeOffset? now = null)
    {
        var reference = new MessageFingerprint(request.Reference, request.IgnoreVolatileFields);
        var gate = new object();
        var matches = new List<SimilarMatch>();
        var truncated = false;
        var refMessage = request.Reference;

        var scan = await TopicScanner.ScanAsync(gateway, request.Topics, request.Range, (_, message) =>
        {
            if (message.Topic == refMessage.Topic && message.Partition == refMessage.Partition && message.Offset == refMessage.Offset)
            {
                return ScanDecision.Continue; // the reference itself
            }

            var candidate = new MessageFingerprint(message, request.IgnoreVolatileFields);
            string explanation;
            double score;
            if (request.Mode == SimilarityMode.Shape)
            {
                score = MessageSimilarity.ShapeScore(reference, candidate);
                explanation = $"{candidate.Shape.Count} field(s), {reference.Shape.Count(candidate.Shape.Contains)} shared";
            }
            else
            {
                score = MessageSimilarity.ContentScore(reference, candidate, out explanation);
            }
            if (score < request.Threshold) return ScanDecision.Continue;

            lock (gate)
            {
                matches.Add(new SimilarMatch(message, score, explanation));
                // Keep only the best MaxResults (trim in batches to stay cheap).
                if (matches.Count > request.MaxResults * 2)
                {
                    matches.Sort((x, y) => y.Score.CompareTo(x.Score));
                    matches.RemoveRange(request.MaxResults, matches.Count - request.MaxResults);
                    truncated = true;
                }
            }
            return ScanDecision.Continue;
        }, cancellationToken, progress, now: now).ConfigureAwait(false);

        var ordered = matches
            .OrderByDescending(m => m.Score)
            .ThenBy(m => m.Message.Timestamp)
            .ToList();
        if (ordered.Count > request.MaxResults)
        {
            ordered.RemoveRange(request.MaxResults, ordered.Count - request.MaxResults);
            truncated = true;
        }
        return new SimilarityResult(ordered, scan, truncated);
    }
}

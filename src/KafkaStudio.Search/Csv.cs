using System.Globalization;
using System.Text;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Search;

/// <summary>Minimal RFC 4180 CSV writer (quotes fields containing separators, quotes or newlines).</summary>
public static class Csv
{
    public static string Write(IReadOnlyList<string> header, IEnumerable<IReadOnlyList<string>> rows)
    {
        var sb = new StringBuilder();
        AppendRow(sb, header);
        foreach (var row in rows) AppendRow(sb, row);
        return sb.ToString();
    }

    public static string Escape(string? field)
    {
        field ??= "";
        // Spreadsheets evaluate a cell starting with =, @, + or - as a formula; a leading apostrophe defuses
        // that. Plain numbers like -5 are left alone.
        if (IsFormulaLike(field)) return "\"'" + field.Replace("\"", "\"\"") + "\"";
        return field.IndexOfAny(new[] { ',', '"', '\n', '\r' }) >= 0 || field.StartsWith(' ') || field.EndsWith(' ')
            ? "\"" + field.Replace("\"", "\"\"") + "\""
            : field;
    }

    private static bool IsFormulaLike(string field) =>
        field.Length > 0 && (field[0] is '=' or '@' ||
                             (field[0] is '+' or '-' && !double.TryParse(field, NumberStyles.Float, CultureInfo.InvariantCulture, out _)));

    private static void AppendRow(StringBuilder sb, IReadOnlyList<string> row)
    {
        for (var i = 0; i < row.Count; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append(Escape(row[i]));
        }
        sb.Append("\r\n");
    }

    /// <summary>Messages as CSV: topic, partition, offset, timestamp, key, headers (name=value; ...), value.</summary>
    public static string FromMessages(IEnumerable<KafkaMessage> messages) => Write(
        new[] { "topic", "partition", "offset", "timestamp", "key", "headers", "value" },
        messages.Select(m => (IReadOnlyList<string>)new[]
        {
            m.Topic, m.Partition.ToString(), m.Offset.ToString(), m.Timestamp.ToString("O"), m.Key ?? "",
            string.Join("; ", m.Headers.Select(h => $"{h.Key}={h.Value}")),
            m.IsTombstone ? "" : m.Value ?? Convert.ToBase64String(m.RawValue!)
        }));
}

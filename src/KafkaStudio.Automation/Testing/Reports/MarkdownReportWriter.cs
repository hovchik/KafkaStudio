using System.Text;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.Automation.Testing.Reports;

/// <summary>
/// Markdown versions of a run: a summary for a pull request / wiki / chat message, and a single-test
/// bug report (steps, expected vs. actual, environment) ready to paste into Jira, Azure Boards or GitHub.
/// </summary>
public static class MarkdownReportWriter
{
    public static string Write(TestRunReport report)
    {
        var sb = new StringBuilder();
        var badge = report.Success ? "✅ PASSED" : report.WasCancelled ? "⏹ STOPPED" : "❌ FAILED";
        sb.AppendLine($"# {report.Name} - {badge}");
        sb.AppendLine();
        sb.AppendLine($"**{report.Summary}** · pass rate {report.PassRate:0.#}% · started {report.StartedAt:yyyy-MM-dd HH:mm:ss}");
        if (report.TagFilter is not null) sb.AppendLine($"\nTag filter: `{report.TagFilter}`");
        sb.AppendLine();

        sb.AppendLine("| | Test | File | Duration | Notes |");
        sb.AppendLine("|---|---|---|---|---|");
        foreach (var r in report.Results)
        {
            var notes = r.Outcome == TestOutcome.Passed
                ? r.IsFlaky ? $"flaky - passed on attempt {r.Attempts}" : ""
                : r.Message ?? "";
            sb.AppendLine($"| {Glyph(r.Outcome)} | {Cell(r.Case.Name)}{Tags(r)} | {Cell(r.Case.FileName)}{(r.Case.Line > 0 ? $":{r.Case.Line}" : "")} | {TestRunReport.FormatDuration(r.Duration)} | {Cell(Truncate(notes, 160))} |");
        }

        var problems = report.Results.Where(r => r.Outcome is TestOutcome.Failed or TestOutcome.Error).ToList();
        if (problems.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine("## Failures");
            foreach (var r in problems)
            {
                sb.AppendLine();
                sb.Append(BugReportBody(r, headingLevel: 3));
            }
        }

        if (report.Environment.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine("## Environment");
            foreach (var (key, value) in report.Environment) sb.AppendLine($"- **{key}:** {value}");
        }
        return sb.ToString();
    }

    /// <summary>A self-contained bug report for one failed test.</summary>
    public static string BugReport(TestCaseResult result, IReadOnlyDictionary<string, string>? environment = null)
    {
        var sb = new StringBuilder();
        sb.Append(BugReportBody(result, headingLevel: 2));
        sb.AppendLine();
        sb.AppendLine("**Environment**");
        sb.AppendLine($"- Run at: {result.StartedAt:yyyy-MM-dd HH:mm:ss zzz}");
        foreach (var (key, value) in environment ?? new Dictionary<string, string>()) sb.AppendLine($"- {key}: {value}");
        return sb.ToString();
    }

    private static string BugReportBody(TestCaseResult r, int headingLevel)
    {
        var sb = new StringBuilder();
        var hashes = new string('#', headingLevel);
        sb.AppendLine($"{hashes} {Glyph(r.Outcome)} {r.Case.Name}");
        sb.AppendLine();
        var location = string.IsNullOrEmpty(r.Case.FilePath) ? "Script Editor" : r.Case.FilePath;
        sb.AppendLine($"- **Test:** `{location}`{(r.Case.Line > 0 ? $" line {r.Case.Line}" : "")}");
        if (r.Case.Tags.Count > 0) sb.AppendLine($"- **Tags:** {string.Join(" ", r.Case.Tags.Select(t => $"`@{t}`"))}");
        sb.AppendLine($"- **Result:** {r.Outcome}{(r.Attempts > 1 ? $" after {r.Attempts} attempts" : "")}");
        if (r.FailedLine is { } line) sb.AppendLine($"- **Failing step:** line {line}");
        sb.AppendLine();
        sb.AppendLine("**Expected vs. actual**");
        sb.AppendLine();
        sb.AppendLine("> " + (r.Message ?? "no details").Replace("\n", "\n> "));

        if (r.Steps.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine("**Steps**");
            sb.AppendLine();
            var i = 1;
            foreach (var step in r.Steps)
            {
                var status = step.Status switch
                {
                    StepStatus.Passed => "✅",
                    StepStatus.Failed => "❌",
                    StepStatus.Cancelled => "⏹",
                    _ => "⏭"
                };
                sb.AppendLine($"{i++}. {status} `{step.Step.Keyword}` (line {step.Step.Line}) - {step.Message}");
            }
        }
        if (r.EarlierFailures.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine("**Earlier attempts**");
            foreach (var earlier in r.EarlierFailures) sb.AppendLine($"- {earlier}");
        }
        return sb.ToString();
    }

    private static string Glyph(TestOutcome outcome) => outcome switch
    {
        TestOutcome.Passed => "✅",
        TestOutcome.Failed => "❌",
        TestOutcome.Error => "⚠️",
        TestOutcome.Cancelled => "⏹",
        _ => "⏭"
    };

    private static string Tags(TestCaseResult r) =>
        r.Case.Tags.Count == 0 ? "" : " " + string.Join(" ", r.Case.Tags.Select(t => $"`@{t}`"));

    private static string Cell(string text) => text.Replace("|", "\\|").Replace("\r", "").Replace("\n", " ");

    private static string Truncate(string text, int max) => text.Length > max ? text[..max] + "…" : text;
}

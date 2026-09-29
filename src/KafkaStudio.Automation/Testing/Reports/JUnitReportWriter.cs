using System.Globalization;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.Automation.Testing.Reports;

/// <summary>
/// Writes a run as JUnit XML - the format Jenkins, GitLab, GitHub Actions test reporters, Azure DevOps and
/// TeamCity all ingest. One <c>&lt;testsuite&gt;</c> per file (or Feature), one <c>&lt;testcase&gt;</c> per
/// scenario; unmet checks are <c>&lt;failure&gt;</c>s, broken scripts / infrastructure problems are
/// <c>&lt;error&gt;</c>s, and every step's outcome goes into <c>&lt;system-out&gt;</c>.
/// </summary>
public static class JUnitReportWriter
{
    public static string Write(TestRunReport report)
    {
        var suites = new XElement("testsuites",
            new XAttribute("name", report.Name),
            new XAttribute("tests", report.Total),
            new XAttribute("failures", report.Failed),
            new XAttribute("errors", report.Errors),
            new XAttribute("skipped", report.Skipped),
            new XAttribute("time", Seconds(report.Duration)),
            new XAttribute("timestamp", report.StartedAt.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture)));

        foreach (var group in report.Results.GroupBy(r => (r.Case.FilePath, r.Case.SuiteName)))
        {
            var results = group.ToList();
            var suite = new XElement("testsuite",
                new XAttribute("name", group.Key.SuiteName),
                new XAttribute("tests", results.Count),
                new XAttribute("failures", results.Count(r => r.Outcome == TestOutcome.Failed)),
                new XAttribute("errors", results.Count(r => r.Outcome == TestOutcome.Error)),
                new XAttribute("skipped", results.Count(r => r.Outcome is TestOutcome.Skipped or TestOutcome.Cancelled)),
                new XAttribute("time", Seconds(TimeSpan.FromTicks(results.Sum(r => r.Duration.Ticks)))),
                new XAttribute("timestamp", results[0].StartedAt.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture)),
                new XAttribute("hostname", Environment.MachineName));
            if (!string.IsNullOrEmpty(group.Key.FilePath)) suite.Add(new XAttribute("file", group.Key.FilePath));

            if (report.Environment.Count > 0 || report.TagFilter is not null)
            {
                var properties = new XElement("properties");
                if (report.TagFilter is not null) properties.Add(Property("tags", report.TagFilter));
                foreach (var (key, value) in report.Environment) properties.Add(Property(key, value));
                suite.Add(properties);
            }

            foreach (var result in results) suite.Add(TestCaseElement(result));
            suites.Add(suite);
        }

        var document = new XDocument(new XDeclaration("1.0", "utf-8", null), suites);
        var sb = new StringBuilder();
        using (var writer = XmlWriter.Create(new Utf8StringWriter(sb), new XmlWriterSettings { Indent = true, Encoding = Encoding.UTF8 }))
        {
            document.Save(writer);
        }
        return sb.ToString();
    }

    private static XElement TestCaseElement(TestCaseResult result)
    {
        var element = new XElement("testcase",
            new XAttribute("name", Clean(result.Case.Name)),
            new XAttribute("classname", Clean(result.Case.SuiteName)),
            new XAttribute("time", Seconds(result.Duration)));
        if (!string.IsNullOrEmpty(result.Case.FilePath)) element.Add(new XAttribute("file", result.Case.FilePath));
        if (result.Case.Line > 0) element.Add(new XAttribute("line", result.Case.Line));

        var details = Clean(StepLog(result));
        switch (result.Outcome)
        {
            case TestOutcome.Failed:
                element.Add(new XElement("failure",
                    new XAttribute("message", Clean(result.Message ?? "failed")),
                    new XAttribute("type", "AssertionFailure"), details));
                break;
            case TestOutcome.Error:
                element.Add(new XElement("error",
                    new XAttribute("message", Clean(result.Message ?? "error")),
                    new XAttribute("type", result.Case.LoadError is not null ? "ScriptError" : "Error"), details));
                break;
            case TestOutcome.Skipped:
            case TestOutcome.Cancelled:
                element.Add(new XElement("skipped", new XAttribute("message", Clean(result.Message ?? "skipped"))));
                break;
        }

        if (result.Steps.Count > 0) element.Add(new XElement("system-out", details));
        if (result.Case.Tags.Count > 0 || result.IsFlaky)
        {
            var properties = new XElement("properties");
            if (result.Case.Tags.Count > 0) properties.Add(Property("tags", string.Join(" ", result.Case.Tags.Select(t => "@" + t))));
            if (result.IsFlaky) properties.Add(Property("flaky", $"passed on attempt {result.Attempts}"));
            element.Add(properties);
        }
        return element;
    }

    /// <summary>One line per step, like the app's step results panel.</summary>
    public static string StepLog(TestCaseResult result)
    {
        var sb = new StringBuilder();
        foreach (var earlier in result.EarlierFailures) sb.AppendLine($"(retried) {earlier}");
        foreach (var step in result.Steps)
        {
            var glyph = step.Status switch
            {
                StepStatus.Passed => "PASS",
                StepStatus.Failed => step.IsError ? "ERROR" : "FAIL",
                StepStatus.Cancelled => "STOP",
                _ => "SKIP"
            };
            sb.AppendLine($"[{glyph}] line {step.Step.Line} {step.Step.Keyword}: {step.Message}");
        }
        return sb.ToString().TrimEnd();
    }

    private static XElement Property(string name, string value) =>
        new("property", new XAttribute("name", Clean(name)), new XAttribute("value", Clean(value)));

    private static string Seconds(TimeSpan d) => d.TotalSeconds.ToString("0.000", CultureInfo.InvariantCulture);

    /// <summary>Drops characters XML 1.0 can't carry (control characters from binary payloads).</summary>
    private static string Clean(string text) =>
        text.Any(c => !XmlConvert.IsXmlChar(c)) ? new string(text.Where(XmlConvert.IsXmlChar).ToArray()) : text;

    private sealed class Utf8StringWriter(StringBuilder sb) : StringWriter(sb)
    {
        public override Encoding Encoding => Encoding.UTF8;
    }
}

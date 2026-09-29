using System.Globalization;
using System.Net;
using System.Text;
using KafkaStudio.Scripting.Runtime;

namespace KafkaStudio.Automation.Testing.Reports;

/// <summary>
/// A single-file HTML report (no external CSS/JS, so it can be mailed, attached to a ticket or published
/// as a CI artifact): summary tiles, a filterable list of tests, and each test's steps. Follows the
/// viewer's light/dark preference.
/// </summary>
public static class HtmlReportWriter
{
    public static string Write(TestRunReport report)
    {
        var sb = new StringBuilder();
        var status = report.Success ? "passed" : report.WasCancelled ? "stopped" : "failed";
        sb.Append($$"""
            <!doctype html>
            <html lang="en">
            <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>{{E(report.Name)}} - {{status}}</title>
            <style>
            :root { --bg:#f6f7f9; --card:#fff; --text:#1d2330; --muted:#667085; --border:#e3e6eb;
                    --pass:#1a7f37; --fail:#cf222e; --error:#b35900; --skip:#6e7781; --accent:#3b5bdb; }
            @media (prefers-color-scheme: dark) {
              :root { --bg:#14171c; --card:#1c2027; --text:#e6e8eb; --muted:#9aa4b2; --border:#2d333b;
                      --pass:#3fb950; --fail:#f85149; --error:#e3913b; --skip:#8b949e; --accent:#7c93f5; }
            }
            * { box-sizing:border-box; }
            body { margin:0; padding:24px 16px; background:var(--bg); color:var(--text);
                   font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif; }
            main { max-width:1100px; margin:0 auto; }
            h1 { font-size:22px; margin:0 0 4px; }
            .muted { color:var(--muted); }
            .badge { display:inline-block; padding:2px 10px; border-radius:999px; font-size:12px; font-weight:600;
                     color:#fff; vertical-align:middle; margin-left:8px; }
            .b-passed { background:var(--pass); } .b-failed { background:var(--fail); } .b-stopped { background:var(--skip); }
            .tiles { display:grid; grid-template-columns:repeat(auto-fit,minmax(130px,1fr)); gap:10px; margin:18px 0; }
            .tile { background:var(--card); border:1px solid var(--border); border-radius:8px; padding:10px 14px; }
            .tile .n { font-size:22px; font-weight:700; } .tile .l { font-size:12px; color:var(--muted); }
            .bar { display:flex; height:8px; border-radius:4px; overflow:hidden; background:var(--border); margin-bottom:18px; }
            .bar span { display:block; }
            .filters { display:flex; flex-wrap:wrap; gap:6px; margin-bottom:10px; }
            .filters button { border:1px solid var(--border); background:var(--card); color:var(--text); border-radius:6px;
                              padding:4px 12px; cursor:pointer; font:inherit; }
            .filters button.on { border-color:var(--accent); color:var(--accent); font-weight:600; }
            details { background:var(--card); border:1px solid var(--border); border-left:4px solid var(--skip);
                      border-radius:6px; margin-bottom:6px; }
            details.passed { border-left-color:var(--pass); } details.failed { border-left-color:var(--fail); }
            details.error { border-left-color:var(--error); }
            summary { cursor:pointer; padding:8px 12px; display:flex; flex-wrap:wrap; gap:4px 12px; align-items:baseline; }
            summary .name { font-weight:600; flex:1 1 280px; min-width:0; overflow-wrap:anywhere; }
            .o { font-weight:700; font-size:12px; text-transform:uppercase; }
            .o.passed { color:var(--pass); } .o.failed { color:var(--fail); } .o.error { color:var(--error); } .o.skipped, .o.cancelled { color:var(--skip); }
            .tag { font-size:11px; color:var(--accent); border:1px solid var(--border); border-radius:4px; padding:0 5px; }
            .body { padding:0 12px 10px; }
            .msg { background:var(--bg); border-radius:4px; padding:8px 10px; white-space:pre-wrap; overflow-wrap:anywhere;
                   font-family:ui-monospace,Consolas,monospace; font-size:12px; margin:0 0 8px; }
            table { width:100%; border-collapse:collapse; font-size:12px; }
            td { border-top:1px solid var(--border); padding:4px 6px; vertical-align:top; overflow-wrap:anywhere; }
            td.s { width:52px; font-weight:700; } td.l { width:60px; color:var(--muted); white-space:nowrap; }
            .table-wrap { overflow-x:auto; }
            footer { margin-top:24px; font-size:12px; }
            </style>
            </head>
            <body>
            <main>
            <h1>{{E(report.Name)}}<span class="badge b-{{status}}">{{status.ToUpperInvariant()}}</span></h1>
            <div class="muted">Started {{E(report.StartedAt.ToString("yyyy-MM-dd HH:mm:ss zzz", CultureInfo.InvariantCulture))}} · took {{E(TestRunReport.FormatDuration(report.Duration))}}{{(report.TagFilter is null ? "" : " · tags " + E(report.TagFilter))}}</div>
            <div class="tiles">
              <div class="tile"><div class="n">{{report.Total}}</div><div class="l">tests</div></div>
              <div class="tile"><div class="n" style="color:var(--pass)">{{report.Passed}}</div><div class="l">passed</div></div>
              <div class="tile"><div class="n" style="color:var(--fail)">{{report.Failed}}</div><div class="l">failed</div></div>
              <div class="tile"><div class="n" style="color:var(--error)">{{report.Errors}}</div><div class="l">errors</div></div>
              <div class="tile"><div class="n" style="color:var(--skip)">{{report.Skipped}}</div><div class="l">skipped</div></div>
              <div class="tile"><div class="n">{{report.PassRate.ToString("0.#", CultureInfo.InvariantCulture)}}%</div><div class="l">pass rate{{(report.Flaky > 0 ? $" · {report.Flaky} flaky" : "")}}</div></div>
            </div>
            <div class="bar">{{Bar(report)}}</div>
            <div class="filters">
              <button class="on" data-f="all">All</button>
              <button data-f="failed error">Failed &amp; errors</button>
              <button data-f="passed">Passed</button>
              <button data-f="skipped cancelled">Skipped</button>
            </div>
            <div id="tests">

            """);

        foreach (var r in report.Results)
        {
            var outcome = r.Outcome.ToString().ToLowerInvariant();
            var open = r.Outcome is TestOutcome.Failed or TestOutcome.Error ? " open" : "";
            sb.Append($"<details class=\"{outcome}\" data-o=\"{outcome}\"{open}><summary>");
            sb.Append($"<span class=\"o {outcome}\">{outcome}</span><span class=\"name\">{E(r.Case.Name)}</span>");
            foreach (var tag in r.Case.Tags) sb.Append($"<span class=\"tag\">@{E(tag)}</span>");
            sb.Append($"<span class=\"muted\">{E(r.Case.FileName)}{(r.Case.Line > 0 ? ":" + r.Case.Line : "")}</span>");
            sb.Append($"<span class=\"muted\">{E(TestRunReport.FormatDuration(r.Duration))}{(r.Attempts > 1 ? $" · {r.Attempts} attempts" : "")}</span>");
            sb.Append("</summary><div class=\"body\">");
            if (r.Message is not null) sb.Append($"<pre class=\"msg\">{E(r.Message)}</pre>");
            foreach (var earlier in r.EarlierFailures) sb.Append($"<div class=\"muted\">retried - {E(earlier)}</div>");
            if (r.Steps.Count > 0)
            {
                sb.Append("<div class=\"table-wrap\"><table>");
                foreach (var step in r.Steps)
                {
                    var (label, cls) = step.Status switch
                    {
                        StepStatus.Passed => ("pass", "passed"),
                        StepStatus.Failed => (step.IsError ? "error" : "fail", step.IsError ? "error" : "failed"),
                        StepStatus.Cancelled => ("stop", "cancelled"),
                        _ => ("skip", "skipped")
                    };
                    sb.Append($"<tr><td class=\"s o {cls}\">{label}</td><td class=\"l\">L{step.Step.Line}</td>");
                    sb.Append($"<td><b>{E(step.Step.Keyword.ToString())}</b> {E(step.Message)}</td></tr>");
                }
                sb.Append("</table></div>");
            }
            sb.Append("</div></details>\n");
        }

        sb.Append("</div>\n");
        if (report.Environment.Count > 0)
        {
            sb.Append("<footer class=\"muted\">");
            sb.Append(string.Join(" · ", report.Environment.Select(kv => $"{E(kv.Key)}: {E(kv.Value)}")));
            sb.Append("</footer>\n");
        }
        sb.Append("""
            <footer class="muted">Generated by KafkaStudio</footer>
            </main>
            <script>
            document.querySelectorAll('.filters button').forEach(function (b) {
              b.addEventListener('click', function () {
                document.querySelectorAll('.filters button').forEach(function (x) { x.classList.remove('on'); });
                b.classList.add('on');
                var f = b.getAttribute('data-f').split(' ');
                document.querySelectorAll('#tests details').forEach(function (d) {
                  d.style.display = f[0] === 'all' || f.indexOf(d.getAttribute('data-o')) >= 0 ? '' : 'none';
                });
              });
            });
            </script>
            </body>
            </html>

            """);
        return sb.ToString();
    }

    private static string Bar(TestRunReport report)
    {
        if (report.Total == 0) return "";
        string Segment(int count, string color) => count == 0 ? "" :
            $"<span style=\"width:{(100.0 * count / report.Total).ToString("0.##", CultureInfo.InvariantCulture)}%;background:var({color})\"></span>";
        return Segment(report.Passed, "--pass") + Segment(report.Failed, "--fail") + Segment(report.Errors, "--error") + Segment(report.Skipped, "--skip");
    }

    private static string E(string text) => WebUtility.HtmlEncode(text);
}

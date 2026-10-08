using KafkaStudio.Scripting;
using KafkaStudio.Scripting.Ast;
using KafkaStudio.Scripting.Parsing;

namespace KafkaStudio.Automation.Testing;

/// <summary>Finds the tests in .kafscript files and folders, and picks the ones a run should execute.</summary>
public static class TestDiscovery
{
    /// <summary>
    /// Every Scenario (and Task, for <see cref="Select"/> to filter) in the given files and folders
    /// (folders are searched recursively for <c>*.kafscript</c>, in path order). A file that fails to parse
    /// becomes one case carrying its <see cref="TestCase.LoadError"/>; a path that doesn't exist too.
    /// </summary>
    public static IReadOnlyList<TestCase> Discover(IEnumerable<string> paths)
    {
        var files = new List<string>();
        var cases = new List<TestCase>();
        foreach (var raw in paths)
        {
            var path = Path.GetFullPath(raw);
            if (Directory.Exists(path))
            {
                try
                {
                    files.AddRange(Directory.EnumerateFiles(path, "*.kafscript", SearchOption.AllDirectories)
                        .OrderBy(f => f, StringComparer.Ordinal));
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    cases.Add(new TestCase(path, null, LoadError: $"can't list '{path}': {ex.Message}"));
                }
            }
            else if (File.Exists(path))
            {
                files.Add(path);
            }
            else
            {
                cases.Add(new TestCase(path, null, LoadError: $"file or folder not found: {path}"));
            }
        }

        foreach (var file in files.Distinct(StringComparer.Ordinal))
        {
            string text;
            try
            {
                text = File.ReadAllText(file);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                cases.Add(new TestCase(file, null, LoadError: $"can't read file: {ex.Message}"));
                continue;
            }
            cases.AddRange(FromSource(text, file));
        }
        return cases;
    }

    /// <summary>The tests in one script's source (e.g. the Script Editor's contents).</summary>
    public static IReadOnlyList<TestCase> FromSource(string source, string filePath = "")
    {
        try
        {
            var document = Parser.Parse(source);
            return document.Blocks.Select(b => new TestCase(filePath, b, document.FeatureName)).ToList();
        }
        catch (KafScriptException ex)
        {
            return new[] { new TestCase(filePath, null, LoadError: ex.Message) };
        }
    }

    /// <summary>Applies the options' tag, name and Task filters. Load errors are always kept.</summary>
    public static IReadOnlyList<TestCase> Select(IEnumerable<TestCase> cases, TestRunOptions options) =>
        cases.Where(c => c.LoadError is not null || IsSelected(c, options)).ToList();

    public static bool IsSelected(TestCase testCase, TestRunOptions options)
    {
        if (testCase.Block is null) return true;
        if (testCase.Block.Kind == BlockKind.Task && !options.IncludeTasks) return false;
        if (!options.Tags.Matches(testCase.Tags)) return false;
        return string.IsNullOrWhiteSpace(options.NameFilter) ||
               testCase.Name.Contains(options.NameFilter.Trim(), StringComparison.OrdinalIgnoreCase);
    }
}

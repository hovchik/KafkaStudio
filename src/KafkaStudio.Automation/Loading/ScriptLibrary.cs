using KafkaStudio.Scripting;
using KafkaStudio.Scripting.Ast;
using KafkaStudio.Scripting.Parsing;

namespace KafkaStudio.Automation.Loading;

public sealed record LoadedScript(string FilePath, ScriptDocument Document);

/// <summary>Loads and parses every ".kafscript" file under a directory, so the app can populate its
/// Scenario/Task list from a project folder the same way a test runner discovers spec files.</summary>
public static class ScriptLibrary
{
    public const string DefaultExtension = ".kafscript";

    public static IReadOnlyList<LoadedScript> LoadDirectory(string directory, string searchPattern = "*.kafscript")
    {
        if (!Directory.Exists(directory))
        {
            throw new DirectoryNotFoundException($"script directory not found: {directory}");
        }

        var results = new List<LoadedScript>();
        foreach (var file in Directory.EnumerateFiles(directory, searchPattern, SearchOption.AllDirectories)
                     .OrderBy(f => f, StringComparer.Ordinal))
        {
            results.Add(LoadFile(file));
        }
        return results;
    }

    public static LoadedScript LoadFile(string path)
    {
        var text = File.ReadAllText(path);
        try
        {
            return new LoadedScript(path, Parser.Parse(text));
        }
        catch (KafScriptException ex)
        {
            throw new KafScriptException($"{Path.GetFileName(path)}: {ex.Message}", ex.Line);
        }
    }
}

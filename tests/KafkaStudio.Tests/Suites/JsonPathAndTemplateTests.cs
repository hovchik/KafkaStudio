using KafkaStudio.Scripting.Runtime;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

public static class JsonPathAndTemplateTests
{
    public static void Register(TestRunner runner)
    {
        runner.Add("JsonPathEvaluator", "reads a top-level field", () =>
        {
            var value = JsonPathEvaluator.Evaluate("""{ "status": "CONFIRMED" }""", "$.status");
            Assert.Equal("CONFIRMED", value);
        });

        runner.Add("JsonPathEvaluator", "reads a nested field", () =>
        {
            var value = JsonPathEvaluator.Evaluate("""{ "order": { "id": "42" } }""", "$.order.id");
            Assert.Equal("42", value);
        });

        runner.Add("JsonPathEvaluator", "reads an array element by index", () =>
        {
            var value = JsonPathEvaluator.Evaluate("""{ "items": [ { "sku": "A" }, { "sku": "B" } ] }""", "$.items[1].sku");
            Assert.Equal("B", value);
        });

        runner.Add("JsonPathEvaluator", "returns null for a missing path", () =>
        {
            var value = JsonPathEvaluator.Evaluate("""{ "status": "CONFIRMED" }""", "$.missing.field");
            Assert.Null(value);
        });

        runner.Add("JsonPathEvaluator", "returns null for invalid json instead of throwing", () =>
        {
            var value = JsonPathEvaluator.Evaluate("not json", "$.status");
            Assert.Null(value);
        });

        runner.Add("JsonPathEvaluator", "returns null for an out-of-range array index", () =>
        {
            var value = JsonPathEvaluator.Evaluate("""{ "items": [1,2] }""", "$.items[5]");
            Assert.Null(value);
        });

        runner.Add("TemplateEngine", "substitutes a known variable", () =>
        {
            var result = TemplateEngine.Render("hello {{name}}", new Dictionary<string, string> { ["name"] = "world" });
            Assert.Equal("hello world", result);
        });

        runner.Add("TemplateEngine", "leaves unknown placeholders untouched", () =>
        {
            var result = TemplateEngine.Render("hello {{missing}}", new Dictionary<string, string>());
            Assert.Equal("hello {{missing}}", result);
        });

        runner.Add("TemplateEngine", "substitutes multiple occurrences", () =>
        {
            var result = TemplateEngine.Render("{{a}}-{{b}}-{{a}}", new Dictionary<string, string> { ["a"] = "x", ["b"] = "y" });
            Assert.Equal("x-y-x", result);
        });
    }
}

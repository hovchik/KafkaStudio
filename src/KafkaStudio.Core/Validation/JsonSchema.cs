using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace KafkaStudio.Core.Validation;

/// <summary>One way a JSON document breaks a schema: where (a <c>$.a.b[0]</c> style path) and why.</summary>
public sealed record SchemaViolation(string Path, string Message)
{
    public override string ToString() => $"{Path}: {Message}";
}

/// <summary>
/// A JSON Schema, for message contract checks ("every message on this topic has an orderId string and a
/// status from this list"). Implements the part of JSON Schema (draft 7 / 2019-09 / 2020-12 wording) that
/// message contracts actually use, with no external dependency:
/// <c>type</c> (incl. a list of types and OpenAPI's <c>nullable</c>), <c>properties</c>, <c>required</c>,
/// <c>patternProperties</c>, <c>additionalProperties</c>, <c>propertyNames</c>, <c>minProperties</c>/<c>maxProperties</c>,
/// <c>items</c>, <c>prefixItems</c>, <c>contains</c> (+ <c>minContains</c>/<c>maxContains</c>),
/// <c>minItems</c>/<c>maxItems</c>/<c>uniqueItems</c>, <c>enum</c>, <c>const</c>,
/// <c>minimum</c>/<c>maximum</c>/<c>exclusiveMinimum</c>/<c>exclusiveMaximum</c>, <c>multipleOf</c>,
/// <c>minLength</c>/<c>maxLength</c>/<c>pattern</c>, <c>format</c> (date-time, date, time, uuid, email,
/// uri, ipv4), <c>allOf</c>/<c>anyOf</c>/<c>oneOf</c>/<c>not</c>, and local <c>$ref</c>s
/// (<c>#/definitions/x</c>, <c>#/$defs/x</c>). Other keywords (title, description, $schema, examples...)
/// are ignored. Problems in the schema itself are reported by <see cref="TryParse"/>, not at validation time.
/// </summary>
public sealed class JsonSchema
{
    private static readonly TimeSpan RegexTimeout = TimeSpan.FromSeconds(1);
    private static readonly ConcurrentDictionary<string, Regex> RegexCache = new();
    private static readonly string[] TypeNames = { "string", "number", "integer", "boolean", "object", "array", "null" };

    private readonly JsonElement _root;

    private JsonSchema(JsonElement root) => _root = root;

    /// <summary>The schema's source text, normalized.</summary>
    public string Text => _root.GetRawText();

    public static JsonSchema Parse(string text) =>
        TryParse(text, out var schema, out var problem) ? schema! : throw new FormatException(problem);

    public static bool TryParse(string text, out JsonSchema? schema, out string? problem)
    {
        schema = null;
        JsonElement root;
        try
        {
            using var doc = JsonDocument.Parse(text, new JsonDocumentOptions { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip });
            root = doc.RootElement.Clone();
        }
        catch (JsonException ex)
        {
            problem = $"the schema is not valid JSON ({ex.Message})";
            return false;
        }

        problem = CheckSchema(root, root, "#");
        if (problem is not null) return false;
        schema = new JsonSchema(root);
        return true;
    }

    /// <summary>Validates a JSON text. Text that isn't JSON at all is reported as a single violation.</summary>
    public IReadOnlyList<SchemaViolation> Validate(string? json)
    {
        if (json is null) return new[] { new SchemaViolation("$", "message has no text value (tombstone or binary payload)") };
        try
        {
            using var doc = JsonDocument.Parse(json);
            return Validate(doc.RootElement);
        }
        catch (JsonException ex)
        {
            return new[] { new SchemaViolation("$", $"value is not valid JSON ({ex.Message})") };
        }
    }

    public IReadOnlyList<SchemaViolation> Validate(JsonElement instance)
    {
        var violations = new List<SchemaViolation>();
        ValidateNode(_root, instance, "$", violations, depth: 0);
        return violations;
    }

    // ------------------------------------------------------------------ validation ----

    private void ValidateNode(JsonElement schema, JsonElement value, string path, List<SchemaViolation> errors, int depth)
    {
        if (depth > 64)
        {
            errors.Add(new SchemaViolation(path, "schema nesting is too deep (recursive $ref?)"));
            return;
        }
        if (schema.ValueKind == JsonValueKind.True) return;
        if (schema.ValueKind == JsonValueKind.False)
        {
            errors.Add(new SchemaViolation(path, "no value is allowed here"));
            return;
        }
        if (schema.ValueKind != JsonValueKind.Object) return;

        if (schema.TryGetProperty("$ref", out var reference))
        {
            ValidateNode(Resolve(_root, reference.GetString()!)!.Value, value, path, errors, depth + 1);
        }

        var nullable = schema.TryGetProperty("nullable", out var n) && n.ValueKind == JsonValueKind.True;
        if (nullable && value.ValueKind == JsonValueKind.Null) return;

        if (schema.TryGetProperty("type", out var type))
        {
            var allowed = type.ValueKind == JsonValueKind.Array
                ? type.EnumerateArray().Select(t => t.GetString()!).ToList()
                : new List<string> { type.GetString()! };
            if (!allowed.Any(t => IsType(value, t)))
            {
                errors.Add(new SchemaViolation(path, $"expected {string.Join(" or ", allowed)} but was {Describe(value)}"));
                return; // further keyword checks would only add noise for a value of the wrong type
            }
        }

        if (schema.TryGetProperty("enum", out var enumValues) &&
            !enumValues.EnumerateArray().Any(e => JsonElement.DeepEquals(e, value)))
        {
            errors.Add(new SchemaViolation(path,
                $"{Short(value)} is not one of the allowed values: {string.Join(", ", enumValues.EnumerateArray().Select(Short))}"));
        }

        if (schema.TryGetProperty("const", out var constValue) && !JsonElement.DeepEquals(constValue, value))
        {
            errors.Add(new SchemaViolation(path, $"expected {Short(constValue)} but was {Short(value)}"));
        }

        switch (value.ValueKind)
        {
            case JsonValueKind.Object:
                ValidateObject(schema, value, path, errors, depth);
                break;
            case JsonValueKind.Array:
                ValidateArray(schema, value, path, errors, depth);
                break;
            case JsonValueKind.String:
                ValidateString(schema, value.GetString()!, path, errors);
                break;
            case JsonValueKind.Number:
                ValidateNumber(schema, value, path, errors);
                break;
        }

        if (schema.TryGetProperty("allOf", out var allOf))
        {
            foreach (var sub in allOf.EnumerateArray()) ValidateNode(sub, value, path, errors, depth + 1);
        }
        if (schema.TryGetProperty("anyOf", out var anyOf) &&
            !anyOf.EnumerateArray().Any(sub => Passes(sub, value, depth)))
        {
            errors.Add(new SchemaViolation(path, "value doesn't match any of the allowed shapes (anyOf)"));
        }
        if (schema.TryGetProperty("oneOf", out var oneOf))
        {
            var matches = oneOf.EnumerateArray().Count(sub => Passes(sub, value, depth));
            if (matches != 1)
            {
                errors.Add(new SchemaViolation(path, matches == 0
                    ? "value doesn't match any of the allowed shapes (oneOf)"
                    : $"value matches {matches} shapes but must match exactly one (oneOf)"));
            }
        }
        if (schema.TryGetProperty("not", out var not) && Passes(not, value, depth))
        {
            errors.Add(new SchemaViolation(path, "value matches a shape that is not allowed (not)"));
        }
    }

    private bool Passes(JsonElement schema, JsonElement value, int depth)
    {
        var scratch = new List<SchemaViolation>();
        ValidateNode(schema, value, "$", scratch, depth + 1);
        return scratch.Count == 0;
    }

    private void ValidateObject(JsonElement schema, JsonElement value, string path, List<SchemaViolation> errors, int depth)
    {
        if (schema.TryGetProperty("required", out var required))
        {
            foreach (var name in required.EnumerateArray().Select(r => r.GetString()!))
            {
                if (!value.TryGetProperty(name, out _))
                {
                    errors.Add(new SchemaViolation(ChildPath(path, name), "required field is missing"));
                }
            }
        }

        var count = 0;
        schema.TryGetProperty("properties", out var properties);
        var hasProperties = properties.ValueKind == JsonValueKind.Object;
        schema.TryGetProperty("patternProperties", out var patternProperties);
        var hasPatternProperties = patternProperties.ValueKind == JsonValueKind.Object;
        schema.TryGetProperty("additionalProperties", out var additional);
        schema.TryGetProperty("propertyNames", out var propertyNames);
        foreach (var property in value.EnumerateObject())
        {
            count++;
            if (propertyNames.ValueKind is JsonValueKind.Object or JsonValueKind.False)
            {
                using var nameDoc = JsonDocument.Parse(JsonSerializer.Serialize(property.Name));
                var nameErrors = new List<SchemaViolation>();
                ValidateNode(propertyNames, nameDoc.RootElement, ChildPath(path, property.Name), nameErrors, depth + 1);
                foreach (var e in nameErrors) errors.Add(new SchemaViolation(e.Path, $"field name {e.Message} (propertyNames)"));
            }

            var covered = false;
            if (hasProperties && properties.TryGetProperty(property.Name, out var propertySchema))
            {
                covered = true;
                ValidateNode(propertySchema, property.Value, ChildPath(path, property.Name), errors, depth + 1);
            }
            if (hasPatternProperties)
            {
                foreach (var patterned in patternProperties.EnumerateObject())
                {
                    bool matched;
                    try { matched = GetRegex(patterned.Name).IsMatch(property.Name); }
                    catch (RegexMatchTimeoutException) { matched = false; }
                    if (!matched) continue;
                    covered = true;
                    ValidateNode(patterned.Value, property.Value, ChildPath(path, property.Name), errors, depth + 1);
                }
            }
            if (covered) continue;

            if (additional.ValueKind == JsonValueKind.False)
            {
                errors.Add(new SchemaViolation(ChildPath(path, property.Name), "field is not allowed by the schema (additionalProperties: false)"));
            }
            else if (additional.ValueKind == JsonValueKind.Object)
            {
                ValidateNode(additional, property.Value, ChildPath(path, property.Name), errors, depth + 1);
            }
        }

        if (schema.TryGetProperty("minProperties", out var min) && count < min.GetDouble())
        {
            errors.Add(new SchemaViolation(path, $"has {count} field(s), expected at least {min.GetDouble()}"));
        }
        if (schema.TryGetProperty("maxProperties", out var max) && count > max.GetDouble())
        {
            errors.Add(new SchemaViolation(path, $"has {count} field(s), expected at most {max.GetDouble()}"));
        }
    }

    private void ValidateArray(JsonElement schema, JsonElement value, string path, List<SchemaViolation> errors, int depth)
    {
        var length = value.GetArrayLength();
        if (schema.TryGetProperty("minItems", out var min) && length < min.GetDouble())
        {
            errors.Add(new SchemaViolation(path, $"has {length} item(s), expected at least {min.GetDouble()}"));
        }
        if (schema.TryGetProperty("maxItems", out var max) && length > max.GetDouble())
        {
            errors.Add(new SchemaViolation(path, $"has {length} item(s), expected at most {max.GetDouble()}"));
        }
        // 2020-12 tuples: prefixItems covers the first N positions, items the rest.
        var prefixCount = 0;
        if (schema.TryGetProperty("prefixItems", out var prefixItems) && prefixItems.ValueKind == JsonValueKind.Array)
        {
            var i = 0;
            foreach (var item in value.EnumerateArray())
            {
                if (i >= prefixItems.GetArrayLength()) break;
                ValidateNode(prefixItems[i], item, $"{path}[{i}]", errors, depth + 1);
                i++;
            }
            prefixCount = prefixItems.GetArrayLength();
        }
        if (schema.TryGetProperty("items", out var items) && items.ValueKind is JsonValueKind.Object or JsonValueKind.True or JsonValueKind.False)
        {
            var i = 0;
            foreach (var item in value.EnumerateArray())
            {
                if (i >= prefixCount) ValidateNode(items, item, $"{path}[{i}]", errors, depth + 1);
                i++;
            }
        }
        if (schema.TryGetProperty("contains", out var contains) && contains.ValueKind is JsonValueKind.Object or JsonValueKind.True or JsonValueKind.False)
        {
            var hits = value.EnumerateArray().Count(item => Passes(contains, item, depth));
            var minContains = schema.TryGetProperty("minContains", out var minC) ? (int)minC.GetDouble() : 1;
            if (hits < minContains)
            {
                errors.Add(new SchemaViolation(path, minContains == 1
                    ? "no item matches the required shape (contains)"
                    : $"only {hits} item(s) match the required shape, expected at least {minContains} (minContains)"));
            }
            if (schema.TryGetProperty("maxContains", out var maxC) && hits > maxC.GetDouble())
            {
                errors.Add(new SchemaViolation(path, $"{hits} item(s) match the shape, expected at most {maxC.GetDouble()} (maxContains)"));
            }
        }
        if (schema.TryGetProperty("uniqueItems", out var unique) && unique.ValueKind == JsonValueKind.True)
        {
            var list = value.EnumerateArray().ToList();
            for (var i = 0; i < list.Count; i++)
            {
                for (var j = i + 1; j < list.Count; j++)
                {
                    if (JsonElement.DeepEquals(list[i], list[j]))
                    {
                        errors.Add(new SchemaViolation($"{path}[{j}]", $"duplicate of item [{i}] (uniqueItems)"));
                        return;
                    }
                }
            }
        }
    }

    private static void ValidateString(JsonElement schema, string text, string path, List<SchemaViolation> errors)
    {
        var length = new StringInfo(text).LengthInTextElements;
        if (schema.TryGetProperty("minLength", out var min) && length < min.GetDouble())
        {
            errors.Add(new SchemaViolation(path, $"is {length} character(s) long, expected at least {min.GetDouble()}"));
        }
        if (schema.TryGetProperty("maxLength", out var max) && length > max.GetDouble())
        {
            errors.Add(new SchemaViolation(path, $"is {length} character(s) long, expected at most {max.GetDouble()}"));
        }
        if (schema.TryGetProperty("pattern", out var pattern))
        {
            var regex = GetRegex(pattern.GetString()!);
            bool matched;
            try { matched = regex.IsMatch(text); }
            catch (RegexMatchTimeoutException) { matched = false; }
            if (!matched) errors.Add(new SchemaViolation(path, $"\"{Truncate(text)}\" doesn't match pattern {pattern.GetString()}"));
        }
        if (schema.TryGetProperty("format", out var format) && format.GetString() is { } f && !MatchesFormat(text, f))
        {
            errors.Add(new SchemaViolation(path, $"\"{Truncate(text)}\" is not a valid {f}"));
        }
    }

    private static void ValidateNumber(JsonElement schema, JsonElement value, string path, List<SchemaViolation> errors)
    {
        // Compare as decimal when both sides fit, so 64-bit ids and money amounts aren't rounded through
        // a double (9007199254740993 would otherwise pass "maximum: 9007199254740992").
        var number = value.GetDouble();
        var exact = value.TryGetDecimal(out var dec);
        int Compare(JsonElement bound) =>
            exact && bound.TryGetDecimal(out var b) ? dec.CompareTo(b) : number.CompareTo(bound.GetDouble());
        string Text() => exact ? dec.ToString(CultureInfo.InvariantCulture) : Format(number);

        // Draft 4 spelled exclusive bounds as booleans that modify minimum/maximum.
        var minExclusive = schema.TryGetProperty("exclusiveMinimum", out var xmin) && xmin.ValueKind == JsonValueKind.True;
        var maxExclusive = schema.TryGetProperty("exclusiveMaximum", out var xmax) && xmax.ValueKind == JsonValueKind.True;

        if (schema.TryGetProperty("minimum", out var min) && (minExclusive ? Compare(min) <= 0 : Compare(min) < 0))
        {
            errors.Add(new SchemaViolation(path, minExclusive
                ? $"{Text()} must be greater than {Format(min.GetDouble())}"
                : $"{Text()} is less than the minimum {Format(min.GetDouble())}"));
        }
        if (schema.TryGetProperty("maximum", out var max) && (maxExclusive ? Compare(max) >= 0 : Compare(max) > 0))
        {
            errors.Add(new SchemaViolation(path, maxExclusive
                ? $"{Text()} must be less than {Format(max.GetDouble())}"
                : $"{Text()} is greater than the maximum {Format(max.GetDouble())}"));
        }
        if (xmin.ValueKind == JsonValueKind.Number && Compare(xmin) <= 0)
        {
            errors.Add(new SchemaViolation(path, $"{Text()} must be greater than {Format(xmin.GetDouble())}"));
        }
        if (xmax.ValueKind == JsonValueKind.Number && Compare(xmax) >= 0)
        {
            errors.Add(new SchemaViolation(path, $"{Text()} must be less than {Format(xmax.GetDouble())}"));
        }
        if (schema.TryGetProperty("multipleOf", out var multiple))
        {
            var divisor = multiple.GetDouble();
            bool isMultiple;
            if (divisor == 0) isMultiple = false;
            else if (exact && multiple.TryGetDecimal(out var decDivisor) && decDivisor != 0) isMultiple = decimal.Remainder(dec, decDivisor) == 0;
            else
            {
                var quotient = number / divisor;
                isMultiple = Math.Abs(quotient - Math.Round(quotient)) <= 1e-9;
            }
            if (!isMultiple)
            {
                errors.Add(new SchemaViolation(path, $"{Text()} is not a multiple of {Format(divisor)}"));
            }
        }
    }

    private static bool IsType(JsonElement value, string type) => type switch
    {
        "string" => value.ValueKind == JsonValueKind.String,
        "number" => value.ValueKind == JsonValueKind.Number,
        "integer" => value.ValueKind == JsonValueKind.Number && IsInteger(value),
        "boolean" => value.ValueKind is JsonValueKind.True or JsonValueKind.False,
        "object" => value.ValueKind == JsonValueKind.Object,
        "array" => value.ValueKind == JsonValueKind.Array,
        "null" => value.ValueKind == JsonValueKind.Null,
        _ => false
    };

    private static bool IsInteger(JsonElement value) =>
        value.TryGetInt64(out _) || (value.TryGetDouble(out var d) && Math.Abs(d % 1) == 0);

    private static bool MatchesFormat(string text, string format) => format switch
    {
        "date-time" => DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out _) && text.Contains('T', StringComparison.OrdinalIgnoreCase),
        "date" => DateOnly.TryParseExact(text, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _),
        "time" => Regex.IsMatch(text, @"^([01]\d|2[0-3]):[0-5]\d:([0-5]\d|60)(\.\d+)?([Zz]|[+-]([01]\d|2[0-3]):[0-5]\d)$", RegexOptions.None, RegexTimeout),
        "uuid" => Guid.TryParseExact(text, "D", out _),
        "email" => Regex.IsMatch(text, @"^[^@\s]+@[^@\s]+\.[^@\s]+$", RegexOptions.None, RegexTimeout),
        "uri" => Uri.TryCreate(text, UriKind.Absolute, out _),
        "ipv4" => System.Net.IPAddress.TryParse(text, out var ip) && ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork && text.Count(c => c == '.') == 3,
        _ => true // unknown formats are annotations only, per the spec
    };

    // ------------------------------------------------------------------ schema checks ----

    /// <summary>Walks the schema once and reports the first malformed keyword, so a typo in a contract
    /// is caught when the script is parsed instead of silently accepting everything.</summary>
    private static string? CheckSchema(JsonElement schema, JsonElement root, string where)
    {
        if (schema.ValueKind is JsonValueKind.True or JsonValueKind.False) return null;
        if (schema.ValueKind != JsonValueKind.Object) return $"{where}: a schema must be an object or true/false";

        foreach (var keyword in schema.EnumerateObject())
        {
            var at = $"{where}/{keyword.Name}";
            var v = keyword.Value;
            string? problem = keyword.Name switch
            {
                "type" => v.ValueKind switch
                {
                    JsonValueKind.String => TypeNames.Contains(v.GetString()) ? null : $"{at}: unknown type '{v.GetString()}' (expected one of {string.Join(", ", TypeNames)})",
                    JsonValueKind.Array => v.EnumerateArray().All(t => t.ValueKind == JsonValueKind.String && TypeNames.Contains(t.GetString()))
                        ? null : $"{at}: every entry must be one of {string.Join(", ", TypeNames)}",
                    _ => $"{at}: must be a type name or a list of type names"
                },
                "properties" or "definitions" or "$defs" => v.ValueKind != JsonValueKind.Object
                    ? $"{at}: must be an object"
                    : v.EnumerateObject().Select(p => CheckSchema(p.Value, root, $"{at}/{p.Name}")).FirstOrDefault(x => x is not null),
                "patternProperties" => v.ValueKind != JsonValueKind.Object
                    ? $"{at}: must be an object"
                    : v.EnumerateObject().Select(p => CheckPattern(p.Name, $"{at}/{p.Name}") ?? CheckSchema(p.Value, root, $"{at}/{p.Name}")).FirstOrDefault(x => x is not null),
                "prefixItems" => v.ValueKind != JsonValueKind.Array
                    ? $"{at}: must be a list of schemas"
                    : v.EnumerateArray().Select((sub, i) => CheckSchema(sub, root, $"{at}/{i}")).FirstOrDefault(x => x is not null),
                "contains" or "propertyNames" => CheckSchema(v, root, at),
                "minContains" or "maxContains" => v.ValueKind == JsonValueKind.Number ? null : $"{at}: must be a number",
                // Keywords this validator doesn't implement: fail loudly instead of silently passing
                // everything a contract meant to restrict.
                "if" or "then" or "else" or "dependentRequired" or "dependentSchemas" or "dependencies"
                    or "unevaluatedProperties" or "unevaluatedItems" =>
                    $"{at}: '{keyword.Name}' isn't supported by KafkaStudio's contract checks - restate the rule with allOf/anyOf/oneOf/not",
                "required" => v.ValueKind == JsonValueKind.Array && v.EnumerateArray().All(r => r.ValueKind == JsonValueKind.String)
                    ? null : $"{at}: must be a list of field names",
                "enum" => v.ValueKind == JsonValueKind.Array ? null : $"{at}: must be a list of values",
                "additionalProperties" or "items" or "not" => v.ValueKind == JsonValueKind.Array && keyword.Name == "items"
                    ? $"{at}: tuple-style 'items' lists aren't supported - use one schema for every item"
                    : CheckSchema(v, root, at),
                "allOf" or "anyOf" or "oneOf" => v.ValueKind != JsonValueKind.Array || v.GetArrayLength() == 0
                    ? $"{at}: must be a non-empty list of schemas"
                    : v.EnumerateArray().Select((sub, i) => CheckSchema(sub, root, $"{at}/{i}")).FirstOrDefault(x => x is not null),
                "minimum" or "maximum" or "multipleOf" or "minLength" or "maxLength" or "minItems" or "maxItems"
                    or "minProperties" or "maxProperties" => v.ValueKind == JsonValueKind.Number ? null : $"{at}: must be a number",
                "exclusiveMinimum" or "exclusiveMaximum" => v.ValueKind is JsonValueKind.Number or JsonValueKind.True or JsonValueKind.False
                    ? null : $"{at}: must be a number",
                "pattern" => v.ValueKind != JsonValueKind.String ? $"{at}: must be a regular expression string" : CheckPattern(v.GetString()!, at),
                "format" => v.ValueKind == JsonValueKind.String ? null : $"{at}: must be a string",
                "$ref" => v.ValueKind != JsonValueKind.String ? $"{at}: must be a string"
                    : Resolve(root, v.GetString()!) is null ? $"{at}: can't resolve '{v.GetString()}' (only local refs like '#/$defs/name' are supported)" : null,
                _ => null
            };
            if (problem is not null) return problem;
        }
        return null;
    }

    private static string? CheckPattern(string pattern, string at)
    {
        try
        {
            GetRegex(pattern);
            return null;
        }
        catch (ArgumentException ex)
        {
            return $"{at}: invalid regular expression: {ex.Message}";
        }
    }

    /// <summary>Resolves a local "#/a/b" JSON pointer against the root schema.</summary>
    private static JsonElement? Resolve(JsonElement root, string reference)
    {
        if (reference == "#") return root;
        if (!reference.StartsWith("#/", StringComparison.Ordinal)) return null;
        var current = root;
        foreach (var raw in reference[2..].Split('/'))
        {
            var segment = Uri.UnescapeDataString(raw).Replace("~1", "/").Replace("~0", "~");
            if (current.ValueKind != JsonValueKind.Object || !current.TryGetProperty(segment, out current)) return null;
        }
        return current;
    }

    private static Regex GetRegex(string pattern)
    {
        if (RegexCache.TryGetValue(pattern, out var cached)) return cached;
        var regex = new Regex(pattern, RegexOptions.CultureInvariant, RegexTimeout);
        if (RegexCache.Count < 512) RegexCache[pattern] = regex;
        return regex;
    }

    private static string ChildPath(string path, string name) =>
        name.Length > 0 && name.All(c => char.IsLetterOrDigit(c) || c == '_') ? $"{path}.{name}" : $"{path}['{name}']";

    private static string Describe(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.String => $"string {Short(value)}",
        JsonValueKind.Number => $"number {value.GetRawText()}",
        JsonValueKind.True or JsonValueKind.False => $"boolean {value.GetRawText()}",
        JsonValueKind.Null => "null",
        JsonValueKind.Object => "an object",
        JsonValueKind.Array => "an array",
        _ => value.ValueKind.ToString()
    };

    private static string Short(JsonElement value)
    {
        var raw = value.GetRawText();
        return raw.Length > 60 ? raw[..60] + "…" : raw;
    }

    private static string Truncate(string text) => text.Length > 60 ? text[..60] + "…" : text;

    private static string Format(double d) => d.ToString("0.############", CultureInfo.InvariantCulture);
}

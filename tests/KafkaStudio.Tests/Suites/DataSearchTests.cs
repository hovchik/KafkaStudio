using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Testing;
using KafkaStudio.Scripting.Parsing;
using KafkaStudio.Scripting.Runtime;
using KafkaStudio.Search;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

/// <summary>The KafkaStudio.Search engine: queries, ranges, existence, bulk checks, similarity,
/// duplicates, diffs, reconciliation, traces, field statistics, script generation and persistence.</summary>
public static class DataSearchTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 10, 12, 0, 0, TimeSpan.Zero);

    private static KafkaMessage Msg(string? value, string? key = null, Dictionary<string, string>? headers = null,
        string topic = "t", long offset = 0, DateTimeOffset? at = null) => new()
    {
        Topic = topic,
        Partition = 0,
        Offset = offset,
        Key = key,
        Value = value,
        RawValue = value is null ? null : System.Text.Encoding.UTF8.GetBytes(value),
        Headers = headers ?? new Dictionary<string, string>(),
        Timestamp = at ?? T0
    };

    private static bool Q(string query, KafkaMessage message, QueryOptions? options = null) =>
        MessageQuery.Parse(query, options).Matches(message);

    /// <summary>A broker + gateway with a small order pipeline: orders → payments → shipments.</summary>
    private static (InMemoryKafkaBroker Broker, IKafkaGateway Gateway) Pipeline()
    {
        var broker = new InMemoryKafkaBroker();
        void Add(string topic, string? key, string value, int secondsAfterT0, Dictionary<string, string>? headers = null) =>
            broker.Append(topic, key, value, System.Text.Encoding.UTF8.GetBytes(value), headers, T0.AddSeconds(secondsAfterT0));

        Add("orders", "ORD-1", """{"orderId":"ORD-1","amount":10,"currency":"EUR","createdAt":"2026-01-10T12:00:00Z"}""", 0);
        Add("orders", "ORD-2", """{"orderId":"ORD-2","amount":25,"currency":"EUR","createdAt":"2026-01-10T12:00:05Z"}""", 5);
        Add("orders", "ORD-3", """{"orderId":"ORD-3","amount":99,"currency":"USD","createdAt":"2026-01-10T12:00:09Z"}""", 9);
        Add("payments", "P-1", """{"orderId":"ORD-1","amount":10,"status":"PAID"}""", 2, new() { ["source"] = "billing" });
        Add("payments", "P-2", """{"orderId":"ORD-2","amount":24,"status":"PAID"}""", 8, new() { ["source"] = "billing" });
        Add("payments", "P-9", """{"orderId":"ORD-9","amount":1,"status":"PAID"}""", 60);
        Add("shipments", "S-1", "shipped ORD-1 via DHL", 30);
        return (broker, TestKafka.NewGateway(broker));
    }

    private static readonly string[] AllTopics = { "orders", "payments", "shipments" };

    public static void Register(TestRunner runner)
    {
        // ------------------------------------------------------------------ query language ----

        runner.Add("Search: query language", "plain text is a case-insensitive contains over key, value and headers", () =>
        {
            var m = Msg("Hello World", key: "K-1", headers: new() { ["trace"] = "abc" });
            Assert.True(Q("world", m));
            Assert.True(Q("k-1", m));
            Assert.True(Q("ABC", m));
            Assert.True(Q("trace", m), "header names count for contains searches");
            Assert.False(Q("nope", m));
            Assert.False(MessageQuery.Parse("world").IsStructured);
        });

        runner.Add("Search: query language", "match modes: exact, whole word, regex, fuzzy, case-sensitive", () =>
        {
            var m = Msg("""{"id":"ORD-42"}""", key: "ORD-42");
            Assert.True(Q("ord-42", m, new QueryOptions { Mode = TextMatchMode.Exact }), "exact on key");
            Assert.False(Q("ORD-4", m, new QueryOptions { Mode = TextMatchMode.Exact }));
            Assert.True(Q("ORD-42", m, new QueryOptions { Mode = TextMatchMode.WholeWord }));
            Assert.False(Q("ORD-4", m, new QueryOptions { Mode = TextMatchMode.WholeWord }), "ORD-4 is part of ORD-42");
            Assert.True(Q("ORD-\\d+", m, new QueryOptions { Mode = TextMatchMode.Regex }));
            Assert.True(Q("ord 0042", Msg("x", key: "ORD-042"), new QueryOptions { Mode = TextMatchMode.Fuzzy }), "fuzzy on key");
            Assert.True(Q("ORD-43", m, new QueryOptions { Mode = TextMatchMode.Fuzzy }), "typo'd id inside JSON");
            Assert.False(Q("ord-42", m, new QueryOptions { CaseSensitive = true }));
            Assert.True(Q("ORD-42", m, new QueryOptions { CaseSensitive = true }));
        });

        runner.Add("Search: query language", "field scope limits plain searches", () =>
        {
            var m = Msg("value-text", key: "key-text", headers: new() { ["h"] = "header-text" });
            Assert.False(Q("value-text", m, new QueryOptions { Fields = SearchFields.Key }));
            Assert.True(Q("key-text", m, new QueryOptions { Fields = SearchFields.Key }));
            Assert.True(Q("header-text", m, new QueryOptions { Fields = SearchFields.Headers }));
            Assert.False(Q("header-text", m, new QueryOptions { Fields = SearchFields.Key | SearchFields.Value }));
        });

        runner.Add("Search: query language", "structured conditions share KafScript's syntax and add more", () =>
        {
            var m = Msg("""{"order":{"id":"ORD-42","amount":120.5},"status":"FAILED","retries":3,"items":[{"sku":"A"},{"sku":"B"}]}""",
                key: "ORD-42", headers: new() { ["source"] = "billing" });
            Assert.True(MessageQuery.Parse("json \"$.order.id\" equals \"ORD-42\"").IsStructured);
            Assert.True(Q("json \"$.order.id\" equals \"ORD-42\" and key equals \"ORD-42\"", m));
            Assert.True(Q("$.order.id = ORD-42", m));
            Assert.True(Q("$.order.amount > 100", m));
            Assert.False(Q("$.order.amount > 200", m));
            Assert.True(Q("$.retries >= 3 and $.retries <= 3", m));
            Assert.True(Q("header \"source\" equals billing", m));
            Assert.True(Q("header source = billing", m));
            Assert.True(Q("key starts with ORD- and key ends with 42", m));
            Assert.True(Q("$.items[-1].sku = B", m));
            Assert.True(Q("$.missing missing and $.status exists", m));
            Assert.True(Q("$.missing is missing", m));
            Assert.True(Q("$.status = OK or ($.status = FAILED and $.retries >= 3)", m));
            Assert.False(Q("not $.status = FAILED", m));
            Assert.True(Q("value not contains \"test\"", m));
            Assert.True(Q("$.status ~ \"^FAIL\"", m));
            Assert.True(Q("$.status equals failed", m), "case-insensitive by default");
            Assert.False(Q("$.status equals failed", m, new QueryOptions { CaseSensitive = true }));
            Assert.True(Q("any similar to \"ORD-0042\"", m));
            Assert.True(Q("topic = t and partition = 0 and offset = 0", m));
        });

        runner.Add("Search: query language", "malformed structured queries give readable errors", () =>
        {
            Assert.False(MessageQuery.TryParse("json \"$.a\" wobbles \"x\"", null, out _, out var e1));
            Assert.Contains("expected a comparison", e1!);
            Assert.False(MessageQuery.TryParse("key equals", null, out _, out var e2));
            Assert.Contains("expected a value", e2!);
            Assert.False(MessageQuery.TryParse("(key = a", null, out _, out var e3));
            Assert.Contains("')'", e3!);
            Assert.False(MessageQuery.TryParse("key matches \"(\"", null, out _, out var e4));
            Assert.Contains("invalid regular expression", e4!);
            Assert.False(MessageQuery.TryParse("[", new QueryOptions { Mode = TextMatchMode.Regex }, out _, out _));
            // Words that merely start like a field are plain text.
            Assert.False(MessageQuery.Parse("keyboard").IsStructured);
            Assert.False(MessageQuery.Parse("value").IsStructured);
            Assert.False(MessageQuery.Parse("(hello)").IsStructured);
            Assert.False(MessageQuery.Parse("not found").IsStructured);
            Assert.False(MessageQuery.Parse("don't panic").IsStructured);
            Assert.True(MessageQuery.Parse("(key = a)").IsStructured);
            Assert.True(MessageQuery.Parse("not (key = a)").IsStructured);
            Assert.Equal("key equals", MessageQuery.Parse("\"key equals\"").Term);
        });

        runner.Add("Search: query language", "a lone field word or a $-word is a plain search, '~=' is explained", () =>
        {
            foreach (var text in new[] { "json", "header", "$19.99", "$19.99 off", "key" })
            {
                Assert.True(MessageQuery.TryParse(text, null, out var q, out var error), $"{text}: {error}");
                Assert.False(q!.IsStructured, text);
                Assert.Equal(text, q.Term);
            }
            Assert.True(Q("$19.99", Msg("price $19.99 today")));
            Assert.True(MessageQuery.Parse("$.price = 19.99").IsStructured);
            Assert.True(MessageQuery.Parse("$.price exists").IsStructured);

            Assert.False(MessageQuery.TryParse("key ~= abc", null, out _, out var tilde));
            Assert.Contains("unknown operator '~='", tilde!);
            Assert.Contains("did you mean 'matches'", tilde!);
        });

        runner.Add("Search: query language", "Describe round-trips through Parse", () =>
        {
            foreach (var text in new[]
            {
                "json \"$.a\" equals \"x \\\"y\\\"\" and (key = a or key = b)",
                "not (key = a and value contains b)",
                "header \"h\" exists or $.n > 5"
            })
            {
                var parsed = MessageQuery.Parse(text);
                var again = MessageQuery.Parse(MessageQuery.Describe(parsed.Root!));
                Assert.Equal(parsed.Root, again.Root, $"round trip failed for {text}: {MessageQuery.Describe(parsed.Root!)}");
            }
        });

        runner.Add("Search: fields", "FieldSelector parses every documented form", () =>
        {
            Assert.Equal(FieldSelector.Key, FieldSelector.Parse("key"));
            Assert.Equal(FieldSelector.Json("$.a.b"), FieldSelector.Parse("$.a.b"));
            Assert.Equal(FieldSelector.Json("$.a"), FieldSelector.Parse("json \"$.a\""));
            Assert.Equal(FieldSelector.Header("trace-id"), FieldSelector.Parse("header:trace-id"));
            Assert.Equal(FieldSelector.Header("trace id"), FieldSelector.Parse("header \"trace id\""));
            Assert.False(FieldSelector.TryParse("bogus", out _, out _));
            Assert.True(FieldSelector.TryParseList("$.a, $['b,c'], key", out var list, out _));
            Assert.Equal(3, list.Count);
            Assert.Equal("c", FieldSelector.Parse("$['b,c']").Read(Msg("""{"b,c":"c"}""")));
        });

        // ------------------------------------------------------------------ similarity helpers ----

        runner.Add("Search: similarity", "text similarity and volatile field detection", () =>
        {
            Assert.Equal(1.0, TextSimilarity.Ratio("ORD-42 ", "ord_42"));
            Assert.True(TextSimilarity.Ratio("ORD-1042", "ORD-1043") >= 0.8);
            Assert.True(TextSimilarity.Ratio("apple", "orange") < 0.5);
            Assert.Equal(2, TextSimilarity.EditDistance("kitten", "sitting", 5) - 1);
            Assert.True(VolatileFields.IsVolatile("$.createdAt", "x"));
            Assert.True(VolatileFields.IsVolatile("headers.trace-id", "x"));
            Assert.True(VolatileFields.IsVolatile("$.anything", "3f2504e0-4f89-11d3-9a0c-0305e82c3301"));
            Assert.True(VolatileFields.IsVolatile("$.x", "2026-01-10T12:00:00Z"));
            Assert.True(VolatileFields.IsVolatile("$.x", "1767960000000"));
            Assert.False(VolatileFields.IsVolatile("$.format", "json"), "'format' must not look like '...At'");
            Assert.False(VolatileFields.IsVolatile("$.status", "PAID"));
        });

        runner.Add("Search: similarity", "volatile names need 'time'/'date' as a whole word; id-like fields keep their values", () =>
        {
            foreach (var path in new[] { "$.updateCount", "$.candidate", "$.timeout", "$.lifetime", "$.runtime", "$.validate", "$.updated" })
            {
                Assert.False(VolatileFields.IsVolatileName(path), $"{path} must not be volatile");
            }
            foreach (var path in new[] { "$.createdAt", "$.timestamp", "$.event_time", "$.orderDate", "$.ts", "$.lastUpdateTime", "$.meta.datetime" })
            {
                Assert.True(VolatileFields.IsVolatileName(path), $"{path} must be volatile");
            }

            const string uuid = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";
            Assert.True(VolatileFields.IsVolatile("$.orderId", uuid), "value shape still counts for diff hints");
            Assert.False(VolatileFields.IsVolatileForGrouping("$.orderId", uuid));
            Assert.False(VolatileFields.IsVolatileForGrouping("$.accountNumber", "1700000000"));
            Assert.True(VolatileFields.IsVolatileForGrouping("$.anything", uuid));
            Assert.True(VolatileFields.IsVolatileForGrouping("$.traceId", uuid), "a volatile name wins over an id-like suffix");

            // "10-05" keeps the leading-zero rule per part; "A-05" still folds to "a5".
            Assert.Equal("105", TextSimilarity.Normalize("10-05"));
            Assert.Equal("a5", TextSimilarity.Normalize("A-05"));
            Assert.Equal("105", TextSimilarity.Normalize("10 05"));
        });

        runner.Add("Search: similarity", "flattening keeps paths and folds arrays into shapes", () =>
        {
            var leaves = JsonFlattener.Flatten("""{"a":{"b":1},"items":[{"sku":"x"},{"sku":"y"}],"odd key":null,"e":[]}""");
            Assert.Equal("$.a.b=1|$.items[0].sku=x|$.items[1].sku=y|$['odd key']=|$.e=[]",
                string.Join("|", leaves.Select(l => $"{l.Path}={l.Value}")));
            Assert.Equal(4, JsonFlattener.Shape(leaves).Count);
            Assert.False(JsonFlattener.TryFlatten("plain text", out _));
            // Paths produced by the flattener must be readable by the JSON path evaluator.
            Assert.Equal(null, JsonPathEvaluator.Evaluate("""{"odd key":null}""", "$['odd key']"));
            Assert.Equal("y", JsonPathEvaluator.Evaluate("""{"items":[{"sku":"x"},{"sku":"y"}]}""", "$.items[1].sku"));
        });

        // ------------------------------------------------------------------ scanning ----

        runner.Add("Search: scanning", "search across topics honours query and range", async () =>
        {
            var (_, gateway) = Pipeline();
            var result = await MessageSearch.RunAsync(gateway, new SearchRequest
            {
                Topics = AllTopics,
                Query = MessageQuery.Parse("ORD-1")
            }, now: T0.AddMinutes(5));
            Assert.Equal(3, result.Hits.Count);
            Assert.Equal(3, result.TopicsWithHits);
            Assert.Equal(7L, result.Scan.MessagesScanned);

            var between = await MessageSearch.RunAsync(gateway, new SearchRequest
            {
                Topics = AllTopics,
                Query = MessageQuery.Parse("$.status = PAID"),
                Range = SearchRange.Between(T0, T0.AddSeconds(10))
            });
            Assert.Equal(2, between.Hits.Count, "P-9 is outside the window");

            var last = await MessageSearch.RunAsync(gateway, new SearchRequest
            {
                Topics = AllTopics,
                Query = MessageQuery.Parse("ORD"),
                Range = SearchRange.LastPeriod(TimeSpan.FromSeconds(40))
            }, now: T0.AddSeconds(60));
            Assert.Equal(2, last.Hits.Count, "only messages from the last 40s (at t=30 and t=60)");

            var newest = await MessageSearch.RunAsync(gateway, new SearchRequest
            {
                Topics = new[] { "orders" },
                Query = MessageQuery.Parse("ORD"),
                Range = SearchRange.Newest(1)
            });
            Assert.Equal("ORD-3", newest.Hits.Single().Key);
        });

        runner.Add("Search: scanning", "existence check stops at the first hit per topic and summarizes", async () =>
        {
            var (_, gateway) = Pipeline();
            var found = await MessageSearch.RunAsync(gateway, new SearchRequest
            {
                Topics = AllTopics,
                Query = MessageQuery.Parse("PAID"),
                FirstHitPerTopic = true
            });
            Assert.Equal(1, found.Hits.Count, "one hit on payments, then that topic stops");
            Assert.Contains("Found on 1 of 3 topic(s): payments (#0@0", found.Summary(existenceCheck: true));

            var missing = await MessageSearch.RunAsync(gateway, new SearchRequest
            {
                Topics = AllTopics,
                Query = MessageQuery.Parse("ORD-777"),
                FirstHitPerTopic = true
            });
            Assert.Equal(0, missing.Hits.Count);
            Assert.Contains("Not found in 3 topic(s) (scanned 7 message(s))", missing.Summary(true));
        });

        runner.Add("Search: scanning", "a hit cap stops the whole scan", async () =>
        {
            var (_, gateway) = Pipeline();
            var result = await MessageSearch.RunAsync(gateway, new SearchRequest
            {
                Topics = AllTopics,
                Query = MessageQuery.Parse("ORD"),
                MaxHits = 2
            });
            Assert.Equal(2, result.Hits.Count);
            Assert.True(result.Truncated);
            Assert.True(result.Scan.StoppedEarly);
        });

        runner.Add("Search: bulk check", "id lists parse from lines and CSV", () =>
        {
            Assert.Equal("a|b|c|d", string.Join("|", IdMatcher.ParseIdList("a\n b ,c;\"d\"\n\na")));
            Assert.Equal("1|2", string.Join("|", IdMatcher.ParseIdList("id,name\n1,x\n2,y", column: 1, skipHeader: true)));
        });

        runner.Add("Search: bulk check", "reports found, missing and duplicated ids in one pass", async () =>
        {
            var (_, gateway) = Pipeline();
            var anywhere = await BulkExistenceChecker.RunAsync(gateway, AllTopics,
                new[] { "ORD-1", "ORD-2", "ORD-404", "ord-3" }, FieldSelector.Any, SearchRange.All);
            var byId = anywhere.Ids.ToDictionary(i => i.Id);
            Assert.Equal(3, byId["ORD-1"].Count, "orders key + payment + shipment text");
            Assert.Equal("orders, payments, shipments", byId["ORD-1"].TopicsText);
            Assert.Equal(2, byId["ORD-2"].Count);
            Assert.False(byId["ORD-404"].Found);
            Assert.False(byId["ord-3"].Found, "case-sensitive by default");
            Assert.Equal("ORD-404", anywhere.Ids[0].Id.StartsWith("ord") ? anywhere.Ids[1].Id : anywhere.Ids[0].Id);
            Assert.Contains("2 of 4 id(s) found, 2 missing, 2 found more than once", anywhere.Summary);
            Assert.Contains("ORD-404,missing,0", anywhere.ToCsv());

            var byField = await BulkExistenceChecker.RunAsync(gateway, AllTopics,
                new[] { "ORD-1", "ord-3" }, FieldSelector.Json("$.orderId"), SearchRange.All, caseSensitive: false);
            var fieldRows = byField.Ids.ToDictionary(i => i.Id);
            Assert.Equal(2, fieldRows["ORD-1"].Count, "only messages whose $.orderId equals it");
            Assert.Equal(1, fieldRows["ord-3"].Count);

            // Case-insensitively, "ORD-1" and "ord-1" are one id and one row.
            var folded = await BulkExistenceChecker.RunAsync(gateway, AllTopics,
                new[] { "ORD-1", "ord-1" }, FieldSelector.Any, SearchRange.All, caseSensitive: false);
            Assert.Equal(1, folded.Ids.Count);
            Assert.Equal(3, folded.Ids[0].Count);
        });

        runner.Add("Search: bulk check", "ids containing delimiters are found inside payloads", () =>
        {
            var matcher = new IdMatcher(new[] { "urn:order:42", "2024-01-01T10:00:00Z", "a/b/c", "ORD-7" }, FieldSelector.Any);
            var m = Msg("""{"ref":"urn:order:42","at":"2024-01-01T10:00:00Z","path":"x/a/b/c/y","note":"see ORD-7."}""");
            Assert.Equal("2024-01-01T10:00:00Z|ORD-7|a/b/c|urn:order:42", string.Join("|", matcher.Match(m).OrderBy(i => i, StringComparer.Ordinal)));
            Assert.Equal(0, matcher.Match(Msg("""{"ref":"urn:order:4"}""")).Count);
            var loose = new IdMatcher(new[] { "URN:ORDER:42" }, FieldSelector.Any, caseSensitive: false);
            Assert.Equal("URN:ORDER:42", loose.Match(m).Single(), "reported as originally given");
        });

        runner.Add("Search: trace", "follows an id through the pipeline in time order", async () =>
        {
            var (_, gateway) = Pipeline();
            var trace = await IdTracer.RunAsync(gateway, AllTopics, "ORD-1", FieldSelector.Any, SearchRange.All);
            Assert.Equal("orders → payments → shipments", trace.Path);
            Assert.Equal(3, trace.Hops.Count);
            Assert.Equal(TimeSpan.FromSeconds(2), trace.Hops[1].SincePrevious);
            Assert.Equal(TimeSpan.FromSeconds(30), trace.Hops[2].SinceFirst);
            Assert.Equal("+28s", trace.Hops[2].SincePreviousText);
            Assert.Contains("spanning 30s", trace.Summary);

            var none = await IdTracer.RunAsync(gateway, AllTopics, "ORD-777", FieldSelector.Any, SearchRange.All);
            Assert.Contains("was not found", none.Summary);
        });

        runner.Add("Search: reconcile", "joins two topics and reports gaps, mismatches and latency", async () =>
        {
            var (_, gateway) = Pipeline();
            var result = await TopicReconciler.RunAsync(gateway, new ReconcileRequest
            {
                TopicA = "orders",
                JoinA = FieldSelector.Key,
                TopicB = "payments",
                JoinB = FieldSelector.Json("$.orderId"),
                CompareFields = new[] { FieldSelector.Json("$.amount") }
            });
            Assert.Equal(1, result.Matched);
            Assert.Equal(1, result.Mismatched);
            Assert.Equal(1, result.OnlyInA);
            Assert.Equal(1, result.OnlyInB);
            Assert.Equal("ORD-3", result.Rows[0].JoinValue, "problems (only in A) first");
            var mismatch = result.Rows.Single(r => r.Status == ReconcileStatus.Mismatched);
            Assert.Equal("ORD-2", mismatch.JoinValue);
            Assert.Equal("$.amount: 25 ≠ 24", mismatch.Differences);
            Assert.Equal(TimeSpan.FromSeconds(3), mismatch.Latency);
            Assert.Equal(TimeSpan.FromSeconds(2.5), result.AverageLatency);
            Assert.Contains("1 matched, 1 different, 1 only in A, 1 only in B", result.Summary);
            Assert.Contains("ORD-2,different,1,1", result.ToCsv());

            // Compare fields follow the request's case rule (and are trimmed): "EUR" vs "eur" is not a difference.
            var broker = new InMemoryKafkaBroker();
            broker.Append("x", "1", """{"cur":" EUR "}""", null, null, T0);
            broker.Append("y", "1", """{"cur":"eur"}""", null, null, T0.AddSeconds(1));
            var folded = await TopicReconciler.RunAsync(TestKafka.NewGateway(broker), new ReconcileRequest
            {
                TopicA = "x", JoinA = FieldSelector.Key, TopicB = "y", JoinB = FieldSelector.Key,
                CompareFields = new[] { FieldSelector.Json("$.cur") }, CaseSensitive = false
            });
            Assert.Equal(1, folded.Matched);
            Assert.Equal(0, folded.Mismatched);

            Assert.Throws<ArgumentException>(() => TopicReconciler.RunAsync(gateway, new ReconcileRequest
            {
                TopicA = "orders", JoinA = FieldSelector.Key, TopicB = "orders", JoinB = FieldSelector.Key
            }).GetAwaiter().GetResult(), "a topic can't be reconciled with itself");
        });

        runner.Add("Search: scanning", "duplicate topics count once and a 'last' range needs a duration", async () =>
        {
            var (_, gateway) = Pipeline();
            var result = await MessageSearch.RunAsync(gateway, new SearchRequest
            {
                Topics = new[] { "orders", "orders", "payments" },
                Query = MessageQuery.Parse("ORD")
            });
            Assert.Equal(2, result.Scan.TopicsTotal);
            Assert.Equal(2, result.Scan.TopicsScanned);

            var noDuration = new SearchRange { Kind = SearchRangeKind.LastDuration };
            Assert.Throws<ArgumentException>(() => noDuration.ToConsumeOptions("t", "g", T0));
            Assert.Throws<ArgumentException>(() => noDuration.Includes(Msg("v"), T0));
            Assert.Equal("last (no duration)", noDuration.Describe());
        });

        runner.Add("Search: similar", "finds near-duplicates (content) and schema variants (shape)", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gateway = TestKafka.NewGateway(broker);
            void Add(string topic, string value) => broker.Append(topic, null, value, null, null, T0);
            Add("a", """{"orderId":"ORD-1","amount":10,"status":"NEW","createdAt":"2026-01-01T00:00:00Z"}""");
            Add("a", """{"orderId":"ORD-1","amount":10,"status":"NEW","createdAt":"2026-01-02T00:00:00Z"}"""); // retry
            Add("b", """{"orderId":"ORD-l","amount":10,"status":"NEW"}""");  // typo'd id, no timestamp
            Add("b", """{"orderId":"ORD-7","amount":99,"status":"CANCELLED"}""");  // same shape, other values
            Add("b", """{"somethingElse":true}""");

            var reference = await FirstMessage(gateway, "a");
            var content = await SimilarityFinder.RunAsync(gateway, new SimilarityRequest
            {
                Reference = reference,
                Topics = new[] { "a", "b" },
                Threshold = 0.7
            });
            Assert.Equal(2, content.Matches.Count, content.Summary);
            Assert.Equal(1.0, content.Matches[0].Score, "the retry is identical once createdAt is ignored");
            Assert.Equal("b", content.Matches[1].Message.Topic);

            var shape = await SimilarityFinder.RunAsync(gateway, new SimilarityRequest
            {
                Reference = reference,
                Topics = new[] { "a", "b" },
                Mode = SimilarityMode.Shape,
                Threshold = 0.99,
                IgnoreVolatileFields = false
            });
            Assert.Equal(1, shape.Matches.Count, "only the retry has exactly the same fields");
            var looseShape = await SimilarityFinder.RunAsync(gateway, new SimilarityRequest
            {
                Reference = reference, Topics = new[] { "a", "b" }, Mode = SimilarityMode.Shape, Threshold = 0.7
            });
            Assert.Equal(3, looseShape.Matches.Count);
        });

        runner.Add("Search: duplicates", "groups by key, value, volatile-insensitive value and fields", () =>
        {
            var messages = new[]
            {
                Msg("""{"id":1,"ts":"2026-01-01T00:00:00Z"}""", key: "a", offset: 0, at: T0),
                Msg("""{"id":1,"ts":"2026-01-01T00:00:09Z"}""", key: "a", offset: 1, at: T0.AddSeconds(9)),
                Msg("""{"id":2}""", key: "b", offset: 2),
                Msg("""{"id":2}""", key: "c", offset: 3),
                Msg(null, key: null, offset: 4)
            };

            var byKey = new DuplicateDetector(DuplicateGroupBy.Key);
            byKey.AddRange(messages);
            Assert.Equal("a", byKey.GetDuplicates().Single().GroupKey);
            Assert.Equal(TimeSpan.FromSeconds(9), byKey.GetDuplicates()[0].Span);
            Assert.Equal(1, byKey.Ungroupable);

            var byValue = new DuplicateDetector(DuplicateGroupBy.Value);
            byValue.AddRange(messages);
            Assert.Equal(1, byValue.GetDuplicates().Count, "only the two {\"id\":2}");

            var retries = new DuplicateDetector(DuplicateGroupBy.ValueIgnoringVolatile);
            retries.AddRange(messages);
            Assert.Equal(1, retries.GetDuplicates().Count, "key a with different ts; b/c differ by key");
            Assert.Equal(2, retries.GetDuplicates()[0].Count);

            var byField = new DuplicateDetector(DuplicateGroupBy.Fields, new[] { FieldSelector.Json("$.id") });
            byField.AddRange(messages);
            Assert.Equal(2, byField.GetDuplicates().Count);
            Assert.Equal(2, byField.ExtraCopies);
        });

        runner.Add("Search: duplicates", "ids are not volatile, null and empty keys differ, FirstSeen covers every copy", () =>
        {
            var orders = new DuplicateDetector(DuplicateGroupBy.ValueIgnoringVolatile);
            orders.AddRange(new[]
            {
                Msg("""{"orderId":"3f2504e0-4f89-11d3-9a0c-0305e82c3301","amount":10,"createdAt":"2026-01-01T00:00:00Z"}""", offset: 0),
                Msg("""{"orderId":"9b2e6f1a-1c2d-4e3f-8a9b-0c1d2e3f4a5b","amount":10,"createdAt":"2026-01-01T00:00:00Z"}""", offset: 1),
                Msg("""{"orderId":"9b2e6f1a-1c2d-4e3f-8a9b-0c1d2e3f4a5b","amount":10,"createdAt":"2026-01-01T00:00:07Z"}""", offset: 2)
            });
            var group = orders.GetDuplicates().Single();
            Assert.Equal(2, group.Count, "different orderIds are different orders; a different createdAt is a retry");
            Assert.Equal("1, 2", string.Join(", ", group.Messages.Select(m => m.Offset)));

            var keyed = new DuplicateDetector(DuplicateGroupBy.KeyAndValue);
            keyed.AddRange(new[] { Msg("v", key: null, offset: 0), Msg("v", key: "", offset: 1), Msg("v", key: null, offset: 2) });
            Assert.Equal(1, keyed.GetDuplicates().Count, "null and empty keys are different groups");
            Assert.Equal(2, keyed.GetDuplicates()[0].Count);

            // The oldest copy arrives after the first MaxMessagesPerGroup copies have already been kept.
            var late = new DuplicateDetector(DuplicateGroupBy.Value);
            late.AddRange(Enumerable.Range(0, DuplicateDetector.MaxMessagesPerGroup + 1)
                .Select(i => Msg("same", offset: i, at: i == DuplicateDetector.MaxMessagesPerGroup ? T0.AddMinutes(-5) : T0.AddSeconds(i))));
            var lateGroup = late.GetDuplicates().Single();
            Assert.Equal(T0.AddMinutes(-5), lateGroup.FirstSeen);
            Assert.Equal(T0.AddSeconds(DuplicateDetector.MaxMessagesPerGroup - 1), lateGroup.LastSeen);
        });

        runner.Add("Search: field stats", "counts presence, nulls, distinct and top values", () =>
        {
            var stats = new FieldStatisticsCollector();
            stats.Add(Msg("""{"status":"PAID","amount":1,"items":[{"sku":"a"},{"sku":"b"}]}""", key: "k1", at: T0));
            stats.Add(Msg("""{"status":"PAID","amount":null}""", key: "k2", at: T0.AddSeconds(1)));
            stats.Add(Msg("""{"status":"FAILED"}""", at: T0.AddSeconds(2)));
            var byPath = stats.GetStatistics().ToDictionary(s => s.Path);

            Assert.Equal(3, stats.MessageCount);
            Assert.Equal(3, byPath["$.status"].Present);
            Assert.Equal(2, byPath["$.status"].Distinct);
            Assert.Equal("PAID", byPath["$.status"].TopValues[0].Value);
            Assert.Equal(2, byPath["$.status"].TopValues[0].Count);
            Assert.Equal(1, byPath["$.amount"].Nulls);
            Assert.Equal(1, byPath["$.amount"].Missing);
            Assert.True(byPath["$.amount"].IsSometimesMissing);
            Assert.Equal(1, byPath["$.items[*].sku"].Present, "array folded, counted once per message");
            Assert.Null(byPath["$.items[*].sku"].Selector);
            Assert.Equal(FieldSelector.Json("$.status"), byPath["$.status"].Selector);
            Assert.Equal(2, byPath["key"].Present);
            Assert.Equal("key", stats.GetStatistics()[0].Path);
        });

        runner.Add("Search: diff", "structural diff with volatile and ignored fields", () =>
        {
            var left = Msg("""{"id":"A","amount":10,"meta":{"ts":"2026-01-01T00:00:00Z","v":1},"gone":true}""", key: "k",
                headers: new() { ["trace-id"] = "1" });
            var right = Msg("""{"id":"A","amount":12,"meta":{"ts":"2026-01-02T00:00:00Z","v":1},"new":"x"}""", key: "k",
                headers: new() { ["trace-id"] = "2" });

            var full = JsonDiff.Compare(left, right);
            Assert.Equal(3, full.Changed, "amount, meta.ts, trace-id header");
            Assert.Equal(1, full.Added);
            Assert.Equal(1, full.Removed);
            Assert.Equal("$.amount:10:12", string.Join(",", full.Entries.Where(e => e.Path == "$.amount").Select(e => $"{e.Path}:{e.Left}:{e.Right}")));

            var quiet = JsonDiff.Compare(left, right, new DiffOptions { IgnoreVolatile = true, IgnorePaths = new[] { "new", "gone" } });
            Assert.Equal(1, quiet.Changed, "only amount remains");
            Assert.Equal(2, quiet.IgnoredVolatile);
            Assert.Contains("1 changed", quiet.Summary);

            var same = JsonDiff.Compare(left, left, new DiffOptions { IncludeUnchanged = true });
            Assert.True(same.IsIdentical);
            Assert.True(same.Entries.Count > 0);
        });

        // ------------------------------------------------------------------ script generation ----

        runner.Add("Search: to KafScript", "generated scripts parse and pass against the broker", async () =>
        {
            var (broker, gateway) = Pipeline();
            var query = MessageQuery.Parse("$.orderId = ord-2 and $.status starts with PA and key contains \"P-\"");
            Assert.True(KafScriptGenerator.TryGenerate(query, new KafScriptGenerator.Options
            {
                Connection = "local",
                Topics = new[] { "payments" },
                Name = "Payment for ORD-2 exists",
                WithinSeconds = 2
            }, out var script, out var error), error ?? "");
            Assert.Contains("Scenario: Payment for ORD-2 exists", script);
            Assert.Contains("matches \"(?i)^ord-2\\\\z\"", script, "anchored with \\z, not $ (which also matches before a trailing newline)");

            var document = Parser.Parse(script);
            var runnerScript = new ScriptRunner(new Dictionary<string, IKafkaGateway> { ["local"] = gateway });
            var result = await runnerScript.RunAsync(document.Blocks[0]);
            Assert.True(result.Success, result.Summary);

            // A search for something that doesn't exist generates a failing check.
            var missing = MessageQuery.Parse("json \"$.orderId\" equals \"ORD-404\"", new QueryOptions { CaseSensitive = true });
            Assert.True(KafScriptGenerator.TryGenerate(missing, new KafScriptGenerator.Options
            {
                Connection = "local", Topics = new[] { "payments" }, WithinSeconds = 1
            }, out var failing, out _));
            Assert.Contains("json \"$.orderId\" equals \"ORD-404\"", failing);
            var failResult = await runnerScript.RunAsync(Parser.Parse(failing).Blocks[0]);
            Assert.False(failResult.Success);

            // Tasks, plain text and regex escaping.
            Assert.True(KafScriptGenerator.TryGenerate(MessageQuery.Parse("a\"b\\c", new QueryOptions { CaseSensitive = true }),
                new KafScriptGenerator.Options { Connection = "local", Topics = new[] { "x", "y" }, TaskSchedule = "every 5 minutes" },
                out var task, out _));
            Assert.Contains("Task: Data exists on y", task);
            Assert.Contains("value contains \"a\\\"b\\\\c\"", task);
            Assert.Equal(2, Parser.Parse(task).Blocks.Count);
            Assert.Equal("Exists status PAID or key ORD-2", KafScriptGenerator.BlockName("Exists: $.status = \"PAID\" or key = 'ORD-2'"));
            Assert.Equal("Data exists", KafScriptGenerator.BlockName("$$$"));
        });

        runner.Add("Search: to KafScript", "untranslatable searches explain why", () =>
        {
            var opts = new KafScriptGenerator.Options { Connection = "c", Topics = new[] { "t" } };
            Assert.False(KafScriptGenerator.TryGenerate(MessageQuery.Parse("key = a or key = b"), opts, out _, out var e1));
            Assert.Contains("'and'", e1!);
            Assert.False(KafScriptGenerator.TryGenerate(MessageQuery.Parse("header h = x"), opts, out _, out var e2));
            Assert.Contains("only key, value and json", e2!);
            Assert.False(KafScriptGenerator.TryGenerate(MessageQuery.Parse("x", new QueryOptions { Mode = TextMatchMode.Fuzzy }), opts, out _, out var e3));
            Assert.Contains("fuzzy", e3!);
            Assert.False(KafScriptGenerator.TryGenerate(MessageQuery.Parse("x"), opts with { Topics = Array.Empty<string>() }, out _, out _));
        });

        // ------------------------------------------------------------------ persistence ----

        runner.Add("Search: persistence", "saved searches and history survive a reload", () =>
        {
            var saved = new SavedSearch
            {
                Name = "failed payments",
                Query = "$.status = FAILED",
                Mode = TextMatchMode.WholeWord,
                Fields = SearchFields.Key | SearchFields.Value,
                RangeKind = SearchRangeKind.LastDuration,
                LastMinutes = 15,
                TopicSetName = "payments"
            };
            Assert.Null(SavedSearchStore.Save(new[] { saved }));
            var loaded = SavedSearchStore.Load().Single();
            Assert.Equal(saved, loaded);
            Assert.Equal(TimeSpan.FromMinutes(15), loaded.ToRange().Last);
            Assert.Equal(SearchFields.Key | SearchFields.Value, loaded.ToQueryOptions().Fields);

            var history = SearchHistoryStore.Push(SearchHistoryStore.Push(new[] { "a", "b" }, "c"), "a");
            Assert.Equal("a|c|b", string.Join("|", history));
            Assert.Null(SearchHistoryStore.Save(history));
            Assert.Equal("a|c|b", string.Join("|", SearchHistoryStore.Load()));
        });

        runner.Add("Search: persistence", "CSV escaping", () =>
        {
            Assert.Equal("a,\"b,c\",\"d\"\"e\"\r\n", Csv.Write(new[] { "a", "b,c", "d\"e" }, Array.Empty<string[]>()));
            Assert.Contains("t,0,0,", Csv.FromMessages(new[] { Msg("v", key: "k") }));
            // Formula-looking cells are defused; numbers are not.
            Assert.Equal("\"'=1+1\"", Csv.Escape("=1+1"));
            Assert.Equal("\"'@SUM(A1)\"", Csv.Escape("@SUM(A1)"));
            Assert.Equal("\"'-cmd|calc\"", Csv.Escape("-cmd|calc"));
            Assert.Equal("\"'+abc\"", Csv.Escape("+abc"));
            Assert.Equal("\"'=\"\"x\"\"\"", Csv.Escape("=\"x\""));
            Assert.Equal("-5", Csv.Escape("-5"));
            Assert.Equal("+1.5e3", Csv.Escape("+1.5e3"));
            Assert.Equal("ORD-1", Csv.Escape("ORD-1"));
        });
    }

    private static async Task<KafkaMessage> FirstMessage(IKafkaGateway gateway, string topic)
    {
        await foreach (var m in gateway.ConsumeAsync(new ConsumeOptions
        {
            Topic = topic,
            ConsumerGroup = Guid.NewGuid().ToString("N"),
            StartPosition = ConsumeStartPosition.Earliest,
            StopAtPartitionEnd = true
        }))
        {
            return m;
        }
        throw new InvalidOperationException("topic is empty");
    }
}

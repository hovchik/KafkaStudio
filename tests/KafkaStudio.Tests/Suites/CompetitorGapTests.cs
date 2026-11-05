using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using KafkaStudio.App.ViewModels.Admin;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Connections;
using KafkaStudio.Core.Display;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Core.Schemas;
using KafkaStudio.Core.Testing;
using KafkaStudio.Core.Topics;
using KafkaStudio.Tests.Harness;

namespace KafkaStudio.Tests.Suites;

/// <summary>Value formats, field masking, Schema Registry / Avro / Protobuf decoding, topic admin, ACLs, Connect, rates, lag history.</summary>
public static class CompetitorGapTests
{
    private const string Suite = "Competitor gaps";

    private sealed class StubHandler(Func<string, (HttpStatusCode, string)> respond) : HttpMessageHandler
    {
        public int Calls;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Calls);
            var (status, body) = respond(request.RequestUri!.PathAndQuery);
            return Task.FromResult(new HttpResponseMessage(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") });
        }
    }

    private static byte[] ZigZag(long value)
    {
        var n = (ulong)((value << 1) ^ (value >> 63));
        var bytes = new List<byte>();
        while (n >= 0x80) { bytes.Add((byte)(n | 0x80)); n >>= 7; }
        bytes.Add((byte)n);
        return bytes.ToArray();
    }

    private static byte[] AvroString(string s)
    {
        var data = Encoding.UTF8.GetBytes(s);
        return ZigZag(data.Length).Concat(data).ToArray();
    }

    private static KafkaMessage Raw(byte[] bytes, IReadOnlyDictionary<string, string>? headers = null) => new()
    {
        Topic = "t", Partition = 0, Offset = 0, Timestamp = DateTimeOffset.UnixEpoch,
        RawValue = bytes, Value = KafkaMessage.DecodeText(bytes), Headers = headers ?? new Dictionary<string, string>()
    };

    private static KafkaMessage Json(string json, IReadOnlyDictionary<string, string>? headers = null) => new()
    {
        Topic = "t", Partition = 0, Offset = 0, Timestamp = DateTimeOffset.UnixEpoch,
        Value = json, RawValue = Encoding.UTF8.GetBytes(json), Headers = headers ?? new Dictionary<string, string>()
    };

    private const string OrderSchema = """
        {"type":"record","name":"Order","namespace":"shop","fields":[
          {"name":"id","type":"long"},
          {"name":"customer","type":"string"},
          {"name":"note","type":["null","string"]},
          {"name":"status","type":{"type":"enum","name":"Status","symbols":["NEW","PAID"]}},
          {"name":"tags","type":{"type":"array","items":"string"}},
          {"name":"attrs","type":{"type":"map","values":"int"}},
          {"name":"ok","type":"boolean"},
          {"name":"price","type":"double"}
        ]}
        """;

    private static byte[] OrderBytes() =>
        ZigZag(42).Concat(AvroString("ada"))
            .Concat(ZigZag(1)).Concat(AvroString("rush"))      // union branch 1 = string
            .Concat(ZigZag(1))                                  // enum PAID
            .Concat(ZigZag(2)).Concat(AvroString("a")).Concat(AvroString("b")).Concat(ZigZag(0))
            .Concat(ZigZag(1)).Concat(AvroString("k")).Concat(ZigZag(7)).Concat(ZigZag(0))
            .Concat(new byte[] { 1 })
            .Concat(BitConverter.GetBytes(12.5))
            .ToArray();

    public static void Register(TestRunner runner)
    {
        runner.Add(Suite, "value formats re-render the exact bytes and leave other messages alone", () =>
        {
            var i32 = Raw(new byte[] { 0, 0, 1, 0 });
            Assert.Equal("256", ValueFormatter.Apply(i32, ValueFormat.Int32).Value);
            Assert.Equal("00000100", ValueFormatter.Apply(i32, ValueFormat.Hex).Value);
            Assert.Equal("AAABAA==", ValueFormatter.Apply(i32, ValueFormat.Base64).Value);
            // wrong length for the format: unchanged rather than guessed
            Assert.True(ReferenceEquals(i32, ValueFormatter.Apply(i32, ValueFormat.Int64)));
            Assert.Equal("1", ValueFormatter.Apply(Raw(new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 }), ValueFormat.Int64).Value);
            Assert.True(ReferenceEquals(i32, ValueFormatter.Apply(i32, ValueFormat.Auto)));
        });

        runner.Add(Suite, "masking hides paths, wildcards, names at any depth and headers", () =>
        {
            var masker = new FieldMasker(["$.card.number", "$.items[*].email", "password", "header:Authorization"]);
            var msg = Json("""{"card":{"number":"4111","holder":"A"},"items":[{"email":"a@x"},{"email":"b@x"}],"user":{"password":"p"},"id":1}""",
                new Dictionary<string, string> { ["authorization"] = "Bearer abc", ["x"] = "y" });
            var masked = masker.Apply(msg);
            var node = JsonNode.Parse(masked.Value!)!;
            Assert.Equal(FieldMasker.Mask, (string)node["card"]!["number"]!);
            Assert.Equal("A", (string)node["card"]!["holder"]!);
            Assert.Equal(FieldMasker.Mask, (string)node["items"]![1]!["email"]!);
            Assert.Equal(FieldMasker.Mask, (string)node["user"]!["password"]!);
            Assert.Equal(1, (int)node["id"]!);
            Assert.Equal(FieldMasker.Mask, masked.Headers["authorization"]);
            Assert.Equal("y", masked.Headers["x"]);
            // the original is untouched
            Assert.Contains("4111", msg.Value!);
            // non-JSON and empty rules pass through
            Assert.True(ReferenceEquals(msg, new FieldMasker([]).Apply(msg)));
            Assert.Equal("plain text", masker.Apply(Json("plain text")).Value);
        });

        runner.Add(Suite, "Avro decoder reads records, unions, enums, arrays, maps and primitives", () =>
        {
            var json = new AvroDecoder(OrderSchema).Decode(OrderBytes())!;
            Assert.Equal(42L, (long)json["id"]!);
            Assert.Equal("ada", (string)json["customer"]!);
            Assert.Equal("rush", (string)json["note"]!);
            Assert.Equal("PAID", (string)json["status"]!);
            Assert.Equal("a,b", string.Join(",", json["tags"]!.AsArray().Select(t => (string)t!)));
            Assert.Equal(7, (int)json["attrs"]!["k"]!);
            Assert.True((bool)json["ok"]!);
            Assert.Equal(12.5, (double)json["price"]!);
            Assert.Throws<FormatException>(() => new AvroDecoder(OrderSchema).Decode(new byte[] { 2 }));
        });

        runner.Add(Suite, "Avro decoder resolves named type references and null unions", () =>
        {
            const string schema = """
                {"type":"record","name":"Outer","fields":[
                  {"name":"a","type":{"type":"record","name":"Inner","fields":[{"name":"n","type":"int"}]}},
                  {"name":"b","type":"Inner"},
                  {"name":"c","type":["null","Inner"]}]}
                """;
            var data = ZigZag(1).Concat(ZigZag(2)).Concat(ZigZag(0)).ToArray();
            var json = new AvroDecoder(schema).Decode(data)!;
            Assert.Equal(1, (int)json["a"]!["n"]!);
            Assert.Equal(2, (int)json["b"]!["n"]!);
            Assert.True(json["c"] is null);
        });

        runner.Add(Suite, "Protobuf wire decoder shows fields by number, nested messages and text", () =>
        {
            // field 1 varint 150, field 2 string "hi", field 3 nested { field 1 varint 1 }
            var bytes = new byte[] { 0x08, 0x96, 0x01, 0x12, 0x02, (byte)'h', (byte)'i', 0x1A, 0x02, 0x08, 0x01 };
            var json = ProtobufWireDecoder.Decode(bytes);
            Assert.Equal(150UL, (ulong)json["1"]!);
            Assert.Equal("hi", (string)json["2"]!);
            Assert.Equal(1UL, (ulong)json["3"]!["1"]!);
            Assert.Throws<FormatException>(() => ProtobufWireDecoder.Decode(new byte[] { 0x08 }));
        });

        runner.Add(Suite, "Schema Registry decoder turns wire-format values into JSON and caches schemas", async () =>
        {
            var handler = new StubHandler(path => path.StartsWith("/schemas/ids/7")
                ? (HttpStatusCode.OK, new JsonObject { ["schema"] = OrderSchema }.ToJsonString())
                : path.StartsWith("/schemas/ids/8")
                    ? (HttpStatusCode.OK, """{"schemaType":"PROTOBUF","schema":"syntax = \"proto3\";"}""")
                    : (HttpStatusCode.NotFound, """{"error_code":40403,"message":"Schema not found"}"""));
            using var client = new SchemaRegistryClient("http://registry:8081", handler: handler);
            var decoder = new SchemaRegistryDecoder(client);

            byte[] Wire(int id, byte[] payload) => new byte[] { 0 }.Concat(new byte[] { 0, 0, 0, (byte)id }).Concat(payload).ToArray();

            var avro = await decoder.DecodeAsync(Raw(Wire(7, OrderBytes())));
            Assert.Equal("ada", (string)JsonNode.Parse(avro.Value!)!["customer"]!);
            Assert.Equal("AVRO #7", avro.Headers[SchemaRegistryDecoder.SchemaHeader]);
            await decoder.DecodeAsync(Raw(Wire(7, OrderBytes())));
            Assert.Equal(1, handler.Calls);

            var proto = await decoder.DecodeAsync(Raw(Wire(8, new byte[] { 0, 0x08, 0x05 })));
            Assert.Equal(5UL, (ulong)JsonNode.Parse(proto.Value!)!["1"]!);

            var missing = Raw(Wire(9, new byte[] { 1, 2 }));
            var failed = await decoder.DecodeAsync(missing);
            Assert.Contains("schema #9", failed.Headers[SchemaRegistryDecoder.ErrorHeader]);
            Assert.True(ReferenceEquals(missing.RawValue, failed.RawValue));

            var plain = Json("""{"a":1}""");
            Assert.True(ReferenceEquals(plain, await decoder.DecodeAsync(plain)));
        });

        runner.Add(Suite, "Schema Registry client lists subjects and versions", async () =>
        {
            var handler = new StubHandler(path => path switch
            {
                "/subjects" => (HttpStatusCode.OK, """["b-value","a-value"]"""),
                "/subjects/a-value/versions" => (HttpStatusCode.OK, "[1,2]"),
                "/subjects/a-value/versions/2" => (HttpStatusCode.OK, """{"id":5,"schema":"{}"}"""),
                _ => (HttpStatusCode.InternalServerError, "boom")
            });
            using var client = new SchemaRegistryClient("http://registry:8081", "u", "p", handler);
            Assert.Equal("a-value,b-value", string.Join(",", await client.ListSubjectsAsync()));
            Assert.Equal("1,2", string.Join(",", await client.ListVersionsAsync("a-value")));
            var v = await client.GetVersionAsync("a-value", 2);
            Assert.Equal(5, v.Id);
            Assert.Equal("AVRO", v.SchemaType);
            await Assert.ThrowsAsync<HttpRequestException>(() => client.GetByIdAsync(99));
        });

        runner.Add(Suite, "Kafka Connect response parsing flags failed connectors and tasks", () =>
        {
            const string body = """
                {"sink-a":{"info":{"name":"sink-a","type":"sink","config":{"topics":"orders","connector.class":"X"}},
                  "status":{"name":"sink-a","connector":{"state":"RUNNING","worker_id":"w1:8083"},"type":"sink",
                  "tasks":[{"id":0,"state":"RUNNING","worker_id":"w1:8083"},{"id":1,"state":"FAILED","worker_id":"w1:8083","trace":"java.lang.Boom"}]}},
                 "src-b":{"info":{"name":"src-b","type":"source","config":{}},
                  "status":{"name":"src-b","connector":{"state":"RUNNING","worker_id":"w2"},"type":"source","tasks":[]}}}
                """;
            var list = KafkaConnectClient.Parse(body);
            Assert.Equal(2, list.Count);
            Assert.Equal("sink-a", list[0].Name);
            Assert.True(list[0].IsFailed);
            Assert.Equal("java.lang.Boom", list[0].Tasks[1].Trace);
            Assert.Equal("orders", list[0].Config["topics"]);
            Assert.False(list[1].IsFailed);
        });

        runner.Add(Suite, "rate sampler computes messages per second between samples", () =>
        {
            var sampler = new TopicRateSampler();
            var t0 = DateTimeOffset.UnixEpoch;
            var first = sampler.Add(new Dictionary<string, long> { ["a"] = 100, ["b"] = 5 }, t0);
            Assert.Equal(0.0, first.Max(r => r.MessagesPerSecond));
            var second = sampler.Add(new Dictionary<string, long> { ["a"] = 150, ["b"] = 5, ["c"] = 9 }, t0.AddSeconds(5));
            Assert.Equal("a", second[0].Topic);
            Assert.Equal(10.0, second[0].MessagesPerSecond);
            Assert.Equal(50L, second[0].NewMessages);
            Assert.Equal(0L, second.First(r => r.Topic == "c").NewMessages);
        });

        runner.Add(Suite, "lag history draws a sparkline and a trend", () =>
        {
            var h = new LagHistory(capacity: 4);
            Assert.Equal("", h.Summary());
            h.Add(0, DateTimeOffset.UnixEpoch);
            Assert.Equal(null, h.Trend());
            foreach (var lag in new long[] { 10, 20, 30, 70 }) h.Add(lag, DateTimeOffset.UnixEpoch);
            Assert.Equal(4, h.Samples.Count);
            Assert.Equal("growing", h.Trend());
            Assert.Equal(4, h.Sparkline().Length);
            Assert.Equal('▁', h.Sparkline()[0]);
            Assert.Equal('█', h.Sparkline()[3]);
            h.Add(10, DateTimeOffset.UnixEpoch);
            Assert.Equal("draining", h.Trend());
        });

        runner.Add(Suite, "demo broker: topic configs, compaction switch, delete topic, grow partitions, delete group, ACLs", async () =>
        {
            var broker = new InMemoryKafkaBroker();
            var gw = TestKafka.NewGateway(broker);
            await gw.CreateTopicAsync("t", 2, 1);

            var before = await gw.GetTopicConfigAsync("t");
            Assert.True(before.First(c => c.Name == "cleanup.policy").IsDefault);
            await gw.AlterTopicConfigAsync("t", new Dictionary<string, string?> { ["cleanup.policy"] = "compact", ["retention.ms"] = "1000" });
            var after = await gw.GetTopicConfigAsync("t");
            Assert.Equal("compact", after.First(c => c.Name == "cleanup.policy").Value);
            Assert.False(after.First(c => c.Name == "retention.ms").IsDefault);
            Assert.True(await gw.IsTopicCompactedAsync("t"));
            await gw.AlterTopicConfigAsync("t", new Dictionary<string, string?> { ["cleanup.policy"] = null });
            Assert.False(await gw.IsTopicCompactedAsync("t"));
            await Assert.ThrowsAsync<KeyNotFoundException>(() => gw.GetTopicConfigAsync("nope"));

            await gw.CreatePartitionsAsync("t", 5);
            Assert.Equal(5, (await gw.DescribeTopicAsync("t")).Partitions.Count);
            await Assert.ThrowsAsync<InvalidOperationException>(() => gw.CreatePartitionsAsync("t", 3));
            var receipt = await gw.ProduceAsync(new ProduceRequest { Topic = "t", Value = "x", Partition = 4 });
            Assert.Equal(4, receipt.Partition);

            // a group with a commit can be deleted; a group with live members cannot
            broker.Commit("t", "g1", 0, 4);
            broker.SimulateActiveMembers("busy", 1);
            await Assert.ThrowsAsync<InvalidOperationException>(() => gw.DeleteConsumerGroupAsync("busy"));
            await gw.DeleteConsumerGroupAsync("g1");
            Assert.False((await gw.ListConsumerGroupsAsync()).Any(g => g.GroupId == "g1"));
            await Assert.ThrowsAsync<KeyNotFoundException>(() => gw.DeleteConsumerGroupAsync("g1"));

            Assert.Equal(0, (await gw.ListAclsAsync()).Count);
            broker.AddAcl(new AclEntry { ResourceType = "Topic", ResourceName = "t", Principal = "User:ada", Operation = "Read", Permission = "Allow" });
            Assert.Equal("User:ada", (await gw.ListAclsAsync())[0].Principal);

            await gw.DeleteTopicAsync("t");
            Assert.False((await gw.ListTopicsAsync()).Contains("t"));
            await Assert.ThrowsAsync<KeyNotFoundException>(() => gw.DeleteTopicAsync("t"));
        });

        runner.Add(Suite, "reader gateway masks and decodes for display while the plain gateway keeps the real bytes", async () =>
        {
            var state = new AppState();
            state.AddConnection(
                new ConnectionProfile { Name = "m", BootstrapServers = "x", MaskedFields = ["password"] },
                TestKafka.NewGateway(new InMemoryKafkaBroker(), "m"));
            var plain = state.Connections["m"];
            await plain.ProduceAsync(new ProduceRequest { Topic = "u", Value = """{"user":"a","password":"secret"}""" });

            Assert.True(state.TryGetReader("m", out var reader));
            var seen = new List<KafkaMessage>();
            await foreach (var m in reader.ConsumeAsync(new ConsumeOptions
            {
                Topic = "u", ConsumerGroup = "g", StartPosition = ConsumeStartPosition.Earliest, StopAtPartitionEnd = true
            })) seen.Add(m);
            Assert.Equal(1, seen.Count);
            Assert.False(seen[0].Value!.Contains("secret"));
            Assert.Equal("true", seen[0].Headers[DisplayPipeline.MaskedHeader]);

            var raw = new List<KafkaMessage>();
            await foreach (var m in plain.ConsumeAsync(new ConsumeOptions
            {
                Topic = "u", ConsumerGroup = "g2", StartPosition = ConsumeStartPosition.Earliest, StopAtPartitionEnd = true
            })) raw.Add(m);
            Assert.Contains("secret", raw[0].Value!);

            state.DisplayFormat = ValueFormat.Hex;
            var hex = new List<KafkaMessage>();
            await foreach (var m in reader.ConsumeAsync(new ConsumeOptions
            {
                Topic = "u", ConsumerGroup = "g3", StartPosition = ConsumeStartPosition.Earliest, StopAtPartitionEnd = true
            })) hex.Add(m);
            Assert.True(hex[0].Value!.StartsWith("7b22"), "value shown as hex");
        });

        runner.Add(Suite, "Cluster Admin: destructive actions wait for a confirmation", async () =>
        {
            var state = new AppState();
            var broker = new InMemoryKafkaBroker();
            state.AddConnection(new ConnectionProfile { Name = "c", BootstrapServers = "x" }, TestKafka.NewGateway(broker, "c"));
            await state.Connections["c"].CreateTopicAsync("orders", 1, 1);

            var vm = new ClusterAdminViewModel(state);
            vm.SelectedConnection = "c";
            await vm.RefreshCommand.ExecuteAsync();
            vm.SelectedTopic = "orders";
            await Task.Delay(100);
            Assert.True(vm.Config.Count > 0);

            vm.DeleteTopicCommand.Execute(null);
            Assert.True(vm.HasPending);
            Assert.True(broker.TopicExists("orders"));
            vm.CancelCommand.Execute(null);
            Assert.True(broker.TopicExists("orders"));

            vm.DeleteTopicCommand.Execute(null);
            await vm.ConfirmCommand.ExecuteAsync();
            Assert.False(broker.TopicExists("orders"));

            // edit a setting, apply
            await state.Connections["c"].CreateTopicAsync("t2", 1, 1);
            await vm.RefreshCommand.ExecuteAsync();
            vm.SelectedTopic = "t2";
            await Task.Delay(100);
            vm.Config.First(r => r.Name == "retention.ms").Value = "5000";
            await vm.ApplyConfigCommand.ExecuteAsync();
            Assert.Equal("5000", broker.GetTopicConfigOverrides("t2")["retention.ms"]);
        });

        runner.Add(Suite, "editing a masked or decoded message in the Producer never resends the hidden or rewritten value", () =>
        {
            var state = new AppState();
            var producer = new KafkaStudio.App.ViewModels.Producer.ProducerViewModel(state);
            producer.LoadMessage(null, Json("""{"password":"••••••"}""", new Dictionary<string, string>
            {
                [DisplayPipeline.MaskedHeader] = "true", ["trace"] = "1"
            }));
            Assert.Equal("", producer.Value);
            Assert.Equal(1, producer.Headers.Count);
            Assert.Equal("trace", producer.Headers[0].Name);
        });
    }
}

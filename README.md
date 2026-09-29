# KafkaStudio

A Windows desktop IDE for working with Kafka day-to-day: browse and scan topics, produce/consume
messages, find data (does it exist, where, and is there anything similar?), and - the part a generic tool like Offset Explorer doesn't do - write checks and automation in
a small, readable scripting language called **KafScript**. It covers the three workflows this project
was built around:

- **Rethrow** - relay a message from one topic to another (one-shot in a script, or continuously via
  the Rethrow Rules screen).
- **Scan and acknowledge** - bulk-read a topic's backlog and acknowledge what you've handled.
- **Cross-topic timing checks** - "when a message is produced on topic A, a related message should
  show up on topic B within N seconds" - written as a readable assertion, not custom code per check.

Built with .NET 10 and Avalonia UI.

## What's in the box

| Project | What it is | External NuGet packages |
|---|---|---|
| `KafkaStudio.Core` | Domain models + the `IKafkaGateway` abstraction everything else is built on, plus an in-memory fake broker/gateway used for tests and "offline demo" mode. | none |
| `KafkaStudio.Scripting` | KafScript: lexer, parser, AST, and the interpreter that runs a parsed script against an `IKafkaGateway`. | none |
| `KafkaStudio.Automation` | The scheduler for `Task` blocks, the rethrow engine/manager, run history, and a loader for `.kafscript` files. | none |
| `KafkaStudio.Search` | Data finding: a query language for messages, existence and bulk id checks, traces, two-topic reconciliation, similarity, duplicates, field statistics, structural diffs, search → KafScript. | none |
| `KafkaStudio.App.ViewModels` | All UI state and logic (MVVM), framework-agnostic. | none |
| `KafkaStudio.Kafka` | The real Kafka client: `ConfluentKafkaGateway`, an `IKafkaGateway` implementation over Confluent.Kafka/librdkafka. | Confluent.Kafka |
| `KafkaStudio.App` | The Avalonia desktop app: windows, views, styling. | Avalonia, Avalonia.Desktop, Avalonia.Themes.Fluent, Avalonia.Fonts.Inter |
| `KafkaStudio.Tests` | A self-contained test suite (108 tests) covering the language, interpreter, scheduler, rethrow engine, data search engine, ViewModels, the sample scripts, and regression tests for every fixed bug. | none (see below) |

Six of the eight projects - Core, Scripting, Automation, Search, App.ViewModels, and Tests, which together are
the engine that does the actual "checks and automation" work this app exists for - have **zero external
dependencies** and build with nothing but the .NET SDK.

## What's verified, and what isn't (read this before judging "does it work")

This solution was built in a sandboxed environment that could reach GitHub, npm, and PyPI, but **not
nuget.org**. That's a hard constraint of where it was authored, not a design choice, and it shaped how
the solution is structured:

- **Fully built and tested, for real, in that sandbox:** `KafkaStudio.Core`, `KafkaStudio.Scripting`,
  `KafkaStudio.Automation`, and `KafkaStudio.App.ViewModels` - i.e. the KafScript language, the
  interpreter, the rethrow engine, the scheduler, and every ViewModel. `dotnet test`-equivalent output
  (108/108 passing) is reproducible by running `dotnet run --project tests/KafkaStudio.Tests`. This
  includes actual end-to-end runs of the rethrow, scan+acknowledge, and cross-topic-timing-check
  scenarios in `/samples` against a simulated in-memory Kafka broker (`InMemoryKafkaBroker`) - not just
  unit tests of isolated pieces, but the real "produce on one topic, watch another, assert on timing"
  race condition working correctly under concurrency. (One such race condition *was* found and fixed
  this way during development - see `WatchHandle`'s doc comment in
  `src/KafkaStudio.Scripting/Runtime/WatchHandle.cs` for the story.)
- **Since then, all seven projects restore, build (with no warnings) and run.** `ConfluentKafkaGateway`
  has been exercised against a real Apache Kafka 3.8 broker (KRaft, single node): multi-partition
  reads, newest-N reads, early-stop/limit reads, empty and missing topics, binary values and tombstones,
  the cross-topic timing check, scan + acknowledge + resume with a pinned group, Rethrow Rules
  (including binary payloads), and an unreachable broker. Every Avalonia screen has been rendered
  headlessly with demo data to check layout and bindings. The app has not been manually click-tested on
  Windows, so a quick smoke test there is still worthwhile.

On a normal Windows dev machine with regular internet access, `dotnet restore` just works for every
project here - the constraint above is specific to the environment this was built in, not to the code
itself.

## Building and running (Windows)

Prerequisites: [.NET 10 SDK](https://dotnet.microsoft.com/download) or later.

```powershell
# From the solution root:
dotnet restore
dotnet build

# Run the desktop app:
dotnet run --project src/KafkaStudio.App

# Run the test suite (fast, no Kafka broker needed):
dotnet run --project tests/KafkaStudio.Tests

# Publish a self-contained Windows executable:
dotnet publish src/KafkaStudio.App -c Release -r win-x64 --self-contained -p:PublishSingleFile=true
```

If `dotnet build`/`restore` reports XAML errors in `KafkaStudio.App`, they're almost certainly small -
see the "what's verified" section above for why, and check the corresponding `.axaml` file; the
ViewModel it binds to (in `KafkaStudio.App.ViewModels`) is already fully working and tested.

You don't need a running Kafka cluster to try the app: on the Connections screen, use "Add demo
(in-memory) connection" to get a simulated in-memory broker you can produce to, consume from, and run
every sample script against.

## Using it

1. **Connections** (top of the sidebar, `Ctrl+K`) - add a real cluster (bootstrap servers, security
   protocol, SASL, CA certificate, extra librdkafka settings) or a demo in-memory one. Connections are
   tested before they're saved, can be edited/reconnected, and on Windows saved passwords are encrypted
   with DPAPI for your user account.
2. **Topics** (`Ctrl+1`) - browse topics, open one to see its **newest** N messages (or all of them),
   filter by text or by field (`$.status = FAILED and key starts with ORD-`), inspect a message
   (metadata, headers, pretty JSON, binary hex dump), copy it, export to JSON/CSV, pin messages side by
   side and see a field-by-field diff, search across every topic, and create topics.
3. **Produce** (`Ctrl+2`) - ad-hoc send with headers, explicit partition, tombstones, "send N times",
   JSON formatting and `{{$uuid}}`/`{{$now}}`-style templates. Any message elsewhere in the app can be
   opened here with **Edit in Producer** to resend or tweak it.
4. **Consume** (`Ctrl+3`) - live tail with pause/resume, live filtering, a message inspector and export.
5. **Scripts** (`Ctrl+4`) - write and run KafScript scenarios; step results stream in live, runs can be
   stopped, scripts open from / save to `.kafscript` files (`Ctrl+O` / `Ctrl+S`, run with `F5`).
6. **Tasks & Checks** (`Ctrl+5`) - register `Task` blocks to run on a schedule; pause/resume, run now,
   pass/fail history. Registered tasks survive restarts.
7. **Rethrow Rules** (`Ctrl+6`) - point-and-click continuous relay from one topic to another, with an
   optional filter and extra header; at-least-once, retries on broker errors, rules are saved.
8. **Find Data** (`Ctrl+7`) - does this (or similar) data exist, and where? Structured or fuzzy search
   over a topic scope and time range, a one-click "Exists?" answer, bulk checks of id lists (found /
   missing / duplicated), an id's journey across topics, two-topic reconciliation (only in A / only in
   B / different), "find similar" (same key, same fields, near-duplicates, same shape), duplicate
   detection, field statistics, saved searches and history, and "turn this search into a KafScript
   check". Every message in the app has **Find similar** and **Trace key** buttons that jump here.

Settings live in `%APPDATA%/KafkaStudio` (override with the `KAFKASTUDIO_DATA_DIR` environment variable).

See [`docs/find-data.md`](docs/find-data.md) for the Find Data guide and query language,
[`docs/kafscript-language.md`](docs/kafscript-language.md) for the full KafScript reference, and
`/samples` for runnable examples of all three priority workflows (rethrow, scan+acknowledge, cross-topic
timing check).

## Architecture notes

- **`IKafkaGateway`** (`KafkaStudio.Core.Abstractions`) is the one seam between all the logic
  (interpreter, scheduler, rethrow engine, ViewModels) and an actual Kafka connection. Everything above
  it is written and tested against this interface; `ConfluentKafkaGateway` and `InMemoryKafkaGateway`
  are its only two implementations. This is what let the language/interpreter/automation layers be
  fully tested without a broker.
- **`AppState.RealGatewayFactory`** is a small but deliberate seam: `KafkaStudio.App.ViewModels` never
  references `KafkaStudio.Kafka` (and therefore never needs the Confluent.Kafka package) - it takes a
  `Func<ConnectionProfile, IKafkaGateway>` instead, which only `KafkaStudio.App`'s composition root
  (`App.axaml.cs`) wires up to the real gateway. That's both good dependency hygiene and the reason the
  ViewModels project could be fully built and tested in a NuGet-restricted environment.
- **KafScript** (see `docs/kafscript-language.md`) is a genuine small language - hand-written lexer,
  recursive-descent parser, AST, and tree-walking interpreter - not a regex/template hack. Scenarios and
  Tasks compile to the same AST node and run through the same interpreter; a Task is just a Scenario
  that's also registered with a schedule.
- **MVVM without a framework dependency**: `KafkaStudio.App.ViewModels` hand-rolls its own
  `ObservableObject`/`RelayCommand` instead of depending on CommunityToolkit.Mvvm, for the same
  zero-NuGet reason as above. `KafkaStudio.App` uses Avalonia's standard ViewModel-first navigation
  convention (`ViewLocator`) to map each ViewModel to its View.

## Repository layout

```
KafkaStudio.slnx
src/
  KafkaStudio.Core/            domain models, IKafkaGateway, in-memory fake broker
  KafkaStudio.Scripting/       KafScript: lexer, parser, AST, interpreter
  KafkaStudio.Automation/      scheduler, rethrow engine, run history, script loader
  KafkaStudio.Search/          data finding: queries, existence/bulk checks, traces, reconcile, similarity
  KafkaStudio.App.ViewModels/  MVVM layer (zero external dependencies)
  KafkaStudio.Kafka/           real Confluent.Kafka-backed IKafkaGateway
  KafkaStudio.App/             Avalonia desktop app
tests/
  KafkaStudio.Tests/           self-contained test suite (108 tests, no external test framework)
samples/
  *.kafscript                  runnable examples of every priority workflow
docs/
  kafscript-language.md        full language reference
  find-data.md                 Find Data screen and query language guide
```

## Why a hand-rolled test harness instead of xUnit

`KafkaStudio.Tests` uses a small custom `Assert`/`TestRunner` (see `tests/KafkaStudio.Tests/Harness/`)
instead of xUnit/NUnit/MSTest, purely because those are also NuGet packages unavailable in the sandbox
this was built in. On your own machine, swapping in xUnit is a mechanical change (add the package,
replace `Assert.*` calls with `Xunit.Assert.*`, add `[Fact]` attributes) if you'd rather have that -
the actual test logic and coverage carries over as-is.

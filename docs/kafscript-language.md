# The KafScript language

KafScript is the small, Gherkin-style language KafkaStudio uses to write checks and automation tasks
against Kafka. It reads like plain sentences on purpose, but the grammar behind each sentence is fixed
and small - that's what makes it possible to parse reliably and give you a useful error message when
something's off, rather than guessing at free-form English.

This document is the complete language reference. Every construct here is backed by tests in
`tests/KafkaStudio.Tests/Suites/LexerParserTests.cs`, `InterpreterTests.cs`, and
`SampleScriptsTests.cs` - if something described here doesn't behave as documented, that's a bug.

## Structure

A `.kafscript` file is one or more blocks. A block is either a **Scenario** (a check you run once and
get a pass/fail result for) or a **Task** (automation you register with a schedule):

```
Scenario: <name>
Given <step>
When <step>
Then <step>
And <step>
...

Task: <name>
schedule every 5 minutes
Given <step>
When <step>
...
```

- `Given`, `When`, `Then`, `And`, and `But` are all equivalent step keywords - use whichever reads best;
  the interpreter doesn't distinguish them. This mirrors Gherkin/Cucumber conventions.
- Lines starting with `#` are comments and can appear between or after steps.
- Blank lines are ignored.
- A block ends at the next `Scenario:`/`Task:` header or end of file.

### Task schedules

A `Task` block may have one `schedule` line right after its name:

| Form                     | Meaning                                              |
|--------------------------|-------------------------------------------------------|
| `schedule run once`      | Runs once when registered, never again automatically. |
| `schedule every 5 minutes` | Re-runs on that interval (units: `ms`, `seconds`, `minutes`, `hours`). |
| `schedule at 9:30`       | Runs once a day at that time (24h clock).             |

A `Scenario` block never has a schedule - it's meant to be run on demand (from the Script Editor's "Run
all", or as part of a Task via the automation scheduler if you want a scheduled check instead of a
scheduled action).

### Values, variables, and JSON payloads

- Short values are double-quoted strings: `"orders"`, `"ORD-42"`.
- Multi-line values (typically JSON bodies) use triple-quoted doc-strings:
  ```
  When produce message to topic "orders" value """
  { "orderId": "{{orderId}}", "status": "CONFIRMED" }
  """
  ```
- `{{name}}` inside any string is replaced at run time with a variable's value (see `set variable` and
  `capture` below). An unset variable is left as literal text (`{{name}}`) rather than failing, so you
  can spot a typo immediately in the output.
- Every step must fit on one line (aside from a doc-string's own internal newlines) - there's no line
  continuation syntax. If a step reads long, that's fine; KafScript favours simple, unambiguous parsing
  over line wrapping.

## Steps

### `use connection "name"`

Selects which registered Kafka connection subsequent steps run against. Required before any step that
talks to Kafka. Connection names are whatever you've named them on the Connections screen (or passed to
`ScriptRunner`'s connections dictionary if you're driving it from code).

```
Given use connection "local"
```

### `produce message to topic "T" [key "K"] [value V] [header "H" to "V"]...`

Sends a message. `key`, `value`, and any number of `header` clauses are optional and can appear in any
order after the topic.

```
When produce message to topic "orders" key "{{orderId}}" value "{ \"status\": \"CONFIRMED\" }"
When produce message to topic "orders" header "trace-id" to "{{traceId}}" header "source" to "checkout"
```

### `watch topic "T" from beginning|end|now`

Opens a live subscription on a topic *immediately* - this step doesn't return until the subscription is
actually registered, which is what makes it safe to follow with a `produce` step and not miss the
message it's watching for. `beginning` replays the topic's full history first; `end`/`now` (equivalent)
only see messages produced from this point on.

```
Given watch topic "shipment-notices" from now
```

This is the step that makes the **cross-topic timing check** race-free: put it before the step that
triggers the reaction you're checking for.

### `expect message on topic "T" within DURATION [where COND [and COND]...]`

The assertion form (typically a `Then` step): waits up to `DURATION` for a message on `T` matching every
condition, and fails the scenario if none arrives in time. If a `watch` step already opened a
subscription on `T`, this reads from it (race-free); otherwise it starts one from `now` for you as a
convenience - for anything time-sensitive, prefer an explicit `watch` first.

```
Then expect message on topic "shipment-notices" within 30 seconds
  where json "$.status" equals "NOTIFIED"
```

(Note: as shown throughout this doc, a real script keeps this on one line - it's wrapped here only for
readability. See `samples/cross-topic-timing-check.kafscript` for the runnable, single-line form.)

### `[a] message arrives [on topic "T"] [within DURATION] [where COND [and COND]...]`

The triggering form (typically a `When` step, used ahead of a `rethrow`/`capture` step): waits for a
matching message the same way `expect` does (default timeout 30 seconds if `within` is omitted), and
sets it as the "last message" for the steps that follow. `topic` can be omitted if a `watch` step already
named one.

```
Given watch topic "orders" from now
When a message arrives on topic "orders" within 10 seconds where json "$.status" equals "CONFIRMED"
```

### `rethrow last message to topic "T" [with key same|"K"] [header "H" to "V"]...`

Republishes the most recently seen message (from `produce`, `expect`, or `message arrives`) to a
different topic - the **rethrow** capability. `with key same` keeps the source message's key; give a
literal key instead if you want to change it. Existing headers on the source message are not carried
over automatically - list any you want with `header ... to ...`.

```
Then rethrow last message to topic "orders-fulfillment" with key same header "relayed-by" to "kafka-studio"
```

For a rethrow that runs continuously in the background rather than once per script run, use the
**Rethrow Rules** screen in the app (backed by `KafkaStudio.Automation.Rethrow.RethrowEngine`) instead -
same idea, always-on.

### `scan topic "T" from beginning|end [limit N]`

Bulk-reads a topic's backlog into the scenario's "scanned messages" list - the **scan and acknowledge**
capability. Unlike `watch`, this is meant to be a bounded read: it stops once it hits `limit` (if given)
or once no new message has arrived for a few seconds (treated as "caught up to the end of the backlog").

```
Then scan topic "orders-dlq" from beginning limit 500
```

### `acknowledge last message` / `acknowledge each scanned message`

Commits the consumer offset for the last message, or for every message collected by the most recent
`scan`. Only valid for messages that were actually consumed (via `watch`/`expect`/`message
arrives`/`scan`) - acknowledging a message you just `produce`d is a clear error, since produced messages
were never associated with a consumer group to commit against.

```
Then acknowledge each scanned message
```

### `log key` / `log value` / `log message` / `log "literal text"`

Writes to the step results log (shown in the app's step results panel, and returned as each
`StepResult.Message` if you're driving `ScriptRunner` from code). `log message` logs a one-line summary
of the last message (topic/partition/offset/key/value); `log key`/`log value` log just that field.
`{{variables}}` are substituted in literal text.

### `set variable NAME to "value"`

Sets a variable for `{{NAME}}` substitution in later steps.

```
Given set variable orderId to "ORD-1042"
```

### `capture json "$.path" as NAME` / `capture key as NAME` / `capture value as NAME`

Pulls a field off the last message into a variable, for use in a later `assert` or in `{{NAME}}`
substitutions. `json "$.path"` supports simple dotted/indexed paths (`$.order.id`, `$.items[0].sku`) -
see the note on JSON paths below.

```
Then capture json "$.orderId" as orderId
And assert orderId equals "ORD-1042"
```

### `wait for DURATION`

Pauses the scenario. Mostly useful for giving an external system a moment before the next step, or for
deliberately spacing out produced messages.

### `assert NAME equals|contains|matches|not equals "value"`

Checks a previously `set`/`capture`d variable and fails the scenario (with a clear message showing the
actual vs. expected value) if it doesn't hold.

## Conditions (`where ...`)

Used by `expect message`, `message arrives`, and (for the equivalent no-script Rethrow Rules feature)
rule filters. Each condition is `<field> <comparator> "<expected>"`, chained with `and`:

- **Field**: `key`, `value`, or `json "$.path"` (reads a field out of the message value, which is
  assumed to be JSON when `json` is used).
- **Comparator**: `equals`, `contains` (substring), `matches` (regular expression), or `not equals`.

```
where key equals "{{orderId}}" and json "$.status" equals "NOTIFIED"
```

### A note on the JSON path subset

`json "$.path"` supports plain dotted field access and array indexing - `$.status`, `$.order.id`,
`$.items[0].sku` - which covers the large majority of real message-shape checks. It does **not**
support JSONPath wildcards, filters, or recursive descent (`$..foo`, `$.items[*]`, `$.items[?(...)]`).
If a path doesn't resolve (missing field, out-of-range index, or the value isn't valid JSON at all), it
evaluates to "not found" rather than throwing, which shows up as a normal condition/assertion failure
with a clear message instead of a crash.

## Durations

`<number> <unit>`, where unit is one of: `ms`/`millisecond`/`milliseconds`, `s`/`sec`/`secs`/
`second`/`seconds`, `m`/`min`/`mins`/`minute`/`minutes`, `h`/`hour`/`hours`.

## Full example: the three workflows from the brief

See `/samples` for these as complete, runnable files:

- `samples/cross-topic-timing-check.kafscript` - produce on one topic, require a correlated message on
  another within N seconds.
- `samples/rethrow.kafscript` - relay a message from one topic to another.
- `samples/scan-and-acknowledge.kafscript` - bulk-read a backlog and acknowledge everything.
- `samples/scheduled-task.kafscript` - two `Task` blocks on different schedules.

## How it's implemented, if you want to extend it

- `src/KafkaStudio.Scripting/Lexing/Lexer.cs` - turns source text into tokens (words, strings,
  doc-strings, numbers, colons, newlines).
- `src/KafkaStudio.Scripting/Parsing/Parser.cs` - hand-written recursive-descent parser producing the
  AST in `src/KafkaStudio.Scripting/Ast/`.
- `src/KafkaStudio.Scripting/Runtime/ScriptRunner.cs` - the interpreter: walks the AST and calls into
  `IKafkaGateway` (see `src/KafkaStudio.Core/Abstractions/IKafkaGateway.cs`).

Adding a new step is a three-step change: add an AST node in `Ast/Actions.cs`, a `Parse...()` method in
`Parser.cs` plus a dispatch line in `ParseAction()`, and an `Execute...()` method in `ScriptRunner.cs`
plus a dispatch line in `ExecuteAsync()`. The test suites above show the pattern for testing each layer
in isolation.

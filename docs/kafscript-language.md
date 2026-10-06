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
- A block's name is the rest of its header line, taken verbatim - any punctuation is fine
  (`Scenario: Refund & cancel (EU) - 50%`).

### Tags, Feature and Background

```
@orders
Feature: Order events

Background:
Given use connection "local"

@smoke @jira:QA-12
Scenario: A new order is accepted
When produce message to topic "orders" value "{ \"status\": \"NEW\" }"
Then assert last message where json "$.status" equals "NEW"
```

- `@tag` tokens on the line(s) directly above a `Scenario`/`Task` tag that block. Tags above the optional
  `Feature:` line are inherited by every block in the file. The QA Lab's Test Runner and the
  `kafkastudio test` CLI select tests with tag expressions such as `@smoke and not @wip` (see
  [`qa-testing.md`](qa-testing.md)).
- `Feature: name` (optional, first line) names the file's suite in test reports.
- `Background:` (optional, before the first block, at most one) holds steps that are run before every
  Scenario and Task in the file.

### Scenario Outline and Examples

```
Scenario Outline: Status <status> is accepted
Given use connection "local"
When produce message to topic "orders" key "<id>" value "{ \"status\": \"<status>\" }"
Then expect message on topic "orders" within <wait> seconds where key equals "<id>"

Examples: happy path
| id    | status    | wait |
| ORD-1 | NEW       | 5    |
| ORD-2 | CONFIRMED | 2.5  |
```

Every data row of every `Examples:` table becomes its own Scenario (tags on the outline apply to each).
`<column>` is substituted inside quoted values and doc-strings. Outside quotes, a `<column>` is replaced
by the cell's tokens, so it can supply a number or a word (`within <wait> seconds`). The outline's name
is substituted the same way; a name without placeholders gets ` (example N)` appended. `\|` is a literal
`|` in a cell. The header row must name every column once, each row must have as many cells as the
header, and a placeholder that isn't a column is an error. A value that makes a step invalid is reported
as `example N (line L): …`. `Scenario Template:` is accepted as a synonym.

### Task schedules

A `Task` block may have one `schedule` line right after its name:

| Form                     | Meaning                                              |
|--------------------------|-------------------------------------------------------|
| `schedule run once`      | Runs once when registered, never again automatically. |
| `schedule every 5 minutes` | Re-runs on that interval (units: `ms`, `seconds`, `minutes`, `hours`; at least 1 second). |
| `schedule at 9:30`       | Runs once a day at that time - 24h clock, **local time** of the machine running KafkaStudio. |

A task never overlaps with itself: if a run is still going when the next one is due, that tick is
skipped. Tasks registered on the Tasks screen are saved and re-registered when the app starts (a
`run once` task is restored paused, so it doesn't fire again on every start).

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
- Built-in dynamic values, evaluated fresh at every occurrence (so two `{{$uuid}}` in one step give two
  different ids - `set variable id to "{{$uuid}}"` first if you need the same one twice):

  | Placeholder      | Value                                    |
  |------------------|------------------------------------------|
  | `{{$uuid}}`      | a new random GUID                        |
  | `{{$now}}`       | current UTC time, ISO 8601               |
  | `{{$timestamp}}` | current Unix time in milliseconds        |
  | `{{$date}}`      | current UTC date, `yyyy-MM-dd`           |
  | `{{$random}}`    | a random non-negative integer            |
  | `{{$randomInt(1,100)}}` | a random integer in the range (inclusive) |
  | `{{$randomDecimal(1,100)}}` | a random number in the range, 2 decimals |
  | `{{$randomString(8)}}` | 8 random letters/digits |
  | `{{$pick(EUR,USD,GBP)}}` | one of the listed values |
  | `{{$now(-15m)}}`, `{{$date(+1d)}}`, `{{$timestamp(+2h)}}` | shifted times (`ms`, `s`, `m`, `h`, `d`) |
  | `{{$index}}`     | 1..N inside `produce N messages`         |

  The same built-ins work in the Produce screen's key, value and header fields.
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

`produce N messages to topic "T" ...` sends N messages (test data seeding). Each copy is rendered
separately, so generators give fresh values per message, and `{{$index}}` is 1..N:

```
When produce 100 messages to topic "customers" key "CUST-{{$index}}" value "{ \"credit\": {{$randomInt(0,5000)}} }"
```

```
When produce message to topic "orders" key "{{orderId}}" value "{ \"status\": \"CONFIRMED\" }"
When produce message to topic "orders" header "trace-id" to "{{traceId}}" header "source" to "checkout"
```

### `watch topic "T" from beginning|end|now`

Opens a live subscription on a topic *immediately* - this step doesn't return until the subscription is
live (the connection reports that its read positions are pinned), which is what makes it safe to follow
with a `produce` step and not miss the message it's watching for, against a real cluster too. `beginning` replays the topic's full history first; `end`/`now` (equivalent)
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

### `expect no message on topic "T" within DURATION [where COND [and COND]...]`

A negative check: passes if no matching message arrives during the whole window, and fails as soon as one
does (the offending message becomes the "last message", and the failure shows its key and value). It
reads from a prior `watch` on `T` when there is one, like `expect`. Put the `watch` before the trigger.

```
Given watch topic "shipments" from now
When produce message to topic "orders" key "ORD-9" value "{ \"status\": \"CANCELLED\" }"
Then expect no message on topic "shipments" within 5 seconds where key equals "ORD-9"
```

### `expect [exactly|at least|at most] N message(s) on topic "T" within DURATION [where ...]`

Counts matching messages. A bare `N` means `exactly`. `at least` passes as soon as N have arrived;
`exactly` and `at most` watch the whole window (so extra messages are caught), failing early once the
count is exceeded. The last matching message becomes the "last message".

```
Then expect exactly 3 messages on topic "invoices" within 10 seconds where json "$.orderId" equals "{{orderId}}"
```

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
literal key instead if you want to change it (without `with key`, the relayed message has no key). The
value is relayed byte-for-byte (binary payloads such as Avro/Protobuf survive intact), and the source
message's headers are carried over; `header ... to ...` adds or overrides individual headers.

```
Then rethrow last message to topic "orders-fulfillment" with key same header "relayed-by" to "kafka-studio"
```

For a rethrow that runs continuously in the background rather than once per script run, use the
**Rethrow Rules** screen in the app (backed by `KafkaStudio.Automation.Rethrow.RethrowEngine`) instead -
same idea, always-on.

### `scan topic "T" from beginning|end|committed [group "G"] [limit N]`

Bulk-reads a topic's backlog into the scenario's "scanned messages" list - the **scan and acknowledge**
capability. Unlike `watch`, this is a bounded read: it stops at `limit` (if given) or once it has read
everything that was on the topic when the scan started.

By default every scan uses a throwaway consumer group, so acknowledging only matters within that run.
Pin a group with `group "G"` to make acknowledgements stick, and use `from committed` to resume after
the last acknowledged message - that's how a recurring "sweep the DLQ" task processes each message once:

```
Then scan topic "orders-dlq" from beginning limit 500
Then scan topic "orders-dlq" from committed group "dlq-sweeper" limit 500
And acknowledge each scanned message
```

`from committed` requires `group`; for a group that has never committed, it starts from the beginning.

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

### `assert NAME <comparator> "value"`

Checks a previously `set`/`capture`d variable and fails the scenario (with a clear message showing the
actual vs. expected value) if it doesn't hold. Any comparator from the Conditions section works;
`assert NAME exists` / `assert NAME not exists` take no value.

### `assert last message where COND [and COND]...`

Checks the last produced/received/scanned message against conditions (same syntax as `where` below),
and reports the first unmet one with the actual value:

```
Then assert last message where header "source" equals "checkout" and json "$.amount" greater than "0"
# fails with: expected json "$.amount" greater than "0", but it was "-5"
```

### `validate last message | each scanned message against schema V | schema file "path"`

Contract testing: checks JSON values against a JSON Schema, given inline (usually a `"""` doc-string)
or as a file. Relative file paths resolve from the `.kafscript` file's folder when run from the Test
Runner or the CLI. Failures list the violations (`$.currency: required field is missing`, up to 5).
`each scanned message` validates everything the last `scan` read and reports how many broke the schema.
A malformed inline schema is a parse error; an unreadable schema file is a run-time error (not a failed
check). See [`qa-testing.md`](qa-testing.md#contracts-json-schema) for the supported keywords.

```
Then scan topic "orders" from beginning limit 1000
And validate each scanned message against schema file "contracts/order-event.schema.json"
```

## Conditions (`where ...`)

Used by `expect message`, `message arrives`, and (for the equivalent no-script Rethrow Rules feature)
rule filters. Each condition is `<field> <comparator> "<expected>"`, chained with `and`:

- **Field**: `key`, `value`, `json "$.path"` (reads a field out of the message value, which is
  assumed to be JSON when `json` is used), or `header "name"`.
- **Comparator**: `equals`, `not equals`, `contains` (substring), `not contains`, `matches` (regular
  expression), `exists` / `not exists` (no value: is the field/header there at all?), and
  `greater than` / `less than` (numbers compare numerically, ISO dates chronologically, anything else
  ordinally).
  Regular expressions are checked when the script is parsed (unless they contain `{{variables}}`) and
  are evaluated with a 1-second timeout, so a pathological pattern fails clearly instead of hanging.

```
where key equals "{{orderId}}" and json "$.status" equals "NOTIFIED"
```

### A note on the JSON path subset

`json "$.path"` supports plain dotted field access and array indexing - `$.status`, `$.order.id`,
`$.items[0].sku` - plus negative indexes counting from the end (`$.items[-1]`) and bracket-quoted names
for keys containing dots or spaces (`$['order.id']`, `$["line items"][0]`). That covers the large
majority of real message-shape checks. A malformed path is reported when the script is parsed. It does **not**
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
- `samples/scheduled-task.kafscript` - a scheduled `Task` block (runs only via Run now).
- `samples/qa/` - a QA pack: contracts, negative checks, a Scenario Outline and test-data seeding
  (see [`qa-testing.md`](qa-testing.md)).

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

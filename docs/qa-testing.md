# Testing Kafka systems with KafkaStudio (QA guide)

This guide is for QA engineers testing systems built on Kafka: services that consume one topic and
produce to another, event contracts between teams, dead-letter handling, and "did the right thing happen
within N seconds". It covers:

1. [Writing tests](#1-writing-tests) - KafScript with tags, Background, Scenario Outlines, negative checks,
   message counts, message assertions, JSON Schema contracts, and test data generation.
2. [QA Lab in the app](#2-qa-lab-in-the-app) (`Ctrl+8`) - the Test Runner and Contract check.
3. [Running tests in CI](#3-running-tests-in-ci) - the `kafkastudio test` command, reports and exit codes.

The full grammar is in [`kafscript-language.md`](kafscript-language.md). Every example here is also in
[`/samples/qa`](../samples/qa), and those samples run as part of the test suite.

---

## 1. Writing tests

A test is a `Scenario` in a `.kafscript` file. Keep a test pack in a folder (for example
`tests/kafka/…`) next to the service it covers, with contracts in a `contracts/` subfolder.

### Tags, Feature and Background

```
@orders
Feature: Order events

Background:
Given use connection "local"
Given set variable orderId to "ORD-{{$randomInt(1000,9999)}}"

@smoke @contract
Scenario: A new order event matches the contract
When produce message to topic "orders" key "{{orderId}}" value """
{ "orderId": "{{orderId}}", "status": "NEW", "amount": 42.50, "currency": "EUR", "createdAt": "{{$now}}" }
"""
Then validate last message against schema file "contracts/order-event.schema.json"
```

- **Tags** (`@smoke`, `@regression`, `@jira:QA-123`) go on the line(s) above a `Scenario`/`Task`. Tags
  above `Feature:` apply to every block in the file. The Test Runner and the CLI select tests with a tag
  expression: `@smoke`, `@smoke and not @wip`, `(@orders or @payments) and not @slow`, or the shorthand
  `@smoke, @api` (meaning *or*).
- **`Feature: name`** is optional. It names the suite in reports; without it, reports use the file name.
- **`Background:`** steps run before every Scenario in the file, which keeps `use connection` and shared
  setup out of each test.
- Scenario names can contain any characters, e.g. `Scenario: Refund & cancel (EU) - 50%`.

### Data-driven tests: Scenario Outline + Examples

```
@orders @regression
Scenario Outline: Order status <status> is relayed to the audit topic
Given use connection "local"
Given watch topic "orders-audit" from now
When produce message to topic "orders" key "<orderId>" value "{ \"orderId\": \"<orderId>\", \"status\": \"<status>\" }"
Then expect message on topic "orders-audit" within <wait> seconds where key equals "<orderId>" and json "$.status" equals "<status>"

Examples: order lifecycle
| orderId  | status    | wait |
| ORD-1001 | NEW       | 5    |
| ORD-1002 | CONFIRMED | 5    |

Examples: edge cases
| orderId  | status    | wait |
| ORD-1004 | CANCELLED | 2    |
```

Each row runs as a separate test and appears separately in reports. `<column>` is replaced inside quoted
values. It can also stand on its own where a number or word goes (`within <wait> seconds`). If the outline
name has no placeholders, rows are named `… (example 1)`, `… (example 2)`. Use `\|` for a literal pipe in a
cell. A bad cell (say `soon` where a number is expected) is reported with its example number and line.

### Checks QA tests need

| Step | What it checks |
|---|---|
| `expect message on topic "T" within 30 seconds where …` | A matching message arrives in time (the classic cross-topic check). |
| `expect no message on topic "T" within 5 seconds where …` | **Negative test:** nothing matching arrives during the whole window. Fails as soon as one does. |
| `expect exactly 3 messages on topic "T" within 10 seconds where …` | **Count:** exactly N (waits the full window so extras are caught). |
| `expect at least 3 messages …` / `expect at most 3 messages …` | At least N (passes as soon as N arrive) / no more than N. |
| `assert last message where header "trace-id" exists and json "$.amount" greater than "0"` | Fields of the last produced/received message. |
| `validate last message against schema file "contracts/x.json"` | **Contract:** the last message matches a JSON Schema. |
| `validate each scanned message against schema """{…}"""` | Every message from the last `scan` matches. |

For any check that waits for a reaction, open a `watch` on the output topic **before** the step that
triggers it. That way the reaction can't slip past before the check starts listening:

```
Given watch topic "shipments" from now
When produce message to topic "orders" key "ORD-9" value "{ \"status\": \"CANCELLED\" }"
Then expect no message on topic "shipments" within 5 seconds where key equals "ORD-9"
```

**Conditions** (in `where …` and `assert last message where …`) read `key`, `value`, `json "$.path"` or
`header "name"`, and compare with `equals`, `not equals`, `contains`, `not contains`, `matches` (regex),
`exists`, `not exists`, `greater than`, or `less than`. `greater than` and `less than` compare numbers as
numbers and ISO dates as dates. `assert NAME exists` / `assert NAME not exists` also work on variables.

A test **fails** when a check isn't met, and **errors** when it couldn't run: unknown connection, broker
unreachable, missing schema file, broken script. Reports keep the two apart, so "the system is wrong" and
"the test environment is wrong" don't get mixed up.

### Contracts (JSON Schema)

`validate … against schema` accepts a schema inline (in `"""…"""`) or from a file. A relative file path
is resolved from the folder of the `.kafscript` file. The supported keywords cover what message contracts
use: `type` (including lists of types and `nullable`), `properties`, `required`, `additionalProperties`,
`enum`, `const`, `minimum`/`maximum`/`exclusiveMinimum`/`exclusiveMaximum`, `multipleOf`,
`minLength`/`maxLength`, `pattern`, `format` (`date-time`, `date`, `time`, `uuid`, `email`, `uri`,
`ipv4`), `items`, `minItems`/`maxItems`/`uniqueItems`, `minProperties`/`maxProperties`,
`allOf`/`anyOf`/`oneOf`/`not`, and local `$ref`s (`#/$defs/x`, `#/definitions/x`). Other keywords, such
as `title`, `description` and `examples`, are ignored.

A mistake in the schema itself is reported when the script is parsed (for example `unknown type 'strng'`,
an invalid regex, or an unresolvable `$ref`), not silently treated as "anything goes". Failures list up to
five violations:

```
message orders#0@12 breaks schema file 'order-event.schema.json': $.currency: required field is missing;
$.status: "LOST" is not one of the allowed values: "NEW", "CONFIRMED", "SHIPPED", "CANCELLED"
```

### Test data

```
When produce 50 messages to topic "customers" key "CUST-{{$index}}" value """
{ "id": "CUST-{{$index}}", "externalId": "{{$uuid}}", "tier": "{{$pick(bronze,silver,gold)}}",
  "credit": {{$randomInt(0,5000)}}, "since": "{{$date(-30d)}}" }
"""
```

`produce N messages` renders each copy separately. The generators:

| Placeholder | Value |
|---|---|
| `{{$index}}` | 1…N within `produce N messages` (and the Producer screen's "send N times") |
| `{{$randomInt(1,100)}}` / `{{$randomDecimal(1,100)}}` | a random integer / a number with two decimals in the range |
| `{{$randomString(8)}}` | 8 random letters/digits |
| `{{$pick(EUR,USD,GBP)}}` | one of the values |
| `{{$now(-15m)}}`, `{{$date(+1d)}}`, `{{$timestamp(-2h)}}` | shifted times (`ms`, `s`, `m`, `h`, `d`) |
| `{{$uuid}}`, `{{$now}}`, `{{$date}}`, `{{$timestamp}}`, `{{$random}}` | as before |

### Environments and variables

Variables passed to a run (`-v customerId=CUST-42` on the CLI, or the Variables box in the Test Runner)
are set before every test starts, so one pack can use different test data per environment. A test with no
`use connection` step runs against the *default* connection: the one picked in the Test Runner, or the
first connection given to the CLI. Leave `use connection` out of a pack to point it at dev, staging and
so on without editing it.

---

## 2. QA Lab in the app

Open **QA Lab** in the sidebar (`Ctrl+8`).

### Test Runner

- **Test sources:** add `.kafscript` files or whole folders (searched recursively). You can also include
  the Script Editor's scenarios. The sources, filters and options are remembered.
- **Filter:** a tag expression and/or part of a test name. The list shows only matching tests, plus any
  file that fails to load (shown as an error rather than dropped silently).
- **Run options:** retries on failure (a test that passes on a retry is marked **flaky**), a per-test
  timeout, stop at the first failure, a default connection, and starting variables (`name=value` lines).
- **Run all** (`F5`), **Run failed**, **Run selected**, **Stop** (`Shift+F5`). Results and each test's
  steps stream in live.
- **Share:** export the last run as **JUnit XML**, **HTML** (a single file with no external assets that
  follows light/dark mode, with filters for failed/passed), or **Markdown**, or **Copy summary**.
  **Copy bug report** on a failed test copies Markdown with the test's location and tags, expected vs.
  actual, every step, earlier attempts and the environment, ready to paste into Jira, Azure Boards or
  GitHub.

### Contract check

- Pick a connection and topic, and whether to check the newest N messages per partition or the whole
  topic (up to 200,000 messages).
- Paste or **Open…** a JSON Schema, or **Infer schema** from the topic's messages (up to 5,000). Inference
  marks fields present in every message as `required`, lists all types seen, and detects `uuid` and
  `date-time` strings. Treat it as a starting point: add enums, patterns and ranges that samples can't
  reveal.
- **Validate** shows checked/valid/invalid counts and the **most common problems** grouped by field (for
  example `312× $.currency required field is missing`). It also lists each offending message; select one
  to see every violation and the message itself.
- **Turn into a check** adds a tagged `@contract` Scenario to the Script Editor that scans the topic and
  validates it against this schema, so the contract stays in the regression pack.

---

## 3. Running tests in CI

`kafkastudio` is a headless command-line runner. It uses the same engine as the Test Runner, so a pack
behaves the same on a laptop and in a pipeline.

```
dotnet run --project src/KafkaStudio.Cli -- test tests/kafka -c local=localhost:9092 --junit reports/junit.xml
# or publish it once: dotnet publish src/KafkaStudio.Cli -c Release -o tools/kafkastudio
```

```
kafkastudio test <file-or-folder>... [options]

  -c, --connection NAME=SERVERS   a Kafka cluster (repeatable)
      --connections FILE          JSON list of connection profiles; ${ENV_VAR} is replaced
      --demo NAME                 an in-memory demo cluster (repeatable)
  -t, --tags EXPR                 "@smoke and not @wip"
  -n, --name TEXT                 only tests whose name contains TEXT
      --include-tasks             also run Task blocks once
      --list                      list the selected tests and exit
  -v, --var NAME=VALUE            starting variable (repeatable)
      --retries N                 re-run failures up to N times (flaky detection)
      --fail-fast                 stop at the first failure
      --timeout DURATION          per-test limit, e.g. 90s, 5m
      --junit FILE / --html FILE / --markdown FILE
  -q, --quiet                     only print failures and the summary
```

**Exit codes:** `0` all passed · `1` failures or errors · `2` bad arguments/config or a cluster that
can't be reached · `3` no tests selected (usually a wrong path or tag filter, which you want to know
about in CI).

Before running anything, the CLI checks that each real cluster answers (30-second limit). An unreachable
broker therefore fails the job straight away instead of hanging in the first test. `Ctrl+C` stops after
the current step and still writes the reports.

### Secured clusters

Put profiles in a JSON file and keep secrets in CI variables:

```json
[
  {
    "name": "staging",
    "bootstrapServers": "kafka-staging:9093",
    "securityProtocol": "SaslSsl",
    "saslMechanism": "ScramSha512",
    "saslUsername": "qa-runner",
    "saslPassword": "${KAFKA_PASSWORD}",
    "sslCaLocation": "/etc/ssl/kafka-ca.pem"
  }
]
```

```
kafkastudio test tests/kafka --connections ci/connections.json --tags "@regression" --junit reports/junit.xml
```

An unset `${VARIABLE}` is an error rather than an empty password.

### GitHub Actions

```yaml
- uses: actions/setup-dotnet@v4
  with: { dotnet-version: "10.0.x" }
- name: Kafka regression tests
  run: >
    dotnet run --project src/KafkaStudio.Cli -c Release --
    test tests/kafka --connections ci/connections.json --tags "@regression and not @wip"
    --retries 1 --timeout 5m --junit reports/junit.xml --html reports/kafka-tests.html
  env:
    KAFKA_PASSWORD: ${{ secrets.KAFKA_PASSWORD }}
- uses: actions/upload-artifact@v4
  if: always()
  with: { name: kafka-test-report, path: reports/ }
```

### GitLab CI

```yaml
kafka-tests:
  image: mcr.microsoft.com/dotnet/sdk:10.0
  script:
    - dotnet run --project src/KafkaStudio.Cli -- test tests/kafka --connections ci/connections.json --junit junit.xml --html kafka-tests.html
  artifacts:
    when: always
    reports: { junit: junit.xml }
    paths: [kafka-tests.html]
```

Jenkins (`junit 'reports/junit.xml'`), Azure DevOps (`PublishTestResults@2` with `testResultsFormat:
JUnit`) and TeamCity read the same JUnit file. In the JUnit output, each file (or Feature) is a
`<testsuite>` and each scenario is a `<testcase>`. Unmet checks are `<failure>`s, broken tests are
`<error>`s, every step goes into `<system-out>`, and tags and flakiness are recorded as properties.

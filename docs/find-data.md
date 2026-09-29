# Finding data

KafkaStudio's **Find Data** screen (`Ctrl+7`) answers "does this data exist, where, and is there anything
like it?". The Topics screen's filter and "search all topics" use the same query language.

Every Find Data tab shares one **scope bar** at the top:

- **Connection**. Topics are listed automatically (⟳ reloads them).
- **Topics**. Either every topic or a saved topic set (sets are saved from Topics → search results).
  You can also narrow by name ("name contains…").
- **Range**. Choose from *All messages*, *Last N minutes*, *Between times* (local time, e.g.
  `2026-01-10 14:30`; leave either end empty for "beginning"/"now"), or *Newest N per partition*.
  Narrowing the range is the biggest speed-up on large topics, because the reader seeks straight to
  the start timestamp or offset instead of reading from the beginning.

Scans run at most 8 topics at a time. Each topic uses a throwaway consumer group, so real consumer
groups' offsets are never touched. A topic that can't be read is reported but doesn't stop the scan,
and every run can be cancelled.

**Help & examples** (`F1`, top right of the scope bar) opens how-tos for the tab you're on: what to do,
step by step, with example queries, ids and field lists. **Try it** fills an example into its tab without
running anything; untick "Only this tab" to browse every how-to.

The message selected in any tab is shown in the detail pane on the right. Every message detail pane in
the app (Topics, Consume, Find Data) has **Find similar** and **Trace key** buttons that jump here.

## Search

Type plain text, or a query.

**Plain text** is matched in the key, value and/or headers (tick which), in one of these modes:

| Mode | Matches when… |
|---|---|
| Contains | the field contains the text anywhere |
| Exact | the whole field equals the text (ignoring surrounding spaces) |
| Whole word / id | the text appears as a whole token: `ORD-4` does **not** match `ORD-42` |
| Regular expression | the regex matches |
| Fuzzy (similar) | the field, or an id-like token inside it, is ≥ 80% similar: catches typos, case, dashes/underscores/spaces and leading zeros (`ord 0042` ≈ `ORD-42`) |

Tick **Match case** for case-sensitive matching (the default is case-insensitive).

**Queries** use the same condition language as KafScript `where` clauses, extended for searching:

```
json "$.order.id" equals "ORD-42" and header "source" equals "billing"
$.order.id = ORD-42                          # bare $.path and = are shorthands
key starts with "ORD-" and not value contains "test"
$.amount > 100 or ($.status = "FAILED" and $.retries >= 3)
$.cancelledAt missing
any similar to "ORD-0042"
```

- **Fields:** `key`, `value`, `json "$.path"` or just `$.path`, `header "name"` (or `header name`),
  `topic`, `partition`, `offset`, and `any` (the key, the value or any header value).
- **Comparisons:** `equals` / `=`, `not equals` / `!=`, `contains`, `not contains`, `matches` / `~`
  (regex), `starts with`, `ends with`, `similar to`, `exists`, `missing` (or `is missing`), and `>`,
  `>=`, `<`, `<=`. The ordering comparisons are numeric when both sides are numbers; otherwise they
  compare text, which also orders ISO timestamps correctly.
- **Combine** with `and`, `or`, `not` and parentheses (`and` binds tighter than `or`).
- **Values:** quote them (`"…"` or `'…'`) when they contain spaces. To search for text that looks like
  a query, quote the whole thing: `"key equals"`.
- JSON paths support `$.a.b`, `$.items[0]`, `$.items[-1]` and `$['odd key']`. This is the same subset
  KafScript supports.

**Search** lists every match (up to 5,000) with a per-topic breakdown. **Exists?** (`Ctrl+Enter`) is the
quick yes/no check: it stops reading each topic at its first match and answers with something like
"Found on 2 of 48 topics: orders (#3@1044, 2026-01-10 10:32:01), …" or "Not found in 48 topics
(scanned 1,204,332 messages)".

Under **Saved searches · history · export · make it a check** you can:

- **Save** a search under a name. This stores the query, the match options and the scope (range, topic
  set, name filter). Picking a saved search restores all of it.
- Re-run a recent query from **History** (the last 30 are kept).
- **Export** the results as JSON or CSV, or copy the one-line summary.
- **Make a check.** This turns the search into a KafScript scenario, or a scheduled **Task**, that
  passes when matching data exists. It opens in the Script Editor for the topics that had matches.
  Case-insensitive and starts/ends-with conditions become anchored `(?i)` regexes. Searches KafScript
  can't express are refused with the reason: `or`, `not (…)`, headers, fuzzy/`similar to`, and
  `missing`.

## Bulk check

Paste a list of ids, or import a CSV/text file. Ids can be one per line or comma/semicolon/tab
separated. Pick a CSV column, or tick "first line is a header", if needed.

"Look in" controls where an id has to appear:

- `any` (default): the key, any header, the whole value, or any id-like token inside the value. This
  finds ids embedded anywhere in a JSON payload without knowing the path.
- A specific field: `key`, `$.orderId`, `header "trace-id"`.

The check reads each topic once and looks every candidate value up in a hash set, so thousands of ids
cost about the same as one. Results list missing ids first, then each found id with its count
(duplicates show as `found ×3`), topics and first-seen time. **Copy missing ids** and **Export CSV** are
one click.

## Trace

Enter an id to see every message carrying it (same "look in" rules as Bulk check) as a timeline across
topics. The path is summarized as `orders → payments → shipments`. Each hop shows the time since the
previous hop and since the first one. **Trace key** on any message opens this tab and runs the trace
for that message's key.

## Reconcile

Pick topic A and topic B and a join field on each (for example `key` on `orders` and `$.orderId` on
`payments`). Optionally add fields to compare between matched pairs (`$.amount, $.currency`). The
result sorts problems first:

- **only in A** (never arrived)
- **only in B**
- **different**, with the differing fields: `$.amount: 25 ≠ 24`
- **matched**

It also reports counts per side, A→B latency (average and maximum), and messages that don't have the
join field. Selecting a row shows a field-by-field diff of the pair, with timestamps and ids ignored.
**Export CSV** saves the whole reconciliation.

## Similar

The **reference** is set by **Find similar** on any message, or by "Use shown message". From there you
can:

- **Search same key.**
- **Search ticked field values.** Tick fields of the reference (its key, headers and every JSON leaf
  are listed; volatile ones are flagged) to search for messages with the same values.
- **Find similar.** This scores every message in scope and lists those above the threshold, best
  first:
  - **Content** scores near-duplicates. Every field in either message counts; equal values score 1,
    near-equal values (typos, case, spacing) score partial credit, and missing or different values
    score 0. With *Ignore timestamps/ids* on, retries that differ only in `createdAt`/`traceId`/UUIDs
    score 100%. Non-JSON values are compared by text similarity.
  - **Shape** scores schema variants: the overlap of the two messages' JSON field paths, ignoring
    values. Arrays are folded, so `$.items[*].sku` is one field.

Selecting a match shows how it differs from the reference.

## Duplicates

Pick a topic (the scope bar's range applies) and group its messages by:

- the same key
- an identical value
- the same key and identical value
- the same value ignoring timestamps and generated ids (catches producer retries)
- the same values of chosen fields (`$.orderId, $.eventType`)

Each group with more than one message lists its count, positions and the time between the first and
last copy.

## Field stats

Pick a topic to see every field (key, headers, and each JSON path with arrays folded) with:

- the share of messages that have it, and how many are missing it
- nulls
- distinct values
- the types seen
- first and last seen times

"Only fields missing from some messages" shows schema drift at a glance. Select a field to see its top
values; **Find** next to a value runs a Search for messages with that value on that topic.

## Topics screen additions

- The **message filter** and **search all topics** accept the same queries. Plain text still does a
  case-insensitive "contains". An invalid query is explained above the list, and every message stays
  shown until it's fixed.
- **Compare messages → Field diff** shows a field-by-field diff of any two pinned messages:
  - changed, added and removed fields across the key, headers and JSON value
  - optionally hides timestamps and generated ids ("Ignore timestamps/ids")
  - optionally shows the equal fields too
  - an "ignore" list of paths or prefixes (`$.meta, headers.trace-id`)
- Loaded messages and search results can be exported as **CSV** as well as JSON.

## For developers

All of this lives in `src/KafkaStudio.Search`, a dependency-free library on top of `IKafkaGateway`.
It is tested against the in-memory broker in `tests/KafkaStudio.Tests/Suites/DataSearchTests.cs`
(engine) and `DataSearchViewModelTests.cs` (screens).

| Type | Role |
|---|---|
| `MessageQuery` | query parser and evaluator |
| `FieldSelector` | "which part of a message" |
| `SearchRange` / `TopicScanner` | bounded, parallel, cancellable topic reads |
| `MessageSearch` | search and Exists? |
| `IdMatcher` / `BulkExistenceChecker` / `IdTracer` | id lookups |
| `TopicReconciler` | two-topic reconciliation |
| `MessageSimilarity` / `SimilarityFinder` | similarity scoring |
| `DuplicateDetector` | duplicate groups |
| `FieldStatisticsCollector` | per-field statistics |
| `JsonDiff` | structural diff |
| `KafScriptGenerator` | search → KafScript |
| `SavedSearchStore` / `SearchHistoryStore` | persistence (in the app's data folder) |

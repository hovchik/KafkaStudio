using KafkaStudio.App.ViewModels.Shared;

namespace KafkaStudio.App.ViewModels.DataSearch;

/// <summary>
/// The Find Data screen's "Help &amp; examples" content: how-tos per tab, with example queries/ids that "Try it"
/// fills in. Content mirrors <c>docs/find-data.md</c>; a test checks every Search example is a valid query.
/// </summary>
public static class FindDataHelp
{
    private const int All = HowToTopicViewModel.AllTabs;
    private const int Search = DataSearchViewModel.SearchTab, Bulk = DataSearchViewModel.BulkTab, Trace = DataSearchViewModel.TraceTab,
                      Reconcile = DataSearchViewModel.ReconcileTab, Similar = DataSearchViewModel.SimilarTab,
                      Duplicates = DataSearchViewModel.DuplicatesTab, FieldStats = DataSearchViewModel.FieldStatsTab;

    public static IReadOnlyList<HowToTopicViewModel> BuildTopics() => new[]
    {
        // ------------------------------------------------------------------ every tab ----
        new HowToTopicViewModel
        {
            Title = "Choose where to look (scope bar)",
            TabIndex = All, TabName = "All tabs",
            Steps = "1. Pick a Connection - its topics load automatically (⟳ reloads them).\n" +
                    "2. Topics: leave '(all topics)', pick a saved topic set (saved from Topics → search results), and/or type part of a name in 'name contains…'.\n" +
                    "3. Range: All messages, Last N minutes, Between times (local time, empty end = now) or Newest N per partition.\n" +
                    "Every tab uses this scope. Scans never move real consumer groups' offsets, and can always be cancelled.",
            Example = "Range: Between times\nfrom 2026-01-10 14:30   to (empty = now)"
        },
        new HowToTopicViewModel
        {
            Title = "Make scans on big topics fast",
            TabIndex = All, TabName = "All tabs",
            Steps = "Narrowing the Range is the biggest speed-up: the reader seeks straight to the start time/offset instead of reading from the beginning.\n" +
                    "• 'Last 60 minutes' when you know roughly when it happened.\n" +
                    "• 'Newest 1000 per partition' for a quick look.\n" +
                    "• A saved topic set or 'name contains…' so only relevant topics are read.\n" +
                    "Use Exists? instead of Search when you only need yes/no - it stops at the first match per topic."
        },
        new HowToTopicViewModel
        {
            Title = "Jump here from any message",
            TabIndex = All, TabName = "All tabs",
            Steps = "Every message detail pane (Topics, Consume, Find Data) has 'Find similar' and 'Trace key' buttons.\n" +
                    "• Find similar → opens the Similar tab with that message as the reference.\n" +
                    "• Trace key → opens the Trace tab and traces the message's key across topics.\n" +
                    "In Find Data, 'Use as Similar reference' above the detail pane does the same for the selected result."
        },

        // ------------------------------------------------------------------ Search ----
        new HowToTopicViewModel
        {
            Title = "Find messages containing some text",
            TabIndex = Search, TabName = "Search",
            Steps = "1. Type the text in the search box.\n" +
                    "2. Tick where to look: Key, Value, Headers.\n" +
                    "3. Pick a match mode: Contains, Exact, Whole word / id (ORD-4 won't match ORD-42), Regular expression, or Fuzzy.\n" +
                    "4. Press Search. Results list every match (up to 5,000) with a per-topic breakdown.",
            Example = "ORD-1042",
            TryText = "ORD-1042"
        },
        new HowToTopicViewModel
        {
            Title = "Check whether something exists (yes/no)",
            TabIndex = Search, TabName = "Search",
            Steps = "1. Type an id, text or query.\n" +
                    "2. Press Exists? (Ctrl+Enter). Each topic stops reading at its first match.\n" +
                    "The answer reads like: 'Found on 2 of 48 topics: orders (#3@1044, …)' or 'Not found in 48 topics (scanned 1,204,332 messages)'.",
            Example = "$.order.id = ORD-42",
            TryText = "$.order.id = ORD-42"
        },
        new HowToTopicViewModel
        {
            Title = "Search by a JSON field",
            TabIndex = Search, TabName = "Search",
            Steps = "Write a query instead of plain text. A bare $.path and = are shorthands for json \"$.path\" equals.\n" +
                    "Paths: $.a.b, $.items[0], $.items[-1] (last), $['odd key'].",
            Example = "json \"$.order.id\" equals \"ORD-42\"\n$.customer.tier = gold\n$.items[-1].sku = SKU-7",
            TryText = "$.customer.tier = gold"
        },
        new HowToTopicViewModel
        {
            Title = "Combine conditions (and / or / not)",
            TabIndex = Search, TabName = "Search",
            Steps = "Join conditions with and, or, not and parentheses ('and' binds tighter than 'or').\n" +
                    "Fields: key, value, $.path, header \"name\", topic, partition, offset, any.",
            Example = "$.amount > 100 or ($.status = \"FAILED\" and $.retries >= 3)",
            TryText = "$.amount > 100 or ($.status = \"FAILED\" and $.retries >= 3)"
        },
        new HowToTopicViewModel
        {
            Title = "Filter by header, key prefix or topic",
            TabIndex = Search, TabName = "Search",
            Steps = "Comparisons: = / equals, != / not equals, contains, not contains, starts with, ends with, ~ / matches (regex), similar to.\n" +
                    "Quote values that contain spaces (\"…\" or '…').",
            Example = "key starts with \"ORD-\" and header \"source\" equals \"billing\" and not value contains \"test\"",
            TryText = "key starts with \"ORD-\" and header \"source\" equals \"billing\" and not value contains \"test\""
        },
        new HowToTopicViewModel
        {
            Title = "Find messages missing a field",
            TabIndex = Search, TabName = "Search",
            Steps = "Use 'missing' (or 'is missing') and 'exists' to find payloads with or without a field - handy for schema drift and half-written events.",
            Example = "$.status = CANCELLED and $.cancelledAt missing",
            TryText = "$.status = CANCELLED and $.cancelledAt missing"
        },
        new HowToTopicViewModel
        {
            Title = "Compare numbers and dates",
            TabIndex = Search, TabName = "Search",
            Steps = ">, >=, <, <= compare numerically when both sides are numbers; otherwise as text, which also orders ISO timestamps correctly.",
            Example = "$.createdAt >= \"2026-01-10T00:00:00Z\" and $.amount < 0",
            TryText = "$.createdAt >= \"2026-01-10T00:00:00Z\" and $.amount < 0"
        },
        new HowToTopicViewModel
        {
            Title = "Tolerate typos and formatting (fuzzy)",
            TabIndex = Search, TabName = "Search",
            Steps = "Either pick the 'Fuzzy (similar)' mode for plain text, or use 'similar to' in a query.\n" +
                    "Matches ≥ 80% similar ids, ignoring case, dashes/underscores/spaces and leading zeros: 'ord 0042' ≈ 'ORD-42'.",
            Example = "any similar to \"ORD-0042\"",
            TryText = "any similar to \"ORD-0042\""
        },
        new HowToTopicViewModel
        {
            Title = "Search for text that looks like a query",
            TabIndex = Search, TabName = "Search",
            Steps = "Wrap the whole thing in quotes to search for it literally.",
            Example = "\"key equals\"",
            TryText = "\"key equals\""
        },
        new HowToTopicViewModel
        {
            Title = "Save, re-run and export a search",
            TabIndex = Search, TabName = "Search",
            Steps = "Open 'Saved searches · history · export · make it a check':\n" +
                    "• Save search stores the query, match options and scope under a name - picking it restores all of it.\n" +
                    "• History re-runs one of the last 30 queries.\n" +
                    "• Export JSON / Export CSV saves the results; Copy summary copies the one-line answer."
        },
        new HowToTopicViewModel
        {
            Title = "Turn a search into a KafScript check or Task",
            TabIndex = Search, TabName = "Search",
            Steps = "1. Run a search that finds what should exist.\n" +
                    "2. Press 'Make a check (Scenario)', or 'Make a Task' with a schedule such as 'every 15 minutes'.\n" +
                    "3. It opens in the Script Editor for the topics that had matches and passes when matching data exists.\n" +
                    "Not convertible (refused with the reason): or, not (…), headers, fuzzy / similar to, missing.",
            Example = "Search:  $.status = CONFIRMED and key starts with \"ORD-\"\n→ Then expect message on topic \"orders\" … where json \"$.status\" equals \"CONFIRMED\" …"
        },

        // ------------------------------------------------------------------ Bulk check ----
        new HowToTopicViewModel
        {
            Title = "Check which ids from a list exist",
            TabIndex = Bulk, TabName = "Bulk check",
            Steps = "1. Paste ids one per line (or comma/semicolon/tab separated), or Import file… (CSV or text).\n" +
                    "2. For a CSV, pick the column and tick 'First line is a header' if needed.\n" +
                    "3. Look in: leave 'any' (key, headers, whole value, or any id-like token in the JSON) or name a field.\n" +
                    "4. Press Check ids. Missing ids are listed first; found ones show count (found ×3 = duplicated), topics and first-seen time.",
            Example = "ORD-1001\nORD-1002\nORD-1003",
            TryText = "ORD-1001\nORD-1002\nORD-1003"
        },
        new HowToTopicViewModel
        {
            Title = "Look for ids in one specific field",
            TabIndex = Bulk, TabName = "Bulk check",
            Steps = "Set 'Look in' to key, a JSON path or a header, so an id that only appears elsewhere (e.g. in a description) doesn't count.",
            Example = "key\n$.orderId\nheader \"trace-id\""
        },
        new HowToTopicViewModel
        {
            Title = "Hand the missing ids to someone",
            TabIndex = Bulk, TabName = "Bulk check",
            Steps = "Tick 'Only missing' to hide the found ones, then 'Copy missing ids' (one per line) or 'Export CSV' for the full result.\n" +
                    "Thousands of ids cost about the same as one: each topic is read once."
        },

        // ------------------------------------------------------------------ Trace ----
        new HowToTopicViewModel
        {
            Title = "Follow one id across topics",
            TabIndex = Trace, TabName = "Trace",
            Steps = "1. Enter the id (e.g. an order number).\n" +
                    "2. Optionally set 'Look in' (same rules as Bulk check).\n" +
                    "3. Press Trace. Every message carrying it is shown as a timeline, summarized as a path like orders → payments → shipments, " +
                    "with the time since the previous hop and since the first one.",
            Example = "ORD-1042",
            TryText = "ORD-1042"
        },
        new HowToTopicViewModel
        {
            Title = "Find where a flow got stuck",
            TabIndex = Trace, TabName = "Trace",
            Steps = "Trace an id that 'never arrived': the last hop in the path is where it stopped. A big gap between hops points at a slow consumer.\n" +
                    "To check many ids at once, use Bulk check on the downstream topic, or Reconcile the two topics."
        },

        // ------------------------------------------------------------------ Reconcile ----
        new HowToTopicViewModel
        {
            Title = "Compare two topics: what's missing?",
            TabIndex = Reconcile, TabName = "Reconcile",
            Steps = "1. Topic A and its join field (e.g. orders, key).\n" +
                    "2. Topic B and its join field (e.g. payments, $.orderId).\n" +
                    "3. Press Reconcile. Problems are sorted first: only in A (never arrived), only in B, different, then matched.\n" +
                    "It also reports counts per side, A→B latency (avg/max) and messages without the join field.",
            Example = "A: orders    join on key\nB: payments  join on $.orderId"
        },
        new HowToTopicViewModel
        {
            Title = "Spot pairs whose values differ",
            TabIndex = Reconcile, TabName = "Reconcile",
            Steps = "Fill 'fields to compare between matched pairs'. Pairs that disagree are listed as 'different' with the fields, e.g. $.amount: 25 ≠ 24.\n" +
                    "Select a row for a field-by-field diff (timestamps and ids ignored); Export CSV saves the whole reconciliation.",
            Example = "$.amount, $.currency",
            TryText = "$.amount, $.currency"
        },

        // ------------------------------------------------------------------ Similar ----
        new HowToTopicViewModel
        {
            Title = "Find near-duplicates of a message",
            TabIndex = Similar, TabName = "Similar",
            Steps = "1. Set the reference: 'Find similar' on any message, or select a result and press 'Use shown message'.\n" +
                    "2. Compare: Content. Keep 'Ignore timestamps/ids' on to catch retries that differ only in createdAt/traceId/UUIDs.\n" +
                    "3. Set the threshold and press Find similar. Matches are listed best first; select one to see how it differs."
        },
        new HowToTopicViewModel
        {
            Title = "Find messages with the same shape (schema variants)",
            TabIndex = Similar, TabName = "Similar",
            Steps = "Pick Compare: Shape. It scores the overlap of JSON field paths, ignoring values (arrays folded: $.items[*].sku is one field).\n" +
                    "Useful to find old/new versions of an event, or producers that send a different payload."
        },
        new HowToTopicViewModel
        {
            Title = "Find messages sharing some field values",
            TabIndex = Similar, TabName = "Similar",
            Steps = "Tick fields of the reference (key, headers and every JSON leaf are listed; volatile ones are flagged) and press " +
                    "'Search ticked field values'. 'Search same key' does it for the key alone. Both run as a query on the Search tab."
        },

        // ------------------------------------------------------------------ Duplicates ----
        new HowToTopicViewModel
        {
            Title = "Find duplicated messages",
            TabIndex = Duplicates, TabName = "Duplicates",
            Steps = "1. Pick a topic (the scope bar's range applies).\n" +
                    "2. Group by: same key, identical value, key + value, value ignoring timestamps/ids (producer retries), or chosen fields.\n" +
                    "3. Press Find duplicates. Each group lists its count, positions and the time between first and last copy."
        },
        new HowToTopicViewModel
        {
            Title = "Find duplicate business events",
            TabIndex = Duplicates, TabName = "Duplicates",
            Steps = "Group by chosen fields and list the fields that make an event unique - two messages with the same values are a duplicate even if ids differ.",
            Example = "$.orderId, $.eventType",
            TryText = "$.orderId, $.eventType"
        },

        // ------------------------------------------------------------------ Field stats ----
        new HowToTopicViewModel
        {
            Title = "Profile a topic's fields",
            TabIndex = FieldStats, TabName = "Field stats",
            Steps = "1. Pick a topic and press Analyse.\n" +
                    "2. Every field (key, headers, each JSON path) shows presence, missing count, nulls, distinct values, types and first/last seen.\n" +
                    "3. Select a field to see its top values; 'Find' next to a value searches the topic for it."
        },
        new HowToTopicViewModel
        {
            Title = "Spot schema drift",
            TabIndex = FieldStats, TabName = "Field stats",
            Steps = "Tick 'Only fields missing from some messages' - fields that only some producers send (or stopped sending) stand out. " +
                    "Two types for one field (e.g. string and number) is another tell. Use 'filter fields…' to narrow long lists.\n" +
                    "To enforce the shape, go to QA Lab → Contract check."
        },
    };
}

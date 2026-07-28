# ADR 0031: Devin CLI SQLite integration

Status: Accepted

## Context

The Devin CLI stores local conversation state in a SQLite database at
`~/.local/share/devin/cli/sessions.db`. The `message_nodes` table holds a forest
of JSON `chat_message` blobs linked by `parent_node_id`; the `sessions` table
holds per-session metadata including the model label, hidden flag, and a
vendor credit-cost aggregate.

Tokscale accounts for local token usage only. A full conversation
reconstruction (fork synthesis, branch anchoring, tool pairing) is not needed
for usage accounting and would import structure this fork does not use.

## Decision

### Scope: token metrics only

The Devin integration reads `message_nodes` joined with `sessions` and emits
one usage record per assistant node that carries a `metadata.metrics` object.
The four token buckets (`input_tokens`, `output_tokens`, `cache_read_tokens`,
`cache_creation_tokens`) map directly to `TokenBreakdown`; `cache_creation_tokens`
becomes `cache_write`. `total_time_ms` is streaming timing and is dropped.

The forest structure is not projected. Twin child nodes that share the same
`message_id` and the same metrics are deduplicated by
`(session_id, message_id)` so each assistant turn is counted once per session.
Nodes without `message_id` fall back to a per-node key and are kept
individually.

### Authority: session model label

The `sessions.model` label is the authoritative model identity for every
usage row in that session. It is preserved verbatim and canonicalized by the
shared pricing/identity pipeline. Provider attribution is inferred from the
model id (for example `claude-opus-4-8-medium` → `anthropic`); unresolvable
providers such as `devin-mini` stay `unknown` rather than being guessed.

### Credit cost is ignored

`sessions.metadata.total_credit_cost` and `total_acu_cost` are not read into
usage cost. Tokscale derives local usage cost from token buckets and its own
pricing table (ADR 0001). Vendor credits, balances, and reseller markup are
not mixed into token-derived cost.

### Discovery and caching

Discovery resolves `~/.local/share/devin/cli/sessions.db` plus any
`scanner.extraScanPaths` roots for the `devin` client id. The database is
opened read-only with committed WAL state via `SqliteWithWal` fingerprinting.
The integration uses the uncached parse path: the Devin CLI maintains a
single live database that changes as a whole, so per-file message-cache
shards would not amortize across independent inputs.

### Boundary behavior

- Rows with all token buckets equal to zero are ignored; failed generations
  with no billable usage do not create usage rows.
- Hidden sessions (`sessions.hidden = 1`) are excluded.
- Malformed `chat_message` JSON is rejected as `malformed-record`; the scan
  continues with sibling rows.
- A token-bearing row without a non-empty session model is rejected as
  `missing-model`.
- A row whose `created_at` cannot be represented as a positive millisecond
  timestamp is rejected as `missing-timestamp`.

## Consequences

Devin local usage appears under the `devin` client id in CLI output, the TUI,
and the cache. Cost is token-derived and may be `$0.00` when the model id is
not priceable; that outcome is explicit, not a guessed price. The forest
structure and credit aggregates are intentionally not represented.

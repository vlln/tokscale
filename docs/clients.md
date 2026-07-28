# Local clients and inputs

`crates/tokscale-core/client-catalog.json` is the canonical client-ID catalog.
The registered vertical integration and decoder schema for each ID are authoritative for
discovery and parsing. This page is the current user-facing map of that
executable contract.

Execute one registered integration and inspect its Data Health with:

```bash
bun run cli -- models --client codex --json --no-spinner
```

For an installed binary, replace `bun run cli --` with `tokscale`. The catalog,
registered integrations, and table below define discovery; runtime input failures
are reported by Models Data Health and the TUI rather than by a second path
inventory.

Every built-in input root in the table is a fixed platform path beneath the
current home directory. `**` means recursive discovery under the stated root.

## Current discovery table

| ID | Display name | Current local inputs | Input semantics |
| --- | --- | --- | --- |
| `opencode` | OpenCode | `~/.local/share/opencode/opencode.db` and `opencode-<channel>.db`; direct files from `scanner.opencodeDbPaths` | Reads SQLite with committed WAL state, joins messages to sessions, validates assistant token payloads, and deduplicates across databases. |
| `claude` | Claude | `~/.claude/projects/**/*.jsonl`; `~/.claude/transcripts/**/*.jsonl` | Reads Claude Code assistant usage and resolves project/workspace metadata from the provider files. |
| `codex` | Codex CLI | `~/.codex/sessions/**/*.jsonl`; `~/.codex/archived_sessions/**/*.jsonl` | Reads provider-written interactive and exec session events with append-aware parsing and stable cross-file deduplication. |
| `gemini` | Gemini CLI | `~/.gemini/tmp/<named-project>/chats/session-*.json` and `session-*.jsonl` | Requires `.project_root` in each named project directory and uses it as workspace identity. |
| `amp` | Amp | `~/.local/share/amp/threads/**/T-*.json` | Reads token-bearing events from each thread usage ledger. |
| `droid` | Droid | `~/.factory/sessions/**/*.settings.json` with the session event stream and Mission metadata beside it | Reads Factory session usage, workspace metadata, and agent-role attribution from the related provider artifacts. |
| `openclaw` | OpenClaw | `~/.openclaw/agents/**/*.jsonl*` | Reads agent session indexes and token-bearing session transcripts from the OpenClaw agent tree. |
| `pi` | Pi | `~/.pi/agent/sessions/**/*.jsonl` | Reads Pi agent session events and usage fields. |
| `omp` | OMP | `~/.omp/agent/sessions/**/*.jsonl` | Reads OMP agent session events, including parent/child session attribution. |
| `kimi` | Kimi | `~/.kimi-code/sessions/**/agents/*/wire.jsonl` | Reads ordered per-agent `usage.record` events and reconciles each model alias with its applicable `llm.request` or current config entry. |
| `qwen` | Qwen | `~/.qwen/projects/**/*.jsonl` | Reads assistant records carrying Qwen usage metadata. |
| `roocode` | Roo Code | `~/.config/Code/User/globalStorage/rooveterinaryinc.roo-cline/tasks/**/ui_messages.json`; `~/.vscode-server/data/User/globalStorage/rooveterinaryinc.roo-cline/tasks/**/ui_messages.json` | Reads task UI messages together with the required `api_conversation_history.json` sibling. |
| `mux` | Mux | `~/.mux/sessions/**/session-usage.json` | Reads per-session model usage summaries and applies stable record deduplication. |
| `kilo` | Kilo | `~/.local/share/kilo/kilo.db` | Reads the Kilo SQLite store with committed WAL state for all Kilo frontends using that data environment. |
| `hermes` | Hermes | `~/.hermes/state.db` | Reads token records from Hermes state and derives cost from Tokscale pricing. |
| `copilot` | Copilot | `~/.copilot/otel/*.jsonl` | Reads Copilot OTEL file-export records and workspace metadata. |
| `goose` | Goose | `~/.local/share/goose/sessions/sessions.db` or `~/Library/Application Support/goose/sessions/sessions.db` | Reads Goose SQLite session stores with committed WAL state. |
| `codebuff` | Codebuff | `~/.config/{manicode,manicode-dev,manicode-staging}/projects/**/chat-messages.json` | Reads Codebuff conversation token records. |
| `codebuddy` | CodeBuddy | `~/.codebuddy/projects/**/*.jsonl`; CodeBuddy IDE and VS Code extension `*.log` trees in the platform application-data directories | Reads assistant/function-call usage and final agent usage, then deduplicates mirrored JSONL and extension-log records. |
| `antigravity` | Antigravity | `~/.gemini/antigravity-cli/conversations/*.db` | Reads AGY CLI SQLite with committed WAL state and decodes the current protobuf token-accounting fields. |
| `zed` | Zed Agent | `~/.local/share/zed/threads/threads.db`; fixed macOS and Windows application-data equivalents | Reads hosted Zed assistant usage from SQLite with committed WAL state and excludes external-agent records by ownership fields. |
| `zcode` | ZCode | `~/.zcode/projects/**/*.jsonl` | Reads Z.ai ADE assistant usage from project transcripts. |
| `kiro` | Kiro | `~/.kiro/sessions/cli/**/*.json` with optional `.jsonl` sidecars; platform `kiro-cli/data.sqlite3`; Kiro `globalStorage/kiro.kiroagent` execution/session files | Combines CLI file, CLI SQLite, and IDE workspace inputs under the `kiro` identity. |
| `junie` | Junie | `~/.junie/sessions/**/events.jsonl` | Reads JetBrains Junie session events and token usage. |
| `warp` | Warp | Platform `warp.sqlite` roots listed under [Warp local token parsing](#warp-local-token-parsing) | Reads local conversation/model token totals from SQLite with committed WAL state. |
| `cline` | Cline | `~/.cline/data/sessions/**/*.messages.json` | Reads the SDK v1 messages envelope and optional root manifest workspace metadata. |
| `commandcode` | Command Code | `~/.commandcode/projects/**/*.jsonl` with the root `config.json` | Reads usage transcripts and uses the required config dependency for model/workspace interpretation. |
| `grok` | Grok | `~/.grok/sessions/**/updates.jsonl` with optional `summary.json` and `events.jsonl` siblings | Reads positive total-token deltas and optional session metadata. |
| `devin` | Devin | `~/.local/share/devin/cli/sessions.db`; direct files from `scanner.extraScanPaths` | Reads SQLite with committed WAL state, extracts assistant `metadata.metrics` token buckets from `message_nodes`, and deduplicates twin nodes by `(session_id, message_id)`. |

## Scanner extensions

Persistent recursive roots use `scanner.extraScanPaths` in
`settings.json`. Keys must be IDs from the table and paths must be non-empty:

```json
{
  "scanner": {
    "extraScanPaths": {
      "codex": [
        "/srv/imports/codex/sessions"
      ],
      "gemini": [
        "/srv/imports/gemini/tmp"
      ],
      "warp": [
        "/mnt/c/Users/me/AppData/Local/warp/Warp/data"
      ]
    }
  }
}
```

Generic extra roots are accepted for every table ID except `opencode`.
OpenCode accepts explicit database files instead:

```json
{
  "scanner": {
    "opencodeDbPaths": [
      "/srv/opencode/opencode-team.db"
    ]
  }
}
```

Each `opencodeDbPaths` entry is an authoritative input. A missing file, a
non-file path, an unreadable database, or a schema mismatch is reported through
Data Health. Automatic OpenCode discovery selects only `opencode.db` and
`opencode-<channel>.db`.

Extra roots use the same filename and schema rules as the adapter's default
root; they do not broaden accepted formats. Canonical paths are deduplicated
before parsing. An explicit `--home` resolves the same fixed built-in layout
beneath the supplied home; configured extra roots remain additional inputs.

## Shared parsing and health rules

Discovery treats an absent built-in root as no local input. Other traversal
errors are input failures. A discovered unit that cannot be opened, queried,
snapshotted, or parsed is unavailable or partial in Data Health; it is not
reported as a clean empty input. Record-level schema failures are counted as
rejections while valid records from the same input remain usable when parsing
can continue.

Adapters require the identity, model, session, timestamp, and token fields
defined by their current schema. Timestamps and token counts must be finite,
representable, and semantically valid for that schema. Provider is optional
usage attribution: Tokscale infers it from a valid model when possible and uses
`unknown` otherwise.

## Warp local token parsing

Warp discovery selects these provider data directories:

- Linux and FreeBSD:
  `~/.local/state/{warp-terminal,warp-terminal-preview,warp-terminal-dev,warp-terminal-local,warp-oss}/warp.sqlite`;
- macOS: `warp.sqlite` beneath
  `~/Library/Group Containers/2BBY89MBSN.dev.warp/Library/Application Support`
  or `~/Library/Application Support`, in
  `{dev.warp.Warp-Stable,dev.warp.Warp,dev.warp.Warp-Preview,dev.warp.Warp-Dev,dev.warp.Warp-Local,dev.warp.WarpOss}`;
- Windows:
  `~\AppData\Local\warp\{Warp,WarpPreview,WarpDev,WarpLocal,WarpOss}\data\warp.sqlite`.

The Warp adapter opens each discovered `warp.sqlite` read-only with its
committed WAL state. It reads `agent_conversations.conversation_data`, emits one
usage row for each positive conversation/model token total, and uses
`last_modified_at` as the report timestamp. Each row requires a
conversation ID, model ID, valid timestamp, and integer token total.

For each `conversation_usage_metadata.token_usage` entry, the authoritative
total is the checked sum of `warp_tokens`, `byok_tokens`, and
`custom_endpoint_tokens`. A missing or null counter contributes zero; a
non-integer, negative, or overflowing counter rejects that entry. This local
acquisition does not call a Warp account or synchronization service.

Warp stores these entries as total-token aggregates rather than input, output,
cache, and reasoning buckets. Tokscale applies the fixed total-only allocation
to the complete set of accepted rows, then performs ordinary pricing and report
aggregation.

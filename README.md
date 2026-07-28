# Tokscale local-first fork

> An independently maintained fork of
> [junhoyeo/tokscale](https://github.com/junhoyeo/tokscale), focused on local
> AI coding-client usage accounting, explicit data semantics, and predictable
> resource usage on large transcript collections.

> [!IMPORTANT]
> This repository is not the upstream release channel and is not a drop-in
> mirror. Supported clients, cost semantics, and selected workflows
> intentionally differ from upstream.
>
> `npx tokscale@latest`, `bunx tokscale@latest`, and the public `tokscale` npm
> package install the upstream distribution, not the code on this branch. Fork
> npm releases use `@juya-ai/tokscale`; use the source-build flow below when
> validating behavior specific to this fork.

![Tokscale TUI overview](.github/assets/tui-overview.png)

## What this fork is

Tokscale reads local state from AI coding clients and turns token-bearing
records into CLI output and TUI views. This fork keeps the terminal-first workflow
from upstream while tightening the rules around local data, client identity,
pricing, and resource usage.

The active maintained branch is `personal/local-clients`.

## Why this fork exists

- **Local-first accounting.** Local usage is built from token-bearing
  records. Vendor-reported spend, credits, balances, and cost-only rows are not
  mixed into derived token cost.
- **Explicit behavior.** Parser failures, missing data, unknown clients, and
  unmatched pricing stay visible instead of being hidden behind guessed aliases
  or fake success paths.
- **Stable client identity.** Client ids, display facts, and generated Rust
  identity data come from `crates/tokscale-core/client-catalog.json`.
- **One canonical generation.** The complete TUI and its headless Models
  projection derive from the same immutable usage generation.
- **Lower memory overhead.** The message pipeline avoids unnecessary clones and
  skips full reloads when input files have not changed.
- **Curated upstream adoption.** Upstream fixes are reviewed and ported
  selectively. This fork does not automatically adopt every upstream client,
  hosted workflow, or release policy.

See [fork scope](docs/fork.md), [maintainer context](CONTEXT.md), and
[architecture decisions](docs/adr/).

## Build this fork

Prerequisites:

- Bun
- A stable Rust toolchain

```bash
git clone --branch personal/local-clients --single-branch \
  https://github.com/makoMakoGo/tokscale.git

cd tokscale
bun install
bun run build:core
```

Run the local wrapper:

```bash
# Launch the interactive TUI
bun run cli

# Script-friendly output
bun run cli -- models --no-spinner

# Inspect one Client's local usage
bun run cli -- models --client codex --no-spinner
```

`bun run cli` executes the code in this checkout through `packages/cli`. The
public npm package named `tokscale` is still the upstream package. Once a fork
release is published, install this fork with:

```bash
npm install -g @juya-ai/tokscale
```

## Common commands

```bash
# TUI
tokscale
tokscale tui
tokscale tui --tab models

# Canonical headless Models projection
tokscale models --no-spinner
tokscale models --no-spinner --json
tokscale models --group-by client,model --no-spinner

# Filters
tokscale tui --client opencode,claude --week
tokscale models --since 2026-01-01 --until 2026-01-31
tokscale models --group-by client,provider,model --json

# TUI-only views; --tab opens the full TUI focused on that tab
tokscale tui --tab usage
tokscale tui --tab monthly
tokscale tui --tab sessions

# Pricing catalog lookup
tokscale pricing lookup claude-sonnet-4-5 --no-spinner
tokscale pricing overrides --json
```

When running from source, replace `tokscale` with `bun run cli --`.

## Supported clients

The canonical client identity list lives in
`crates/tokscale-core/client-catalog.json`. Full local input details are in
[supported clients](docs/clients.md).

Current catalog entries include:

OpenCode, Claude, Codex CLI, Gemini CLI, Amp, Droid, OpenClaw,
Pi, OMP, Kimi, Qwen CLI, Roo Code, Mux, Kilo,
Hermes Agent, Copilot, Goose, Codebuff, CodeBuddy, Antigravity, Zed Agent,
ZCode, Kiro, Junie, Warp, Cline, Command Code, Grok Build, and Devin.

Some catalog entries have explicit boundaries:

- `grok` and local `warp.sqlite` expose token totals without bucket splits, so
  Tokscale applies the fixed total-only bucket allocation from ADR 0010.
- `commandcode` is transcript-estimated usage, not authoritative vendor token
  accounting.
- `antigravity` reads current AGY CLI SQLite/WAL data directly through its
  registered integration (ADR 0007).

## Data and pricing semantics

Local usage uses one cost meaning: the estimated price of parsed token buckets
under Tokscale's pricing service. App-reported cost fields are ignored for
local usage because they can represent subscriptions, credits, bundle
balances, reseller markup, rounded UI totals, or aggregate spend.

Tokscale canonicalizes model ids before grouping and pricing, stripping
release, date, free-channel, and route decorations that this fork does not
preserve as model identity.

Exact custom overrides from `custom-pricing.json` are checked first. Otherwise,
Tokscale searches LiteLLM, OpenRouter, and models.dev using exact canonical
model ids or exact provider-scoped model ids. Pricing never guesses by prefix,
substring, or fuzzy matching.

If a model cannot be priced, its derived cost remains `$0.00` instead of using
a private guessed price. Details: [pricing semantics](docs/pricing.md).

## Documentation

- [Fork scope and upstream relationship](docs/fork.md)
- [Supported clients and data locations](docs/clients.md)
- [CLI usage](docs/cli.md)
- [Configuration](docs/configuration.md)
- [Pricing semantics](docs/pricing.md)
- [Development and testing](docs/development.md)
- [Architecture decisions](docs/adr/)
- [Upstream port logs](docs/upstream/)

The scan performance helper in `scripts/measure-scan-performance.sh` requires
`jq` and GNU time. It prefers `gtime`; otherwise, it uses `/usr/bin/time` only
after verifying support for GNU `-f` and `-o` options.

## Upstream relationship

This repository intentionally remains in GitHub's fork network to preserve
project provenance. The `personal/local-clients` branch is maintained as a
content-ahead variant: upstream changes are reviewed and selectively ported
rather than merged wholesale.

Fork behavior follows this repository's ADRs and may differ from upstream.
Refer to the upstream repository for official upstream packages, hosted
services, community links, and documentation.

## License and attribution

Based on [Tokscale](https://github.com/junhoyeo/tokscale) by Junho Yeo.

This fork remains available under the MIT License. See [LICENSE](LICENSE).

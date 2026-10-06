# Pump Copy Trader

A Rust service that observes one mainnet wallet through Helius LaserStream and
copies supported Pump.fun bonding-curve and PumpSwap trades through Helius Sender.
It includes the execution hot path, a durable SQLite journal, and a local read-only
database UI. Execution is mainnet only.

## Setup

1. Copy `config.example.toml` to `config.toml` and set the source wallet, nearest
   LaserStream region, token policy, sizing limits, and slippage.
2. Copy `.env.example` to `.env`. Set `HELIUS_API_KEY` and an absolute
   `COPY_TRADER_KEYPAIR_PATH` pointing to the existing copier keypair.
3. Fund the copier with its input assets and enough SOL for rent, fees, and the
   Sender tip. Mainnet LaserStream requires an eligible Helius plan.
4. Check the setup with `doctor`. Set `execution.allow_live_mainnet = true` to
   enable `run`; the example leaves trading disabled.

```bash
cargo run -- --config config.toml doctor
cargo run -- --config config.toml status --limit 20
cargo run -- --config config.toml run
```

`doctor` checks connectivity and balances without submitting a transaction.
`status` opens and migrates SQLite without requiring a signer or network access.
`COPY_TRADER_CONFIG` remains an alternative to `--config`.

The local extraction preserves your ignored `.env` and trading configuration,
including the live gate, and points to the existing keypair. It starts with a
separate empty `copy_trader.sqlite`; source history and stream cursors are not
copied. Secrets, local configuration, keypairs, databases, and build outputs are
excluded from Git.

## Supported routes and unsupported outcomes

Pump.fun supports the existing legacy, V2, and V3 buy/sell instruction rewrites,
including recognized exact-quote-input buys. These bonding-curve copies require
`mainnet.skip = true`. PumpSwap retains source-direct copying and its pool-based
quote/discovery route. Other DEX execution and Surfpool are excluded.

A source must contain one recognized Pump trade, attributable to the tracked
wallet's economic input and output. Unknown or malformed Pump layouts, unsupported
venues or token capabilities, ambiguous trades, and multiple swaps end cleanly
with `unsupported`. A non-Pump source never becomes a Pump copy through discovery.

The journal records the source signature, slot, structured reason, and available
timings. An already reserved attempt also becomes `unsupported`. No copy signature
or landing slot is invented, and nothing is submitted or automatically retried.
The worker logs a concise INFO outcome and moves to the next observation.

Reasons contain `code` and `message`: `unsupported_dex`, `unsupported_instruction`,
`unsupported_token`, `ambiguous_trade`, or `multi_hop`. They appear in the CLI and
the UI's Unsupported filter and trade inspector, including sources without a copy
attempt. Existing policy skips, such as stale signals, disallowed mints, and
insufficient balance, remain `skipped`. Operational failures retain their error
handling rather than being reported as unsupported.

## Execution hot path

```toml
[mainnet]
skip = true
fixed_priority_fee_micro_lamports = 100000
```

`source_direct` remains an alias for `skip`; configure only one. The fixed fee
must not exceed `max_priority_fee_micro_lamports`. Settings take effect on restart.
Source-direct output uses `floor(copy_input * source_output / source_input)` and
configured slippage. Invalid estimates and overflow are rejected. This uses the
source trade's price rather than a fresh quote.

Classic SPL Pump.fun SOL buys can reuse successful source token metadata and avoid
a mint RPC read. Other routes inspect mints, including Token-2022 extensions.
Native PumpSwap direct copies check only the copier's WSOL account, rather than
fetching or scanning pools. An existing WSOL account is reported as unsupported
to preserve the account-lifecycle safeguard; an RPC failure remains an execution
error. Native direct copies create a WSOL ATA before the swap and close it afterward
on both buys and sells. Wallet-derived volume and cashback accounts are rewritten
for the copier when present in the source instruction.

Sizing, token policy, account rewriting, signing, duplicate reservations,
confirmation, and balance reconciliation remain enabled.

The extracted baseline skips transaction simulation and pre-send fee checks in
both routing modes. `skip = false` enables PumpSwap quotes and pool discovery;
it does not restore simulation. The journal records
`{"skipped":true,"reason":"hot_path_no_simulation"}`. Sender uses
`skipPreflight=true` and disables retries. Failed landed transactions can cost fees.

The service warms the blockhash cache before receiving signals and refreshes it
in the background. Entries are valid for less than two seconds from request start,
with more than 20 blocks of validity remaining; refreshes have a 1.5-second timeout.
The hot path requires a fresh cached blockhash. Wallet balances refresh in the
background every 250 ms after each refresh completes, for SOL and configured token
accounts. Input balances must be cached before live execution can proceed.
The output baseline uses the cache when available. Post-confirmation reconciliation
reads balances from RPC. Pool catalogs warm and refresh only in quoted mode
(`mainnet.skip = false`). Source-direct mode starts no pool scans and copies the
decoded source instruction even when its pool is absent from the catalog.
Sender connections are pinged on startup and every 30 seconds.

Token admission defaults to an allowlist. All-token admission requires an explicit
minimum/maximum envelope; configured token entries remain optional per-mint
limits and prewarming hints. Percentage and fixed input sizing are supported.
Balance caches can lag the chain; source-price estimates can
be inaccurate after pool movement. Slippage limits are checked by the on-chain
instruction.

Copies remain sequential through confirmation and reconciliation. Disconnect
recovery journals missed signatures as `missed_offline` without executing them.
Only live stream updates can execute. Processed source observations can still be
removed by a fork.

## SQLite journal and timings

Startup applies the retained additive migrations. Historical schema objects for
the original backend remain for migration compatibility but have no seeding
runtime in this service. Connections use WAL and `synchronous=FULL`.

Observation, intent, and unique attempt reservation writes are awaited before
submission. Route and signed-transaction writes happen after the Sender response,
including errors. A crash between sending and persisting the signature can leave
an uncertain attempt; startup marks it `unknown` and never retries it automatically.
Confirmed landing slots are saved before reconciliation, including on-chain failures.

Timing JSON includes receipt-to-send offsets, preparation, route, Sender,
confirmation, reconciliation, and measured database calls. Missing stages remain
absent. RPC and database stages overlap, so their summed times are not wall time.
Database durations use microseconds; most pipeline durations use milliseconds.

One background task batches up to 32 timing records from a bounded 256-record
queue. Queue overflow or write failures can drop telemetry with a warning;
critical journal writes are separate. The queue drains when the worker finishes
normally. Process termination, including Ctrl-C, can lose pending timing records.
Console logging remains synchronous.

```bash
RUST_LOG=pump_copy_trader=debug cargo run -- --config config.toml run
python3 scripts/report_db_timings.py --db copy_trader.sqlite --target mainnet --status landed --limit 500
cargo run --offline --example logging_latency -- --samples 200 --sink-delay-us 1000 --rpc-delay-ms 2
```

The report is read-only; the logging example is a local model of logging overhead.
Neither starts trading.

## Local database UI

```bash
python3 -B db_ui/server.py
```

Open **http://127.0.0.1:8765**. The server defaults to this repository's journal,
binds to loopback, and can run alongside the trader. Use `--db /path/to/journal.sqlite`
and `--port 8766` for another file or port.

The UI provides a journal with outcome filters, trade details, landing slots,
timings, an execution flow diagram, table browsing, and a read-only SQL console.
Source-only unsupported and skipped observations are included. Refresh explicitly
reloads data; there is no background polling. The UI never runs migrations.
Queries have a two-second budget and a 500-row limit. Writes, attached databases,
extension loading, and cross-origin API access are blocked. All assets stay local.

## Verification

```bash
cargo build --locked
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings -W clippy::all
cargo test --locked --all-targets
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s db_ui -p 'test_*.py'
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_*.py'
node --check db_ui/static/app.js
```

Rust tests use temporary databases and loopback mock RPC servers. They cover Pump
rewrites, bounded routing, recovery, duplicate protection, unsupported outcomes,
and malformed source payloads without submitting to a live network.

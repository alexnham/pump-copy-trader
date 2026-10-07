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
including recognized exact-quote-input buys. Both protocols build
only from decoded source instructions. Pool caches, account scans, quotes, and
route discovery have been removed. Other DEX execution and Surfpool are excluded.

A source must contain one recognized Pump trade, attributable to the tracked
wallet's economic input and output. Unknown or malformed Pump layouts, unsupported
venues or token capabilities, ambiguous trades, and multiple swaps end cleanly
with `unsupported`. A non-Pump source cannot become a Pump copy.

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

Source-instruction copying is always enabled. `skip` defaults to true, and
`source_direct` remains an alias; configure only one. Explicit `skip = false` is
rejected because quote/discovery routing no longer exists. The fixed fee must
not exceed `max_priority_fee_micro_lamports`. Settings take effect on restart.

`[routing]` now contains only `timeout_ms` (default 2000), the source-builder
budget. `race_timeout_ms` remains a timeout alias for older configurations.
Remove `pool_refresh_seconds` and `max_pools_per_dex`; the local and example
configuration files have already been migrated.
Source-direct output uses `floor(copy_input * source_output / source_input)` and
configured slippage. Invalid estimates and overflow are rejected. This uses the
source trade's price rather than a fresh quote.

Pump.fun and PumpSwap buys and sells reuse matching source transaction metadata,
including Token-2022 decimals and token program IDs. Extension inspection runs in
background; the first copy can be sent before inspection finishes. Completed
inspection results apply to later copies. Missing or ambiguous source metadata
falls back to mint RPC. Validated fallback reads are cached for 60 seconds (up to
4096 mints). Caches reset when the trader restarts.
PumpSwap `buy` and `buy_v2` request the slippage-adjusted token output and
keep the copied input amount as the maximum spending limit. These exact-output
buys may spend less than the configured input amount. Exact-input buys and sells
use the slippage-adjusted output as their minimum received amount.

Native PumpSwap copies keep the copier's canonical WSOL account open. Buys use
fresh cached WSOL first and wrap only the shortfall; a missing or stale WSOL
cache falls back to wrapping the full input without another pre-send RPC read.
The WSOL cache is warmed at startup, refreshed in the background even when zero,
and invalidated on submission. Confirmed PumpSwap copies refresh the WSOL
balance for subsequent buys. Sells leave proceeds as WSOL. Account creation is
idempotent and existing WSOL accounts are validated at startup. No automatic
WSOL closure occurs. Native SOL remains necessary for fees, tips, and account
creation. Timing records include `cached_wsol_lamports` and `wrap_lamports`.
Wallet-derived volume and cashback accounts are rewritten
for the copier when present in the source instruction.

Token-to-SOL sells follow the source's sold fraction: copier token balance × source
sold amount ÷ source pre-sell token balance, rounded down. A full source exit sells
all held tokens of that mint in the copier's associated token account, even when
the copied buy received fewer tokens. Buy sizing is not applied again to exits.
Sell sizes bypass generic token-count minimum/maximum limits; zero holdings and
fractions that round to zero are skipped. Buys and token-to-token swaps retain
configured sizing and limits.

The source denominator comes from owned pre-token balances included in the source
transaction; accounts absent from that transaction are not counted. The copier
balance uses the existing balance cache/read, so no additional RPC is introduced.
Missing or inconsistent source balances reject the exit. The copier's entire
held balance of that mint is treated as its position; use a dedicated copier
wallet if manually held tokens of the same mint must remain separate. The journal
records both pre-sell balances in the attempt's timing metadata for inspection.

Sizing, token policy, account rewriting, signing, duplicate reservations,
confirmation, and balance reconciliation remain enabled.

The source-instruction path skips transaction simulation and pre-send fee checks.
The journal records
`{"skipped":true,"reason":"hot_path_no_simulation"}`. Sender uses
`skipPreflight=true` and disables retries. Failed landed transactions can cost fees.

The service warms the blockhash cache before receiving signals and refreshes it
in the background. Entries are valid for less than two seconds from request start,
with more than 20 blocks of validity remaining; refreshes have a 1.5-second timeout.
The hot path requires a fresh cached blockhash. Wallet balances refresh in the
background 500 ms after each refresh completes, with up to eight account reads
in flight. SOL and token accounts with positive or uninitialized balances are
refreshed; newly encountered token accounts join this set automatically. Zero
balances remain cached but are not polled while idle.

Balance entries expire one second after request start. A cache miss or stale
entry triggers a targeted RPC balance read for that account, including mints
absent from configuration. A missing token account is cached as zero only after
RPC confirms its absence; transport failures and invalid balances remain errors.
Submission and confirmation invalidate the affected token balances and SOL.
Post-confirmation input/output reads immediately update the cache, and token-to-token
copies also refresh SOL to account for fees. Failed landed transactions refresh
SOL as well. Older in-flight reads cannot overwrite a newer update or refill an
invalidated entry. Reconciliation always reads RPC, regardless of cache freshness.

Swap accounts come from the source instruction, with
copier-specific accounts rewritten locally. No pool state or route catalog is
loaded, cached, scanned, or refreshed.
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

Startup applies the retained additive migrations. The historical `quoted_output`
column stores the source-price output estimate. Historical schema objects for
the original backend remain for migration compatibility but have no seeding
runtime in this service. Connections use WAL and `synchronous=FULL`.

Live observation, cursor, intent, attempt, and outcome writes enter one ordered
background journal queue. Submission does not wait for SQLite. An in-memory
signature set claims attempts immediately and is seeded from persisted attempts
at startup. The queue holds up to 4096 writes; a full or failed queue rejects
new writes rather than waiting for disk. Persistence failure stops the service.
Startup and reconnect recovery still query SQLite.

A crash after sending but before the queued reservation persists can allow that
source transaction to be copied again after restart. Pending journal records can
also be lost. Shutdown drains accepted journal writes after stopping the service
tasks. Connections retain WAL and `synchronous=FULL` for completed writes.

Decoding shares one parsed account/instruction context across its passes. Mint
strings are decoded once per transaction, fixed program/tip addresses are constants,
and canonical ATA derivations use an 8192-entry cache keyed by owner, mint, and
program. Sequential trade execution is retained.

`payload_decode_us` measures LaserStream payload conversion; `observation_enqueue_us`
measures observation journal enqueueing. `queue_wait_us` measures the time from
starting the execution-channel send to worker receipt, including backpressure.
`ingress_to_worker_us` includes payload conversion and the queue wait, so these
fields overlap. `pre_decode_checks_us` covers admission checks before decoding.
`pre_route_preparation_us` covers metadata, sizing, funding, and reservation after
decoding. `serialization_us` and `cache_invalidation_us` isolate those operations;
`post_route_preparation_us` includes both plus remaining pre-send bookkeeping.

`decode_us`, `route_instruction_build_us`, `transaction_build_us`, and
`transaction_sign_us` measure isolated durations. `sender_request_us` measures the
full Sender request including network response time. `receipt_to_send_start_us`
is a cumulative offset; existing `_complete_ms` offsets remain for compatibility.

Timing JSON includes receipt-to-send offsets, preparation, route, Sender,
confirmation, and reconciliation. Background database writes are excluded from
the live execution timings. The bounded telemetry queue feeds the same ordered
journal writer; telemetry overflow is logged. Console logs use a bounded 4096-entry background writer. If the queue fills,
logs are dropped rather than blocking execution. Normal process exit attempts to flush
the writer; abrupt termination can lose queued logs. Verbose decode diagnostics
require debug logging.

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
rewrites, source-only routing, recovery, duplicate protection, unsupported outcomes,
and malformed source payloads without submitting to a live network.

Output reconciliation uses confirmed `getTransaction` metadata: token output is
the transaction's post-balance minus its pre-balance, with an absent pre-balance
treated as zero for a newly created account. Native SOL output includes the
recorded transaction fee and configured sender tip. Output-balance RPC reads run
after confirmation to refresh the wallet cache; submission does not wait for an
output baseline read. `reconciliation_metadata_ms` measures metadata retrieval,
including polling when confirmed metadata is not available yet. Retrieval is
bounded by `confirmation_timeout_seconds`; invalid metadata produces an error
rather than assuming a received amount.

Sender accepts the global HTTPS endpoint and Helius regional HTTP endpoints
(`slc`, `ewr`, `lon`, `fra`, `ams`, `sg`, `tyo`). For a bot hosted near Toronto,
Newark is `http://ewr-sender.helius-rpc.com/fast`. Connection warming uses the
configured endpoint's `/ping` path.

### Receipt-to-send latency

Live gRPC metadata keeps inner instruction data and loaded addresses in binary
form. RPC recovery still accepts JSON/base58 metadata. This avoids encoding and
then decoding the same inner instructions and addresses before submission.
Instruction inspection details are collected only when debug logging is enabled.
Source and copier volume PDAs are cached by program and wallet and prewarmed at
startup; token account addresses retain their existing cache.

After rebuilding and restarting, inspect recent samples with:

```sh
cargo run --release -- latency --limit 100
```

The command reports p50/p95/p99 and maximum values in microseconds, plus the
fraction of measured submissions below 1,000 µs. Missing measurements are excluded;
zero is a valid measurement. The limit selects recent source records, including
skips and failures, so the measured sample count may be smaller. Sender response
latency is reported separately from receipt-to-send. Existing records are retained,
so use a recent window to compare after restart. Cache-miss RPC reads and the serial
worker's confirmation/reconciliation can still increase latency during bursts.
These optimizations alone do not establish a live sub-1 ms p95 guarantee.

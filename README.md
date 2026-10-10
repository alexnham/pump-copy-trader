# Pump Copy Trader

A Rust service that observes one mainnet wallet through Helius LaserStream and
copies supported Pump.fun bonding-curve and PumpSwap trades through Helius Sender.
It includes the execution hot path, a durable SQLite journal, and a local read-only
database UI. Execution is mainnet only.

## Setup

Building requires Rust 1.98.1 or newer for the transaction wire decoder.

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


`[signal.preconfirmations]` is enabled by default and subscribes to Helius
`preconfSubscribe` at `wss://beta.helius-rpc.com/`, using `HELIUS_API_KEY`. Set
`enabled = false` to use LaserStream alone. `doctor` checks the subscription when
enabled. The feed filters for the source wallet and includes Helius and BAM
preconfirmations. Successful and unknown-status messages may trigger eligible buys;
known failed messages are discarded before execution. BAM signals arrive before
execution, so the source trade may subsequently fail. Reconnects use bounded backoff while
LaserStream continues at `signal.commitment`.

Early execution supports a single direct Pump.fun or PumpSwap buy with canonical
wallet token accounts and SOL/WSOL quote. Buy amounts come from instruction limits:
exact-input buys use the specified input and minimum output; exact-output buys use
the maximum input and requested output. Percentage sizing therefore uses the
source's input budget for exact-output buys, rather than its eventual actual spend.
Zero output limits, routed/CPI trades, noncanonical accounts, and sells defer to
LaserStream. Sells retain position-relative sizing from processed pre-sell balances.
Legacy, v0, and v1 wire transactions are decoded; v0 lookup mappings are learned
from LaserStream metadata and an unknown mapping defers to the processed feed.
Mint safety checks, funding limits, copy confirmation, and settlement remain active.

Signatures are deduplicated in the worker and reserved through the shared journal
before submission. Preparation failures before reservation remain eligible for
processed fallback. Preconfirmation observations are journaled with their origin;
the later LaserStream observation supplies the processed slot without submitting
another copy. Timing records include `preconfirmation = 1` for early signals and
`preconfirmation_status_unknown = 1` when execution used an unknown-status signal.
Raw observation payloads retain `status: "unknown"` instead of claiming success.
A successful leader execution is provisional and may not land on the canonical
chain; a copy can execute even if its source later drops. Preconfirmations require
an eligible Helius plan and coverage varies by leader. See the
[subscription reference](https://www.helius.dev/docs/pre-confirmations/preconf-subscribe).

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
background with a shared four-request limit; the first copy can be sent before
inspection finishes. Completed
inspection results apply to later copies. Missing or ambiguous source metadata
falls back to mint RPC. Validated fallback reads are cached for 60 seconds (up to
4096 mints). Caches reset when the trader restarts.
PumpSwap `buy` and `buy_v2` request the slippage-adjusted token output and
keep the copied input amount as the maximum spending limit. These exact-output
buys may spend less than the configured input amount. Exact-input buys and sells
use the slippage-adjusted output as their minimum received amount.

Native PumpSwap copies keep the copier's canonical WSOL account open. Buys use
cached WSOL first and wrap only the shortfall; a missing or invalidated WSOL
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

Balance entries do not expire with age: execution reuses the last cached value,
assuming this pipeline is the only writer to wallet balances. A missing or
invalidated entry triggers a targeted RPC balance read for that account, including
mints absent from configuration. A missing token account is cached as zero only after
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

Preparation and submission stay ordered. After a Sender acknowledgment, each copy
confirms and reconciles in a background task while the worker takes the next signal.
At most 64 settlement tasks run at once. Per-account reservations prevent pending
copies from reusing input funds, including WSOL, tips, worst-case priority fees,
and a conservative 0.01 SOL allowance per copy for account rent and base fees.
Tracked input accounts reuse this conservative balance budget during an overlapping
batch, avoiding another pre-send balance RPC after each submission. RPC refreshes
do not credit the budget while copies are pending. Reservations remain deducted
for the entire overlapping batch, then affected cache entries are invalidated before fresh balances can be used. Ambiguous sends
and confirmation timeouts retain their budgets until restart recovery.
Graceful shutdown drains queued observations and settlement tasks before the journal.
Disconnect recovery journals missed signatures as `missed_offline` without executing them.
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
at startup. The queue holds up to 16,384 writes; a full or failed queue rejects
new writes rather than waiting for disk. Persistence failure stops the service.
Startup and reconnect recovery still query SQLite.

A crash after sending but before the queued reservation persists can allow that
source transaction to be copied again after restart. Pending journal records can
also be lost. Shutdown drains accepted journal writes after stopping the service
tasks. Connections retain WAL and `synchronous=FULL` for completed writes.

Decoding shares one parsed account/instruction context across its passes. Mint
strings are decoded once per transaction, fixed program/tip addresses are constants,
and canonical ATA derivations use an 8192-entry cache keyed by owner, mint, and
program. `execution.preparation_workers` enables 1–16 preparation lanes (default 4).
Each lane decodes and builds independent Pump.fun buys. Both feeds and duplicate
observations of a signature use the same FIFO lane. Exit sizing and WSOL funding
remain under a shared admission gate; all final fund reservations use that gate.
Sender submission and settlement run in bounded background tasks.

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
so use a recent window to compare after restart. Cache-miss RPC reads can still
increase latency during bursts. `execution.max_concurrent_sends` defaults to 8
and accepts 1–64: this bounds simultaneous submission calls, independently of the
wallet-wide 64-copy bound covering preparation, queued submissions, and settlement. A fan-out call may issue
multiple endpoint requests. Signed bytes and route metadata are queued for the
ordered journal before dispatch; fan-out persistence is queued on the same writer.
Normal journal writes remain asynchronous, with a bounded 16,384-write buffer;
this absorbs the journal backlog from the 1,000-copy benchmark but does not raise
SQLite's sustained write throughput. Overflow still reports an error.

`submission_wait_us` measures waiting for a submission permit.
`receipt_to_send_start_us` is measured in the background task after acquiring it,
so it includes both worker queueing and submission-slot wait. Shutdown drains
submission and settlement tasks. Unknown responses retain reserved funds; known
results release the batch budget only when all preparation, submission, and
settlement are idle. `preparation_worker_id` identifies the lane in timing JSON.
The local and example config use four workers and 32 concurrent sends; legacy
configs without `max_concurrent_sends` keep the default of eight.
These optimizations alone do not establish a live sub-1 ms p95 guarantee.

Trade input limits stay under `[[tokens]]`. `maximum_input` caps percentage or
fixed input sizing instead of skipping an oversized copy. For SOL,
`maximum_input = "0.05"` caps swap input at 0.05 SOL; fees, tips, and account rent
are additional. Inputs below `minimum_input` are skipped. Token-to-SOL exits
continue to mirror the source sold fraction of the copier position. Config
changes take effect after restarting the trader.

### Source transaction v1 support

Live LaserStream ingestion accepts legacy, v0, and v1 sources. V1 config presence
identifies the format; inline keys, instruction bytes, and signer/writable flags
are normalized into the existing read-only source view without base58 conversions
or added RPC calls. The source budget config is retained as `sourceV1Config` in
metadata. This view is not the original signed wire message and must never be
serialized, signature-verified, or submitted as that source transaction.
V1 lookup tables, loaded addresses, invalid headers/indexes, and oversized account
or instruction counts are rejected. Copies still use the existing legacy builder
with local compute budget and fee settings; source v1 budgets are not copied.
Reconciliation opts into RPC transaction version 1. Reconnect recovery remains a
signature audit, not replay of missed transactions.

Run `cargo test --release benchmark_v1_source_decode -- --ignored --nocapture`
for a 64-account/64-instruction source conversion benchmark. This excludes stream
protobuf parsing, queueing, and trade execution; live receipt-to-send percentiles
must be measured separately and are not guaranteed below 1 ms.

PumpSwap copies encode `buy` as base output followed by maximum quote input,
`buy_exact_quote_in` as quote input followed by minimum base output, and `sell`
as base input followed by minimum quote output. These layouts are checked against
the PumpSwap IDL and covered by amount-order regression tests.

## Transaction gap

Trade details show **Transaction gap**, the number of transactions strictly
between the source and copy in finalized block order, including votes and failed
transactions. The journal also shows **Tx gap**. Adjacent transactions count as
zero; unavailable data stays blank.

Run `cargo run --release -- transaction-gaps` as a separate process to enrich
new and historical copies. It polls four attempts every 30 seconds, uses its own
runtime and HTTP pool, and does not start trading or require a signer. Lookups
use signature-only blocks, retry after five minutes, and are capped at 512 slots
and 60 seconds per attempt. SQLite and RPC quota remain shared resources.

### Durable nonce fan-out (opt-in)

Set `mainnet.fanout.enabled = true`, list initialized nonce account public keys
in `nonce_accounts`, and configure 1–8 `[[mainnet.fanout.routes]]` entries.
Both config files include disabled examples. Each route requires a unique `name`,
HTTP(S) `url`, `tip_account`, `tip_lamports`, and
`priority_fee_micro_lamports` (bounded by the existing maximum).
`submit_timeout_ms` bounds each concurrent submission (default 1500).
When enabled these routes replace the single Sender submission; the existing
Sender settings remain the default when disabled.

Routes must accept standard JSON-RPC `sendTransaction` with a base64 legacy
transaction, `skipPreflight=true`, and `maxRetries=0`. Bundle APIs and direct TPU
transport and address lookup tables are not implemented. Fan-out rejects any
variant exceeding Solana’s 1232-byte packet limit; larger PumpSwap routes may
require a future versioned-transaction implementation. Supply each provider's required tip account and
minimum fee/tip. Recognized Helius Sender URLs automatically receive
`HELIUS_API_KEY` from the environment / `.env` at startup, including the single
Sender URL and fan-out routes. Omit `api-key` from those URLs; empty keys and
`YOUR_API_KEY` / `YOUR_HELIUS_API_KEY` placeholders also use the environment key.
Explicit real keys are preserved. Other providers' URLs retain their own
configured authentication. Resolved keys are not written back to config or
stored in the variants journal. Identical variants are journaled once and can be sent to
multiple endpoints.

Create and fund nonce accounts ahead of time using your Solana CLI, with the
copier wallet as nonce authority. For example, using your usual CLI RPC config:

```sh
solana-keygen new --outfile nonce-1.json
solana create-nonce-account nonce-1.json 0.002 --nonce-authority COPIER_WALLET_PUBKEY --keypair /path/to/copier.json
solana-keygen pubkey nonce-1.json
```

Ensure the funding meets the network's nonce-account rent requirement. Add the
last command's public key to `nonce_accounts`. The trader validates finalized
account state, ownership, initialization, and authority at startup; it does not
create accounts or spend account-creation funds automatically. Use a dedicated
pool for this trader and the same durable SQLite database across restarts.

Preparation reserves a cached nonce, places `AdvanceNonceAccount` first, and
signs one variant per route. All variants execute the same swap but may have
different fees/tips. The ordered background journal atomically stores all
variants plus the nonce use, without blocking submission on the commit.
Submission requests run concurrently and
settlement polls all signatures, including after every route reports an error.
The actual confirmed signature replaces the initial journal signature for
balance reconciliation and slot tracking. Fee reservations cover the maximum
configured route tip and priority fee.

Unsent preparation releases its lease. Once persistence is queued, the
nonce remains held until a finalized read proves advancement; request errors,
confirmation timeouts, and dropped tasks never free it. Pool exhaustion fails
preparation without sending. Nonces do not expire: an unresolved transaction
could execute later, and a swap execution failure can still consume a nonce.
Startup recovery checks every persisted variant and refuses to start trading
while a previous fan-out remains unresolved. Do not delete the database to bypass
that check. Resolve the signature history/on-chain outcome before restarting;
there is no automatic cancellation or retry with a fresh nonce.

Nonce rules follow the [Solana durable nonce documentation](https://solana.com/docs/core/transactions/durable-nonces).

### BlockRazor fan-out route

Set `BLOCKRAZOR_API_KEY` in the environment or `.env`, and add the BlockRazor
route shown in `config.example.toml` to the enabled nonce fan-out. Existing routes
default to `provider = "json_rpc"`; BlockRazor uses `provider = "blockrazor"`.
Its HTTP endpoint receives a direct JSON payload with base64 signed bytes,
`mode = "fast"`, `safeWindow = 5`, and `revertProtection = false`, authenticated
with a sensitive `apikey` header. Keys are never added to route URLs or the journal.
Official BlockRazor Solana endpoints and tip accounts are validated. Submission
uses the existing fan-out timeout and shared-nonce winner tracking; even failed
acknowledgements remain eligible for on-chain settlement. Authenticated `/health`
requests warm the connection at startup and every 30 seconds. HTTP redirects are
rejected so authenticated requests cannot be redirected to other hosts.

The local configuration adds New York HTTPS alongside the two Helius routes,
with a 100,000-lamport tip and the same 100,000 micro-lamport priority fee.
BlockRazor documents a default 3 TPS submission limit; higher throughput needs
an approved limit. This integration does not restart or submit live trades.

Protocol reference: https://docs.blockrazor.io/transaction-submission/transaction-sending/solana/send-transaction/request-example/rust

### NextBlock fan-out route

Set `NEXTBLOCK_API_KEY` in the environment or `.env`. A route with
`provider = "nextblock"` submits to `/api/v2/submit` using an Authorization
header containing the raw API key (no Bearer prefix). The body carries
`transaction.content` as base64 signed bytes, `skipPreFlight = true`,
`disableRetries = true`, and the protection/snipe flags disabled. It retains
shared-nonce fan-out signing, bounded submission timeout, acknowledgement-signature
validation, and on-chain winner tracking. `/api/v2/tipfloor` warms the connection.
Credentials are sensitive headers, excluded from config/URLs/journal, and redirects
are rejected. Enabled NextBlock routes require the environment key at startup.

The local configuration adds the New York endpoint with a 1,000,000-lamport tip
and 100,000 micro-lamports/CU priority fee. This stays within the existing largest
route tip budget. One region is configured to conserve submission quota. Other
available hostnames: frankfurt, amsterdam, london, singapore, tokyo, slc, dublin,
and vilnius, each under `nextblock.io`. Restart the rebuilt trader to apply it.

Reference: https://docs.nextblock.io/api/submit-transaction

Fan-out confirmation stores the confirmed variant's `route_name` as
`copy_attempts.landed_route`, alongside its signature. The status command and DB
UI expose it. The migration backfills previously confirmed matching variants;
unconfirmed and ordinary single-sender attempts retain NULL. This identifies
the winning signed variant, not the physical relay when identical signed bytes
were sent through multiple endpoints.

Validation for route attribution: 140 Rust tests and 17 DB UI tests passed,
including confirmed-route selection, unknown-signature rejection, historical
backfill, and unresolved-record NULL handling.

### Detailed nonce fan-out latency

Timing JSON includes `variant_N_build_us` and `variant_N_sign_us` for each route
index N, plus summed `variant_build_us`, `signing_only_us`, and
`variant_size_checks_us`. The existing `transaction_sign_us` remains a combined
stage for compatibility and includes additional variant builds and size checks.
Per-route values overlap the aggregate values and must not be summed together.

The `database` object separates `fanout_journal_barrier`, `fanout_db_begin`,
`fanout_db_writes`, and `fanout_db_commit` (each has `elapsed_us` and `calls`).
`fanout_persist_total` is their parent duration; database totals count the parent
once and exclude its nested stages. WAL/FULL durability remains enabled for
background commits; the live worker does not wait for a pre-send commit.

Run the isolated four-route baseline without sending live transactions:

```sh
BENCH_FANOUT=1 BENCH_PREPARATION_WORKERS=4 BENCH_CONCURRENT_SENDS=32 cargo test --lib --release --offline benchmark_random_laserstream_to_sender -- --ignored --nocapture
```

This imports the four configured routes' provider types, tips and fees, overrides
every endpoint with a local mock, and uses fixture authentication keys. It uses
one mock durable nonce and waits for each previous copy's recorded outcome and
mock finalized nonce refresh before delivering the next receipt. It measures
per-copy processing cost, not burst handling or sustained 200 TPS. The mock marks
only one variant per nonce as landed, and verifies 1,000 landed copies, 4,000
submissions and four durable variant records per copy. The journal is a temporary
on-disk SQLite database; runtime startup, prior-copy waiting, and fixture generation
are excluded from receipt-to-send timing. Production finality and networking are
not modeled. Source observations are synthetic Pump.fun buys, not a replay of the
reported SellV2 transaction.

`fee_balance_lookup_us` isolates the native balance lookup used to reserve fees,
tips, and rent after route construction; it can include an RPC cache miss.
`fanout_variants_serialize_us` isolates preparing the durable variant records.
Both are inside `post_route_preparation_us`, which is broader than the DB commit.

### QuickNode fanout route

QuickNode uses the existing `provider = "json_rpc"` fanout transport and standard
[`sendTransaction`](https://www.quicknode.com/docs/solana/sendTransaction). Add a
route alongside the existing providers (with fanout enabled and nonce accounts
configured):

```toml
[[mainnet.fanout.routes]]
name = "quicknode"
provider = "json_rpc"
url = "https://YOUR-ENDPOINT.solana-mainnet.quiknode.pro/YOUR-TOKEN/"
tip_account = "11111111111111111111111111111111"
tip_lamports = 0
priority_fee_micro_lamports = 100000
```

The standard RPC route has no tip transfer when `tip_lamports = 0`; it uses the
configured priority fee and races the other routes with the same durable nonce.
Keep the real endpoint token in ignored `config.toml`.

Fanout journal writes now run on the ordered background writer; submission does not wait for SQLite. Graceful shutdown drains the writer. An abrupt crash or persistence failure can lose submitted variant and nonce records, reducing restart recovery guarantees. Nonce leases remain reserved in memory until finalized advancement.

For new fanout copies, sender request and response timings follow the landed signature’s successful acknowledgment, rather than the slowest route. Identical signatures use the earliest successful acknowledgment and do not establish endpoint attribution. If the landed signature had no successful acknowledgment, these timings are unavailable. `fanout_all_requests_us` records the full fanout drain duration separately. Historical timings are unchanged.

Astralane Iris fanout uses `provider = "astralane"`, `ASTRALANE_API_KEY` in a sensitive `api_key` header, and standard base64 `sendTransaction` requests. The configured New York HTTPS route tips 0.001 SOL, matching the documented free-tier minimum (5 TPS); higher tiers may allow lower tips. Health checks use `getHealth`. Credentials and provider error bodies are excluded from logged errors. Confirmation remains on the existing Solana RPC. See https://astralane.gitbook.io/docs/low-latency/submit-transactions and https://astralane.gitbook.io/docs/low-latency/send-txn-fee-tiers.

Terminal preconfirmation supports only the verified 60-byte, 39-account single-route native-SOL Pump.fun exact-input buy. It copies directly through Pump.fun, without a Terminal fee. These copies use a **1 raw token unit minimum output**, as configured by the implementation: they do not provide the usual slippage-based price protection. The configured input budget still applies. Other Terminal layouts defer to processed. Accepted copies carry `terminal_preconfirmation: 1`; rejected frames log their signature and reason at INFO.

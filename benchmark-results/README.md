# LaserStream receipt to Sender benchmark

Run on October 9, 2026, on this Mac, using the release build. Each run successfully submitted 1,000 synthetic Pump.fun buys through the production payload conversion, observation journal, execution queue, decoder, source-direct routing, transaction construction, signing, and HTTP Sender client. All endpoints are loopback mocks; no trades were placed.

Sources use unique mint/account addresses and randomized signature bytes and output amounts. Synthetic signatures are fixtures, not cryptographically valid source signatures. Configuration uses config.example.toml with fixed 0.001 SOL sizing, all-token policy, source-direct routing, fan-out disabled, and a temporary on-disk SQLite journal with the production background writer. The blockhash refresher runs during the benchmark. Console tracing is not enabled.

Paced arrivals sleep 5 ms between inputs (Tokio timer scheduling adds overhead). Burst arrivals enqueue as fast as possible through a bounded 256-item channel; reported queue time includes backpressure. Source fixture generation is excluded. Timing begins with an already parsed LaserStream SubscribeUpdate, matching the application receipt boundary. Transport/protobuf deserialization, real LaserStream delivery, actual RPC/Sender networking, and on-chain landing are not measured. The local RPC fixture closes HTTP connections and has Tokio response scheduling overhead, so Sender request timings are not production network predictions.

| Receipt to send start | Mean | p50 | p95 | p99 | Max |
|---|---:|---:|---:|---:|---:|
| paced | 0.664 ms | 0.300 ms | 2.354 ms | 3.244 ms | 12.844 ms |
| burst | 372.135 ms | 403.922 ms | 456.473 ms | 464.099 ms | 465.349 ms |

Paced: 784/1,000 (78.4%) reached send-start within 1 ms. Burst p50 queue wait: 403.824 ms. The sequential worker waits for the local Sender response before processing the next input; queued burst latency accumulates that wait.

Reproduce:

```sh
cargo test --lib --release --offline benchmark_random_laserstream_to_sender -- --ignored --nocapture
BENCH_BURST=1 cargo test --lib --release --offline benchmark_random_laserstream_to_sender -- --ignored --nocapture
```

Raw stage timings are in laserstream-sender-paced.json and laserstream-sender-burst.json. Stage values overlap; do not sum cumulative and isolated timings.

## Concurrent submission results

After implementing background submission, each of these runs sent and recorded
all 1,000 copies successfully. Preparation still runs sequentially. The 64-copy
lifecycle limit covers waiting submissions, active sends, and settlement; a shared
semaphore limits simultaneous send calls. Unknown submissions retain their debits.
Timing begins sending only after the semaphore permit is acquired.

The journal buffer was increased from 4,096 to 16,384 pending writes because the
faster burst filled the smaller buffer. This provides burst buffering, not an
increase in sustained SQLite throughput. Completed writes retain WAL/FULL settings.
Both current concurrency comparisons below use the larger buffer and error-only
tracing. Tests remain loopback-only; these are not live network predictions.

| Scenario | Mean | p50 | p95 | p99 | Max |
|---|---:|---:|---:|---:|---:|
| burst, 1 concurrent | 429.328 ms | 519.879 ms | 564.867 ms | 571.944 ms | 573.355 ms |
| burst, 8 concurrent | 72.073 ms | 77.031 ms | 91.269 ms | 97.259 ms | 99.632 ms |
| paced, 8 concurrent | 0.644 ms | 0.303 ms | 2.174 ms | 3.181 ms | 5.017 ms |

Compared with the original sequential worker, concurrency 8 lowered burst median
latency from 403.922 ms to 77.031 ms (5.24x) and p99 from 464.099 ms to 97.259 ms
(4.77x). In the current code, concurrency 1 had median 519.879 ms, versus 77.031 ms
at concurrency 8 (6.75x). These are individual runs; host scheduling and fixture
response scheduling cause variation.

Use `execution.max_concurrent_sends = 8` (default; supported range 1–64). Rebuild
and restart the service to apply code changes. The running live trader was not
restarted by this benchmark work.

Reproduce the concurrency comparison:

```sh
BENCH_BURST=1 BENCH_CONCURRENT_SENDS=1 cargo test --lib --release --offline benchmark_random_laserstream_to_sender -- --ignored --nocapture
BENCH_BURST=1 BENCH_CONCURRENT_SENDS=8 cargo test --lib --release --offline benchmark_random_laserstream_to_sender -- --ignored --nocapture
BENCH_CONCURRENT_SENDS=8 cargo test --lib --release --offline benchmark_random_laserstream_to_sender -- --ignored --nocapture
```

Validation: `cargo test --release --offline` passed 122 library tests and 8
integration tests; three manual benchmarks were ignored by the normal suite.

## Preparation worker pool

The trader now supports `execution.preparation_workers` (1–16, default 4).
Duplicate signatures and both feeds share a FIFO lane, preserving early-feed
fallback behavior. Independent Pump.fun buy preparation can run concurrently.
Exit sizing and WSOL funding remain serialized, and a shared gate checks final
fund reservations. Submission and the 64-copy lifecycle cap are shared across
all workers. Worker queues hold one input each, so adding workers does not
multiply the main ingress buffer. Standalone pool callers use an ordered
background journal and drain it before returning.

Batch reservations remain held while any copy is preparing, submitting, or
settling, preventing balance-cache resets during concurrent preparation.
Background mint inspection is limited to four RPCs, and whole-cache expiry
sweeps run every 256 lookups; the requested mint's expiry is checked each time.

Each following run sent and landed all 1,000 fixture transactions, retained all
1,000 timing records, and verified all configured worker IDs appeared:

| Scenario | Mean | p50 | p95 | p99 | Max |
|---|---:|---:|---:|---:|---:|
| burst, 1 worker(s), 32 sends | 57.506 ms | 63.508 ms | 71.363 ms | 78.974 ms | 79.329 ms |
| burst, 4 worker(s), 8 sends | 81.844 ms | 91.985 ms | 100.391 ms | 104.795 ms | 108.284 ms |
| burst, 4 worker(s), 32 sends | 51.069 ms | 55.708 ms | 65.562 ms | 67.226 ms | 73.876 ms |
| paced, 4 worker(s), 32 sends | 0.686 ms | 0.305 ms | 2.410 ms | 3.722 ms | 13.545 ms |

The combined changes reduce the prior eight-send implementation's burst median
from 77.031 ms to 55.708 ms, and p99 from 97.259 ms to 67.226 ms. In the current
implementation, four workers with 32 sends gave median 55.708 ms versus
63.508 ms for one worker with 32 sends. These are single-run observations,
not guarantees; worker count alone did not eliminate shared or network overhead.
Paced latency remained around 0.3 ms at the median.

The local config and example config now set `preparation_workers = 4` and
`max_concurrent_sends = 32`. Legacy defaults remain four workers and eight sends.
The live service was not restarted. All endpoints in the benchmark are loopback.

Reproduce:

```sh
BENCH_BURST=1 BENCH_PREPARATION_WORKERS=1 BENCH_CONCURRENT_SENDS=32 cargo test --lib --release --offline benchmark_random_laserstream_to_sender -- --ignored --nocapture
BENCH_BURST=1 BENCH_PREPARATION_WORKERS=4 BENCH_CONCURRENT_SENDS=32 cargo test --lib --release --offline benchmark_random_laserstream_to_sender -- --ignored --nocapture
BENCH_PREPARATION_WORKERS=4 BENCH_CONCURRENT_SENDS=32 cargo test --lib --release --offline benchmark_random_laserstream_to_sender -- --ignored --nocapture
```

Final validation: `cargo test --release --offline` passed 125 library tests and
8 integration tests (133 total); 3 manual benchmarks were ignored. Checks cover
shared send and lifecycle limits, reservations held across in-flight preparation,
unknown submissions, duplicate feed handling, shutdown draining, and mint RPC
bounds/expiry. JavaScript syntax and diff whitespace checks also passed.

## Requested 5 ms gap rerun

Re-ran 1,000 transactions with four preparation workers, 32 send slots, and a
5 ms Tokio sleep between enqueues (timer scheduling adds overhead; total run
6.38 seconds). All 1,000 mock copies landed and all timing records were retained.
Receipt to send start: mean 0.635 ms, p50 0.242 ms,
p95 2.180 ms, p99 2.998 ms, max 14.378 ms.
Under 1 ms: 772/1,000 (77.2%). The paced JSON file contains this latest run.

## Eight and sixteen worker comparison

Each run used 1,000 transactions and 32 send slots. All copies landed in the mock and all timing records were retained. Runs were sequential to avoid competition between benchmarks. Four-worker values below are from the previous runs; these individual measurements do not establish statistical significance.

| Arrival pattern | Workers | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| 5 ms gap | 4 | 0.242 ms | 2.180 ms | 2.998 ms |
| 5 ms gap | 8 | 0.274 ms | 2.265 ms | 3.664 ms |
| 5 ms gap | 16 | 0.299 ms | 2.186 ms | 3.140 ms |
| Burst | 4 | 55.708 ms | 65.562 ms | 67.226 ms |
| Burst | 8 | 62.472 ms | 69.146 ms | 71.919 ms |
| Burst | 16 | 64.763 ms | 75.456 ms | 78.318 ms |

## Detailed four-route baseline

See [fanout-baseline.md](fanout-baseline.md) for the instrumented four-route,
one-nonce baseline and its limitations. The recorded run contains 1,000 landed
mock copies, 4,000 submissions, and four durably stored variants per copy.

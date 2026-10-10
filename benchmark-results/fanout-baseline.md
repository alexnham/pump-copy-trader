# Four-route nonce fan-out baseline

Release build, 1,000 synthetic Pump.fun buys, four preparation workers, 32 send slots. All 1,000 mock copies landed, all 4,000 submissions completed, and each copy retained four durable variant records. The trader and transaction-gap checker continued running during this local benchmark.

The four configured provider types, tips, and fees are used, with every endpoint replaced by loopback and fixture authentication. A temporary on-disk SQLite journal keeps production WAL/FULL durability and its pre-send nonce commit. One mock nonce is reused after the previous outcome is persisted and a mock finalized read shows advancement. Prior-copy waiting and fixture generation are outside receipt timing. This measures isolated per-copy cost, not fixed-5-ms arrivals or burst TPS; production finality and provider networking are not modeled. No live transactions were sent.

These are synthetic buys, not a replay of the reported SellV2. The benchmark does not start the production wallet-balance refresh task: the isolated copies have a cold initial balance RPC, unlike the reported sell’s cached input balance.

| Stage | Median | p95 | p99 |
|---|---:|---:|---:|
| Receipt to send start | 3.847 ms | 7.240 ms | 10.519 ms |
| Initial balance RPC | 1.910 ms | 3.366 ms | 5.544 ms |
| Four signatures only | 0.221 ms | 0.497 ms | 0.951 ms |
| All variant builds | 0.042 ms | 0.091 ms | 0.274 ms |
| Variant size checks | 0.001 ms | 0.003 ms | 0.005 ms |
| Late fee-balance lookup | 0.001 ms | 0.004 ms | 0.009 ms |
| Variant record serialization | 0.012 ms | 0.026 ms | 0.069 ms |
| Persistence total | 1.164 ms | 2.911 ms | 7.151 ms |
| Journal barrier | 0.396 ms | 1.080 ms | 3.462 ms |
| Acquire DB transaction | 0.067 ms | 0.157 ms | 0.523 ms |
| Nonce and variant writes | 0.312 ms | 0.685 ms | 1.104 ms |
| Durable DB commit | 0.328 ms | 0.771 ms | 3.596 ms |

Do not sum aggregate and per-route values. Persistence total overlaps its four DB substages; database totals count that parent once. All 1,000 samples classify persistence as pre-send work rather than post-send work.

Signing alone is about 0.2 ms at the median for four variants. The larger combined signing stage in the earlier live transaction remains unexplained without its new per-route measurements. The fee-balance lookup timer also separates a potential late balance RPC from journal latency.

Route index legend:
- 0: ewr-swqos; signing median 65 µs.
- 1: ewr-max; signing median 51 µs.
- 2: newyork-blockrazor; signing median 50 µs.
- 3: newyork-nextblock; signing median 50 µs.

Reproduce:

```sh
BENCH_FANOUT=1 BENCH_PREPARATION_WORKERS=4 BENCH_CONCURRENT_SENDS=32 cargo test --lib --release --offline benchmark_random_laserstream_to_sender -- --ignored --nocapture
```

Validation: 134 library tests and 8 integration tests passed (142 total); three manual benchmarks are ignored by the regular suite. The manual fan-out benchmark passed separately. JavaScript syntax and diff whitespace checks passed.

Live detailed timing fields require restarting the rebuilt trader. The running processes were inspected but not changed.

# Progress

Built phase by phase per spec section 12. Each phase was built with `dotnet build` and its tests run
before moving on. The final state: **69 tests, all passing** (`dotnet test`, about 1 minute).

| Phase | Delivered | Tests |
|---|---|---|
| 1. Skeleton and SDK | Solution with the 13 projects from spec 8.2, SDK contracts (section 7 plus D-03/D-04), two sample strategies (`MarketMaker`, `ThresholdBuyer`). | Compiles; samples are exercised by later tests. |
| 2. Core | `SpscRing`, `MpscRing` (Vyukov, per-slot sequence), padded cursors, `LongMap`, `MonotonicClock`/`VirtualClock`, `LatencyHistogram` (HDR-style), `IdleStrategy` (BusySpin/SpinYield/Backoff), `EventLoopThread`, thread affinity (Windows/Linux/no-op). | Ordering, wraparound, full detection, **4 producers × 1M no loss/in order (acceptance 1)**, histogram precision, map vs Dictionary, zero allocation, loop with each idle mode. |
| 3. Sequencer and journal | Host `Sequencer`; memory-mapped `JournalWriter` (growable, zero-tail end marker), `JournalReader`, explicit field-by-field `JournalCodec`, `JournalService` (one thread draining per-strategy SPSC rings). | Round trip, growth, reading a live journal, codec round trip, blob payloads. |
| 4. Reference and market data | Immutable versioned `ReferenceDataSnapshot` + atomic-swap service; deterministic `SimulatedFeed` (seeded L2 random walk, gap injection, snapshots); `OrderBook`; `FeedHandler` (per-instrument sequence check, gap → stale → snapshot → recovered); `MarketDataInbox` with conflation / overflow / detach. | Snapshot lookups/versioning, book ops, feed determinism, book matches generator, gap recovery, **backpressure: conflated quotes, zero lost trades in order, detach when hopeless (acceptance 7, unit)**. |
| 5. Risk and OMS | `OrderManager` (state machine, pending cancel/replace, CumQty-based idempotent fills, ExecId high-water dedupe, positions with avg px and realised PnL, open qty per side); `PreTradeRisk` (kill, exchange up, validity, ref data, lot/tick, max qty, stale/no data, price band, fat finger, notional, position incl. pending, gross, rate limit). | **Every reject code, pending qty counted (acceptance 4)**; **OMS property test: 300 random sequences, invariants, OMS = model exchange, duplicates change nothing (acceptance 5)**. |
| 6. SimExchange and gateway | FIX-like codec (`FixWriter`/`FixMessage`/`FixFramer`, no allocation); `ExchangeGateway` (single thread: session, seq numbers, heartbeats, throttle, resend, gap fill, status-request reconciliation, reject-while-down, block/cancel-all); `SimExchange` (persistent sessions, matching, partial fills, resend store, status requests, disconnect injection). | Codec round trip and framing, price parsing, zero-alloc encode, **drop after N orders → no lost, no duplicates, OMS = exchange (acceptance 6)**, fail closed while down. |
| 7. Host | `StrategyEngine` (context, risk + OMS inline, dispatch, fault isolation, kill/slow/stop, budget, timers, async, journalling), `StrategySlot` (two inboxes, never-drop control path), `StrategyHost` (feed, gateway, embedded exchange in Paper, journal, watchdog, placement by core budget, hot reload, metrics), `AssemblyLoadContext` loader and zip packages, HTTP API (`/health`, `/metrics`, deploy/start/stop/kill, journal). | **Sleeper stopped, sibling p99 unaffected (acceptance 2)**; slow strategy conflated then killed (acceptance 7, host); fault isolation; hung handler watchdog; kill cancels in the same iteration; ALC loading; capacity refusal; **10M ticks allocate 0 bytes (acceptance 8)**. |
| 8. Replay | `JournalReplayer` (rebuild from SessionStart, virtual clock, byte compare, digest); `BacktestRunner` (ticks + in-process matcher on virtual time). | **Live Paper session (MarketMaker + Chaos strategy using timers, async, local rejects, kill) replayed 100× identical and equal to the live outputs (acceptance 3)**; changed strategy detected; deterministic backtest. |
| 9. Control plane | ASP.NET Core minimal API: host registration, package upload/validation, placement (dedicated vs sandbox, reuse on redeploy, refuse when full), start/stop/kill, global kill, aggregated metrics, journal proxy, replays, heartbeat monitor with alerts. | **Deploy → start → kill → stop over REST; kill cancels within one loop iteration; replay via REST matches (acceptance 10)**; placement refusal; host marked Down after missed heartbeats. |
| 10. Benchmarks and hardening | Latency harness (coordinated-omission-safe), fan-out and allocation runs, BenchmarkDotNet micro-benchmarks, `docs/PERFORMANCE.md` with measured numbers. | **Harness produces the latency report (acceptance 9)**. Fixes found here: global ExecId set → per-order high-water (allocation), smaller `HostEvent` (fan-out), heartbeat alert ordering. |

## Known gaps (honest list)

- **Fan-out target missed**: 0.30M ticks/s to 50 subscribers against a 1M target (PERFORMANCE.md has the
  cause and the fix).
- **p99.9 tick-to-order target missed** on this laptop (303 µs vs 200 µs) for one strategy. The p99 target is met.
- No process supervisor, so "restart the host in recover mode" is detection and alert only (D-19).
- The watchdog cannot reclaim a hung thread; the host must be restarted (D-18).
- No ReadyToRun publish profile, and the host does not gate live data on warm-up (D-25).
- No NUMA-aware core selection; cores are listed explicitly (D-22).
- REST endpoints have no authentication (non-goal for v1, but needed before real use).
- The 50-strategy "soak" is the 10 s benchmark rows plus the test suite, not a multi-hour run.

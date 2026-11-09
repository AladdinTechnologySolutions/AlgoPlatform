# Algo Strategy Hosting Platform: Requirements and Architecture

Purpose of this file: a build spec for Claude Code. Build it phase by phase (section 12), keep every phase compiling and tested, and do not skip the tests in section 13.

Origin: QRT final-round system design question. "Design a platform where strategy algo developers add their automation components. The platform offers order, reference data and market data services. With many developers adding components to test strategies, how do you make all components run fast? The platform also sends orders to the exchange."

---

## 1. Goals

1. Strategy developers write a small component against a stable SDK, drop it into the platform, and run it against live data, recorded data, or a simulated exchange.
2. Every component runs fast and predictably. One developer's slow or crashing component must not hurt anyone else.
3. The same strategy binary runs in three modes without code changes: **Live**, **Paper** (live data, simulated exchange), **Replay** (recorded data, simulated exchange, as fast as possible).
4. Orders reach the exchange only through a controlled path: pre-trade risk, then order management, then exchange gateway.
5. Everything is reproducible. A journal of all inputs lets us replay a session and get identical outputs.

## 2. Non-goals (v1)

- Real exchange connectivity. v1 ships a FIX-like TCP simulator exchange; a real gateway is a later adapter.
- Multi-datacentre deployment, GUI, strategy marketplace, billing.
- Non-.NET strategies.

## 3. Actors

| Actor | Needs |
|---|---|
| Strategy developer | SDK, local test harness, deploy and start/stop, logs, metrics, replay of a bad day |
| Risk / compliance | Hard limits, kill switch, audit trail of every order |
| Platform engineer | Capacity view, per-strategy latency, isolation, safe deploys |

## 4. Functional requirements

**FR-1 Strategy SDK.** A strategy implements `IStrategy` (section 7). It receives events and may submit, amend and cancel orders through `IStrategyContext`. It has no access to threads, sockets, files or wall-clock time other than through the context.

**FR-2 Deployment.** A developer uploads a strategy package (assembly plus a `strategy.json` manifest: name, version, instruments, parameters, mode, resource class). The control plane validates it, assigns it to a host shard, loads it and starts it. Hot reload means stop, load new version, start; state is not migrated in v1.

**FR-3 Market data service.** Ingests a feed (simulated feed in v1), normalises to an internal `Tick` / `BookUpdate` format, maintains an order book per instrument, and fans out to subscribed strategies. Supports conflation per instrument for slow consumers (latest-value only for quotes; trades are never conflated).

**FR-4 Reference data service.** Instrument master (id, symbol, tick size, lot size, currency, trading hours, status, limit price bands). Served as an **immutable versioned snapshot** held in memory by each host. Updates arrive as a new snapshot version; hosts swap the pointer atomically between events. No per-event remote lookups.

**FR-5 Order service (OMS).** Owns order state machine, ID generation, and the authoritative position per strategy. Handles: New, Cancel, Replace, ack, partial fill, fill, reject, cancel-ack. Invariant: `CumQty + LeavesQty = OrderQty` while the order is live. Fills are idempotent (dedupe by ExecutionId).

**FR-6 Pre-trade risk.** Runs in-process before an order leaves the strategy host. Checks: max order size, max notional, max position per instrument and per strategy, price band against reference data, rate limit (orders per second), fat-finger, kill switch. **Fail closed**: if any input to the check is missing or stale, reject.

**FR-7 Exchange gateway.** Single-writer sessions to the exchange: owns the TCP session, sequence numbers, heartbeats, encoding, throttling. Translates internal orders to wire messages and wire reports to internal events. On disconnect: reconnect, recover by sequence numbers, then reconcile open orders by OrderId/ClOrdId (state is rebuilt from exchange reports, never guessed); duplicate execution reports are ignored.

**FR-8 Sequencer and journal.** Every event entering a strategy host (market data, reference data version change, execution reports, timers, control commands) is stamped with a monotonically increasing sequence number and appended to a binary journal before delivery. Strategy outputs (order requests) are journalled too.

**FR-9 Replay.** Given a journal and a strategy version, a replay run reproduces exactly the same order requests, byte for byte. Used for regression tests and post-incident analysis.

**FR-10 Kill switch.** Per strategy, per host, and global. Effect: stop sending new orders, cancel working orders, flatten optionally. Must take effect within one event-loop iteration.

**FR-11 Observability.** Per strategy: events/sec, queue depth, handler time p50/p99/p99.9/max, orders sent/rejected, tick-to-order latency histogram, GC counts. Exposed via an HTTP `/metrics` endpoint (Prometheus text format) and a structured log.

**FR-12 Control plane API.** REST: upload package, list strategies, start/stop, kill, get metrics, start replay, get journal range.

## 5. Non-functional requirements and targets

These are design targets to measure against, not claims. Build a benchmark in Phase 6 and report what is actually achieved.

| Area | Target |
|---|---|
| Tick-to-order inside a host (event in, order request out), warm | p99 under 50 microseconds, p99.9 under 200 microseconds |
| Event-loop handler budget | 20 microseconds default; breach is counted and logged; 10 consecutive breaches stops the strategy |
| Market data fan-out | 1 million ticks/sec per feed handler to 50 subscribed strategies, no loss for trades |
| Allocation on hot path | Zero steady-state allocations (verify with `GC.GetAllocatedBytesForCurrentThread`) |
| Isolation | A strategy crash, infinite loop or memory leak affects only its own host process |
| Determinism | Replay output identical across 100 runs |
| Availability | Gateway recovers from a dropped session without duplicate or lost orders |

## 6. How "all components run fast" is achieved (core design decisions)

1. **One event loop per strategy, single-threaded.** The strategy owns its state; no locks inside strategy code. (See `EventLoopDemo/Program.cs` for the pattern.)
2. **Dedicated, pinned cores for loops.** Each loop thread is pinned to a core (NUMA-aware); I/O and housekeeping threads run on other cores. The capacity planner never schedules more loops than free cores.
3. **Bounded lock-free queues between threads.** Power-of-two ring buffers with sequence numbers, padded to avoid false sharing. SPSC where possible (feed handler to host) and MPSC only where needed.
4. **Hot path separated from control path.** Hot path: market data, strategy, risk, order encode, send. Control path: deploy, config, metrics scraping, logging, persistence. Control work runs on other threads and posts results back as events.
5. **Slow work is offloaded and posted back.** Strategies that need heavy computation use `context.RunAsync(work, onDone)`; the result returns as an event. The loop never blocks.
6. **Process isolation per host, not per strategy.** A *strategy host* is a process that runs N strategy loops (one thread each, one core each). Strategies from different developers are placed in different hosts when untrusted or when a resource class demands it. In-process assembly loading (`AssemblyLoadContext`) is used inside a host so there is no serialisation between market data and strategies on the same host.
7. **Shared market data via one feed handler per host, not per strategy.** The host's feed handler decodes once, updates the book once, and publishes to each strategy's inbox. Strategies read an immutable or copy-on-publish view.
8. **Pre-trade risk and order encoding in-process.** No network hop between strategy and risk. Only the gateway hop remains.
9. **Reference data as immutable snapshot.** No remote calls on the hot path.
10. **Allocation discipline.** Structs for events, `ArrayPool` / pre-allocated pools, no LINQ, no closures, no boxing, no string formatting on the hot path. `GCSettings.LatencyMode = SustainedLowLatency`, Server GC off for hosts, `TieredPGO` off and `TieredCompilation` quick-jit settings tuned, ReadyToRun plus a warm-up phase before the host accepts live data.
11. **Backpressure policy is explicit.** Inbox is bounded. If it fills: market data quotes are conflated to latest per instrument; trades and execution reports are never dropped; if still full, the strategy is marked *slow*, kill-switched and alerted.
12. **Scaling out.** Shard by instrument group or by strategy. Add hosts (and cores/machines), never share mutable state between loops. Capacity is bounded by cores and by the feed handler, which is why the feed handler is per host.
13. **Deterministic replay for testing at speed.** Replay mode drives the same loop with a virtual clock and no sleeping, so thousands of strategy-days can be tested quickly in parallel.

## 7. Core contracts (SDK)

```csharp
namespace AlgoPlatform.Sdk;

public readonly record struct InstrumentId(int Value);
public enum Side : byte { Buy = 1, Sell = 2 }
public enum OrdType : byte { Market = 1, Limit = 2 }
public enum Tif : byte { Day = 0, Ioc = 3, Fok = 4 }
public enum OrdStatus : byte { New, Working, PartiallyFilled, Filled, Cancelled, Rejected, PendingCancel, PendingReplace }

public readonly record struct Tick(long Seq, long TimestampNs, InstrumentId Instrument, long BidPx, long BidQty, long AskPx, long AskQty, long LastPx, long LastQty);
// Prices are fixed-point integers (price * 10^4) to avoid double on the hot path.

public readonly record struct ExecutionReport(
    long Seq, long OrderId, long ExecId, InstrumentId Instrument, Side Side,
    OrdStatus Status, long LastPx, long LastQty, long CumQty, long LeavesQty, int RejectCode);

public readonly record struct NewOrder(InstrumentId Instrument, Side Side, OrdType Type, Tif Tif, long Px, long Qty);

public interface IStrategy
{
    void OnStart(IStrategyContext ctx);
    void OnTick(in Tick tick);
    void OnExecution(in ExecutionReport report);
    void OnTimer(long timerId);
    void OnReferenceDataChanged(IReferenceData snapshot);
    void OnAsyncResult(long requestId, ReadOnlySpan<byte> payload);
    void OnStop();
}

public interface IStrategyContext
{
    long NowNs { get; }                                   // virtual clock in replay
    IReferenceData Reference { get; }                     // current immutable snapshot
    Position GetPosition(InstrumentId id);
    OrderHandle Submit(in NewOrder order);                // runs pre-trade risk inline; returns Rejected handle on failure
    void Cancel(long orderId);
    void Replace(long orderId, long newPx, long newQty);
    void SetTimer(long timerId, long afterNs);
    long RunAsync(Func<byte[]> work);                     // off-loop; result returns via OnAsyncResult
    void Log(LogLevel level, string template, long a = 0, long b = 0);   // non-allocating, buffered
}

public interface IReferenceData
{
    int Version { get; }
    bool TryGet(InstrumentId id, out InstrumentInfo info);
}

public readonly record struct InstrumentInfo(InstrumentId Id, string Symbol, long TickSize, long LotSize, long MinPx, long MaxPx, bool Tradable);
public readonly record struct Position(long NetQty, long AvgPx, long RealisedPnl);
public readonly record struct OrderHandle(long OrderId, bool Accepted, int RejectCode);
```

Strategy manifest `strategy.json`:

```json
{
  "name": "meanrev-eu-1",
  "version": "1.2.0",
  "entryType": "MeanRev.EuStrategy, MeanRev",
  "instruments": ["VOD.L", "BARC.L"],
  "mode": "Paper",
  "resourceClass": "dedicated-core",
  "limits": { "maxOrderQty": 1000, "maxPosition": 5000, "maxNotional": 2000000, "maxOrdersPerSec": 50 },
  "parameters": { "window": 200, "entryZ": 2.0 }
}
```

## 8. Architecture

### 8.1 Logical view

```
                      +--------------------- Control plane ---------------------+
 developer ---REST--->| Package store | Scheduler/placement | Metrics | Journal  |
                      +-----------+--------------------------------------+------+
                                  | deploy / start / kill                | metrics
                                  v                                      ^
 +------------------------ Strategy host process (one per shard) ----------------------+
 |                                                                                     |
 |  Feed handler thread --> Sequencer + Journal --> [inbox ring per strategy]          |
 |   (decode, book)             (seq no., append)        |            |                 |
 |                                                       v            v                 |
 |                                         [Loop thread A]     [Loop thread B] ...      |
 |                                          strategy A          strategy B              |
 |                                          risk + OMS client   risk + OMS client       |
 |                                                \            /                        |
 |                                          [outbound ring: orders]                     |
 |                                                   |                                  |
 |                                       Gateway thread (single writer)                 |
 +---------------------------------------------------|----------------------------------+
                                                     v  TCP, FIX-like
                                              Exchange (simulator in v1)
```

Reports flow back: Exchange to Gateway thread, then Sequencer + Journal, then the owning strategy's inbox and the OMS state.

### 8.2 Components (projects)

| Project | Responsibility |
|---|---|
| `AlgoPlatform.Sdk` | Public contracts from section 7. No dependencies. |
| `AlgoPlatform.Core` | Ring buffers, event loop, sequencer, journal, clock abstraction, metrics histogram. |
| `AlgoPlatform.MarketData` | Simulated feed, normaliser, order book, per-strategy fan-out with conflation. |
| `AlgoPlatform.ReferenceData` | Snapshot model, loader (JSON file in v1), versioned swap. |
| `AlgoPlatform.Oms` | Order state machine, ID generator, position keeper, idempotent fill handling. |
| `AlgoPlatform.Risk` | Pre-trade checks, kill switch, rate limiter. |
| `AlgoPlatform.Gateway` | Exchange session, sequence recovery, encoder/decoder, throttle. |
| `AlgoPlatform.Host` | Process that wires the above, loads strategies via `AssemblyLoadContext`, pins threads. |
| `AlgoPlatform.ControlPlane` | ASP.NET Core minimal API: packages, placement, start/stop/kill, metrics aggregation. |
| `AlgoPlatform.SimExchange` | TCP simulated exchange: matching against a synthetic book, configurable latency, disconnect injection. |
| `AlgoPlatform.Replay` | Replay runner with virtual clock; compares output journals. |
| `AlgoPlatform.Strategies.Samples` | Two sample strategies (a market maker, a threshold buyer) used by tests. |
| `AlgoPlatform.Tests` / `AlgoPlatform.Benchmarks` | xUnit tests, BenchmarkDotNet and a custom latency harness. |

### 8.3 Key mechanisms

**Ring buffer.** Capacity is a power of two (index = seq & mask). Producer and consumer cursors each sit on their own cache line (use `[StructLayout]` padding of 64 bytes or 128 bytes). SPSC uses plain volatile reads and writes; MPSC claims a slot with `Interlocked.CompareExchange` on the producer cursor and publishes with a per-slot sequence number. Events are structs stored in a pre-allocated array.

**Event loop.** One thread per strategy, pinned, `while (running)` with busy-spin and `SpinWait` yield fallback. Per event: dequeue, handle, record handler time. Run-to-completion: handlers never wait. Housekeeping (metrics flush, log drain) happens on other threads.

**Sequencer and journal.** All events get `Seq` from a single atomic counter at the host boundary. The journal is a memory-mapped append-only file of fixed-header binary records (`Seq`, `Type`, `Length`, payload). Written by a dedicated thread from its own ring so the hot path never does I/O. Snapshots of strategy state are not required in v1; replay starts from the beginning of a session.

**Order lifecycle (happy path).**
1. Tick arrives in strategy inbox, `OnTick` runs.
2. Strategy calls `ctx.Submit(order)`.
3. Risk checks run inline against strategy-owned state and the reference snapshot. Reject returns a failed `OrderHandle` immediately.
4. OMS assigns `OrderId`, records `PendingNew`, and pushes the encoded order to the outbound ring.
5. Gateway thread (single writer) sends it with the next outbound sequence number.
6. Exchange ack and fill reports are decoded by the gateway, sequenced and journalled, then delivered as `ExecutionReport` events to the owning strategy's inbox and applied to OMS state.

**Gateway recovery.** On disconnect: stop sending, mark all strategies "exchange down" (risk rejects new orders), reconnect, exchange a resend request from the last received sequence number, apply replayed reports (dedupe by `ExecId`), then query open orders and reconcile by `OrderId`. Orders in `PendingNew` with no acknowledgement are resolved via order status request, never assumed lost or filled. Only after reconciliation do strategies resume.

**Isolation levels.**
- Between strategies on one host: separate threads and inboxes, separate `AssemblyLoadContext`, watchdog on handler time. A bad strategy is stopped and cannot touch another's state, but a fatal runtime fault (stack overflow, `Environment.FailFast`) kills the host. Hence the placement rule below.
- Between hosts: separate processes. Placement rule: untrusted or experimental strategies go on a shared "sandbox" host with low priority; production strategies get their own host or a host with trusted peers.

**Placement and capacity.** The scheduler knows each host's core budget. Dedicated-core strategies take one core each. Shared-class strategies run on a sandbox host. If no capacity exists, deployment is rejected with a clear message, not silently overcommitted.

## 9. Data and interfaces

- Internal events: fixed-size binary structs; no JSON, no strings, no per-event allocation.
- Gateway wire format v1: a simplified FIX-like tag/value text protocol over TCP (`35=D` New Order, `35=8` Execution Report, `35=F` Cancel, `35=G` Replace, `35=9` Cancel Reject, `35=A` Logon, `35=0` Heartbeat, `35=2` Resend Request, `35=4` Sequence Reset, `35=5` Logout), checksum optional. Tags used: 11 ClOrdID, 37 OrderID, 17 ExecID, 55 Symbol, 54 Side, 38 OrderQty, 40 OrdType, 44 Price, 59 TimeInForce, 39 OrdStatus, 150 ExecType, 14 CumQty, 151 LeavesQty, 31 LastPx, 32 LastQty, 34 MsgSeqNum, 43 PossDupFlag. Encoding writes directly into a pre-allocated `byte[]`; no `string.Format`.
- Control plane: REST + JSON (not on the hot path).
- Metrics: Prometheus text at `/metrics` on each host and aggregated by the control plane.

## 10. Failure modes and responses

| Failure | Response |
|---|---|
| Strategy throws | Catch at loop boundary, cancel its working orders, mark Faulted, alert; other strategies continue |
| Handler exceeds budget repeatedly | Kill-switch the strategy and mark Slow |
| Inbox full | Conflate quotes; if still full, kill-switch the strategy |
| Feed gap (sequence gap) | Request snapshot, mark instrument stale, risk rejects orders in it until recovered |
| Gateway disconnect | Section 8.3 recovery; risk fails closed meanwhile |
| Duplicate execution report | Ignored by ExecId |
| Reference data missing for instrument | Order rejected (fail closed) |
| Host process crash | Control plane detects missed heartbeats, alerts, restarts the host in "recover" mode: reconcile with exchange before enabling strategies |
| Clock jump | All logic uses the host's monotonic clock; wall clock only for logging |

## 11. Technology

- .NET 8 (or latest LTS available), C#, nullable enabled, `AllowUnsafeBlocks` only in Core ring buffer and gateway encoder.
- ASP.NET Core minimal API for the control plane.
- xUnit, BenchmarkDotNet, plus a custom latency recorder (HdrHistogram-style fixed buckets, no external dependency required).
- Thread pinning: `SetThreadAffinityMask` / `sched_setaffinity` via P/Invoke, behind an interface with a no-op fallback so tests run anywhere.
- No message broker in v1; in-process rings and TCP only.

## 12. Build order (phases)

Each phase ends with passing tests and a short note in `docs/PROGRESS.md`.

1. **Skeleton and SDK.** Solution, projects, section 7 contracts, sample strategy compiles.
2. **Core.** SPSC and MPSC ring buffers, event loop with spin/yield, clock abstraction (real and virtual), latency histogram. Tests: ordering, wraparound, concurrent producers, no loss.
3. **Sequencer and journal.** Binary journal writer/reader, sequence stamping, replay reader.
4. **Reference data, market data.** Snapshot model with atomic swap, simulated feed (deterministic with a seed), order book, per-strategy fan-out with conflation.
5. **Risk and OMS.** State machine, positions, idempotent fills, all pre-trade checks, kill switch.
6. **SimExchange and Gateway.** Matching simulator, session, sequence recovery, reconcile, disconnect injection.
7. **Host.** Wire everything, load strategy via `AssemblyLoadContext`, thread pinning, watchdog, metrics endpoint.
8. **Replay runner.** Virtual clock, run a journal through a strategy, byte-compare outputs.
9. **Control plane.** Upload, placement, start/stop/kill, metrics aggregation.
10. **Benchmarks and hardening.** Latency harness, allocation checks, soak test with 50 strategies, write results into `docs/PERFORMANCE.md` with actual numbers.

## 13. Acceptance tests (must exist and pass)

1. **Ring buffer:** 4 producers x 1 million events each to one consumer, no loss, per-producer order preserved.
2. **Event loop isolation:** a strategy that sleeps 50 ms in `OnTick` is stopped by the watchdog; a sibling strategy's p99 latency is unaffected (within 20%).
3. **Determinism:** run the same journal through the same strategy 100 times; all output journals identical.
4. **Risk:** orders over size, over position, off price band, over rate limit, with kill switch on, and with missing reference data are each rejected with the right reject code; pending quantity is counted so concurrent in-flight orders cannot breach the position limit.
5. **OMS invariants:** property test with random sequences of acks, partial fills, cancels, duplicate reports; `CumQty + LeavesQty = OrderQty` always holds while live; duplicate `ExecId` never changes state.
6. **Gateway recovery:** SimExchange drops the TCP connection mid-session after N orders; after reconnect there are no lost orders, no duplicates, and OMS state equals exchange state.
7. **Backpressure:** a deliberately slow strategy causes quote conflation but zero lost trades or execution reports; if it stays slow it is kill-switched.
8. **Allocation:** after warm-up, 10 million ticks through the hot path allocate zero bytes on the loop thread.
9. **Latency report:** tick-to-order p50/p99/p99.9/max for 1, 10 and 50 strategies per host, written to `docs/PERFORMANCE.md`, noting that coordinated omission is avoided by timestamping at intended send time.
10. **Control plane:** deploy, start, kill, stop via REST against a running host; kill switch cancels working orders within one loop iteration.

## 14. Repository layout

```
/src
  AlgoPlatform.Sdk/
  AlgoPlatform.Core/
  AlgoPlatform.MarketData/
  AlgoPlatform.ReferenceData/
  AlgoPlatform.Oms/
  AlgoPlatform.Risk/
  AlgoPlatform.Gateway/
  AlgoPlatform.Host/
  AlgoPlatform.ControlPlane/
  AlgoPlatform.SimExchange/
  AlgoPlatform.Replay/
  AlgoPlatform.Strategies.Samples/
/tests
  AlgoPlatform.Tests/
  AlgoPlatform.Benchmarks/
/docs
  PROGRESS.md
  PERFORMANCE.md
  DESIGN_DECISIONS.md
AlgoPlatform.sln
```

## 15. Rules for Claude Code

- Work one phase at a time. Run `dotnet build` and `dotnet test` before moving on; do not claim a phase is done without green tests.
- Never use `lock`, `Task`, LINQ, `async/await`, string formatting, or allocations on code reachable from the event loop. Add a unit test or analyser comment where this is deliberately relaxed.
- Keep public SDK types stable; changes to `AlgoPlatform.Sdk` need a note in `DESIGN_DECISIONS.md`.
- Do not invent performance numbers. Only report numbers produced by the benchmark harness, with the machine description.
- Where a decision here is ambiguous, choose the simpler option, record it in `DESIGN_DECISIONS.md`, and continue.

---

## Appendix A: Interview talking points (for Yang, not for the build)

**Opening (30 seconds).** "I'd split the platform into a hot path and a control path. On the hot path, each strategy is a single-threaded event loop on its own pinned core, fed by bounded lock-free queues, with risk checks and order encoding in-process. Everything slow, such as deployment, config and logging, stays off that path. Isolation comes from process-per-host and a watchdog per strategy, and every input is sequenced and journalled so I can replay it deterministically."

**Where `channels` and `async/await` fit.** They are fine for the control plane, deployment, and ms-level hand-offs. They are not the hot path: `await` adds allocation and scheduling variance, and thread-pool hand-offs add microseconds of jitter. For the hot path, use dedicated threads and ring buffers.

**Scaling answer.** "Shard by instrument group or strategy; each shard has its own feed handler and loops; never share mutable state. Capacity is cores. If the feed handler saturates, split the feed by instrument range."

**Measure first.** State your target (for example tick-to-order p99), instrument with histograms, find the biggest contributor, fix that one, re-measure. Name the cost of each fix (burned cores, complexity, less flexibility).

**Likely follow-ups.** What if a strategy is slow? (watchdog, conflation, kill switch.) What if the exchange disconnects? (section 8.3.) How do you test? (replay with virtual clock, determinism test.) How do you stop one developer hurting others? (isolation levels, placement, resource classes.) What about GC? (pre-allocation, pooling, SustainedLowLatency, warm-up.)

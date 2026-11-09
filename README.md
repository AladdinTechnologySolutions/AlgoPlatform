# Algo Strategy Hosting Platform

An implementation of [AlgoPlatform_Requirements_Architecture.md](AlgoPlatform_Requirements_Architecture.md):
a platform where strategy developers drop in a small component that runs on its own single-threaded
event loop. Market data, reference data, pre-trade risk and order management sit in-process with it, and
orders reach a (simulated) exchange through one gateway thread.

- .NET 8, C#, no external dependencies except xUnit and BenchmarkDotNet.
- 69 tests covering all ten acceptance tests in spec section 13: `dotnet test` (about 1 minute).
- Architecture and workflows: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
- Measured numbers, including the targets that were missed, are in [docs/PERFORMANCE.md](docs/PERFORMANCE.md). The decisions behind the design are in [docs/DESIGN_DECISIONS.md](docs/DESIGN_DECISIONS.md), and phase-by-phase status is in [docs/PROGRESS.md](docs/PROGRESS.md).

## Run it

```powershell
dotnet build
dotnet test

# Terminal 1: control plane (REST)
dotnet run --project src/AlgoPlatform.ControlPlane -- --Url=http://127.0.0.1:5100
# Terminal 2: a strategy host in Paper mode (embedded simulated exchange), registers itself
dotnet run --project src/AlgoPlatform.Host -- --HostId=host-1 --Url=http://127.0.0.1:5101 --ControlPlane=http://127.0.0.1:5100
# Terminal 3: package the sample market maker, deploy, start, kill, stop, then replay the session
./scripts/demo.ps1

# Benchmarks
dotnet run -c Release --project tests/AlgoPlatform.Benchmarks -- report 10
dotnet run -c Release --project tests/AlgoPlatform.Benchmarks -- micro --job short
```

## The one-paragraph answer

The platform has a hot path and a control path. On the hot path, each strategy is a single-threaded event loop on
its own core, fed by bounded lock-free rings. Risk checks and order encoding run in-process, and nothing on that
path allocates, locks or awaits. Everything slow (deploy, config, logging, metrics, persistence) runs on other
threads and posts results back as events. Isolation comes from a process per host plus a watchdog per strategy.
Every input is sequenced and journalled, so a session can be replayed deterministically, byte for byte.

## Reading order (follow one tick from the feed to the exchange)

1. **The contract a developer codes against**: [Contracts.cs](src/AlgoPlatform.Sdk/Contracts.cs), and a
   real strategy, [MarketMaker.cs](src/AlgoPlatform.Strategies.Samples/MarketMaker.cs).
2. **Queues between threads**: [SpscRing.cs](src/AlgoPlatform.Core/Collections/SpscRing.cs) (cached cursors,
   padding), then [MpscRing.cs](src/AlgoPlatform.Core/Collections/MpscRing.cs) (CAS claim, per-slot sequence).
3. **A tick is born**: [FeedHandler.cs](src/AlgoPlatform.MarketData/FeedHandler.cs) decodes once, updates the
   [OrderBook](src/AlgoPlatform.MarketData/OrderBook.cs) once and fans out. Backpressure is in
   [MarketDataInbox.cs](src/AlgoPlatform.MarketData/MarketDataInbox.cs) (conflate quotes, never trades).
4. **The loop**: [StrategySlot.cs](src/AlgoPlatform.Host/Hosting/StrategySlot.cs) (control first, timers, then
   market data) and [StrategyEngine.cs](src/AlgoPlatform.Host/Engine/StrategyEngine.cs), the heart of the system:
   journal the input, dispatch, catch faults, and on `Submit` run risk and the OMS inline.
5. **Risk and OMS**: [PreTradeRisk.cs](src/AlgoPlatform.Risk/PreTradeRisk.cs) (fail closed; pending quantity counts),
   [OrderManager.cs](src/AlgoPlatform.Oms/OrderManager.cs) (state machine; fills idempotent by CumQty delta and ExecId).
6. **Out to the exchange**: [ExchangeGateway.cs](src/AlgoPlatform.Gateway/ExchangeGateway.cs), a single writer.
   Read the class comment, then `OnLogon` → `StartReconcile` → `OnExecutionReport` for the recovery story.
   The other side is [SimExchange.cs](src/AlgoPlatform.SimExchange/SimExchange.cs).
7. **Determinism**: [JournalService.cs](src/AlgoPlatform.Core/Journal/JournalService.cs), then
   [JournalReplayer.cs](src/AlgoPlatform.Replay/JournalReplayer.cs). D-05 to D-07 in DESIGN_DECISIONS explain why
   replay reproduces a live, multi-threaded run exactly.
8. **Wiring and control**: [StrategyHost.cs](src/AlgoPlatform.Host/Hosting/StrategyHost.cs) (threads, placement,
   watchdog, metrics) and [ControlPlaneState.cs](src/AlgoPlatform.ControlPlane/ControlPlaneState.cs).

## Spec → code → proof

| Spec | Code | Test |
|---|---|---|
| 6.1 one loop per strategy | `StrategySlot`, `EventLoopThread` | `HostTests` |
| 6.3 bounded lock-free queues | `SpscRing`, `MpscRing` | `RingBufferTests` (acceptance 1) |
| 6.5 slow work offloaded | `ctx.RunAsync` → `AsyncResult` event | `ChaosStrategy` in `DeterminismTests` |
| 6.10 allocation discipline | structs, pre-sized maps, explicit codecs | `Ten_million_ticks_allocate_nothing...` (acceptance 8) |
| 6.11 backpressure | `MarketDataInbox`, control overflow → `MarkSlow` | `BackpressureTests`, `Persistently_slow...` (acceptance 7) |
| FR-5 OMS | `OrderManager` | `OmsTests` property test (acceptance 5) |
| FR-6 risk | `PreTradeRisk` | `RiskTests` (acceptance 4) |
| FR-7 gateway recovery | `ExchangeGateway` | `GatewayRecoveryTests` (acceptance 6) |
| FR-8/9 journal and replay | `JournalService`, `JournalReplayer` | `DeterminismTests` (acceptance 3) |
| FR-10 kill switch | `StrategyEngine.HandleControl` | `KillJournalCheck` (acceptance 10) |
| Isolation | budget + `MarkSlow`, watchdog | `Sleeping_strategy_is_stopped...` (acceptance 2) |
| FR-11 metrics | `StrategyHost.MetricsText` | `ControlPlaneTests` |
| FR-12 control plane | `ControlPlaneApp` | `ControlPlaneTests` (acceptance 10) |
| Section 5 targets | `LatencyHarness` | `LatencyReportTests` (acceptance 9), PERFORMANCE.md |

## Things to try while studying

- Break determinism on purpose. In `StrategyEngine`, make `NowNs` return `MonotonicClock.Now()`, run
  `DeterminismTests`, and watch replay diverge. Then read D-05.
- Remove the pending-quantity terms in `PreTradeRisk.CheckNew` and see which risk test fails, and why it matters
  when several orders are in flight.
- In the recovery test, set `DropAfterOrders` to a different N, or comment out `StartReconcile`'s status
  requests, and see which assertion catches the lost order.
- Make `HostEvent` bigger again (remove the explicit layout) and re-run `-- report` to see fan-out drop.
- Turn on `PinThreads=true` and pass `--Cores=2,3,4,5` to the host. Compare tails.

## Layout

```
src/   Sdk  Core  MarketData  ReferenceData  Oms  Risk  Gateway  SimExchange  Host  Replay  ControlPlane  Strategies.Samples
tests/ AlgoPlatform.Tests (xUnit)   AlgoPlatform.Benchmarks (harness + BenchmarkDotNet)
docs/  PROGRESS.md  PERFORMANCE.md (+ PERFORMANCE.results.md, generated)  DESIGN_DECISIONS.md
samples/market-maker/strategy.json   scripts/demo.ps1
```

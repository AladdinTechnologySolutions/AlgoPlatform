# Performance

Every number here was produced by the benchmark harness in `tests/AlgoPlatform.Benchmarks` on the
machine described below. Nothing is estimated. To regenerate:

```
dotnet run -c Release --project tests/AlgoPlatform.Benchmarks -- report 10   # latency, fan-out, allocation -> docs/PERFORMANCE.results.md
dotnet run -c Release --project tests/AlgoPlatform.Benchmarks -- micro --job short
```

**Machine:** Intel Core i7-9850H @ 2.60 GHz (6 cores / 12 logical), 16 GB, Windows 11 Pro (10.0.26200),
.NET 8.0.28, workstation GC, Stopwatch resolution 100 ns. This is a laptop with **no thread pinning, no core
isolation, power management on and other software running**. Treat the tails as an upper bound for this
design on this machine, not as what a tuned Linux box with isolated cores would show.

## Summary against the targets (spec section 5)

| Target | Result | Verdict |
|---|---|---|
| Tick-to-order p99 < 50 µs, warm | 1 strategy: **p50 1.2 µs, p99 17.4 µs** | Met |
| Tick-to-order p99.9 < 200 µs | 1 strategy: **303 µs** | Not met (OS and GC tails, see below) |
| Handler budget 20 µs, breaches counted, 10 consecutive stop | Enforced and tested (acceptance 2); handler p99 1.3–2.4 µs | Met |
| 1M ticks/s per feed handler to 50 strategies, no lost trades | **No lost trades** (14,730,150 of 14,730,150). Throughput **0.30M ticks/s to 50** (15M deliveries/s); 1.4M ticks/s to 10; 3.6M to 1 | Throughput **not met** for 50 subscribers |
| Zero steady-state allocation on the hot path | **0 bytes over 10M ticks and 1M orders** | Met |
| Isolation | Sleeper is stopped; sibling p99 10.2 µs alone vs 9.7 µs next to it | Met (in-process; see D-18 for hung threads) |
| Determinism, 100 runs | 100/100 identical, and byte-for-byte equal to the live run | Met |
| Gateway recovery | No lost or duplicate orders, OMS = exchange (acceptance 6) | Met |

## Tick-to-order inside a host

Measured from the feed message's **intended** send time to the order being handed to the gateway ring.
The simulated feed is paced on a fixed schedule and every message carries the time it was *scheduled* to go out.
If any stage stalls, the delay is charged to every message behind it, so **coordinated omission is avoided**:
a stall shows up as latency instead of as missing samples. Paper mode (embedded SimExchange over loopback TCP),
journal on, 20,000 feed messages/s across 4 instruments, one instrument per strategy. The strategy sends an IOC
at the touch whenever it has no order working, so not every tick produces an order.

| Strategies | Idle mode | Samples | p50 µs | p99 µs | p99.9 µs | max µs | Handler p99 µs | GC gen0/1/2 in 10 s |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 1 | BusySpin | 9,046 | 1.2 | 17.4 | 303.1 | 5,072.9 | 2.4 | 20/6/1 |
| 10 | SpinYield | 3,331 | 5.5 | 335.9 | 983.0 | 1,768.8 | 1.7 | 456/130/0 |
| 50 | SpinYield | 7,457 | 32.3 | 589.8 | 901.1 | 1,908.5 | 1.3 | 1,102/27/0 |
| 50 | Backoff | 7,622 | 36.9 | 786.4 | 18,874.4 | 19,522.4 | 1.3 | 1,107/29/0 |

What the table says:

1. **One strategy on a spinning core is fast.** The median is about a microsecond. The handler itself
   (strategy, risk, OMS, journal enqueue, ring push) is ~1–2 µs at p99.
2. **Capacity is cores** (spec 6.12). With 10 or 50 loops plus the feed handler, gateway, journal thread and
   the in-process exchange on 12 logical processors, the loops are time-sliced by the OS. The median is
   still tens of µs, but p99 jumps to hundreds of µs. This is what the capacity planner prevents: a
   dedicated-core strategy is refused when no core is free (tested). The 10- and 50-strategy rows deliberately
   oversubscribe to show what that rule protects against.
3. **Backoff** (sleeping idle loops) costs 1–19 ms tails, because `Thread.Sleep(1)` on Windows wakes late.
   It belongs on sandbox hosts only.
4. **GC.** The loop threads allocate nothing, but the GC counts are not zero. The allocations come from the
   in-process SimExchange (strings and lists for every FIX message) and from control-path threads. A gen0
   collection pauses every managed thread, including the loops, so it is a likely source of the p99.9/max
   tails. Two things would remove it: in Live mode the exchange is a separate process, and a production
   host would keep simulator and REST traffic out of the trading process.

## Market-data fan-out

One feed handler thread, unpaced synthetic feed (one instrument, 10% trades), N subscriber inboxes drained by
up to 6 consumer threads.

| Subscribers | Ticks/s per feed handler | Deliveries/s | Trades published × subs | Trades received | Conflated |
|---:|---:|---:|---:|---:|---:|
| 1 | 3,553,746 | 3.55M | 294,603 | 294,603 | 0 |
| 10 | 1,436,763 | 14.4M | 2,946,030 | 2,946,030 | 0 |
| 50 | 298,853 | 14.9M | 14,730,150 | 14,730,150 | 0 |

**The 1M ticks/s to 50 subscribers target is not met.** Above ~10 subscribers the handler is bound by
copying a ~130-byte event into each subscriber's ring (~15M copies/s, ~65 ns each, mostly cache misses on
50 different rings). Shrinking the event (D-17) helped about 44% in an isolated run. The structural fix is
the Disruptor pattern: one shared broadcast ring per feed handler, with each strategy holding only its own
read cursor. Publishing then costs one write regardless of N. Per-subscriber conflation becomes "a reader
that falls a lap behind skips to the latest value per instrument". This is the first thing to change if
fan-out matters more than per-subscriber backpressure isolation.

## Hot-path allocation

10,000,000 ticks and 1,000,000 orders with fills, through the market-data inbox, engine, risk, OMS,
journal ring and order ring on one thread: **0 bytes allocated** (`GC.GetAllocatedBytesForCurrentThread`),
about 395 ns per tick including the simulated exchange work done on the same thread. Acceptance test 8 checks the
same thing.

(An earlier run showed 40 bytes. It was the harness's own `Stopwatch.StartNew()` inside the measured window,
not platform code. Fixed.)

## Micro-benchmarks (BenchmarkDotNet, ShortRun job, single thread)

| Operation | Mean | Allocated |
|---|---:|---:|
| SPSC ring enqueue + dequeue | 2.2 ns | 0 |
| MPSC ring enqueue + dequeue (uncontended) | 8.6 ns | 0 |
| Latency histogram record | 1.0 ns | 0 |
| Monotonic clock read (QPC) | 12.5 ns | 0 |
| Pre-trade risk check (all checks pass) | 36.8 ns | 0 |
| FIX NewOrderSingle encode (with checksum) | 287.6 ns | 0 |

FIX encoding is the slowest step that is not I/O. It runs on the gateway thread, not the strategy loop.
Precomputing the constant header bytes and the checksum of fixed fields would roughly halve it.

## Where to look next (measure, fix the biggest, re-measure)

1. Fan-out: shared broadcast ring (above).
2. Tails: run on Linux with `isolcpus`/`nohz_full`, pin the loops (`PinThreads=true`), keep the simulator
   out of process, and set `GCSettings.LatencyMode`/`GCHeapAffinitize` and a larger gen0 budget for the
   remaining control-path allocations.
3. FIX encode: precomputed header and incremental checksum.

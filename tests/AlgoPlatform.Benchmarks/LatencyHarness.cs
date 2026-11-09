using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using AlgoPlatform.Core.Collections;
using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Journal;
using AlgoPlatform.Core.Threading;
using AlgoPlatform.Core.Timing;
using AlgoPlatform.Host.Engine;
using AlgoPlatform.Host.Hosting;
using AlgoPlatform.MarketData;
using AlgoPlatform.ReferenceData;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Benchmarks;

/// <summary>Submits an IOC at the touch on every tick when it has no order working; alternates side with position.</summary>
public sealed class LatencyProbeStrategy : StrategyBase
{
    private long _working;

    public override void OnTick(in Tick tick)
    {
        if (_working != 0 || tick.BidPx <= 0 || tick.AskPx <= 0) return;
        bool buy = Ctx.GetPosition(tick.Instrument).NetQty <= 0;
        var h = Ctx.Submit(new NewOrder(tick.Instrument, buy ? Side.Buy : Side.Sell, OrdType.Limit, Tif.Ioc, buy ? tick.AskPx : tick.BidPx, 100));
        if (h.Accepted) _working = h.OrderId;
    }

    public override void OnExecution(in ExecutionReport r)
    {
        if (r.OrderId == _working && r.Status is OrdStatus.Filled or OrdStatus.Cancelled or OrdStatus.Rejected) _working = 0;
    }
}

public sealed record LatencyRow(int Strategies, string Idle, double FeedRate, long Samples, LatencyHistogram TickToOrder,
    LatencyHistogram Handler, LatencyHistogram Inbox, long Ticks, long Orders, long Fills, long Breaches, long Gen0, long Gen1, long Gen2);

/// <summary>
/// The custom latency harness (Phase 10). Everything it prints comes from histograms recorded while it runs;
/// nothing is estimated. Coordinated omission: the simulated feed is paced, each message carries the time it
/// was *scheduled* to be sent, and tick-to-order is measured from that intended time, so a stalled pipeline
/// shows up as latency instead of as fewer samples.
/// </summary>
public static class LatencyHarness
{
    private static readonly string[] Symbols = ["VOD.L", "BARC.L", "AAPL", "MSFT"];

    public static LatencyRow RunHost(int strategies, IdleMode idle, double feedRate, TimeSpan warmup, TimeSpan measure)
    {
        var options = new HostOptions
        {
            HostId = $"bench-{strategies}",
            Mode = RunMode.Paper,
            DataDirectory = Path.Combine(Path.GetTempPath(), "algoplatform-bench", Guid.NewGuid().ToString("N")[..8]),
            Cores = Enumerable.Range(0, Math.Max(Environment.ProcessorCount, strategies + 2)).ToArray(),
            StrategyIdle = idle,
            InfrastructureIdle = idle == IdleMode.BusySpin ? IdleMode.BusySpin : IdleMode.SpinYield,
            FeedMessagesPerSecond = feedRate,
            HandlerBudgetNs = 20_000,
            MaxConsecutiveBreaches = int.MaxValue,   // measure, do not kill, in the benchmark
        };
        using var host = new StrategyHost(options);
        host.Start();
        for (int i = 0; i < strategies; i++)
        {
            var manifest = new StrategyManifest
            {
                Name = $"probe-{i}", Version = "1", EntryType = "x, y", Instruments = [Symbols[i % Symbols.Length]],
                Limits = new RiskLimits { MaxOrderQty = 1000, MaxPosition = 1_000_000, MaxNotional = long.MaxValue / 4, MaxOrdersPerSec = 100_000 },
            };
            host.Deploy(manifest, () => new LatencyProbeStrategy());
            host.Start(manifest.Name);
        }
        Thread.Sleep(warmup);
        foreach (var s in host.Slots)
        {
            s.Engine!.Metrics.TickToOrder.Reset();
            s.Engine.Metrics.HandlerTime.Reset();
            s.Engine.Metrics.InboxLatency.Reset();
        }
        long ticks0 = host.Slots.Sum(s => s.Engine!.Metrics.Ticks);
        long orders0 = host.Slots.Sum(s => s.Engine!.Metrics.OrdersSent);
        long fills0 = host.Slots.Sum(s => s.Engine!.Metrics.Fills);
        long breaches0 = host.Slots.Sum(s => s.Engine!.Metrics.BudgetBreaches);
        int g0 = GC.CollectionCount(0), g1 = GC.CollectionCount(1), g2 = GC.CollectionCount(2);
        Thread.Sleep(measure);

        var t2o = new LatencyHistogram();
        var handler = new LatencyHistogram();
        var inbox = new LatencyHistogram();
        foreach (var s in host.Slots)
        {
            t2o.Add(s.Engine!.Metrics.TickToOrder);
            handler.Add(s.Engine.Metrics.HandlerTime);
            inbox.Add(s.Engine.Metrics.InboxLatency);
        }
        var row = new LatencyRow(strategies, idle.ToString(), feedRate, t2o.Count, t2o, handler, inbox,
            host.Slots.Sum(s => s.Engine!.Metrics.Ticks) - ticks0,
            host.Slots.Sum(s => s.Engine!.Metrics.OrdersSent) - orders0,
            host.Slots.Sum(s => s.Engine!.Metrics.Fills) - fills0,
            host.Slots.Sum(s => s.Engine!.Metrics.BudgetBreaches) - breaches0,
            GC.CollectionCount(0) - g0, GC.CollectionCount(1) - g1, GC.CollectionCount(2) - g2);
        try { Directory.Delete(options.DataDirectory, recursive: true); } catch (IOException) { }
        return row;
    }

    public sealed record FanoutResult(int Subscribers, long Ticks, double Seconds, long TradesPublished, long TradesReceived, long Conflated, bool AnyDetached)
    {
        public double TicksPerSecond => Ticks / Seconds;
    }

    /// <summary>
    /// Feed handler throughput: an unpaced simulated feed through one feed handler thread to N inboxes, drained
    /// by consumer threads. Counts trade ticks published vs received per subscriber (trades must never be lost).
    /// </summary>
    public static FanoutResult RunFanout(int subscribers, long messages, int consumerThreads)
    {
        var feed = new SimulatedFeed(new SimulatedFeedOptions
        {
            Seed = 1, MaxMessages = messages, TradeProbability = 0.1,
            Instruments = [(new InstrumentId(1), Price.FromDouble(100), 100)],
        });
        var handler = new FeedHandler(feed, new Sequencer(), MonotonicClock.Instance);
        var inboxes = Enumerable.Range(0, subscribers).Select(i => new MarketDataInbox(4096, 1 << 17) { Name = i.ToString() }).ToArray();
        foreach (var ib in inboxes) handler.Subscribe(ib, [new InstrumentId(1)]);
        var trades = new long[subscribers];
        var done = 0;
        var consumers = Enumerable.Range(0, consumerThreads).Select(c => new Thread(() =>
        {
            while (true)
            {
                bool finished = Volatile.Read(ref done) == 1;
                int got = 0;
                for (int i = c; i < subscribers; i += consumerThreads)
                {
                    while (inboxes[i].TryDequeue(out HostEvent e))
                    {
                        got++;
                        if (e.Tick.IsTrade) trades[i]++;
                    }
                }
                if (finished && got == 0) return;
                if (got == 0) Thread.SpinWait(20);
            }
        }) { IsBackground = true }).ToArray();
        foreach (var t in consumers) t.Start();

        var sw = Stopwatch.StartNew();
        while (handler.PollOnce() > 0) { }
        while (inboxes.Any(i => i.HasBacklog)) handler.PollOnce();
        sw.Stop();
        Volatile.Write(ref done, 1);
        foreach (var t in consumers) t.Join();

        // Trade ticks published = trade messages in the feed (each is one tick).
        long published = 0;
        var replay = new SimulatedFeed(new SimulatedFeedOptions
        {
            Seed = 1, MaxMessages = messages, TradeProbability = 0.1, Instruments = [(new InstrumentId(1), Price.FromDouble(100), 100)],
        });
        while (replay.TryRead(out var m)) if (m.Type == FeedMessageType.Trade) published++;
        return new FanoutResult(subscribers, handler.TicksPublished, sw.Elapsed.TotalSeconds, published * subscribers, trades.Sum(),
            inboxes.Sum(i => i.Conflated), inboxes.Any(i => i.Detached));
    }

    /// <summary>Same procedure as acceptance test 8: hot path on the calling thread, allocated bytes before/after.</summary>
    public static (long Ticks, long Orders, long Bytes, double NsPerTick) RunAllocation(long ticks)
    {
        var refData = ReferenceDataSnapshot.Default();
        var outbound = new MpscRing<OrderCommand>(1 << 12);
        var journal = new JournalService(1 << 12);
        journal.Start(IdleMode.SpinYield);
        JournalChannel channel = journal.Open(null);
        var manifest = new StrategyManifest
        {
            Name = "alloc", Version = "1", EntryType = "x, y", Instruments = ["VOD.L"],
            Limits = new RiskLimits { MaxOrderQty = 1000, MaxPosition = 1_000_000, MaxNotional = long.MaxValue / 4, MaxOrdersPerSec = 100_000 },
        };
        var engine = new StrategyEngine(1, manifest, new LatencyProbeStrategy(), new RingOrderSink(outbound), channel,
            new EngineOptions { HandlerBudgetNs = long.MaxValue });
        var inbox = new MarketDataInbox(1024, 1024);
        var vod = new InstrumentId(1);
        long seq = 0, execId = 0, t = 1_000_000_000;
        var boot = new[]
        {
            new HostEvent { Type = EventType.ReferenceData, A = 1, Payload = refData },
            new HostEvent { Type = EventType.ExchangeStatus, A = 1 },
            new HostEvent { Type = EventType.SessionStart, Payload = StrategyEngine.SessionPayload(1, "b", manifest) },
        };
        for (int i = 0; i < boot.Length; i++)
        {
            boot[i].Seq = ++seq;
            boot[i].TimestampNs = t;
            engine.Process(ref boot[i]);
        }

        void Run(long n)
        {
            for (long i = 0; i < n; i++)
            {
                t += 1_000;
                long bid = 1_000_000 + (i & 7) * 100;
                var tick = new HostEvent
                {
                    Type = EventType.Tick, Seq = ++seq, TimestampNs = t, PublishNs = t,
                    Tick = new Tick(seq, t, vod, bid, 500, bid + 200, 500, 0, 0),
                };
                inbox.Publish(in tick);
                while (inbox.TryDequeue(out HostEvent md)) engine.Process(ref md);
                while (outbound.TryDequeue(out OrderCommand cmd))
                {
                    var fill = new HostEvent
                    {
                        Type = EventType.Execution, Seq = ++seq, TimestampNs = t,
                        Exec = new ExecutionReport(seq, cmd.OrderId, ++execId, vod, cmd.Side, OrdStatus.Filled, cmd.Px, cmd.Qty, cmd.Qty, 0, 0),
                    };
                    engine.Process(ref fill);
                }
            }
        }

        Run(200_000);
        long orders0 = engine.Metrics.OrdersSent;
        long start = Stopwatch.GetTimestamp();   // not Stopwatch.StartNew(): that allocates inside the measured window
        long before = GC.GetAllocatedBytesForCurrentThread();
        Run(ticks);
        long bytes = GC.GetAllocatedBytesForCurrentThread() - before;
        var elapsed = Stopwatch.GetElapsedTime(start);
        channel.Close();
        journal.Dispose();
        return (ticks, engine.Metrics.OrdersSent - orders0, bytes, elapsed.TotalNanoseconds / ticks);
    }

    public static string MachineDescription()
    {
        string cpu = "unknown CPU";
        try
        {
            if (OperatingSystem.IsWindows())
            {
                using var key = Microsoft.Win32.Registry.LocalMachine.OpenSubKey(@"HARDWARE\DESCRIPTION\System\CentralProcessor\0");
                cpu = (key?.GetValue("ProcessorNameString") as string)?.Trim() ?? cpu;
            }
            else if (File.Exists("/proc/cpuinfo"))
            {
                cpu = File.ReadLines("/proc/cpuinfo").FirstOrDefault(l => l.StartsWith("model name"))?.Split(':')[1].Trim() ?? cpu;
            }
        }
        catch (Exception) { }
        return $"{cpu}, {Environment.ProcessorCount} logical processors, {RuntimeInformation.OSDescription}, " +
               $"{RuntimeInformation.FrameworkDescription}, Stopwatch resolution {MonotonicClock.ResolutionNs:F0} ns, " +
               $"Server GC {System.Runtime.GCSettings.IsServerGC}, no thread pinning, no core isolation";
    }

    public static string Us(long ns) => (ns / 1000.0).ToString("F1", CultureInfo.InvariantCulture);

    public static string Render(List<LatencyRow> rows, List<FanoutResult> fanout, (long Ticks, long Orders, long Bytes, double NsPerTick) alloc,
        string machine, TimeSpan measure)
    {
        var sb = new StringBuilder();
        sb.AppendLine("<!-- Generated by tests/AlgoPlatform.Benchmarks (dotnet run -c Release -- report). Numbers below are measured, not estimated. -->");
        sb.AppendLine();
        sb.AppendLine("## Measured results");
        sb.AppendLine();
        sb.AppendLine($"Machine: {machine}.");
        sb.AppendLine();
        sb.AppendLine($"Run: {DateTime.UtcNow:yyyy-MM-dd HH:mm} UTC, {measure.TotalSeconds:F0} s measured per row after warm-up, Paper mode " +
                      "(embedded SimExchange over loopback TCP), journal on, one instrument per strategy across 4 instruments.");
        sb.AppendLine();
        sb.AppendLine("### Tick-to-order inside a host (target p99 < 50 us, p99.9 < 200 us)");
        sb.AppendLine();
        sb.AppendLine("Measured from the feed message's *intended* send time to the order being handed to the gateway ring.");
        sb.AppendLine();
        sb.AppendLine("| Strategies | Idle mode | Feed msg/s | Samples | p50 us | p99 us | p99.9 us | max us | Handler p99 us | Ticks handled | Orders | Budget breaches | GC gen0/1/2 |");
        sb.AppendLine("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|");
        foreach (var r in rows)
        {
            sb.AppendLine($"| {r.Strategies} | {r.Idle} | {r.FeedRate:F0} | {r.Samples} | {Us(r.TickToOrder.Percentile(50))} | {Us(r.TickToOrder.Percentile(99))} | " +
                          $"{Us(r.TickToOrder.Percentile(99.9))} | {Us(r.TickToOrder.Max)} | {Us(r.Handler.Percentile(99))} | {r.Ticks} | {r.Orders} | {r.Breaches} | {r.Gen0}/{r.Gen1}/{r.Gen2} |");
        }
        sb.AppendLine();
        sb.AppendLine("### Market data fan-out (target 1M ticks/s to 50 strategies, no lost trades)");
        sb.AppendLine();
        sb.AppendLine("| Subscribers | Ticks published | Seconds | Ticks/s (per feed handler) | Deliveries/s | Trades published x subs | Trades received | Quotes conflated | Detached |");
        sb.AppendLine("|---:|---:|---:|---:|---:|---:|---:|---:|---|");
        foreach (var f in fanout)
            sb.AppendLine($"| {f.Subscribers} | {f.Ticks} | {f.Seconds:F2} | {f.TicksPerSecond:N0} | {f.TicksPerSecond * f.Subscribers:N0} | {f.TradesPublished} | {f.TradesReceived} | {f.Conflated} | {f.AnyDetached} |");
        sb.AppendLine();
        sb.AppendLine("### Hot-path allocation (target zero)");
        sb.AppendLine();
        sb.AppendLine($"{alloc.Ticks:N0} ticks, {alloc.Orders:N0} orders and fills through inbox, engine, risk, OMS, journal ring and order ring on one thread: " +
                      $"**{alloc.Bytes} bytes allocated** (`GC.GetAllocatedBytesForCurrentThread`), {alloc.NsPerTick:F0} ns per tick including the simulated exchange work.");
        return sb.ToString();
    }
}

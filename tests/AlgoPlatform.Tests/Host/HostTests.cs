using AlgoPlatform.Core.Collections;
using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Journal;
using AlgoPlatform.Core.Threading;
using AlgoPlatform.Core.Timing;
using AlgoPlatform.Host.Engine;
using AlgoPlatform.Host.Hosting;
using AlgoPlatform.Host.Loading;
using AlgoPlatform.MarketData;
using AlgoPlatform.ReferenceData;
using AlgoPlatform.Sdk;
using AlgoPlatform.Strategies.Samples;
using Xunit.Abstractions;

namespace AlgoPlatform.Tests.Host;

public class HostTests(ITestOutputHelper output)
{
    private static readonly string[] AllInstruments = ["VOD.L", "BARC.L", "AAPL", "MSFT"];

    /// <summary>Acceptance test 2: a strategy sleeping 50 ms per tick is stopped; its sibling's p99 is unaffected.</summary>
    [Fact]
    public void Sleeping_strategy_is_stopped_and_sibling_latency_is_unaffected()
    {
        (long baselineP99, _) = RunSibling(withSleeper: false);
        (long withSleeperP99, StrategyStatus sleeperStatus) = RunSibling(withSleeper: true);
        output.WriteLine($"sibling inbox latency p99: alone {baselineP99} ns, next to sleeper {withSleeperP99} ns");

        Assert.Equal(StrategyStatus.Slow, sleeperStatus);
        // Within 20%, plus 2 us of absolute slack for timer resolution and run-to-run noise (DESIGN_DECISIONS D-13).
        Assert.True(withSleeperP99 <= baselineP99 * 1.2 + 2_000,
            $"sibling p99 degraded: {baselineP99} ns -> {withSleeperP99} ns");
    }

    private static (long P99, StrategyStatus SleeperStatus) RunSibling(bool withSleeper)
    {
        var options = TestSupport.HostOptions("isolation", feedRate: 20_000, idle: IdleMode.BusySpin);
        options.EnableGateway = false;
        options.EnableJournal = false;
        using var host = new StrategyHost(options);
        host.Start();
        host.Deploy(TestSupport.Manifest("sibling", AllInstruments), () => new CountingStrategy());
        host.Start("sibling");
        if (withSleeper)
        {
            host.Deploy(TestSupport.Manifest("sleeper", AllInstruments), () => new SleepyStrategy(50));
            host.Start("sleeper");
        }
        Thread.Sleep(500);                                  // warm-up
        var sibling = host.GetSlot("sibling")!.Engine!;
        sibling.Metrics.InboxLatency.Reset();
        Thread.Sleep(2_000);
        long p99 = sibling.Metrics.InboxLatency.Percentile(99);
        StrategyStatus status = withSleeper ? host.GetSlot("sleeper")!.Status : StrategyStatus.Created;
        if (withSleeper)
        {
            Assert.True(sibling.Metrics.Ticks > 10_000);
            Assert.True(host.GetSlot("sleeper")!.Engine!.Metrics.BudgetBreaches >= options.MaxConsecutiveBreaches);
        }
        return (p99, status);
    }

    /// <summary>Acceptance test 7 (host level): a strategy that cannot keep up is conflated, then kill-switched.</summary>
    [Fact]
    public void Persistently_slow_strategy_is_conflated_then_kill_switched()
    {
        var options = TestSupport.HostOptions("slow", feedRate: 20_000);
        options.EnableGateway = false;
        options.MarketDataRingCapacity = 64;
        options.MarketDataOverflowCapacity = 256;
        using var host = new StrategyHost(options);
        host.Start();
        host.Deploy(TestSupport.Manifest("slowpoke", AllInstruments), () => new SleepyStrategy(2));   // 2 ms: under the 5 ms budget
        host.Start("slowpoke");
        var slot = host.GetSlot("slowpoke")!;
        Assert.True(TestSupport.WaitUntil(() => slot.Status == StrategyStatus.Slow, TimeSpan.FromSeconds(20)));
        Assert.True(slot.MdInbox!.Conflated > 0);
        Assert.True(slot.MdInbox.Detached);
        Assert.True(slot.Engine!.Risk.Killed);
    }

    [Fact]
    public void Throwing_strategy_is_faulted_and_neighbour_keeps_running()
    {
        using var host = new StrategyHost(TestSupport.HostOptions("fault", feedRate: 5_000));
        host.Start();
        host.Deploy(TestSupport.Manifest("thrower", AllInstruments), () => new ThrowingStrategy());
        host.Deploy(TestSupport.Manifest("steady", AllInstruments), () => new CountingStrategy());
        host.Start("thrower");
        host.Start("steady");
        var thrower = host.GetSlot("thrower")!;
        var steady = host.GetSlot("steady")!;
        Assert.True(TestSupport.WaitUntil(() => thrower.Status == StrategyStatus.Faulted, TimeSpan.FromSeconds(10)));
        Assert.Contains("boom", thrower.Engine!.LastError);
        long before = steady.Engine!.Metrics.Ticks;
        Assert.True(TestSupport.WaitUntil(() => steady.Engine.Metrics.Ticks > before + 500, TimeSpan.FromSeconds(10)));
        Assert.Equal(StrategyStatus.Running, steady.Status);
    }

    [Fact]
    public void Hung_handler_is_caught_by_the_watchdog()
    {
        var options = TestSupport.HostOptions("hung", feedRate: 2_000);
        options.HungThresholdMs = 200;
        using var host = new StrategyHost(options);
        host.Start();
        var gate = new ManualResetEventSlim();
        var hang = new HangingStrategy(gate);
        host.Deploy(TestSupport.Manifest("hang"), () => hang);
        host.Start("hang");
        var slot = host.GetSlot("hang")!;
        Assert.True(TestSupport.WaitUntil(() => slot.HungReported, TimeSpan.FromSeconds(10)));
        Assert.True(slot.MdInbox!.Detached);
        gate.Set();   // a real infinite loop would stay stuck; here we let it go to check the follow-up
        Assert.True(TestSupport.WaitUntil(() => slot.Status == StrategyStatus.Slow, TimeSpan.FromSeconds(10)));
    }

    private sealed class HangingStrategy(ManualResetEventSlim gate) : StrategyBase
    {
        private int _ticks;
        public override void OnTick(in Tick tick)
        {
            if (++_ticks == 50) gate.Wait();
        }
    }

    [Fact]
    public void Kill_switch_cancels_all_working_orders_in_the_same_loop_iteration()
    {
        using var host = new StrategyHost(TestSupport.HostOptions("kill", feedRate: 5_000));
        host.Start();
        host.Deploy(TestSupport.Manifest("mm", ["VOD.L", "BARC.L"]), () => new MarketMaker());
        host.Start("mm");
        var slot = host.GetSlot("mm")!;
        Assert.True(TestSupport.WaitUntil(() => slot.Engine!.Oms.LiveOrderCount >= 2, TimeSpan.FromSeconds(10)));
        host.Kill("mm");
        Assert.True(TestSupport.WaitUntil(() => slot.Status == StrategyStatus.Killed, TimeSpan.FromSeconds(5)));
        Assert.True(TestSupport.WaitUntil(() => slot.Engine!.Oms.LiveOrderCount == 0, TimeSpan.FromSeconds(5)));
        long sent = slot.Engine!.Metrics.OrdersSent;
        Thread.Sleep(300);
        Assert.Equal(sent, slot.Engine.Metrics.OrdersSent);   // nothing new goes out
        Assert.True(slot.Engine.Metrics.OrdersRejected > 0);  // ... the strategy kept trying and risk said no
        Assert.True(host.Stop("mm", TimeSpan.FromSeconds(5)));
        host.Journal.WaitClosed(slot.Journal!, TimeSpan.FromSeconds(5));
        KillJournalCheck.AssertCancelsFollowKillImmediately(JournalReader.ReadAll(slot.Journal!.Path!));
    }

    [Fact]
    public void Strategy_loads_from_a_package_in_its_own_load_context()
    {
        string dir = TestSupport.TempDir("pkg");
        File.Copy(typeof(MarketMaker).Assembly.Location, Path.Combine(dir, "AlgoPlatform.Strategies.Samples.dll"));
        File.WriteAllText(Path.Combine(dir, StrategyLoader.ManifestFile), TestSupport.Manifest("alc-mm").ToJson());
        var loaded = StrategyLoader.Load(dir);
        var instance = loaded.Factory();
        Assert.NotSame(typeof(MarketMaker), instance.GetType());   // a separate copy of the assembly
        Assert.Same(loaded.Context, System.Runtime.Loader.AssemblyLoadContext.GetLoadContext(instance.GetType().Assembly));

        using var host = new StrategyHost(TestSupport.HostOptions("alc", feedRate: 5_000));
        host.Start();
        host.DeployDirectory(dir);
        host.Start("alc-mm");
        var slot = host.GetSlot("alc-mm")!;
        Assert.True(TestSupport.WaitUntil(() => slot.Engine!.Metrics.OrdersSent > 0, TimeSpan.FromSeconds(10)));
        Assert.True(host.Stop("alc-mm", TimeSpan.FromSeconds(5)));
        Assert.Equal(StrategyStatus.Stopped, slot.Status);
        Assert.True(host.Remove("alc-mm"));
    }

    [Fact]
    public void Deployment_is_refused_when_the_host_has_no_free_core()
    {
        var options = TestSupport.HostOptions("capacity");
        options.Cores = [0, 1, 2];   // feed, gateway, one strategy core
        using var host = new StrategyHost(options);
        host.Start();
        host.Deploy(TestSupport.Manifest("one"), () => new CountingStrategy());
        var ex = Assert.Throws<CapacityException>(() => host.Deploy(TestSupport.Manifest("two"), () => new CountingStrategy()));
        Assert.Contains("no free core", ex.Message);
        Assert.Throws<CapacityException>(() =>
            host.Deploy(TestSupport.Manifest("shared", resourceClass: ResourceClasses.Shared), () => new CountingStrategy()));
    }

    /// <summary>
    /// Acceptance test 8: after warm-up, 10 million ticks through the hot path (market data inbox, engine,
    /// risk, OMS, journal ring, order ring, execution reports) allocate zero bytes on the loop thread.
    /// </summary>
    [Fact]
    public void Ten_million_ticks_allocate_nothing_on_the_loop_thread()
    {
        var refData = ReferenceDataSnapshot.Default();
        var outbound = new MpscRing<OrderCommand>(1 << 12);
        var control = new MpscRing<HostEvent>(1 << 12);
        var journal = new JournalService(1 << 12);
        journal.Start(IdleMode.SpinYield);
        JournalChannel channel = journal.Open(null);   // drained and discarded by the journal thread
        var manifest = TestSupport.Manifest("alloc", limits: l => { l.MaxOrdersPerSec = 100_000; });
        var strategy = new PingPongStrategy();
        var engine = new StrategyEngine(1, manifest, strategy, new RingOrderSink(outbound), channel,
            new EngineOptions { HandlerBudgetNs = long.MaxValue, Clock = MonotonicClock.Instance });
        var inbox = new MarketDataInbox(1024, 1024);
        var vod = new InstrumentId(1);
        long seq = 0, execId = 0, t = 1_000_000_000;

        void Boot(HostEvent e)
        {
            e.Seq = ++seq;
            e.TimestampNs = t;
            engine.Process(ref e);
        }
        Boot(new HostEvent { Type = EventType.ReferenceData, A = 1, Payload = refData });
        Boot(new HostEvent { Type = EventType.ExchangeStatus, A = 1 });
        Boot(new HostEvent { Type = EventType.SessionStart, Payload = StrategyEngine.SessionPayload(1, "t", manifest) });

        void Run(long ticks)
        {
            for (long i = 0; i < ticks; i++)
            {
                t += 1_000;
                long bid = 1_000_000 + (i & 7) * 100;
                var tick = new HostEvent
                {
                    Type = EventType.Tick, Seq = ++seq, TimestampNs = t, PublishNs = t,
                    Tick = new Tick(seq, t, vod, bid, 500, bid + 200, 500, (i & 15) == 0 ? bid : 0, (i & 15) == 0 ? 10 : 0),
                };
                inbox.Publish(in tick);
                while (inbox.TryDequeue(out HostEvent md)) engine.Process(ref md);

                // Act as the exchange: every order is acknowledged then filled.
                while (outbound.TryDequeue(out OrderCommand cmd))
                {
                    if (cmd.Type != CommandType.New) continue;
                    long px = cmd.Side == Side.Buy ? bid + 200 : bid;
                    var ack = new HostEvent
                    {
                        Type = EventType.Execution, Seq = ++seq, TimestampNs = t,
                        Exec = new ExecutionReport(seq, cmd.OrderId, ++execId, vod, cmd.Side, OrdStatus.Working, 0, 0, 0, cmd.Qty, 0),
                    };
                    var fill = new HostEvent
                    {
                        Type = EventType.Execution, Seq = ++seq, TimestampNs = t,
                        Exec = new ExecutionReport(seq, cmd.OrderId, ++execId, vod, cmd.Side, OrdStatus.Filled, px, cmd.Qty, cmd.Qty, 0, 0),
                    };
                    control.TryEnqueue(ack);
                    control.TryEnqueue(fill);
                }
                while (control.TryDequeue(out HostEvent ev)) engine.Process(ref ev);
            }
        }

        Run(200_000);   // warm-up: JIT tiering, first-touch of every array
        long before = GC.GetAllocatedBytesForCurrentThread();
        Run(10_000_000);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        channel.Close();
        journal.Dispose();

        output.WriteLine($"orders sent {engine.Metrics.OrdersSent}, fills {engine.Metrics.Fills}, allocated {allocated} bytes");
        Assert.True(engine.Metrics.OrdersSent > 500_000);
        Assert.Equal(StrategyStatus.Running, engine.Status);
        Assert.Equal(0, engine.Oms.LiveOrderCount);
        Assert.Equal(0, allocated);
    }
}

/// <summary>Sends an IOC at the touch every 10th tick, alternating sides so the position stays bounded.</summary>
public sealed class PingPongStrategy : StrategyBase
{
    private long _n;
    public override void OnTick(in Tick tick)
    {
        if (++_n % 10 != 0 || tick.BidPx <= 0) return;
        bool buy = Ctx.GetPosition(tick.Instrument).NetQty <= 0;
        Ctx.Submit(new NewOrder(tick.Instrument, buy ? Side.Buy : Side.Sell, OrdType.Limit, Tif.Ioc, buy ? tick.AskPx : tick.BidPx, 100));
    }
}

public static class KillJournalCheck
{
    /// <summary>
    /// "Within one event-loop iteration": in the journal, the Kill input record is followed directly by a
    /// cancel for every order that was live at that moment, before any further input is handled.
    /// </summary>
    public static void AssertCancelsFollowKillImmediately(List<JournalRecord> records)
    {
        var live = new HashSet<long>();
        for (int i = 0; i < records.Count; i++)
        {
            var r = records[i];
            if (r.IsOutput)
            {
                var c = JournalCodec.DecodeOutput(r);
                if (c.Type == CommandType.New && c.RejectCode == 0) live.Add(c.OrderId);
                continue;
            }
            var e = JournalCodec.DecodeInput(r);
            if (e.Type == EventType.Execution && e.Exec.Status is OrdStatus.Filled or OrdStatus.Cancelled or OrdStatus.Rejected)
                live.Remove(e.Exec.OrderId);
            if (e.Type != EventType.Control || (ControlCode)e.A is not (ControlCode.Kill or ControlCode.KillAndFlatten)) continue;

            Assert.NotEmpty(live);
            var cancelled = new HashSet<long>();
            for (int j = i + 1; j < records.Count && records[j].IsOutput; j++)
            {
                var c = JournalCodec.DecodeOutput(records[j]);
                if (c.Type == CommandType.Cancel) cancelled.Add(c.OrderId);
            }
            Assert.Equal(live.OrderBy(x => x), cancelled.OrderBy(x => x));
            return;
        }
        Assert.Fail("no kill record in the journal");
    }
}

using System.Diagnostics;
using System.Text.Json;
using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Core.Threading;
using AlgoPlatform.Host.Hosting;
using AlgoPlatform.Sdk;

// Several tests measure latency or drive real threads; running them side by side would make them flaky.
[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace AlgoPlatform.Tests;

public static class TestSupport
{
    public const string MarketMakerType = "AlgoPlatform.Strategies.Samples.MarketMaker, AlgoPlatform.Strategies.Samples";
    public const string ThresholdBuyerType = "AlgoPlatform.Strategies.Samples.ThresholdBuyer, AlgoPlatform.Strategies.Samples";

    public static StrategyManifest Manifest(string name, string[]? instruments = null, Action<RiskLimits>? limits = null,
        Dictionary<string, double>? parameters = null, string resourceClass = ResourceClasses.DedicatedCore, string entryType = MarketMakerType)
    {
        var m = new StrategyManifest
        {
            Name = name,
            Version = "1.0.0",
            EntryType = entryType,
            Instruments = instruments ?? ["VOD.L"],
            Mode = "Paper",
            ResourceClass = resourceClass,
            Limits = new RiskLimits { MaxOrderQty = 1000, MaxPosition = 100_000, MaxNotional = 100_000_000, MaxOrdersPerSec = 10_000 },
        };
        limits?.Invoke(m.Limits);
        if (parameters is not null)
            foreach (var (k, v) in parameters) m.Parameters[k] = JsonSerializer.SerializeToElement(v);
        return m;
    }

    public static string TempDir(string prefix)
    {
        string dir = Path.Combine(Path.GetTempPath(), "algoplatform-tests", prefix + "-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        return dir;
    }

    /// <summary>Host options suitable for tests: no pinning, plenty of virtual cores, journal in a temp dir.</summary>
    public static HostOptions HostOptions(string name, double feedRate = 2_000, IdleMode idle = IdleMode.SpinYield) => new()
    {
        HostId = name,
        DataDirectory = TempDir(name),
        Cores = Enumerable.Range(0, 66).ToArray(),
        PinThreads = false,
        StrategyIdle = idle,
        InfrastructureIdle = idle,
        FeedMessagesPerSecond = feedRate,
        HandlerBudgetNs = 5_000_000,   // generous unless a test is about the budget: CI machines jitter
    };

    public static bool WaitUntil(Func<bool> condition, TimeSpan timeout, int pollMs = 5)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            if (condition()) return true;
            Thread.Sleep(pollMs);
        }
        return condition();
    }

    public static async Task<bool> WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout, int pollMs = 20)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            if (await condition()) return true;
            await Task.Delay(pollMs);
        }
        return await condition();
    }
}

/// <summary>Does nothing but count; used as the "well-behaved neighbour".</summary>
public sealed class CountingStrategy : StrategyBase
{
    public long Ticks;
    public override void OnTick(in Tick tick) => Ticks++;
}

/// <summary>Sleeps in every OnTick: the badly behaved strategy the watchdog has to stop.</summary>
public sealed class SleepyStrategy(int sleepMs) : StrategyBase
{
    public long Ticks;
    public override void OnTick(in Tick tick)
    {
        Ticks++;
        Thread.Sleep(sleepMs);
    }
}

public sealed class ThrowingStrategy : StrategyBase
{
    public long Ticks;
    public override void OnTick(in Tick tick)
    {
        if (++Ticks == 20) throw new InvalidOperationException("boom");
    }
}

/// <summary>
/// Exercises every input kind the engine journals: timers, async results, submits, replaces that risk
/// rejects locally (internal events), cancels and fills. Deterministic given its inputs.
/// </summary>
public sealed class ChaosStrategy : StrategyBase
{
    private InstrumentId _id;
    private long _lastBid;
    private long _lastAsk;
    private long _resting;
    private long _counter;

    public override void OnStart(IStrategyContext ctx)
    {
        base.OnStart(ctx);
        _id = ctx.Instruments[0];
        ctx.SetTimer(1, 1_000_000);
    }

    public override void OnTick(in Tick tick)
    {
        if (tick.Instrument != _id || tick.BidPx <= 0) return;
        _lastBid = tick.BidPx;
        _lastAsk = tick.AskPx;
        if (++_counter % 7 == 0) Ctx.Submit(new NewOrder(_id, Side.Buy, OrdType.Limit, Tif.Ioc, tick.AskPx, 10));
    }

    public override void OnTimer(long timerId)
    {
        Ctx.SetTimer(1, 1_000_000);
        if (_lastBid == 0) return;
        if (_resting == 0)
        {
            var h = Ctx.Submit(new NewOrder(_id, Side.Buy, OrdType.Limit, Tif.Day, _lastBid - 500, 20));
            if (h.Accepted) _resting = h.OrderId;
        }
        else if (_counter % 3 == 0)
        {
            Ctx.Replace(_resting, _lastBid - 300, 0);   // invalid quantity: rejected locally (internal event)
        }
        else if (_counter % 3 == 1)
        {
            Ctx.Replace(_resting, _lastBid - 400, 30);
        }
        else
        {
            Ctx.Cancel(_resting);
        }
        if (_counter % 5 == 0) Ctx.RunAsync(() => BitConverter.GetBytes(_counter));
    }

    public override void OnAsyncResult(long requestId, ReadOnlySpan<byte> payload)
    {
        if (payload.Length == 8 && _lastAsk > 0)
            Ctx.Submit(new NewOrder(_id, Side.Sell, OrdType.Limit, Tif.Ioc, _lastBid, 10));
    }

    public override void OnExecution(in ExecutionReport report)
    {
        if (report.OrderId == _resting && report.Status is OrdStatus.Filled or OrdStatus.Cancelled or OrdStatus.Rejected) _resting = 0;
    }
}

using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Journal;
using AlgoPlatform.Host.Engine;
using AlgoPlatform.Host.Hosting;
using AlgoPlatform.MarketData;
using AlgoPlatform.ReferenceData;
using AlgoPlatform.Replay;
using AlgoPlatform.Sdk;
using AlgoPlatform.Strategies.Samples;
using Xunit.Abstractions;

namespace AlgoPlatform.Tests.Replay;

public class DeterminismTests(ITestOutputHelper output)
{
    /// <summary>
    /// Records a live Paper session (real threads, real TCP to the embedded exchange, nondeterministic timing),
    /// then returns the journal paths of the strategies that ran.
    /// </summary>
    private static Dictionary<string, (string Path, Func<IStrategy> Factory)> RecordSession()
    {
        var options = TestSupport.HostOptions("determinism", feedRate: 5_000);
        options.EmbeddedExchange.MaxFillQty = 40;   // partial fills
        using var host = new StrategyHost(options);
        host.Start();
        host.Deploy(TestSupport.Manifest("mm", ["VOD.L", "BARC.L"], parameters: new() { ["spreadTicks"] = 1 }), () => new MarketMaker());
        host.Deploy(TestSupport.Manifest("chaos", ["AAPL"]), () => new ChaosStrategy());
        host.Start("mm");
        host.Start("chaos");
        Thread.Sleep(1_500);
        host.Kill("chaos");   // a control input in the middle of the session
        Thread.Sleep(200);
        var paths = new Dictionary<string, (string, Func<IStrategy>)>();
        foreach (var name in new[] { "mm", "chaos" })
        {
            var slot = host.GetSlot(name)!;
            Assert.True(host.Stop(name, TimeSpan.FromSeconds(5)));
            Assert.True(host.Journal.WaitClosed(slot.Journal!, TimeSpan.FromSeconds(5)));
            paths[name] = (slot.Journal!.Path!, name == "mm" ? () => new MarketMaker() : () => new ChaosStrategy());
        }
        return paths;
    }

    /// <summary>Acceptance test 3: the same journal through the same strategy 100 times gives identical outputs,
    /// and they are byte-for-byte the outputs of the original live run.</summary>
    [Fact]
    public void Replaying_a_journal_100_times_reproduces_the_live_outputs_exactly()
    {
        foreach (var (name, (path, factory)) in RecordSession())
        {
            var records = JournalReader.ReadAll(path);
            var kinds = records.Where(r => !r.IsOutput).GroupBy(r => (EventType)r.Type).ToDictionary(g => g.Key, g => g.Count());
            output.WriteLine($"{name}: {records.Count} records, inputs {string.Join(", ", kinds.Select(k => $"{k.Key}={k.Value}"))}");
            Assert.True(kinds.GetValueOrDefault(EventType.Execution) > 10, "session produced too few execution reports");

            string? digest = null;
            for (int run = 0; run < 100; run++)
            {
                ReplayResult result = JournalReplayer.Run(records, factory);
                Assert.True(result.MatchesOriginal, $"{name} run {run}: first mismatch at output {result.FirstMismatch}");
                Assert.True(result.Outputs.Count > 10);
                digest ??= result.Digest;
                Assert.Equal(digest, result.Digest);
            }
        }
    }

    [Fact]
    public void Replay_detects_a_changed_strategy()
    {
        var (path, _) = RecordSession()["mm"];
        // Same strategy type, different parameter: outputs must diverge from the recording.
        var different = JournalReplayer.Run(JournalReader.ReadAll(path), () => new WiderMarketMaker());
        Assert.False(different.MatchesOriginal);
    }

    /// <summary>A "new version" of the market maker that quotes one tick wider than the manifest says.</summary>
    private sealed class WiderMarketMaker : StrategyBase
    {
        private readonly MarketMaker _inner = new();
        public override void OnStart(IStrategyContext ctx) => _inner.OnStart(new WiderContext(ctx));
        public override void OnTick(in Tick tick) => _inner.OnTick(in tick);
        public override void OnExecution(in ExecutionReport report) => _inner.OnExecution(in report);

        private sealed class WiderContext(IStrategyContext c) : IStrategyContext
        {
            public long NowNs => c.NowNs;
            public IReferenceData Reference => c.Reference;
            public IReadOnlyList<InstrumentId> Instruments => c.Instruments;
            public Position GetPosition(InstrumentId id) => c.GetPosition(id);
            public OrderHandle Submit(in NewOrder order) => c.Submit(in order);
            public void Cancel(long orderId) => c.Cancel(orderId);
            public void Replace(long orderId, long newPx, long newQty) => c.Replace(orderId, newPx, newQty);
            public void SetTimer(long timerId, long afterNs) => c.SetTimer(timerId, afterNs);
            public long RunAsync(Func<byte[]> work) => c.RunAsync(work);
            public void Log(LogLevel level, string template, long a = 0, long b = 0) => c.Log(level, template, a, b);
            public double GetParameter(string name, double defaultValue) =>
                name == "spreadTicks" ? c.GetParameter(name, defaultValue) + 1 : c.GetParameter(name, defaultValue);
        }
    }

    [Fact]
    public void Backtest_over_a_synthetic_feed_is_deterministic_and_trades()
    {
        var refData = ReferenceDataSnapshot.Default();
        var manifest = TestSupport.Manifest("bt", ["VOD.L"], parameters: new() { ["spreadTicks"] = 1, ["requoteTicks"] = 4 });
        SimulatedFeedOptions Feed() => new()
        {
            Seed = 5,
            Instruments = [(new InstrumentId(1), Price.FromDouble(100), 100)],
        };
        var a = BacktestRunner.FromSimulatedFeed(Feed(), 50_000, manifest, new MarketMaker(), refData);
        var b = BacktestRunner.FromSimulatedFeed(Feed(), 50_000, manifest, new MarketMaker(), refData);
        output.WriteLine($"ticks {a.Ticks}, orders {a.Orders}, fills {a.Fills}, position {a.Positions["VOD.L"]}, {a.Elapsed.TotalMilliseconds:F0} ms");
        Assert.True(a.Fills > 10);
        Assert.Equal(a.Positions["VOD.L"], b.Positions["VOD.L"]);
        Assert.Equal(JournalReplayer.DigestOf(a.Outputs), JournalReplayer.DigestOf(b.Outputs));
        Assert.Equal(StrategyStatus.Running, a.FinalStatus);
    }
}

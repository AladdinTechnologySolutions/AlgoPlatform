using AlgoPlatform.Core.Collections;
using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Core.Timing;
using AlgoPlatform.Gateway.Fix;
using AlgoPlatform.Oms;
using AlgoPlatform.ReferenceData;
using AlgoPlatform.Risk;
using AlgoPlatform.Sdk;
using BenchmarkDotNet.Attributes;

namespace AlgoPlatform.Benchmarks;

/// <summary>BenchmarkDotNet micro-benchmarks of the building blocks on the hot path (single thread).</summary>
[MemoryDiagnoser]
public class MicroBenchmarks
{
    private readonly SpscRing<long> _spsc = new(1024);
    private readonly MpscRing<long> _mpsc = new(1024);
    private readonly LatencyHistogram _hist = new();
    private readonly FixWriter _fix = new();
    private readonly byte[] _symbol = "VOD.L"u8.ToArray();
    private readonly ReferenceDataSnapshot _ref = ReferenceDataSnapshot.Default();
    private readonly OrderManager _oms = new(1);
    private PreTradeRisk _risk = null!;
    private readonly NewOrder _order = new(new InstrumentId(1), Side.Buy, OrdType.Limit, Tif.Day, 1_000_000, 100);

    [GlobalSetup]
    public void Setup()
    {
        _risk = new PreTradeRisk(new RiskLimits { MaxOrdersPerSec = 1_000_000, MaxPosition = 1_000_000, MaxNotional = long.MaxValue / 4 }, 1);
        _risk.SetExchangeUp(true);
        _risk.OnTick(new Tick(1, 0, new InstrumentId(1), 999_900, 1, 1_000_100, 1, 0, 0));
    }

    [Benchmark] public long SpscEnqueueDequeue()
    {
        _spsc.TryEnqueue(42);
        _spsc.TryDequeue(out long v);
        return v;
    }

    [Benchmark] public long MpscEnqueueDequeue()
    {
        _mpsc.TryEnqueue(42);
        _mpsc.TryDequeue(out long v);
        return v;
    }

    [Benchmark] public void HistogramRecord() => _hist.Record(12_345);

    [Benchmark] public long ClockRead() => MonotonicClock.Now();

    [Benchmark] public int PreTradeRiskCheck() => _risk.CheckNew(in _order, _ref, _oms, 1_000_000_000);

    [Benchmark] public int FixEncodeNewOrder() =>
        _fix.Begin(FixMsgType.NewOrderSingle).Add(FixTags.SenderCompId, _symbol).Add(FixTags.MsgSeqNum, 12345)
            .Add(FixTags.ClOrdId, 1_099_511_627_777).Add(FixTags.Symbol, _symbol).AddChar(FixTags.Side, (byte)'1')
            .Add(FixTags.OrderQty, 100).AddChar(FixTags.OrdType, (byte)'2').AddPrice(FixTags.Price, 1_000_100)
            .AddChar(FixTags.TimeInForce, (byte)'0').Finish().Length;
}

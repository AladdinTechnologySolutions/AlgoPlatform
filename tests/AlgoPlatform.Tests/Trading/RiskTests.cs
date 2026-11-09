using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Journal;
using AlgoPlatform.Host.Engine;
using AlgoPlatform.Oms;
using AlgoPlatform.ReferenceData;
using AlgoPlatform.Risk;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Tests.Trading;

/// <summary>Acceptance test 4: each pre-trade check rejects with the right code; pending quantity counts.</summary>
public class RiskTests
{
    private static readonly InstrumentId Vod = new(1);
    private static readonly long Px100 = Price.FromDouble(100);
    private readonly ReferenceDataSnapshot _ref = ReferenceDataSnapshot.Default();
    private readonly OrderManager _oms = new(1);
    private readonly PreTradeRisk _risk;
    private const long Now = 10_000_000_000;

    public RiskTests()
    {
        _risk = new PreTradeRisk(new RiskLimits
        {
            MaxOrderQty = 1000, MaxPosition = 5000, MaxNotional = 2_000_000, MaxOrdersPerSec = 5, FatFingerBps = 500,
            MaxGrossPosition = 8000,
        }, instrumentCount: 1);
        _risk.SetExchangeUp(true);
        _risk.OnTick(new Tick(1, 0, Vod, Price.FromDouble(99.99), 10, Price.FromDouble(100.01), 10, 0, 0));
    }

    private int Check(long qty = 100, long? px = null, Side side = Side.Buy, InstrumentId? id = null, OrdType type = OrdType.Limit) =>
        _risk.CheckNew(new NewOrder(id ?? Vod, side, type, Tif.Day, px ?? Px100, qty), _ref, _oms, Now);

    [Fact] public void Valid_order_passes() => Assert.Equal(RejectCodes.None, Check());
    [Fact] public void Over_max_order_size() => Assert.Equal(RejectCodes.MaxOrderQty, Check(qty: 1001));
    [Fact] public void Off_price_band() => Assert.Equal(RejectCodes.PriceBand, Check(px: Price.FromDouble(160)));
    [Fact] public void Fat_finger() => Assert.Equal(RejectCodes.FatFinger, Check(px: Price.FromDouble(110)));
    [Fact] public void Off_tick_size() => Assert.Equal(RejectCodes.InvalidOrder, Check(px: Price.FromDouble(100.005)));
    [Fact] public void Missing_reference_data_fails_closed() => Assert.Equal(RejectCodes.MissingReferenceData, Check(id: new InstrumentId(99)));
    [Fact] public void Null_reference_data_fails_closed() =>
        Assert.Equal(RejectCodes.MissingReferenceData, _risk.CheckNew(new NewOrder(Vod, Side.Buy, OrdType.Limit, Tif.Day, Px100, 1), null, _oms, Now));

    [Fact]
    public void Over_notional()
    {
        var risk = new PreTradeRisk(new RiskLimits { MaxOrderQty = 1000, MaxPosition = 5000, MaxNotional = 50_000, MaxOrdersPerSec = 5 }, 1);
        risk.SetExchangeUp(true);
        risk.OnTick(new Tick(1, 0, Vod, Px100, 1, Px100 + 100, 1, 0, 0));
        Assert.Equal(RejectCodes.MaxNotional, risk.CheckNew(new NewOrder(Vod, Side.Buy, OrdType.Limit, Tif.Day, Px100, 1000), _ref, _oms, Now));
        Assert.Equal(RejectCodes.None, risk.CheckNew(new NewOrder(Vod, Side.Buy, OrdType.Limit, Tif.Day, Px100, 400), _ref, _oms, Now));
    }

    [Fact]
    public void Kill_switch()
    {
        _risk.Kill();
        Assert.Equal(RejectCodes.KillSwitch, Check());
    }

    [Fact]
    public void Exchange_down_fails_closed()
    {
        _risk.SetExchangeUp(false);
        Assert.Equal(RejectCodes.ExchangeDown, Check());
    }

    [Fact]
    public void Stale_or_missing_market_data_fails_closed()
    {
        _risk.SetStale(Vod, true);
        Assert.Equal(RejectCodes.StaleMarketData, Check());
        _risk.SetStale(Vod, false);
        Assert.Equal(RejectCodes.None, Check());
        Assert.Equal(RejectCodes.StaleMarketData, Check(id: new InstrumentId(2), px: Price.FromDouble(200)));   // never saw a tick
    }

    [Fact]
    public void Rate_limit_uses_a_sliding_one_second_window()
    {
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(RejectCodes.None, Check());
            _risk.RecordSent(Now + i * 1_000_000);
        }
        Assert.Equal(RejectCodes.RateLimit, _risk.CheckNew(new NewOrder(Vod, Side.Buy, OrdType.Limit, Tif.Day, Px100, 1), _ref, _oms, Now + 900_000_000));
        Assert.Equal(RejectCodes.None, _risk.CheckNew(new NewOrder(Vod, Side.Buy, OrdType.Limit, Tif.Day, Px100, 1), _ref, _oms, Now + 1_000_000_001));
    }

    [Fact]
    public void Pending_quantity_counts_so_in_flight_orders_cannot_breach_position_limit()
    {
        // Five unacknowledged 1000-lot buys use the whole 5000 limit even though nothing has filled.
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(RejectCodes.None, Check(qty: 1000));
            _oms.Create(_oms.NextOrderId(), new NewOrder(Vod, Side.Buy, OrdType.Limit, Tif.Day, Px100, 1000));
        }
        Assert.Equal(0, _oms.GetPosition(Vod).NetQty);
        Assert.Equal(RejectCodes.MaxPosition, Check(qty: 1));
        Assert.Equal(RejectCodes.None, Check(qty: 1000, side: Side.Sell));   // the other side is not blocked

        // Cancelling one frees its quantity.
        long first = OrderIds.Make(1, 1);
        _oms.RequestCancel(first);
        _oms.Apply(new ExecutionReport(0, first, 1, Vod, Side.Buy, OrdStatus.Cancelled, 0, 0, 0, 0, 0));
        Assert.Equal(RejectCodes.None, Check(qty: 1000));
    }

    [Fact]
    public void Per_strategy_gross_limit_counts_positions_and_all_working_orders()
    {
        for (int i = 0; i < 5; i++) _oms.Create(_oms.NextOrderId(), new NewOrder(Vod, Side.Buy, OrdType.Limit, Tif.Day, Px100, 1000));
        Assert.Equal(RejectCodes.None, Check(qty: 1000, side: Side.Sell));
        for (int i = 0; i < 3; i++) _oms.Create(_oms.NextOrderId(), new NewOrder(Vod, Side.Sell, OrdType.Limit, Tif.Day, Px100, 1000));
        // Per instrument the sell side is fine (3000 + 1000 <= 5000), but 8000 working + 1000 exceeds the 8000 gross limit.
        Assert.Equal(RejectCodes.MaxStrategyPosition, Check(qty: 1000, side: Side.Sell));
    }

    [Fact]
    public void Engine_submit_returns_the_reject_code_and_journals_the_rejected_request()
    {
        var sink = new AlgoPlatform.Replay.CapturingJournalSink();
        var manifest = TestSupport.Manifest("r", limits: l => { l.MaxOrderQty = 10; });
        var engine = new StrategyEngine(1, manifest, new CountingStrategy(), NullOrderSink.Instance, sink);
        Start(engine);
        OrderHandle ok = engine.Submit(new NewOrder(Vod, Side.Buy, OrdType.Limit, Tif.Day, Px100, 10));
        OrderHandle bad = engine.Submit(new NewOrder(Vod, Side.Buy, OrdType.Limit, Tif.Day, Px100, 11));
        Assert.True(ok.Accepted);
        Assert.False(bad.Accepted);
        Assert.Equal(RejectCodes.MaxOrderQty, bad.RejectCode);
        Assert.Equal(2, sink.Outputs.Count);   // audit trail includes the reject
    }

    internal static void Start(StrategyEngine engine, ReferenceDataSnapshot? refData = null)
    {
        refData ??= ReferenceDataSnapshot.Default();
        var events = new[]
        {
            new HostEvent { Type = EventType.ReferenceData, Seq = 1, TimestampNs = Now, A = refData.Version, Payload = refData },
            new HostEvent { Type = EventType.ExchangeStatus, Seq = 2, TimestampNs = Now, A = 1 },
            new HostEvent { Type = EventType.SessionStart, Seq = 3, TimestampNs = Now, Payload = StrategyEngine.SessionPayload(engine.StrategyId, "t", engine.Manifest) },
            new HostEvent { Type = EventType.Tick, Seq = 4, TimestampNs = Now, Tick = new Tick(4, Now, Vod, Price.FromDouble(99.99), 100, Price.FromDouble(100.01), 100, 0, 0) },
        };
        for (int i = 0; i < events.Length; i++) engine.Process(ref events[i]);
    }
}

using System.Text;
using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Gateway;
using AlgoPlatform.Gateway.Fix;
using AlgoPlatform.Host.Hosting;
using AlgoPlatform.Oms;
using AlgoPlatform.Sdk;
using AlgoPlatform.SimExchange;

namespace AlgoPlatform.Tests.Gateway;

public class FixCodecTests
{
    [Fact]
    public void Writer_output_parses_back_with_length_and_checksum()
    {
        var w = new FixWriter();
        w.Begin(FixMsgType.NewOrderSingle).Add(FixTags.MsgSeqNum, 7).Add(FixTags.ClOrdId, 1234567890123)
            .Add(FixTags.Symbol, "VOD.L"u8).AddChar(FixTags.Side, (byte)'1').AddPrice(FixTags.Price, Price.FromDouble(101.25));
        byte[] wire = w.Finish().ToArray();
        string text = Encoding.ASCII.GetString(wire).Replace('\u0001', '|');
        Assert.StartsWith("8=FIX.4.4|9=", text);
        Assert.Contains("|44=101.2500|", text);
        Assert.Matches(@"\|10=\d{3}\|$", text);

        var framer = new FixFramer();
        var msg = new FixMessage();
        // Deliver in awkward chunks: the framer must reassemble.
        for (int i = 0; i < wire.Length; i += 5)
        {
            int n = Math.Min(5, wire.Length - i);
            wire.AsSpan(i, n).CopyTo(framer.WritableSpan());
            framer.Advance(n);
            if (i + n < wire.Length) Assert.False(framer.TryRead(msg));
        }
        Assert.True(framer.TryRead(msg));
        Assert.Equal(FixMsgType.NewOrderSingle, msg.MsgType);
        Assert.Equal(1234567890123, msg.GetLong(FixTags.ClOrdId));
        Assert.True(msg.TryGetPrice(FixTags.Price, out long px));
        Assert.Equal(Price.FromDouble(101.25), px);
        Assert.Equal("VOD.L", Encoding.ASCII.GetString(msg.GetBytes(FixTags.Symbol)));
    }

    [Theory]
    [InlineData("100", 1_000_000)]
    [InlineData("0.5", 5_000)]
    [InlineData("12.34567", 123_456)]
    [InlineData("-1.25", -12_500)]
    public void Prices_parse_to_fixed_point(string text, long expected)
    {
        Assert.True(FixMessage.TryParsePrice(Encoding.ASCII.GetBytes(text), out long px));
        Assert.Equal(expected, px);
    }

    [Fact]
    public void Encoding_does_not_allocate()
    {
        var w = new FixWriter();
        byte[] sym = "VOD.L"u8.ToArray();
        for (int i = 0; i < 1000; i++) w.Begin(FixMsgType.NewOrderSingle).Add(FixTags.ClOrdId, i).Add(FixTags.Symbol, sym).Finish();
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 100_000; i++)
            w.Begin(FixMsgType.NewOrderSingle).Add(FixTags.ClOrdId, i).Add(FixTags.Symbol, sym).AddPrice(FixTags.Price, i).Finish();
        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
    }
}

/// <summary>Submits a fixed number of orders from a timer: alternating marketable and resting buys, cancelling some.</summary>
public sealed class OrderSprayStrategy(int total) : StrategyBase
{
    private InstrumentId _id;
    private long _bid;
    private int _sent;
    private readonly List<long> _resting = new();
    public volatile bool Done;

    public override void OnStart(IStrategyContext ctx)
    {
        base.OnStart(ctx);
        _id = ctx.Instruments[0];
        ctx.SetTimer(1, 2_000_000);
    }

    public override void OnTick(in Tick tick)
    {
        if (tick.Instrument == _id && tick.BidPx > 0) _bid = tick.BidPx;
    }

    public override void OnTimer(long timerId)
    {
        if (_sent >= total)
        {
            Done = true;
            return;
        }
        Ctx.SetTimer(1, 2_000_000);
        if (_bid == 0) return;
        bool marketable = _sent % 2 == 0;
        // The SimExchange quotes 100.00 / 100.02 in Live mode, so 100.05 always crosses and 99.50 always rests.
        long px = marketable ? Price.FromDouble(100.05) : Price.FromDouble(99.50);
        var h = Ctx.Submit(new NewOrder(_id, Side.Buy, OrdType.Limit, Tif.Day, px, 100 + _sent));
        if (!h.Accepted) return;   // e.g. exchange down while recovering: try again next timer
        _sent++;
        if (!marketable) _resting.Add(h.OrderId);
        if (_resting.Count >= 3)
        {
            Ctx.Cancel(_resting[0]);
            _resting.RemoveAt(0);
        }
    }
}

public class GatewayRecoveryTests
{
    /// <summary>
    /// Acceptance test 6: the SimExchange drops the TCP connection mid-session after N orders (and swallows the
    /// responses to the Nth). After reconnect, resend and reconcile there are no lost orders, no duplicates, and
    /// OMS state equals exchange state.
    /// </summary>
    [Fact]
    public void Dropped_session_recovers_without_lost_or_duplicate_orders()
    {
        using var exchange = new SimExchange.SimExchange(new SimExchangeOptions { MaxFillQty = 50, DropAfterOrders = 15, HeartbeatIntervalMs = 200 });
        exchange.Start();

        var options = TestSupport.HostOptions("gw-recovery");
        options.Mode = RunMode.Live;
        options.Gateway = new GatewayOptions { Port = exchange.Port, HeartbeatIntervalMs = 200, ReconnectDelayMs = 50 };
        options.RetainTerminalOrders = true;
        using var host = new StrategyHost(options);
        host.Start();
        var strategy = new OrderSprayStrategy(total: 40);
        host.Deploy(TestSupport.Manifest("spray"), () => strategy);
        host.Start("spray");

        Assert.True(TestSupport.WaitUntil(() => strategy.Done, TimeSpan.FromSeconds(30)), "strategy never finished sending");
        Assert.True(TestSupport.WaitUntil(() => host.Gateway!.IsUp && host.Gateway.Outbound.Count == 0, TimeSpan.FromSeconds(10)));
        Thread.Sleep(500);   // let the last reports arrive

        StrategySlot slot = host.GetSlot("spray")!;
        slot.Thread!.RequestStop();
        Assert.True(slot.Thread.Join(TimeSpan.FromSeconds(5)));   // loop stopped: its OMS can be read safely

        Assert.Equal(1, exchange.InjectedDrops);
        Assert.True(host.Gateway!.Disconnects >= 1);
        Assert.True(exchange.Logons >= 2);
        Assert.True(exchange.ResendRequestsServed >= 1, "gateway never asked for the missed reports");
        Assert.True(exchange.StatusRequests >= 1, "gateway never reconciled open orders");
        Assert.Equal(0, exchange.DuplicateClOrdIds);

        OrderManager oms = slot.Engine!.Oms;
        Assert.Equal(0, oms.InvariantViolations);
        var omsOrders = oms.Snapshot().ToDictionary(o => o.OrderId);
        var exOrders = exchange.GetOrders().ToDictionary(o => o.ClOrdId);
        Assert.True(exOrders.Count >= 15);

        foreach (var e in exOrders.Values)
        {
            Assert.True(omsOrders.TryGetValue(e.ClOrdId, out OrderState o), $"exchange order {e.ClOrdId} unknown to the OMS");
            Assert.Equal(e.CumQty, o.CumQty);
            Assert.Equal(e.LeavesQty, o.LeavesQty);
            Assert.Equal(MapStatus(e.Status), o.Status);
            Assert.Equal(PendingAction.None, o.Pending);
        }
        foreach (var o in omsOrders.Values)
        {
            Assert.NotEqual(OrdStatus.PendingNew, o.Status);   // nothing left in limbo
            if (exOrders.ContainsKey(o.OrderId)) continue;
            // Never reached the exchange: must be explicitly rejected, never assumed filled.
            Assert.Equal(OrdStatus.Rejected, o.Status);
            Assert.Contains(o.RejectCode, new[] { RejectCodes.UnknownAtExchange, RejectCodes.ExchangeDown });
        }
        long exFilled = exOrders.Values.Sum(e => e.CumQty);
        Assert.Equal(exFilled, oms.GetPosition(slot.Instruments[0]).NetQty);
    }

    private static OrdStatus MapStatus(char c) => c switch
    {
        '0' => OrdStatus.Working,
        '1' => OrdStatus.PartiallyFilled,
        '2' => OrdStatus.Filled,
        '4' => OrdStatus.Cancelled,
        _ => OrdStatus.Rejected,
    };

    [Fact]
    public void Orders_while_disconnected_are_rejected_and_risk_fails_closed()
    {
        var options = TestSupport.HostOptions("gw-down");
        options.Mode = RunMode.Live;
        options.Gateway = new GatewayOptions { Port = 1, ReconnectDelayMs = 1000 };   // nothing listens there
        using var host = new StrategyHost(options);
        host.Start();
        var strategy = new OrderSprayStrategy(total: 5);
        host.Deploy(TestSupport.Manifest("down"), () => strategy);
        host.Start("down");
        var slot = host.GetSlot("down")!;
        Assert.True(TestSupport.WaitUntil(() => slot.Engine!.Metrics.OrdersRejected > 3, TimeSpan.FromSeconds(10)));
        Assert.Equal(0, slot.Engine!.Metrics.OrdersSent);
        Assert.False(slot.Engine.Risk.ExchangeUp);
    }
}

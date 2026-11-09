using AlgoPlatform.Sdk;

namespace AlgoPlatform.Strategies.Samples;

/// <summary>
/// Buys with an IOC limit order at the ask whenever the ask is at or below a threshold, one order at a
/// time, until it holds maxPosition. Parameters: threshold (price, default 1e9 = always), qty (100),
/// maxPosition (1000). Allocation-free.
/// </summary>
public sealed class ThresholdBuyer : StrategyBase
{
    private long _threshold;
    private long _qty;
    private long _maxPosition;
    private long _working;

    public long OrdersSubmitted { get; private set; }
    public long Fills { get; private set; }

    public override void OnStart(IStrategyContext ctx)
    {
        base.OnStart(ctx);
        _threshold = Price.FromDouble(ctx.GetParameter("threshold", 1e9));
        _qty = (long)ctx.GetParameter("qty", 100);
        _maxPosition = (long)ctx.GetParameter("maxPosition", 1000);
    }

    public override void OnTick(in Tick tick)
    {
        if (_working != 0 || tick.AskPx <= 0 || tick.AskPx > _threshold) return;
        if (Ctx.GetPosition(tick.Instrument).NetQty + _qty > _maxPosition) return;
        OrderHandle h = Ctx.Submit(new NewOrder(tick.Instrument, Side.Buy, OrdType.Limit, Tif.Ioc, tick.AskPx, _qty));
        if (!h.Accepted) return;
        _working = h.OrderId;
        OrdersSubmitted++;
    }

    public override void OnExecution(in ExecutionReport r)
    {
        if (r.OrderId != _working) return;
        if (r.LastQty > 0) Fills++;
        if (r.Status is OrdStatus.Filled or OrdStatus.Cancelled or OrdStatus.Rejected) _working = 0;
    }
}

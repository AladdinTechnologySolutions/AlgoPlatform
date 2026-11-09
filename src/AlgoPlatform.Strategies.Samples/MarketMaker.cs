using AlgoPlatform.Sdk;

namespace AlgoPlatform.Strategies.Samples;

/// <summary>
/// Quotes one bid and one ask around the mid of each instrument, skewed against its position, and
/// moves the quotes with Replace when the mid drifts. Exercises submit, replace, cancel and fills.
/// Parameters: spreadTicks (2), size (100), requoteTicks (1), skewTicksPerSize (1).
/// Hot path rules: no allocation, no LINQ, no locks; all state lives in pre-sized arrays.
/// </summary>
public sealed class MarketMaker : StrategyBase
{
    private const int MaxInstruments = 4096;

    private struct Quote
    {
        public long OrderId;
        public long Px;
        public bool Acked;   // exchange has acknowledged it, so it can be replaced
        public bool Busy;    // a replace is in flight
    }

    private readonly Quote[] _bids = new Quote[MaxInstruments];
    private readonly Quote[] _asks = new Quote[MaxInstruments];
    private readonly bool[] _mine = new bool[MaxInstruments];
    private long _spreadTicks;
    private long _size;
    private long _requoteTicks;
    private long _skewTicksPerSize;

    public long Quotes { get; private set; }
    public long Requotes { get; private set; }

    public override void OnStart(IStrategyContext ctx)
    {
        base.OnStart(ctx);
        _spreadTicks = (long)ctx.GetParameter("spreadTicks", 2);
        _size = (long)ctx.GetParameter("size", 100);
        _requoteTicks = (long)ctx.GetParameter("requoteTicks", 1);
        _skewTicksPerSize = (long)ctx.GetParameter("skewTicksPerSize", 1);
        for (int i = 0; i < ctx.Instruments.Count; i++) _mine[ctx.Instruments[i].Value] = true;
    }

    public override void OnTick(in Tick tick)
    {
        int i = tick.Instrument.Value;
        if (!_mine[i] || tick.BidPx <= 0 || tick.AskPx <= 0) return;
        if (!Ctx.Reference.TryGet(tick.Instrument, out InstrumentInfo info)) return;

        long tickSize = info.TickSize;
        long mid = (tick.BidPx + tick.AskPx) / 2;
        long net = Ctx.GetPosition(tick.Instrument).NetQty;
        long skew = -(net / _size) * _skewTicksPerSize * tickSize;   // lean against the position

        long bidPx = Floor(mid - _spreadTicks * tickSize + skew, tickSize);
        long askPx = Ceil(mid + _spreadTicks * tickSize + skew, tickSize);
        Work(ref _bids[i], tick.Instrument, Side.Buy, bidPx, tickSize);
        Work(ref _asks[i], tick.Instrument, Side.Sell, askPx, tickSize);
    }

    private void Work(ref Quote q, InstrumentId id, Side side, long px, long tickSize)
    {
        if (q.OrderId == 0)
        {
            OrderHandle h = Ctx.Submit(new NewOrder(id, side, OrdType.Limit, Tif.Day, px, _size));
            if (!h.Accepted) return;
            q = new Quote { OrderId = h.OrderId, Px = px };
            Quotes++;
            return;
        }
        if (!q.Acked || q.Busy) return;
        long diff = px > q.Px ? px - q.Px : q.Px - px;
        if (diff < _requoteTicks * tickSize) return;
        Ctx.Replace(q.OrderId, px, _size);
        q.Px = px;
        q.Busy = true;
        Requotes++;
    }

    public override void OnExecution(in ExecutionReport r)
    {
        int i = r.Instrument.Value;
        if ((uint)i >= MaxInstruments) return;
        ref Quote q = ref (r.Side == Side.Buy ? ref _bids[i] : ref _asks[i]);
        if (q.OrderId != r.OrderId) return;
        q.Busy = false;
        if (r.Status is OrdStatus.Filled or OrdStatus.Cancelled or OrdStatus.Rejected)
        {
            q = default;   // gone: a fresh quote goes out on the next tick
            return;
        }
        q.Acked = true;
    }

    public override void OnStop()
    {
        for (int i = 0; i < MaxInstruments; i++)
        {
            if (_bids[i].OrderId != 0) Ctx.Cancel(_bids[i].OrderId);
            if (_asks[i].OrderId != 0) Ctx.Cancel(_asks[i].OrderId);
        }
    }

    private static long Floor(long px, long tick) => px - ((px % tick) + tick) % tick;
    private static long Ceil(long px, long tick) => Floor(px + tick - 1, tick);
}

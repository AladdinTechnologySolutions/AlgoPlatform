using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Oms;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Risk;

/// <summary>
/// Sliding one-second window over the last N order timestamps (event time, so it replays identically).
/// </summary>
public sealed class RateLimiter
{
    private readonly long[] _times;
    private readonly long _windowNs;
    private int _next;
    private int _count;

    public RateLimiter(int maxPerWindow, long windowNs = 1_000_000_000)
    {
        _times = new long[Math.Max(1, maxPerWindow)];
        _windowNs = windowNs;
    }

    public bool WouldAllow(long nowNs) => _count < _times.Length || nowNs - _times[_next] >= _windowNs;

    public void Record(long nowNs)
    {
        _times[_next] = nowNs;
        _next = (_next + 1) % _times.Length;
        if (_count < _times.Length) _count++;
    }
}

/// <summary>
/// In-process pre-trade checks (FR-6), run inline in Submit/Replace on the strategy's own loop thread.
/// All inputs are strategy-owned state updated by journalled events (ticks, instrument status,
/// exchange status, kill), so the same journal always produces the same decisions.
/// Fail closed: missing reference data, no market data, stale data or exchange down all reject.
/// </summary>
public sealed class PreTradeRisk
{
    private readonly RiskLimits _limits;
    private readonly long _maxGross;
    private readonly RateLimiter _rate;
    private readonly long[] _lastMid;
    private readonly bool[] _stale;

    public PreTradeRisk(RiskLimits limits, int instrumentCount, int maxInstrumentId = 4095)
    {
        _limits = limits;
        _maxGross = limits.MaxGrossPosition > 0 ? limits.MaxGrossPosition : limits.MaxPosition * Math.Max(1, instrumentCount);
        _rate = new RateLimiter(limits.MaxOrdersPerSec);
        _lastMid = new long[maxInstrumentId + 1];
        _stale = new bool[maxInstrumentId + 1];
    }

    public bool Killed { get; private set; }
    public bool ExchangeUp { get; private set; }
    public RiskLimits Limits => _limits;

    public void Kill() => Killed = true;
    public void SetExchangeUp(bool up) => ExchangeUp = up;

    public void SetStale(InstrumentId id, bool stale)
    {
        if ((uint)id.Value < (uint)_stale.Length) _stale[id.Value] = stale;
    }

    public void OnTick(in Tick t)
    {
        int i = t.Instrument.Value;
        if ((uint)i >= (uint)_lastMid.Length) return;
        if (t.BidPx > 0 && t.AskPx > 0) _lastMid[i] = (t.BidPx + t.AskPx) / 2;
        else if (t.LastPx > 0) _lastMid[i] = t.LastPx;
    }

    public long LastMid(InstrumentId id) => _lastMid[id.Value];

    /// <summary>Returns <see cref="RejectCodes.None"/> or the first failing check.</summary>
    public int CheckNew(in NewOrder o, IReferenceData? refData, OrderManager oms, long nowNs)
    {
        if (Killed) return RejectCodes.KillSwitch;
        if (!ExchangeUp) return RejectCodes.ExchangeDown;
        if (o.Qty <= 0 || (o.Type == OrdType.Limit && o.Px <= 0)) return RejectCodes.InvalidOrder;

        int code = CheckInstrument(o.Instrument, o.Type, o.Px, o.Qty, refData, out long refPx);
        if (code != 0) return code;

        long notionalPx = o.Type == OrdType.Limit ? o.Px : refPx;
        // Qty is already bounded by MaxOrderQty, so px * qty cannot overflow a long for realistic prices.
        if (notionalPx * o.Qty / Price.Scale > _limits.MaxNotional) return RejectCodes.MaxNotional;

        // Position check counts working orders on the same side, so concurrent in-flight orders cannot breach it.
        Position p = oms.GetPosition(o.Instrument);
        if (o.Side == Side.Buy)
        {
            if (p.NetQty + oms.OpenBuyQty(o.Instrument) + o.Qty > _limits.MaxPosition) return RejectCodes.MaxPosition;
        }
        else if (p.NetQty - oms.OpenSellQty(o.Instrument) - o.Qty < -_limits.MaxPosition)
        {
            return RejectCodes.MaxPosition;
        }
        if (oms.GrossPosition + oms.TotalOpenQty + o.Qty > _maxGross) return RejectCodes.MaxStrategyPosition;

        if (!_rate.WouldAllow(nowNs)) return RejectCodes.RateLimit;
        return RejectCodes.None;
    }

    /// <summary>Checks for a cancel/replace to a new price and total quantity.</summary>
    public int CheckReplace(in OrderState o, long newPx, long newQty, IReferenceData? refData, OrderManager oms, long nowNs)
    {
        if (Killed) return RejectCodes.KillSwitch;
        if (!ExchangeUp) return RejectCodes.ExchangeDown;
        if (newQty <= o.CumQty || (o.Type == OrdType.Limit && newPx <= 0)) return RejectCodes.InvalidOrder;
        long addedLeaves = newQty - o.CumQty;

        int code = CheckInstrument(o.Instrument, o.Type, newPx, newQty, refData, out long refPx);
        if (code != 0) return code;

        long notionalPx = o.Type == OrdType.Limit ? newPx : refPx;
        if (notionalPx * newQty / Price.Scale > _limits.MaxNotional) return RejectCodes.MaxNotional;

        long extra = Math.Max(0, addedLeaves - o.LeavesQty);   // only an increase adds exposure
        Position p = oms.GetPosition(o.Instrument);
        if (o.Side == Side.Buy)
        {
            if (p.NetQty + oms.OpenBuyQty(o.Instrument) + extra > _limits.MaxPosition) return RejectCodes.MaxPosition;
        }
        else if (p.NetQty - oms.OpenSellQty(o.Instrument) - extra < -_limits.MaxPosition)
        {
            return RejectCodes.MaxPosition;
        }
        if (oms.GrossPosition + oms.TotalOpenQty + extra > _maxGross) return RejectCodes.MaxStrategyPosition;

        if (!_rate.WouldAllow(nowNs)) return RejectCodes.RateLimit;
        return RejectCodes.None;
    }

    /// <summary>Call after an order or replace actually goes out.</summary>
    public void RecordSent(long nowNs) => _rate.Record(nowNs);

    private int CheckInstrument(InstrumentId id, OrdType type, long px, long qty, IReferenceData? refData, out long refPx)
    {
        refPx = 0;
        if (refData is null || !refData.TryGet(id, out InstrumentInfo info)) return RejectCodes.MissingReferenceData;
        if (!info.Tradable) return RejectCodes.NotTradable;
        if (qty % info.LotSize != 0) return RejectCodes.InvalidOrder;
        if (type == OrdType.Limit && px % info.TickSize != 0) return RejectCodes.InvalidOrder;
        if (qty > _limits.MaxOrderQty) return RejectCodes.MaxOrderQty;

        int i = id.Value;
        refPx = (uint)i < (uint)_lastMid.Length ? _lastMid[i] : 0;
        if (refPx <= 0 || _stale[i]) return RejectCodes.StaleMarketData;

        if (type == OrdType.Limit)
        {
            if (px < info.MinPx || px > info.MaxPx) return RejectCodes.PriceBand;
            // Fat finger: limit price too far from the last mid.
            if (Math.Abs(px - refPx) * 10_000 > (long)_limits.FatFingerBps * refPx) return RejectCodes.FatFinger;
        }
        return RejectCodes.None;
    }
}

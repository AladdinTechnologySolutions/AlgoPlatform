using System.Runtime.CompilerServices;
using AlgoPlatform.Core.Collections;
using AlgoPlatform.Core.Events;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Oms;

public enum PendingAction : byte { None, Cancel, Replace }

/// <summary>One order as the OMS sees it. <see cref="Status"/> is the exchange-confirmed state;
/// <see cref="Pending"/> is a cancel/replace we asked for and the exchange has not answered yet.</summary>
public struct OrderState
{
    public long OrderId;
    public InstrumentId Instrument;
    public Side Side;
    public OrdType Type;
    public Tif Tif;
    public long Px;
    public long OrderQty;
    public long CumQty;
    public long LeavesQty;
    public OrdStatus Status;          // PendingNew, Working, PartiallyFilled, Filled, Cancelled, Rejected
    public PendingAction Pending;
    public long PendingPx;
    public long PendingQty;
    public int RejectCode;
    public long LastExecId;           // high-water mark for duplicate detection (exchange ExecIds increase per session)

    public readonly bool IsLive => Status is OrdStatus.PendingNew or OrdStatus.New or OrdStatus.Working or OrdStatus.PartiallyFilled;

    /// <summary>Status as exposed to strategies: a pending cancel/replace shows as PendingCancel/PendingReplace.</summary>
    public readonly OrdStatus ExternalStatus => !IsLive ? Status : Pending switch
    {
        PendingAction.Cancel => OrdStatus.PendingCancel,
        PendingAction.Replace => OrdStatus.PendingReplace,
        _ => Status,
    };
}

public struct PositionState
{
    public long NetQty;
    public long AvgPx;
    public long RealisedPnl;   // in (fixed-point price * qty) units
}

public enum ApplyResult : byte { Applied, Duplicate, UnknownOrder }

/// <summary>
/// Per-strategy order management (FR-5): order ids, the order state machine, idempotent fill handling
/// (dedupe by ExecId, fills accounted by CumQty delta) and the authoritative position. ExecIds are
/// deduplicated per order with a high-water mark rather than a global set, so memory stays bounded and
/// the hot path never allocates (see DESIGN_DECISIONS D-08). Owned by one
/// strategy loop thread; no locks. Also tracks open (working) quantity per instrument and side so
/// pre-trade risk can count in-flight orders against position limits.
/// </summary>
public sealed class OrderManager
{
    private readonly int _strategyId;
    private readonly LongMap<OrderState> _orders;
    private readonly PositionState[] _positions;
    private readonly long[] _openBuy;
    private readonly long[] _openSell;
    private long _nextCounter;
    private long _grossPosition;
    private long _totalOpen;

    public OrderManager(int strategyId, int maxInstrumentId = 4095, int expectedLiveOrders = 4096)
    {
        _strategyId = strategyId;
        _orders = new LongMap<OrderState>(expectedLiveOrders);
        _positions = new PositionState[maxInstrumentId + 1];
        _openBuy = new long[maxInstrumentId + 1];
        _openSell = new long[maxInstrumentId + 1];
    }

    /// <summary>Keep filled/cancelled/rejected orders in memory (tests, audits). Default false: terminal orders are removed
    /// so a long-running strategy does not grow its order table.</summary>
    public bool RetainTerminalOrders { get; init; }

    public int StrategyId => _strategyId;
    public int OrderCount => _orders.Count;
    public int LiveOrderCount { get; private set; }
    public long GrossPosition => _grossPosition;
    public long TotalOpenQty => _totalOpen;
    public long DuplicateReports { get; private set; }
    public long UnknownReports { get; private set; }
    public long InvariantViolations { get; private set; }

    public long NextOrderId() => OrderIds.Make(_strategyId, ++_nextCounter);

    public void Create(long orderId, in NewOrder o)
    {
        var state = new OrderState
        {
            OrderId = orderId, Instrument = o.Instrument, Side = o.Side, Type = o.Type, Tif = o.Tif, Px = o.Px,
            OrderQty = o.Qty, LeavesQty = o.Qty, Status = OrdStatus.PendingNew,
        };
        _orders.TryAdd(orderId, state);
        LiveOrderCount++;
        AdjustOpen(state.Instrument.Value, state.Side, Exposure(in state));
    }

    public bool TryGet(long orderId, out OrderState state) => _orders.TryGetValue(orderId, out state);

    /// <summary>Marks a cancel as requested. False if the order is unknown, not live, or already being cancelled.</summary>
    public bool RequestCancel(long orderId)
    {
        ref OrderState o = ref _orders.GetValueRefOrNullRef(orderId);
        if (Unsafe.IsNullRef(ref o) || !o.IsLive || o.Pending == PendingAction.Cancel) return false;
        // Supersedes a pending replace. PendingQty is kept, so exposure stays conservative until the exchange answers.
        o.Pending = PendingAction.Cancel;
        return true;
    }

    /// <summary>True if the order is acknowledged, live, has no request outstanding, and newQty is above CumQty.</summary>
    public bool CanReplace(long orderId, long newQty, out OrderState state)
    {
        if (!_orders.TryGetValue(orderId, out state)) return false;
        return state.IsLive && state.Status != OrdStatus.PendingNew && state.Pending == PendingAction.None && newQty > state.CumQty;
    }

    public void RequestReplace(long orderId, long newPx, long newQty)
    {
        ref OrderState o = ref _orders.GetValueRefOrNullRef(orderId);
        if (Unsafe.IsNullRef(ref o)) return;
        long before = Exposure(in o);
        o.Pending = PendingAction.Replace;
        o.PendingPx = newPx;
        o.PendingQty = newQty;
        AdjustOpen(o.Instrument.Value, o.Side, Exposure(in o) - before);
    }

    /// <summary>Applies an execution report. Duplicates (an ExecId already seen for the order) never change state.</summary>
    public ApplyResult Apply(in ExecutionReport r)
    {
        ref OrderState o = ref _orders.GetValueRefOrNullRef(r.OrderId);
        if (Unsafe.IsNullRef(ref o))
        {
            UnknownReports++;   // includes duplicates for orders already finished and removed
            return ApplyResult.UnknownOrder;
        }
        if (r.ExecId != 0)
        {
            if (r.ExecId <= o.LastExecId)
            {
                DuplicateReports++;
                return ApplyResult.Duplicate;
            }
            o.LastExecId = r.ExecId;
        }
        if (!o.IsLive)
        {
            // A late report for a finished order (e.g. a status reply after a fill). Nothing can change.
            return ApplyResult.Duplicate;
        }

        long exposureBefore = Exposure(in o);

        if (r.RejectCode != 0 && r.Status != OrdStatus.Rejected)
        {
            // Cancel/replace reject: the order itself is unchanged.
            o.Pending = PendingAction.None;
            o.PendingQty = 0;
            o.RejectCode = r.RejectCode;
        }
        else
        {
            // Fills are accounted by CumQty delta, so a re-sent report without an ExecId cannot double count.
            if (r.CumQty > o.CumQty)
            {
                long fillQty = r.CumQty - o.CumQty;
                long fillPx = r.LastQty > 0 ? r.LastPx : o.Px;
                ApplyFill(o.Instrument.Value, o.Side, fillQty, fillPx);
                o.CumQty = r.CumQty;
            }

            switch (r.Status)
            {
                case OrdStatus.Rejected:
                    o.Status = OrdStatus.Rejected;
                    o.RejectCode = r.RejectCode;
                    o.LeavesQty = 0;
                    o.Pending = PendingAction.None;
                    break;
                case OrdStatus.Cancelled:
                    o.Status = OrdStatus.Cancelled;
                    o.LeavesQty = 0;
                    o.Pending = PendingAction.None;
                    break;
                case OrdStatus.Filled:
                    o.Status = OrdStatus.Filled;
                    o.LeavesQty = 0;
                    o.OrderQty = o.CumQty;
                    o.Pending = PendingAction.None;
                    break;
                default:   // New / Working / PartiallyFilled / PendingNew from the exchange
                    if (o.PendingQty > 0 && r.LastQty == 0)
                    {
                        // A non-fill, non-terminal report while a replace is outstanding is the replace ack.
                        o.Px = o.PendingPx;
                        o.PendingQty = 0;
                        if (o.Pending == PendingAction.Replace) o.Pending = PendingAction.None;
                    }
                    o.LeavesQty = Math.Max(0, r.LeavesQty);
                    o.OrderQty = o.CumQty + o.LeavesQty;
                    o.Status = o.CumQty > 0 ? OrdStatus.PartiallyFilled : OrdStatus.Working;
                    if (o.LeavesQty == 0)
                    {
                        o.Status = OrdStatus.Filled;
                        o.Pending = PendingAction.None;
                    }
                    break;
            }
        }

        if (o.IsLive && o.CumQty + o.LeavesQty != o.OrderQty) InvariantViolations++;
        AdjustOpen(o.Instrument.Value, o.Side, Exposure(in o) - exposureBefore);

        if (!o.IsLive)
        {
            LiveOrderCount--;
            if (!RetainTerminalOrders) _orders.Remove(r.OrderId);
        }
        return ApplyResult.Applied;
    }

    public Position GetPosition(InstrumentId id)
    {
        if ((uint)id.Value >= (uint)_positions.Length) return default;
        ref PositionState p = ref _positions[id.Value];
        return new Position(p.NetQty, p.AvgPx, p.RealisedPnl);
    }

    public long OpenBuyQty(InstrumentId id) => _openBuy[id.Value];
    public long OpenSellQty(InstrumentId id) => _openSell[id.Value];

    /// <summary>Copies the ids of live orders into <paramref name="buffer"/>; returns how many. Allocation-free.</summary>
    public int CollectLiveOrders(Span<long> buffer)
    {
        int n = 0;
        for (int i = 0; i < _orders.SlotCount && n < buffer.Length; i++)
        {
            if (!_orders.IsUsed(i)) continue;
            ref OrderState o = ref _orders.ValueAt(i);
            if (o.IsLive) buffer[n++] = o.OrderId;
        }
        return n;
    }

    /// <summary>Visits every order (live and, if retained, terminal). Control path / tests.</summary>
    public List<OrderState> Snapshot()
    {
        var list = new List<OrderState>(_orders.Count);
        for (int i = 0; i < _orders.SlotCount; i++)
            if (_orders.IsUsed(i)) list.Add(_orders.ValueAt(i));
        return list;
    }

    /// <summary>Working quantity that could still turn into position: leaves, or the replace quantity if larger.</summary>
    private static long Exposure(in OrderState o)
    {
        if (!o.IsLive) return 0;
        long pendingLeaves = o.PendingQty > 0 ? o.PendingQty - o.CumQty : 0;
        return Math.Max(o.LeavesQty, pendingLeaves);
    }

    private void AdjustOpen(int inst, Side side, long delta)
    {
        if (delta == 0) return;
        if (side == Side.Buy) _openBuy[inst] += delta;
        else _openSell[inst] += delta;
        _totalOpen += delta;
    }

    private void ApplyFill(int inst, Side side, long qty, long px)
    {
        ref PositionState p = ref _positions[inst];
        long signed = side == Side.Buy ? qty : -qty;
        long oldAbs = Math.Abs(p.NetQty);
        if (p.NetQty == 0 || Math.Sign(p.NetQty) == Math.Sign(signed))
        {
            p.AvgPx = (p.AvgPx * oldAbs + px * qty) / (oldAbs + qty);
            p.NetQty += signed;
        }
        else
        {
            long closing = Math.Min(oldAbs, qty);
            p.RealisedPnl += (px - p.AvgPx) * closing * Math.Sign(p.NetQty);
            p.NetQty += signed;
            if (p.NetQty == 0) p.AvgPx = 0;
            else if (qty > closing) p.AvgPx = px;   // flipped: the remainder opened at this price
        }
        _grossPosition += Math.Abs(p.NetQty) - oldAbs;
    }
}

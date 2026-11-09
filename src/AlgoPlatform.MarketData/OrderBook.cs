using AlgoPlatform.Sdk;

namespace AlgoPlatform.MarketData;

/// <summary>
/// Price-level book for one instrument with fixed maximum depth. Levels are kept sorted in plain
/// arrays (best first), so updates are a short scan plus an Array.Copy shift: no allocation.
/// </summary>
public sealed class OrderBook
{
    public const int MaxDepth = 32;

    private readonly long[] _bidPx = new long[MaxDepth];
    private readonly long[] _bidQty = new long[MaxDepth];
    private readonly long[] _askPx = new long[MaxDepth];
    private readonly long[] _askQty = new long[MaxDepth];
    private int _bidCount;
    private int _askCount;

    public OrderBook(InstrumentId instrument) => Instrument = instrument;

    public InstrumentId Instrument { get; }
    public int BidDepth => _bidCount;
    public int AskDepth => _askCount;
    public long BestBidPx => _bidCount > 0 ? _bidPx[0] : 0;
    public long BestBidQty => _bidCount > 0 ? _bidQty[0] : 0;
    public long BestAskPx => _askCount > 0 ? _askPx[0] : 0;
    public long BestAskQty => _askCount > 0 ? _askQty[0] : 0;
    public long LastTradePx { get; private set; }

    public (long Px, long Qty) BidLevel(int i) => (_bidPx[i], _bidQty[i]);
    public (long Px, long Qty) AskLevel(int i) => (_askPx[i], _askQty[i]);

    /// <summary>Sets the quantity at a price level; qty 0 deletes the level.</summary>
    public void Apply(Side side, long px, long qty)
    {
        if (side == Side.Buy) Apply(_bidPx, _bidQty, ref _bidCount, px, qty, descending: true);
        else Apply(_askPx, _askQty, ref _askCount, px, qty, descending: false);
    }

    public void OnTrade(long px) => LastTradePx = px;

    public void Clear()
    {
        _bidCount = 0;
        _askCount = 0;
    }

    private static void Apply(long[] pxs, long[] qtys, ref int count, long px, long qty, bool descending)
    {
        int i = 0;
        while (i < count && (descending ? pxs[i] > px : pxs[i] < px)) i++;
        bool exists = i < count && pxs[i] == px;
        if (qty <= 0)
        {
            if (!exists) return;
            Array.Copy(pxs, i + 1, pxs, i, count - i - 1);
            Array.Copy(qtys, i + 1, qtys, i, count - i - 1);
            count--;
            return;
        }
        if (exists)
        {
            qtys[i] = qty;
            return;
        }
        if (i >= MaxDepth) return;   // worse than everything we keep
        int move = Math.Min(count, MaxDepth - 1) - i;
        if (move > 0)
        {
            Array.Copy(pxs, i, pxs, i + 1, move);
            Array.Copy(qtys, i, qtys, i + 1, move);
        }
        pxs[i] = px;
        qtys[i] = qty;
        if (count < MaxDepth) count++;
    }
}

using AlgoPlatform.Core.Timing;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.MarketData;

public enum FeedMessageType : byte { BookUpdate = 1, Trade = 2 }

/// <summary>Raw feed message before normalisation. FeedSeq is per instrument (like a per-channel sequence).</summary>
public struct FeedMessage
{
    public FeedMessageType Type;
    public InstrumentId Instrument;
    public long FeedSeq;
    public Side Side;          // BookUpdate: book side. Trade: aggressor side.
    public long Px;
    public long Qty;           // BookUpdate: new level quantity (0 deletes). Trade: traded quantity.
    public long IntendedNs;    // when the message was scheduled to be sent (0 if unpaced); used to avoid coordinated omission
}

public sealed class FeedSnapshot
{
    public required InstrumentId Instrument { get; init; }
    public required long FeedSeq { get; init; }
    public required (long Px, long Qty)[] Bids { get; init; }
    public required (long Px, long Qty)[] Asks { get; init; }
    public long LastTradePx { get; init; }
}

/// <summary>Source of raw market data. v1 has only the simulator; a real feed is a later adapter.</summary>
public interface IFeedSource
{
    /// <summary>Non-blocking. Returns false when no message is available right now.</summary>
    bool TryRead(out FeedMessage message);

    /// <summary>Full book for recovery after a sequence gap.</summary>
    FeedSnapshot Snapshot(InstrumentId instrument);
}

public sealed class SimulatedFeedOptions
{
    public ulong Seed { get; set; } = 42;
    public List<(InstrumentId Id, long InitialMid, long TickSize)> Instruments { get; set; } = new();
    public int Levels { get; set; } = 5;
    public double MoveProbability { get; set; } = 0.2;
    public double TradeProbability { get; set; } = 0.1;
    /// <summary>0 = as fast as the consumer reads. Otherwise messages are released on a fixed schedule.</summary>
    public double MessagesPerSecond { get; set; }
    public long MaxMessages { get; set; } = long.MaxValue;
    public IClock Clock { get; set; } = MonotonicClock.Instance;
}

/// <summary>
/// Deterministic synthetic L2 feed: for a given seed and instrument list it always produces the same
/// message sequence. Each instrument has a mid that random-walks by one tick and a fixed number of
/// levels per side at mid +/- k ticks. Gaps can be injected to exercise recovery.
/// </summary>
public sealed class SimulatedFeed : IFeedSource
{
    private sealed class InstrumentState
    {
        public InstrumentId Id;
        public long MidTicks;
        public long TickSize;
        public long Seq;
        public long[] BidQty = [];
        public long[] AskQty = [];
        public long LastTradePx;
        public volatile bool DropNext;
    }

    private readonly SimulatedFeedOptions _options;
    private readonly InstrumentState[] _instruments;
    private readonly FeedMessage[] _batch = new FeedMessage[8];
    private int _batchCount;
    private int _batchPos;
    private ulong _rng;
    private long _produced;
    private long _startNs;
    private readonly long _intervalNs;

    public SimulatedFeed(SimulatedFeedOptions options)
    {
        _options = options;
        _rng = options.Seed == 0 ? 0x9E3779B97F4A7C15UL : options.Seed;
        _intervalNs = options.MessagesPerSecond > 0 ? (long)(1e9 / options.MessagesPerSecond) : 0;
        _instruments = new InstrumentState[options.Instruments.Count];
        for (int i = 0; i < _instruments.Length; i++)
        {
            var (id, mid, tick) = options.Instruments[i];
            var s = new InstrumentState
            {
                Id = id, MidTicks = mid / tick, TickSize = tick,
                BidQty = new long[options.Levels], AskQty = new long[options.Levels],
            };
            for (int l = 0; l < options.Levels; l++)
            {
                s.BidQty[l] = RandomQty();
                s.AskQty[l] = RandomQty();
            }
            _instruments[i] = s;
        }
    }

    public long Produced => _produced;

    /// <summary>The next message for this instrument is consumed but never delivered (a sequence gap).</summary>
    public void InjectGap(InstrumentId id) => Find(id).DropNext = true;

    /// <summary>Single reader (the feed handler thread). Snapshot is called from the same thread.</summary>
    public bool TryRead(out FeedMessage message)
    {
        while (true)
        {
            if (_batchPos >= _batchCount)
            {
                if (_produced >= _options.MaxMessages || _instruments.Length == 0) { message = default; return false; }
                long intended = 0;
                if (_intervalNs > 0)
                {
                    long now = _options.Clock.NowNs;
                    if (_startNs == 0) _startNs = now;
                    intended = _startNs + _produced * _intervalNs;
                    if (now < intended) { message = default; return false; }
                }
                GenerateBatch(intended);
            }
            message = _batch[_batchPos++];
            _produced++;
            var s = Find(message.Instrument);
            if (s.DropNext)
            {
                s.DropNext = false;
                continue;   // swallowed: the consumer will see a gap
            }
            return true;
        }
    }

    public FeedSnapshot Snapshot(InstrumentId id)
    {
        var s = Find(id);
        int levels = _options.Levels;
        var bids = new (long, long)[levels];
        var asks = new (long, long)[levels];
        for (int l = 0; l < levels; l++)
        {
            bids[l] = ((s.MidTicks - l - 1) * s.TickSize, s.BidQty[l]);
            asks[l] = ((s.MidTicks + l + 1) * s.TickSize, s.AskQty[l]);
        }
        return new FeedSnapshot { Instrument = id, FeedSeq = s.Seq, Bids = bids, Asks = asks, LastTradePx = s.LastTradePx };
    }

    private InstrumentState Find(InstrumentId id)
    {
        foreach (var s in _instruments) if (s.Id == id) return s;
        throw new ArgumentException($"Unknown instrument {id.Value}");
    }

    private void GenerateBatch(long intended)
    {
        _batchCount = 0;
        _batchPos = 0;
        var s = _instruments[(int)(Next() % (ulong)_instruments.Length)];
        int levels = _options.Levels;
        double r = NextDouble();
        if (r < _options.MoveProbability && s.MidTicks > levels + 2)
        {
            bool up = (Next() & 1) == 0;
            if (up)
            {
                // New best bid at the old mid, worst bid falls off; best ask (old mid + 1) is consumed, new worst ask appears.
                Emit(s, FeedMessageType.BookUpdate, Side.Buy, (s.MidTicks - levels) * s.TickSize, 0, intended);
                Emit(s, FeedMessageType.BookUpdate, Side.Sell, (s.MidTicks + 1) * s.TickSize, 0, intended);
                s.MidTicks++;
                Shift(s.BidQty, insertFront: true);
                Shift(s.AskQty, insertFront: false);
                Emit(s, FeedMessageType.BookUpdate, Side.Buy, (s.MidTicks - 1) * s.TickSize, s.BidQty[0], intended);
                Emit(s, FeedMessageType.BookUpdate, Side.Sell, (s.MidTicks + levels) * s.TickSize, s.AskQty[levels - 1], intended);
            }
            else
            {
                Emit(s, FeedMessageType.BookUpdate, Side.Sell, (s.MidTicks + levels) * s.TickSize, 0, intended);
                Emit(s, FeedMessageType.BookUpdate, Side.Buy, (s.MidTicks - 1) * s.TickSize, 0, intended);
                s.MidTicks--;
                Shift(s.AskQty, insertFront: true);
                Shift(s.BidQty, insertFront: false);
                Emit(s, FeedMessageType.BookUpdate, Side.Sell, (s.MidTicks + 1) * s.TickSize, s.AskQty[0], intended);
                Emit(s, FeedMessageType.BookUpdate, Side.Buy, (s.MidTicks - levels) * s.TickSize, s.BidQty[levels - 1], intended);
            }
        }
        else if (r < _options.MoveProbability + _options.TradeProbability)
        {
            bool buyAggressor = (Next() & 1) == 0;
            long[] qtys = buyAggressor ? s.AskQty : s.BidQty;
            long px = buyAggressor ? (s.MidTicks + 1) * s.TickSize : (s.MidTicks - 1) * s.TickSize;
            long qty = Math.Min(qtys[0], 1 + (long)(Next() % 300));
            s.LastTradePx = px;
            Emit(s, FeedMessageType.Trade, buyAggressor ? Side.Buy : Side.Sell, px, qty, intended);
            qtys[0] -= qty;
            if (qtys[0] <= 0) qtys[0] = RandomQty();   // replenished at the same price
            Emit(s, FeedMessageType.BookUpdate, buyAggressor ? Side.Sell : Side.Buy, px, qtys[0], intended);
        }
        else
        {
            bool bid = (Next() & 1) == 0;
            int level = (int)(Next() % (ulong)levels);
            long qty = RandomQty();
            if (bid) s.BidQty[level] = qty; else s.AskQty[level] = qty;
            long px = bid ? (s.MidTicks - level - 1) * s.TickSize : (s.MidTicks + level + 1) * s.TickSize;
            Emit(s, FeedMessageType.BookUpdate, bid ? Side.Buy : Side.Sell, px, qty, intended);
        }
    }

    private void Shift(long[] qtys, bool insertFront)
    {
        if (insertFront)
        {
            Array.Copy(qtys, 0, qtys, 1, qtys.Length - 1);
            qtys[0] = RandomQty();
        }
        else
        {
            Array.Copy(qtys, 1, qtys, 0, qtys.Length - 1);
            qtys[^1] = RandomQty();
        }
    }

    private void Emit(InstrumentState s, FeedMessageType type, Side side, long px, long qty, long intended)
    {
        _batch[_batchCount++] = new FeedMessage
        {
            Type = type, Instrument = s.Id, FeedSeq = ++s.Seq, Side = side, Px = px, Qty = qty, IntendedNs = intended,
        };
    }

    private long RandomQty() => 100 + (long)(Next() % 901);

    private ulong Next()
    {
        // xorshift64*: explicit so the sequence never depends on the runtime's Random implementation.
        _rng ^= _rng >> 12;
        _rng ^= _rng << 25;
        _rng ^= _rng >> 27;
        return _rng * 0x2545F4914F6CDD1DUL;
    }

    private double NextDouble() => (Next() >> 11) * (1.0 / (1UL << 53));
}

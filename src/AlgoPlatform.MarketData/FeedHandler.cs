using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Threading;
using AlgoPlatform.Core.Timing;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.MarketData;

/// <summary>
/// One per host (spec 6.7). Reads the raw feed, checks per-instrument sequence numbers, maintains one
/// order book per instrument, and publishes a normalised <see cref="Tick"/> to every subscribed inbox
/// whenever the top of book changes or a trade prints. Decode once, update the book once, fan out N times.
/// Runs on its own thread via <see cref="EventLoopThread"/>; nothing here allocates in steady state.
/// </summary>
public sealed class FeedHandler : ILoopWork
{
    private readonly IFeedSource _source;
    private readonly Sequencer _sequencer;
    private readonly IClock _clock;
    private readonly OrderBook?[] _books;
    private readonly long[] _expectedSeq;
    private readonly bool[] _initialised;
    private readonly object _subGate = new();
    private MarketDataInbox[][] _subsByInstrument;
    private MarketDataInbox[] _allSubs = [];

    private long _messages;
    private long _ticks;
    private long _gaps;

    public FeedHandler(IFeedSource source, Sequencer sequencer, IClock clock, int maxInstrumentId = 4095)
    {
        _source = source;
        _sequencer = sequencer;
        _clock = clock;
        _books = new OrderBook?[maxInstrumentId + 1];
        _expectedSeq = new long[maxInstrumentId + 1];
        _initialised = new bool[maxInstrumentId + 1];
        _subsByInstrument = new MarketDataInbox[maxInstrumentId + 1][];
        for (int i = 0; i < _subsByInstrument.Length; i++) _subsByInstrument[i] = [];
    }

    public long Messages => Volatile.Read(ref _messages);
    public long TicksPublished => Volatile.Read(ref _ticks);
    public long Gaps => Volatile.Read(ref _gaps);

    /// <summary>Pre-creates the book so the first message does not allocate.</summary>
    public void AddInstrument(InstrumentId id) => _books[id.Value] ??= new OrderBook(id);

    public OrderBook? Book(InstrumentId id) => _books[id.Value];

    /// <summary>Control path. Copy-on-write so the feed thread reads subscriber arrays without locks.</summary>
    public void Subscribe(MarketDataInbox inbox, IEnumerable<InstrumentId> instruments)
    {
        lock (_subGate)
        {
            var next = (MarketDataInbox[][])_subsByInstrument.Clone();
            foreach (var id in instruments)
            {
                AddInstrument(id);
                if (Array.IndexOf(next[id.Value], inbox) < 0) next[id.Value] = [.. next[id.Value], inbox];
            }
            Volatile.Write(ref _subsByInstrument, next);
            if (Array.IndexOf(_allSubs, inbox) < 0) Volatile.Write(ref _allSubs, [.. _allSubs, inbox]);
        }
    }

    public void Unsubscribe(MarketDataInbox inbox)
    {
        lock (_subGate)
        {
            var next = (MarketDataInbox[][])_subsByInstrument.Clone();
            for (int i = 0; i < next.Length; i++)
                if (Array.IndexOf(next[i], inbox) >= 0) next[i] = Array.FindAll(next[i], s => s != inbox);
            Volatile.Write(ref _subsByInstrument, next);
            Volatile.Write(ref _allSubs, Array.FindAll(_allSubs, s => s != inbox));
        }
    }

    public int PollOnce()
    {
        int work = 0;
        MarketDataInbox[] all = Volatile.Read(ref _allSubs);
        for (int i = 0; i < all.Length; i++)
            if (all[i].HasBacklog) work += all[i].Flush();

        for (int n = 0; n < 256 && _source.TryRead(out FeedMessage msg); n++)
        {
            Process(in msg);
            work++;
        }
        return work;
    }

    private void Process(in FeedMessage msg)
    {
        Volatile.Write(ref _messages, _messages + 1);
        int id = msg.Instrument.Value;
        if ((uint)id >= (uint)_books.Length) return;
        OrderBook book = _books[id] ??= new OrderBook(msg.Instrument);

        if (!_initialised[id] || msg.FeedSeq != _expectedSeq[id])
        {
            if (_initialised[id] && msg.FeedSeq < _expectedSeq[id]) return;   // duplicate or old
            Recover(book, msg.IntendedNs);
            if (msg.FeedSeq < _expectedSeq[id]) return;                          // already in the snapshot
            if (msg.FeedSeq != _expectedSeq[id]) return;                         // still inconsistent; next message retries
        }
        _expectedSeq[id] = msg.FeedSeq + 1;

        long bid = book.BestBidPx, bidQty = book.BestBidQty, ask = book.BestAskPx, askQty = book.BestAskQty;
        if (msg.Type == FeedMessageType.Trade)
        {
            book.OnTrade(msg.Px);
            PublishTick(book, msg.Qty, msg.IntendedNs);
            return;
        }
        book.Apply(msg.Side, msg.Px, msg.Qty);
        if (book.BestBidPx != bid || book.BestBidQty != bidQty || book.BestAskPx != ask || book.BestAskQty != askQty)
            PublishTick(book, 0, msg.IntendedNs);
    }

    /// <summary>Gap recovery (spec section 10): mark stale, rebuild from a snapshot, mark recovered.</summary>
    private void Recover(OrderBook book, long intendedNs)
    {
        int id = book.Instrument.Value;
        bool wasLive = _initialised[id];
        if (wasLive)
        {
            Volatile.Write(ref _gaps, _gaps + 1);
            PublishStatus(book.Instrument, stale: true);
        }
        FeedSnapshot snap = _source.Snapshot(book.Instrument);
        book.Clear();
        foreach (var (px, qty) in snap.Bids) book.Apply(Side.Buy, px, qty);
        foreach (var (px, qty) in snap.Asks) book.Apply(Side.Sell, px, qty);
        if (snap.LastTradePx != 0) book.OnTrade(snap.LastTradePx);
        _expectedSeq[id] = snap.FeedSeq + 1;
        _initialised[id] = true;
        if (wasLive) PublishStatus(book.Instrument, stale: false);
        PublishTick(book, 0, intendedNs);
    }

    private void PublishTick(OrderBook book, long tradeQty, long intendedNs)
    {
        long seq = _sequencer.Next();
        long now = _clock.NowNs;
        var evt = new HostEvent
        {
            Type = EventType.Tick,
            Seq = seq,
            TimestampNs = now,
            PublishNs = intendedNs != 0 ? intendedNs : now,
            Tick = new Tick(seq, now, book.Instrument, book.BestBidPx, book.BestBidQty, book.BestAskPx, book.BestAskQty,
                tradeQty > 0 ? book.LastTradePx : 0, tradeQty),
        };
        MarketDataInbox[] subs = Volatile.Read(ref _subsByInstrument)[book.Instrument.Value];
        for (int i = 0; i < subs.Length; i++) subs[i].Publish(in evt);
        Volatile.Write(ref _ticks, _ticks + 1);
    }

    private void PublishStatus(InstrumentId id, bool stale)
    {
        long seq = _sequencer.Next();
        var evt = new HostEvent
        {
            Type = EventType.InstrumentStatus, Seq = seq, TimestampNs = _clock.NowNs, A = id.Value, B = stale ? 1 : 0,
        };
        MarketDataInbox[] subs = Volatile.Read(ref _subsByInstrument)[id.Value];
        for (int i = 0; i < subs.Length; i++) subs[i].Publish(in evt);
    }
}

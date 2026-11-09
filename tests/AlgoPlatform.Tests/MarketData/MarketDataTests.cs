using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Timing;
using AlgoPlatform.MarketData;
using AlgoPlatform.ReferenceData;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Tests.MarketData;

public class ReferenceDataTests
{
    [Fact]
    public void Snapshot_lookups_and_fixed_point_conversion()
    {
        var s = ReferenceDataSnapshot.Default();
        Assert.True(s.TryGet(new InstrumentId(1), out var vod));
        Assert.Equal("VOD.L", vod.Symbol);
        Assert.Equal(100, vod.TickSize);            // 0.01 * 10^4
        Assert.Equal(Price.FromDouble(150), vod.MaxPx);
        Assert.False(s.TryGet(new InstrumentId(999), out _));
        Assert.False(s.TryGet(new InstrumentId(-1), out _));
        Assert.True(s.TryGetBySymbol("MSFT", out var msft));
        Assert.Equal(4, msft.Id.Value);
    }

    [Fact]
    public void Publish_swaps_atomically_and_forces_a_higher_version()
    {
        var svc = new ReferenceDataService(ReferenceDataSnapshot.Default());
        ReferenceDataSnapshot? seen = null;
        svc.Changed += s => seen = s;
        var next = svc.Publish(ReferenceDataSnapshot.Default());   // same version number as current
        Assert.Equal(2, next.Version);
        Assert.Same(next, svc.Current);
        Assert.Same(next, seen);
    }

    [Fact]
    public void Invalid_documents_are_refused()
    {
        Assert.Throws<InvalidDataException>(() => ReferenceDataSnapshot.FromJson("""{"version":1,"instruments":[{"id":0,"symbol":"X"}]}"""));
        Assert.Throws<InvalidDataException>(() => ReferenceDataSnapshot.FromJson(
            """{"version":1,"instruments":[{"id":1,"symbol":"X"},{"id":1,"symbol":"Y"}]}"""));
    }
}

public class OrderBookTests
{
    [Fact]
    public void Levels_stay_sorted_and_deletes_work()
    {
        var b = new OrderBook(new InstrumentId(1));
        b.Apply(Side.Buy, 100, 5);
        b.Apply(Side.Buy, 102, 7);
        b.Apply(Side.Buy, 101, 6);
        b.Apply(Side.Sell, 105, 1);
        b.Apply(Side.Sell, 103, 2);
        Assert.Equal(102, b.BestBidPx);
        Assert.Equal(7, b.BestBidQty);
        Assert.Equal(103, b.BestAskPx);
        Assert.Equal((101, 6), b.BidLevel(1));
        b.Apply(Side.Buy, 102, 0);
        Assert.Equal(101, b.BestBidPx);
        b.Apply(Side.Buy, 101, 9);
        Assert.Equal(9, b.BestBidQty);
        Assert.Equal(2, b.BidDepth);
    }

    [Fact]
    public void Depth_is_capped()
    {
        var b = new OrderBook(new InstrumentId(1));
        for (int i = 0; i < OrderBook.MaxDepth + 10; i++) b.Apply(Side.Sell, 1000 + i, 1);
        Assert.Equal(OrderBook.MaxDepth, b.AskDepth);
        b.Apply(Side.Sell, 1, 1);   // better than everything: inserted, worst falls off
        Assert.Equal(1, b.BestAskPx);
        Assert.Equal(OrderBook.MaxDepth, b.AskDepth);
    }
}

public class FeedTests
{
    private static SimulatedFeedOptions Options(ulong seed) => new()
    {
        Seed = seed,
        Instruments = [(new InstrumentId(1), Price.FromDouble(100), 100), (new InstrumentId(2), Price.FromDouble(200), 100)],
        MaxMessages = 20_000,   // the generator finishes its last batch, so its state is comparable with the handler's book
    };

    [Fact]
    public void Simulated_feed_is_deterministic_for_a_seed()
    {
        var a = new SimulatedFeed(Options(7));
        var b = new SimulatedFeed(Options(7));
        var c = new SimulatedFeed(Options(8));
        bool anyDifferent = false;
        for (int i = 0; i < 10_000; i++)
        {
            Assert.True(a.TryRead(out var ma));
            Assert.True(b.TryRead(out var mb));
            Assert.True(c.TryRead(out var mc));
            Assert.Equal(ma, mb);
            anyDifferent |= !ma.Equals(mc);
        }
        Assert.True(anyDifferent);
    }

    [Fact]
    public void Feed_handler_publishes_ticks_matching_the_generator_book()
    {
        var feed = new SimulatedFeed(Options(3));
        var handler = new FeedHandler(feed, new Sequencer(), new VirtualClock());
        var inbox = new MarketDataInbox(1 << 16);
        handler.Subscribe(inbox, [new InstrumentId(1), new InstrumentId(2)]);
        while (handler.PollOnce() > 0) { }

        long lastSeq = 0;
        int ticks = 0;
        while (inbox.TryDequeue(out var e))
        {
            Assert.Equal(EventType.Tick, e.Type);
            Assert.True(e.Seq > lastSeq);   // host sequence numbers are monotonic
            Assert.True(e.Tick.BidPx < e.Tick.AskPx);
            lastSeq = e.Seq;
            ticks++;
        }
        Assert.True(ticks > 1_000);
        var snap = feed.Snapshot(new InstrumentId(1));
        Assert.Equal(snap.Bids[0].Px, handler.Book(new InstrumentId(1))!.BestBidPx);
        Assert.Equal(snap.Asks[0].Px, handler.Book(new InstrumentId(1))!.BestAskPx);
    }

    [Fact]
    public void Sequence_gap_marks_instrument_stale_then_recovers_from_snapshot()
    {
        var feed = new SimulatedFeed(Options(11));
        var handler = new FeedHandler(feed, new Sequencer(), new VirtualClock());
        var inbox = new MarketDataInbox(1 << 16);
        var id = new InstrumentId(1);
        handler.Subscribe(inbox, [id]);
        handler.PollOnce();
        feed.InjectGap(id);
        while (handler.PollOnce() > 0) { }

        Assert.Equal(1, handler.Gaps);
        var statuses = new List<long>();
        while (inbox.TryDequeue(out var e))
            if (e.Type == EventType.InstrumentStatus) statuses.Add(e.B);
        Assert.Equal([1L, 0L], statuses);   // stale, then recovered
        var snap = feed.Snapshot(id);
        var book = handler.Book(id)!;
        for (int l = 0; l < snap.Bids.Length; l++) Assert.Equal(snap.Bids[l], book.BidLevel(l));
    }
}

/// <summary>Acceptance test 7 (unit level): conflation for quotes, never for trades, kill when hopeless.</summary>
public class BackpressureTests
{
    private static HostEvent Quote(int inst, long px) => new()
    {
        Type = EventType.Tick, Tick = new Tick(0, 0, new InstrumentId(inst), px, 1, px + 1, 1, 0, 0),
    };

    private static HostEvent Trade(int inst, long n) => new()
    {
        Type = EventType.Tick, Tick = new Tick(0, 0, new InstrumentId(inst), 0, 0, 0, 0, n, 1),
    };

    [Fact]
    public void Slow_consumer_gets_conflated_quotes_but_every_trade_in_order()
    {
        var inbox = new MarketDataInbox(ringCapacity: 16, overflowCapacity: 4096);
        var rng = new Random(1);
        var tradesPublished = new Dictionary<int, List<long>>();
        var tradesReceived = new Dictionary<int, List<long>>();
        var lastQuoteReceived = new Dictionary<int, long>();
        var lastQuotePublished = new Dictionary<int, long>();
        long tradeNo = 0;

        void Consume(int max)
        {
            for (int i = 0; i < max && inbox.TryDequeue(out var e); i++)
            {
                int inst = e.Tick.Instrument.Value;
                if (e.Tick.IsTrade)
                {
                    if (!tradesReceived.TryGetValue(inst, out var l)) tradesReceived[inst] = l = new();
                    l.Add(e.Tick.LastPx);
                }
                else lastQuoteReceived[inst] = e.Tick.BidPx;
            }
        }

        for (int i = 0; i < 20_000; i++)
        {
            int inst = rng.Next(1, 4);
            if (rng.NextDouble() < 0.1)
            {
                long n = ++tradeNo;
                if (!tradesPublished.TryGetValue(inst, out var l)) tradesPublished[inst] = l = new();
                l.Add(n);
                inbox.Publish(Trade(inst, n));
            }
            else
            {
                inbox.Publish(Quote(inst, i));
                lastQuotePublished[inst] = i;
            }
            if (i % 10 == 0) Consume(3);   // consumer much slower than producer
        }
        while (inbox.HasBacklog || inbox.Depth > 0)
        {
            inbox.Flush();
            Consume(int.MaxValue);
        }

        Assert.False(inbox.Detached);
        Assert.True(inbox.Conflated > 1_000, $"conflated {inbox.Conflated}");
        foreach (var (inst, published) in tradesPublished) Assert.Equal(published, tradesReceived[inst]);   // zero lost, in order
        foreach (var (inst, px) in lastQuotePublished) Assert.Equal(px, lastQuoteReceived[inst]);           // latest value wins
    }

    [Fact]
    public void Consumer_that_stays_slow_is_detached_and_reported()
    {
        var inbox = new MarketDataInbox(ringCapacity: 16, overflowCapacity: 64);
        int fired = 0;
        inbox.OnOverflow = _ => fired++;
        for (int i = 0; i < 1_000; i++) inbox.Publish(Trade(1, i));
        Assert.True(inbox.Detached);
        Assert.Equal(1, fired);
        Assert.Equal(80, inbox.Depth + inbox.Backlog);   // ring + overflow were filled, nothing after detach
    }
}

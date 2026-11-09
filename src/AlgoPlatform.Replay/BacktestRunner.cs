using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Journal;
using AlgoPlatform.Core.Timing;
using AlgoPlatform.Host.Engine;
using AlgoPlatform.MarketData;
using AlgoPlatform.ReferenceData;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Replay;

public sealed class BacktestResult
{
    public required long Ticks { get; init; }
    public required long Orders { get; init; }
    public required long Fills { get; init; }
    public required long Rejects { get; init; }
    public required Dictionary<string, Position> Positions { get; init; }
    public required StrategyStatus FinalStatus { get; init; }
    public required TimeSpan Elapsed { get; init; }
    public required List<byte[]> Outputs { get; init; }
}

/// <summary>
/// "Replay" run mode (goal 3): recorded or synthetic market data plus a simulated exchange, driven as
/// fast as possible on one thread with a virtual clock. Timers fire on virtual time. The matcher fills
/// marketable orders at the touch and rests the rest until a later tick crosses them, so the same
/// inputs always give the same fills.
/// </summary>
public sealed class BacktestRunner
{
    private sealed class SimOrder
    {
        public long OrderId;
        public InstrumentId Instrument;
        public Side Side;
        public OrdType Type;
        public Tif Tif;
        public long Px;
        public long Qty;
        public long Cum;
    }

    private sealed class QueueSink(BacktestRunner owner) : IOrderSink
    {
        public void Send(in OrderCommand cmd) => owner._pending.Enqueue(cmd);
    }

    private readonly Queue<OrderCommand> _pending = new();
    private readonly Dictionary<long, SimOrder> _book = new();
    private readonly Dictionary<int, (long Bid, long Ask)> _quotes = new();
    private readonly VirtualClock _clock = new();
    private StrategyEngine _engine = null!;
    private long _seq;
    private long _execId;
    private long _fills;

    /// <summary>Backtests over ticks recorded in a journal (any strategy's journal on the same instruments).</summary>
    public static BacktestResult FromJournal(string journalPath, StrategyManifest manifest, IStrategy strategy, ReferenceDataSnapshot refData)
    {
        var ticks = new List<Tick>();
        foreach (var r in JournalReader.ReadAll(journalPath))
            if (r.Type == (int)EventType.Tick) ticks.Add(JournalCodec.DecodeInput(r).Tick);
        return new BacktestRunner().Run(ticks, manifest, strategy, refData);
    }

    /// <summary>Backtests over a deterministic synthetic feed.</summary>
    public static BacktestResult FromSimulatedFeed(SimulatedFeedOptions feedOptions, long messages, StrategyManifest manifest, IStrategy strategy,
        ReferenceDataSnapshot refData)
    {
        feedOptions.MessagesPerSecond = 0;
        feedOptions.MaxMessages = messages;
        var feed = new SimulatedFeed(feedOptions);
        var collector = new MarketDataInbox(1 << 20, 1 << 4);
        var handler = new FeedHandler(feed, new Sequencer(), new VirtualClock());
        handler.Subscribe(collector, feedOptions.Instruments.Select(i => i.Id));
        var ticks = new List<Tick>();
        long t = 0;
        while (handler.PollOnce() > 0)
        {
            while (collector.TryDequeue(out HostEvent e))
                if (e.Type == EventType.Tick) ticks.Add(e.Tick with { TimestampNs = t += 100_000 });   // 10k ticks/s of virtual time
        }
        return new BacktestRunner().Run(ticks, manifest, strategy, refData);
    }

    public BacktestResult Run(IReadOnlyList<Tick> ticks, StrategyManifest manifest, IStrategy strategy, ReferenceDataSnapshot refData)
    {
        var started = DateTime.UtcNow;
        var capture = new CapturingJournalSink();
        _engine = new StrategyEngine(1, manifest, strategy, new QueueSink(this), capture,
            new EngineOptions { ReplayMode = false, Clock = _clock, HandlerBudgetNs = long.MaxValue, AsyncRunner = RunAsyncInline });

        long t0 = ticks.Count > 0 ? ticks[0].TimestampNs : 0;
        Deliver(new HostEvent { Type = EventType.ReferenceData, TimestampNs = t0, A = refData.Version, Payload = refData });
        Deliver(new HostEvent { Type = EventType.ExchangeStatus, TimestampNs = t0, A = 1 });
        Deliver(new HostEvent { Type = EventType.SessionStart, TimestampNs = t0, Payload = StrategyEngine.SessionPayload(1, "backtest", manifest) });

        foreach (Tick tick in ticks)
        {
            FireTimers(tick.TimestampNs);
            _quotes[tick.Instrument.Value] = (tick.BidPx, tick.AskPx);
            Deliver(new HostEvent { Type = EventType.Tick, TimestampNs = tick.TimestampNs, Tick = tick with { Seq = _seq + 1 } });
            MatchResting(tick.Instrument, tick.TimestampNs);
        }

        var positions = new Dictionary<string, Position>();
        foreach (string symbol in manifest.Instruments)
            if (refData.TryGetBySymbol(symbol, out var info)) positions[symbol] = _engine.Oms.GetPosition(info.Id);

        return new BacktestResult
        {
            Ticks = ticks.Count,
            Orders = _engine.Metrics.OrdersSent,
            Fills = _fills,
            Rejects = _engine.Metrics.OrdersRejected,
            Positions = positions,
            FinalStatus = _engine.Status,
            Elapsed = DateTime.UtcNow - started,
            Outputs = capture.Outputs,
        };
    }

    private void Deliver(HostEvent e)
    {
        e.Seq = ++_seq;
        if (e.Type == EventType.Tick) e.Tick = e.Tick with { Seq = e.Seq };
        _clock.Set(e.TimestampNs);
        _engine.Process(ref e);
        // Orders the strategy just sent (and async results) are handled before the next market event.
        while (_pending.Count > 0 || _asyncResults.Count > 0)
        {
            if (_pending.Count > 0)
            {
                Handle(_pending.Dequeue(), e.TimestampNs);
                continue;
            }
            var (id, result) = _asyncResults.Dequeue();
            Deliver(new HostEvent { Type = EventType.AsyncResult, TimestampNs = e.TimestampNs, A = id, Payload = result });
        }
    }

    private void FireTimers(long now)
    {
        while (_engine.Timers.TryPopDue(now, out long id)) Deliver(new HostEvent { Type = EventType.Timer, TimestampNs = now, A = id });
    }

    private void RunAsyncInline(int strategyId, long requestId, Func<byte[]> work)
    {
        byte[] result = work();
        _asyncResults.Enqueue((requestId, result));
    }

    private readonly Queue<(long Id, byte[] Result)> _asyncResults = new();

    private void Handle(OrderCommand c, long now)
    {
        switch (c.Type)
        {
            case CommandType.New:
                var o = new SimOrder { OrderId = c.OrderId, Instrument = c.Instrument, Side = c.Side, Type = c.OrdType, Tif = c.Tif, Px = c.Px, Qty = c.Qty };
                _book[o.OrderId] = o;
                Report(o, OrdStatus.Working, 0, 0, now);
                TryFill(o, now);
                if (_book.ContainsKey(o.OrderId) && o.Tif is Tif.Ioc or Tif.Fok)
                {
                    _book.Remove(o.OrderId);
                    Report(o, OrdStatus.Cancelled, 0, 0, now);
                }
                break;
            case CommandType.Cancel:
                if (_book.Remove(c.OrderId, out SimOrder? co)) Report(co, OrdStatus.Cancelled, 0, 0, now);
                else Reject(c, now);
                break;
            case CommandType.Replace:
                if (_book.TryGetValue(c.OrderId, out SimOrder? ro) && c.Qty > ro.Cum)
                {
                    ro.Px = c.Px;
                    ro.Qty = c.Qty;
                    Report(ro, ro.Cum > 0 ? OrdStatus.PartiallyFilled : OrdStatus.Working, 0, 0, now);
                    TryFill(ro, now);
                }
                else Reject(c, now);
                break;
        }
    }

    private void MatchResting(InstrumentId id, long now)
    {
        if (_book.Count == 0) return;
        foreach (SimOrder o in _book.Values.Where(o => o.Instrument == id).OrderBy(o => o.OrderId).ToList()) TryFill(o, now);
    }

    private void TryFill(SimOrder o, long now)
    {
        if (!_book.ContainsKey(o.OrderId)) return;   // cancelled meanwhile
        if (!_quotes.TryGetValue(o.Instrument.Value, out var q) || q.Bid <= 0 || q.Ask <= 0) return;
        bool marketable = o.Type == OrdType.Market || (o.Side == Side.Buy ? o.Px >= q.Ask : o.Px <= q.Bid);
        if (!marketable) return;
        long px = o.Side == Side.Buy ? q.Ask : q.Bid;
        long qty = o.Qty - o.Cum;
        o.Cum = o.Qty;
        _book.Remove(o.OrderId);
        _fills++;
        Report(o, OrdStatus.Filled, px, qty, now);
    }

    private void Report(SimOrder o, OrdStatus status, long lastPx, long lastQty, long now)
    {
        long leaves = status is OrdStatus.Filled or OrdStatus.Cancelled or OrdStatus.Rejected ? 0 : o.Qty - o.Cum;
        var r = new ExecutionReport(0, o.OrderId, ++_execId, o.Instrument, o.Side, status, lastPx, lastQty, o.Cum, leaves, 0);
        Deliver(new HostEvent { Type = EventType.Execution, TimestampNs = now, Exec = r });
    }

    private void Reject(OrderCommand c, long now)
    {
        _engine.Oms.TryGet(c.OrderId, out var st);
        var r = new ExecutionReport(0, c.OrderId, 0, c.Instrument, c.Side, st.Status, 0, 0, st.CumQty, st.LeavesQty, RejectCodes.UnknownOrder);
        Deliver(new HostEvent { Type = EventType.Execution, TimestampNs = now, Exec = r });
    }
}

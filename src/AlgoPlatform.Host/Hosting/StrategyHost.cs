using System.Runtime;
using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Journal;
using AlgoPlatform.Core.Logging;
using AlgoPlatform.Core.Metrics;
using AlgoPlatform.Core.Threading;
using AlgoPlatform.Core.Timing;
using AlgoPlatform.Gateway;
using AlgoPlatform.Host.Engine;
using AlgoPlatform.Host.Loading;
using AlgoPlatform.MarketData;
using AlgoPlatform.ReferenceData;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Host.Hosting;

/// <summary>
/// A strategy host process (spec 8.1): one feed handler, one gateway, one journal writer, a watchdog,
/// and N strategy loops, each on its own thread (and core when pinning is on). Everything slow (deploy,
/// metrics, logging, persistence) runs on other threads and talks to the loops through their inboxes.
/// </summary>
public sealed class StrategyHost : IDisposable
{
    private readonly object _gate = new();
    private readonly Dictionary<string, StrategySlot> _slots = new(StringComparer.Ordinal);
    private readonly List<int> _freeCores = new();
    private readonly IThreadAffinity _affinity;
    private readonly IClock _clock = MonotonicClock.Instance;
    private int _nextStrategyId;

    private EventLoopThread? _feedThread;
    private EventLoopThread? _tapThread;
    private Thread? _watchdog;
    private volatile bool _running;

    public StrategyHost(HostOptions options)
    {
        Options = options;
        _affinity = ThreadAffinity.Create(options.PinThreads);
        Sequencer = new Sequencer();
        ReferenceData = new ReferenceDataService(options.ReferenceData
            ?? (options.ReferenceDataPath is not null ? ReferenceDataSnapshot.Load(options.ReferenceDataPath) : ReferenceDataSnapshot.Default()));
        Log = new LogBuffer(options.LogOutput ?? TextWriter.Null, id => NameOf(id));
        Journal = new JournalService(options.JournalRingCapacity);
    }

    public HostOptions Options { get; }
    public Sequencer Sequencer { get; }
    public ReferenceDataService ReferenceData { get; }
    public LogBuffer Log { get; }
    public JournalService Journal { get; }
    public FeedHandler? Feed { get; private set; }
    public ExchangeGateway? Gateway { get; private set; }
    public SimExchange.SimExchange? EmbeddedExchange { get; private set; }
    public bool IsRunning => _running;
    public string JournalDirectory => Path.Combine(Options.DataDirectory, "journal");
    public string PackageDirectory => Path.Combine(Options.DataDirectory, "packages");

    public int DedicatedCapacity => Options.Sandbox ? 0 : Math.Max(0, Options.EffectiveCores().Length - 2);

    public int FreeDedicatedCores
    {
        get { lock (_gate) return Options.Sandbox ? 0 : _freeCores.Count; }
    }

    public int FreeSandboxSlots
    {
        get { lock (_gate) return Options.Sandbox ? Options.MaxSandboxStrategies - ActiveCount() : 0; }
    }

    // ------------------------------------------------------------------ lifecycle

    public void Start()
    {
        if (_running) return;
        GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
        Directory.CreateDirectory(JournalDirectory);
        Directory.CreateDirectory(PackageDirectory);

        int[] cores = Options.EffectiveCores();
        int feedCore = Options.PinThreads && cores.Length > 0 ? cores[0] : -1;
        int gatewayCore = Options.PinThreads && cores.Length > 1 ? cores[1] : -1;
        lock (_gate)
        {
            _freeCores.Clear();
            if (!Options.Sandbox) _freeCores.AddRange(cores.Skip(2));
        }

        Log.Start();
        Journal.Start();

        ReferenceDataSnapshot refData = ReferenceData.Current;
        ReferenceData.Changed += OnReferenceDataChanged;

        IFeedSource source = Options.FeedSource ?? new SimulatedFeed(new SimulatedFeedOptions
        {
            Seed = Options.FeedSeed,
            MessagesPerSecond = Options.FeedMessagesPerSecond,
            Instruments = refData.All.Select(i => (i.Id, InitialMid(i), i.TickSize)).ToList(),
        });
        Feed = new FeedHandler(source, Sequencer, _clock);
        foreach (var i in refData.All) Feed.AddInstrument(i.Id);

        if (Options.EnableGateway)
        {
            if (Options.Mode == RunMode.Paper)
            {
                EmbeddedExchange = new SimExchange.SimExchange(Options.EmbeddedExchange);
                EmbeddedExchange.Start();
                Options.Gateway.Host = "127.0.0.1";
                Options.Gateway.Port = EmbeddedExchange.Port;
                StartQuoteTap(refData);
            }
            Gateway = new ExchangeGateway(Options.Gateway, Sequencer, _clock, Log);
            Gateway.SetSymbols(refData.All.Select(i => (i.Id, i.Symbol)));
            Gateway.Start(Options.InfrastructureIdle, gatewayCore, _affinity);
        }

        _feedThread = new EventLoopThread("feed-handler", Feed, Options.InfrastructureIdle, feedCore, _affinity);
        _feedThread.Start();

        _running = true;
        _watchdog = new Thread(WatchdogLoop) { IsBackground = true, Name = "watchdog" };
        _watchdog.Start();
        Log.Host(LogLevel.Info, $"host {Options.HostId} started in {Options.Mode} mode");
    }

    private static long InitialMid(InstrumentInfo i)
    {
        long mid = (i.MinPx + i.MaxPx) / 2;
        return mid - mid % i.TickSize;
    }

    /// <summary>Paper mode: the embedded exchange matches against this host's own market data.</summary>
    private void StartQuoteTap(ReferenceDataSnapshot refData)
    {
        var tap = new MarketDataInbox(Options.MarketDataRingCapacity, Options.MarketDataOverflowCapacity) { Name = "sim-tap" };
        Feed!.Subscribe(tap, refData.All.Select(i => i.Id));
        var exchange = EmbeddedExchange!;
        var symbols = refData.All.ToDictionary(i => i.Id.Value, i => i.Symbol);
        _tapThread = new EventLoopThread("sim-quote-tap", new DelegateWork(() =>
        {
            int n = 0;
            while (n < 256 && tap.TryDequeue(out HostEvent e))
            {
                n++;
                if (e.Type == EventType.Tick && e.Tick.BidPx > 0 && e.Tick.AskPx > 0 &&
                    symbols.TryGetValue(e.Tick.Instrument.Value, out string? sym))
                    exchange.SetQuote(sym, e.Tick.BidPx, e.Tick.AskPx);
            }
            return n;
        }), IdleMode.Backoff);
        _tapThread.Start();
    }

    // ------------------------------------------------------------------ deployment

    public StrategySlot DeployPackage(byte[] zip)
    {
        string dir = StrategyLoader.Extract(zip, PackageDirectory);
        return DeployDirectory(dir);
    }

    public StrategySlot DeployDirectory(string directory)
    {
        LoadedStrategy loaded = StrategyLoader.Load(directory);
        return Deploy(loaded.Manifest, loaded.Factory, loaded.Context);
    }

    /// <summary>Registers a strategy. Hot reload: an existing strategy with the same name is stopped first (state is not migrated).</summary>
    public StrategySlot Deploy(StrategyManifest manifest, Func<IStrategy> factory, StrategyLoadContext? context = null)
    {
        var errors = manifest.Validate();
        if (errors.Count > 0) throw new InvalidOperationException("Invalid manifest: " + string.Join("; ", errors));
        ReferenceDataSnapshot refData = ReferenceData.Current;
        foreach (string symbol in manifest.Instruments)
            if (!refData.TryGetBySymbol(symbol, out _)) throw new InvalidOperationException($"Unknown instrument {symbol}");

        StrategySlot? old;
        lock (_gate) _slots.TryGetValue(manifest.Name, out old);
        if (old is not null)
        {
            Stop(old.Name, TimeSpan.FromSeconds(5));
            Remove(old.Name);
        }

        lock (_gate)
        {
            if (Options.Sandbox && !manifest.IsSandbox)
                throw new CapacityException($"host {Options.HostId} is a sandbox host; '{manifest.ResourceClass}' strategies need a production host");
            if (!Options.Sandbox && manifest.IsSandbox)
                throw new CapacityException($"host {Options.HostId} is a production host; 'shared' strategies run on sandbox hosts");
            if (Options.Sandbox && ActiveCount() >= Options.MaxSandboxStrategies)
                throw new CapacityException($"sandbox host {Options.HostId} is full ({Options.MaxSandboxStrategies} strategies)");
            if (!Options.Sandbox && _freeCores.Count == 0)
                throw new CapacityException($"host {Options.HostId} has no free core for a dedicated-core strategy");

            int id = ++_nextStrategyId;
            var slot = new StrategySlot(id, manifest, factory, context, Options.ControlRingCapacity, _clock, Sequencer);
            if (!Options.Sandbox)
            {
                slot.Core = _freeCores[0];
                _freeCores.RemoveAt(0);
            }
            _slots[manifest.Name] = slot;
            Log.Host(LogLevel.Info, $"deployed {manifest.Name} {manifest.Version} as strategy {id}");
            return slot;
        }
    }

    public void Start(string name)
    {
        StrategySlot slot = GetSlot(name) ?? throw new KeyNotFoundException(name);
        lock (_gate)
        {
            if (slot.Engine is not null && !slot.Finished) return;   // already running
            if (slot.Finished)
            {
                // Restart after a stop: a fresh session (new strategy id, new journal) on the same core and code.
                slot = new StrategySlot(++_nextStrategyId, slot.Manifest, slot.Factory, slot.LoadContext, Options.ControlRingCapacity,
                    _clock, Sequencer) { Core = slot.Core };
                _slots[name] = slot;
            }
            ReferenceDataSnapshot refData = ReferenceData.Current;
            slot.Instruments = slot.Manifest.Instruments
                .Select(s => refData.TryGetBySymbol(s, out var info) ? info.Id : default)
                .Where(id => id.Value != 0).ToArray();

            IJournalSink journal = NullJournalSink.Instance;
            if (Options.EnableJournal)
            {
                string file = Path.Combine(JournalDirectory, $"{slot.Name}-{slot.StrategyId}-{DateTime.UtcNow:yyyyMMddTHHmmssfff}.journal");
                slot.Journal = Journal.Open(file);
                journal = slot.Journal;
            }
            IOrderSink sink = Gateway is not null ? new RingOrderSink(Gateway.Outbound) : NullOrderSink.Instance;
            var engine = new StrategyEngine(slot.StrategyId, slot.Manifest, slot.Factory(), sink, journal, new EngineOptions
            {
                HandlerBudgetNs = Options.HandlerBudgetNs,
                MaxConsecutiveBreaches = Options.MaxConsecutiveBreaches,
                RetainTerminalOrders = Options.RetainTerminalOrders,
                Clock = _clock,
                Log = Log,
                AsyncRunner = RunAsyncWork,
            });
            engine.StatusChanged = OnStatusChanged;
            slot.Engine = engine;
            slot.MdInbox = new MarketDataInbox(Options.MarketDataRingCapacity, Options.MarketDataOverflowCapacity)
            {
                Name = slot.Name,
                OnOverflow = inbox => RequestSlow(slot, "market data overflow"),
            };
            slot.OnFinished = s => ThreadPool.QueueUserWorkItem(_ => Cleanup(s));

            // Order matters: ref data and session start are the first two events the loop sees.
            slot.PostControl(new HostEvent { Type = EventType.ReferenceData, Seq = Sequencer.Next(), TimestampNs = _clock.NowNs, A = refData.Version, Payload = refData });
            slot.PostControl(new HostEvent
            {
                Type = EventType.SessionStart, Seq = Sequencer.Next(), TimestampNs = _clock.NowNs,
                Payload = StrategyEngine.SessionPayload(slot.StrategyId, Options.HostId, slot.Manifest),
            });
            Feed!.Subscribe(slot.MdInbox, slot.Instruments);
            Gateway?.Register(slot);

            IdleMode idle = Options.Sandbox ? IdleMode.Backoff : Options.StrategyIdle;
            slot.Thread = new EventLoopThread("strategy-" + slot.Name, slot, idle, Options.PinThreads ? slot.Core : -1, _affinity);
            slot.Thread.Start();
        }
        Log.Host(LogLevel.Info, $"started {name}");
    }

    /// <summary>Graceful stop: OnStop, cancel working orders, wait for them to finish, exit the loop.</summary>
    public bool Stop(string name, TimeSpan? wait = null)
    {
        StrategySlot? slot = GetSlot(name);
        if (slot?.Engine is null || slot.Finished) return true;
        PostControl(slot, ControlCode.Stop);
        if (wait is null) return true;
        var deadline = DateTime.UtcNow + wait.Value;
        while (!slot.Finished && DateTime.UtcNow < deadline) Thread.Sleep(5);
        if (!slot.Finished) return false;
        slot.Thread?.Join(TimeSpan.FromSeconds(1));
        return true;
    }

    public void Kill(string name, bool flatten = false)
    {
        StrategySlot slot = GetSlot(name) ?? throw new KeyNotFoundException(name);
        if (slot.Engine is null || slot.Finished) return;
        PostControl(slot, flatten ? ControlCode.KillAndFlatten : ControlCode.Kill);
        Log.Host(LogLevel.Warn, $"kill switch: {name}");
    }

    /// <summary>Host-level kill switch.</summary>
    public void KillAll(bool flatten = false)
    {
        foreach (var slot in Slots) if (slot.Engine is not null && !slot.Finished) Kill(slot.Name, flatten);
    }

    public bool Remove(string name)
    {
        StrategySlot? slot;
        lock (_gate)
        {
            if (!_slots.TryGetValue(name, out slot)) return false;
            if (slot.Engine is not null && !slot.Finished) throw new InvalidOperationException($"{name} is still running; stop it first");
            _slots.Remove(name);
            if (slot.Core >= 0) _freeCores.Add(slot.Core);
        }
        slot.LoadContext?.Unload();
        return true;
    }

    public StrategySlot? GetSlot(string name)
    {
        lock (_gate) return _slots.TryGetValue(name, out var s) ? s : null;
    }

    public IReadOnlyList<StrategySlot> Slots
    {
        get { lock (_gate) return _slots.Values.ToList(); }
    }

    /// <summary>Publishes a new reference data version to every running loop (swapped between events).</summary>
    public void PublishReferenceData(ReferenceDataSnapshot next) => ReferenceData.Publish(next);

    private void OnReferenceDataChanged(ReferenceDataSnapshot snap)
    {
        Gateway?.SetSymbols(snap.All.Select(i => (i.Id, i.Symbol)));
        foreach (var slot in Slots)
        {
            if (slot.Engine is null || slot.Finished) continue;
            slot.PostControl(new HostEvent { Type = EventType.ReferenceData, Seq = Sequencer.Next(), TimestampNs = _clock.NowNs, A = snap.Version, Payload = snap });
        }
    }

    private void PostControl(StrategySlot slot, ControlCode code) =>
        slot.PostControl(new HostEvent { Type = EventType.Control, Seq = Sequencer.Next(), TimestampNs = _clock.NowNs, A = (long)code });

    private void RunAsyncWork(int strategyId, long requestId, Func<byte[]> work)
    {
        StrategySlot? slot = Slots.FirstOrDefault(s => s.StrategyId == strategyId);
        if (slot is null) return;
        ThreadPool.UnsafeQueueUserWorkItem(_ =>
        {
            byte[] result;
            try { result = work(); }
            catch (Exception ex)
            {
                Log.Log(strategyId, LogLevel.Error, "RunAsync work threw: " + ex.Message, requestId);
                result = [];
            }
            slot.PostControl(new HostEvent
            {
                Type = EventType.AsyncResult, Seq = Sequencer.Next(), TimestampNs = _clock.NowNs, A = requestId, Payload = result,
            });
        }, null);
    }

    private void OnStatusChanged(StrategyEngine engine, StrategyStatus status)
    {
        if (status is StrategyStatus.Slow or StrategyStatus.Faulted)
        {
            // No more callbacks will run; stop feeding it market data.
            StrategySlot? slot = Slots.FirstOrDefault(s => s.StrategyId == engine.StrategyId);
            slot?.MdInbox?.Detach();
            Log.Log(engine.StrategyId, LogLevel.Error, "ALERT strategy is " + status);
        }
    }

    private void RequestSlow(StrategySlot slot, string reason)
    {
        Log.Log(slot.StrategyId, LogLevel.Error, "ALERT " + reason + ": kill-switching");
        PostControl(slot, ControlCode.MarkSlow);
    }

    private void Cleanup(StrategySlot slot)
    {
        if (slot.MdInbox is not null) Feed?.Unsubscribe(slot.MdInbox);
        Gateway?.Unregister(slot);
        slot.Journal?.Close();
        Log.Host(LogLevel.Info, $"stopped {slot.Name}");
    }

    private int ActiveCount()
    {
        int n = 0;
        foreach (var s in _slots.Values) if (!s.Finished) n++;
        return n;
    }

    private string NameOf(int strategyId)
    {
        lock (_gate)
        {
            foreach (var s in _slots.Values) if (s.StrategyId == strategyId) return s.Name;
        }
        return strategyId.ToString();
    }

    // ------------------------------------------------------------------ watchdog

    /// <summary>
    /// Catches loops stuck inside one handler (infinite loop, blocking call). The loop thread cannot be
    /// aborted, so the watchdog acts around it: the gateway refuses its orders and cancels its open ones,
    /// its market data is detached, and an alert is raised. The host should be restarted to reclaim the thread.
    /// </summary>
    private void WatchdogLoop()
    {
        long threshold = Options.HungThresholdMs * 1_000_000L;
        while (_running)
        {
            Thread.Sleep(10);
            long now = _clock.NowNs;
            foreach (var slot in Slots)
            {
                StrategyEngine? e = slot.Engine;
                if (e is null || slot.Finished || slot.HungReported) continue;
                long start = e.HandlerStartNs;
                if (start == 0 || now - start < threshold) continue;
                slot.HungReported = true;
                slot.MdInbox?.Detach();
                Gateway?.BlockAndCancelAll(slot.StrategyId);
                PostControl(slot, ControlCode.MarkSlow);
                Log.Log(slot.StrategyId, LogLevel.Error, "ALERT handler hung (ms); orders cancelled at the gateway", (now - start) / 1_000_000);
            }
        }
    }

    // ------------------------------------------------------------------ metrics

    public string MetricsText()
    {
        var w = new PrometheusWriter();
        var host = ("host", Options.HostId);
        foreach (var slot in Slots)
        {
            StrategyEngine? e = slot.Engine;
            if (e is null) continue;
            var l = new[] { host, ("strategy", slot.Name) };
            StrategyMetrics m = e.Metrics;
            w.Gauge("algo_strategy_status", "Strategy status (0 created,1 running,2 killed,3 slow,4 faulted,5 stopping,6 stopped)", (int)slot.Status, l);
            w.Counter("algo_strategy_events_total", "Events handled", m.Events, l);
            w.Counter("algo_strategy_ticks_total", "Ticks handled", m.Ticks, l);
            w.Gauge("algo_strategy_queue_depth", "Inbox depth", slot.MdInbox?.Depth ?? 0, [.. l, ("queue", "market_data")]);
            w.Gauge("algo_strategy_queue_depth", "Inbox depth", slot.ControlDepth, [.. l, ("queue", "control")]);
            w.Counter("algo_strategy_md_conflated_total", "Quotes conflated for this strategy", slot.MdInbox?.Conflated ?? 0, l);
            w.Counter("algo_strategy_orders_sent_total", "Orders sent", m.OrdersSent, l);
            w.Counter("algo_strategy_orders_rejected_total", "Orders rejected by pre-trade risk", m.OrdersRejected, l);
            w.Counter("algo_strategy_cancels_sent_total", "Cancels sent", m.CancelsSent, l);
            w.Counter("algo_strategy_fills_total", "Fills", m.Fills, l);
            w.Counter("algo_strategy_budget_breaches_total", "Handler budget breaches", m.BudgetBreaches, l);
            w.Counter("algo_strategy_journal_stalls_total", "Times the loop waited for journal ring space", slot.Journal?.Stalls ?? 0, l);
            w.Summary("algo_strategy_handler_seconds", "Strategy handler time", m.HandlerTime, l);
            w.Summary("algo_strategy_tick_to_order_seconds", "Tick published to order handed to the gateway", m.TickToOrder, l);
            w.Summary("algo_strategy_inbox_latency_seconds", "Tick published to handler start", m.InboxLatency, l);
        }
        for (int gen = 0; gen <= 2; gen++)
            w.Counter("algo_gc_collections_total", "GC collections", GC.CollectionCount(gen), host, ("gen", gen.ToString()));
        if (Feed is not null)
        {
            w.Counter("algo_feed_messages_total", "Raw feed messages", Feed.Messages, host);
            w.Counter("algo_feed_ticks_total", "Normalised ticks published", Feed.TicksPublished, host);
            w.Counter("algo_feed_gaps_total", "Sequence gaps recovered", Feed.Gaps, host);
        }
        if (Gateway is not null)
        {
            w.Gauge("algo_gateway_up", "1 if the exchange session is up", Gateway.IsUp ? 1 : 0, host);
            w.Counter("algo_gateway_orders_sent_total", "New orders sent to the exchange", Gateway.OrdersSent, host);
            w.Counter("algo_gateway_reports_total", "Execution reports received", Gateway.ReportsReceived, host);
            w.Counter("algo_gateway_duplicate_reports_total", "Duplicate execution reports ignored", Gateway.DuplicateReports, host);
            w.Counter("algo_gateway_disconnects_total", "Exchange session drops", Gateway.Disconnects, host);
        }
        w.Counter("algo_log_dropped_total", "Log entries dropped (ring full)", Log.Dropped, host);
        return w.ToString();
    }

    public void Dispose()
    {
        if (!_running) return;
        foreach (var slot in Slots) Stop(slot.Name);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(3);
        while (Slots.Any(s => s.Engine is not null && !s.Finished) && DateTime.UtcNow < deadline) Thread.Sleep(5);
        foreach (var slot in Slots)
        {
            slot.Thread?.RequestStop();
            slot.Thread?.Join(TimeSpan.FromSeconds(1));
            slot.Journal?.Close();
        }
        _running = false;
        _watchdog?.Join(TimeSpan.FromSeconds(1));
        _feedThread?.RequestStop();
        _feedThread?.Join(TimeSpan.FromSeconds(1));
        _tapThread?.RequestStop();
        _tapThread?.Join(TimeSpan.FromSeconds(1));
        Gateway?.Dispose();
        EmbeddedExchange?.Dispose();
        Journal.Dispose();
        Log.Dispose();
        ReferenceData.Changed -= OnReferenceDataChanged;
    }
}

/// <summary>Deployment refused because the host has no room for it (spec 8.3: reject, never overcommit).</summary>
public sealed class CapacityException(string message) : InvalidOperationException(message);

/// <summary>Adapts a delegate to <see cref="ILoopWork"/> for small housekeeping loops.</summary>
internal sealed class DelegateWork(Func<int> poll) : ILoopWork
{
    public int PollOnce() => poll();
}

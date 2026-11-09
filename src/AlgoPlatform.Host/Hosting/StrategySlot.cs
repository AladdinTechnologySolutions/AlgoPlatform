using AlgoPlatform.Core.Collections;
using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Journal;
using AlgoPlatform.Core.Threading;
using AlgoPlatform.Core.Timing;
using AlgoPlatform.Host.Engine;
using AlgoPlatform.Host.Loading;
using AlgoPlatform.MarketData;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Host.Hosting;

/// <summary>
/// One running strategy inside a host: its two inboxes, its engine and its loop thread.
/// <list type="bullet">
/// <item>Market data inbox: SPSC from the feed handler, with conflation (see <see cref="MarketDataInbox"/>).</item>
/// <item>Control inbox: MPSC from the gateway (execution reports), the host (kill/stop/ref data) and async workers.
/// Never drops: if the ring is full, events go to a locked overflow queue and the strategy is marked slow.</item>
/// </list>
/// Each poll drains control first (execution reports and kills have priority), fires due timers, then
/// handles a batch of market data.
/// </summary>
public sealed class StrategySlot : IEventTarget, ILoopWork
{
    private readonly MpscRing<HostEvent> _control;
    private readonly Queue<HostEvent> _overflow = new();
    private readonly object _overflowGate = new();
    private readonly IClock _clock;
    private readonly Sequencer _sequencer;
    private readonly long _stopTimeoutNs;
    private int _overflowCount;
    private int _slowRequested;

    internal StrategySlot(int strategyId, StrategyManifest manifest, Func<IStrategy> factory, StrategyLoadContext? loadContext,
        int controlCapacity, IClock clock, Sequencer sequencer, long stopTimeoutNs = 2_000_000_000)
    {
        StrategyId = strategyId;
        Manifest = manifest;
        Factory = factory;
        LoadContext = loadContext;
        _control = new MpscRing<HostEvent>(controlCapacity);
        _clock = clock;
        _sequencer = sequencer;
        _stopTimeoutNs = stopTimeoutNs;
    }

    public int StrategyId { get; }
    public string Name => Manifest.Name;
    public StrategyManifest Manifest { get; }
    public Func<IStrategy> Factory { get; }
    public StrategyLoadContext? LoadContext { get; internal set; }

    public StrategyEngine? Engine { get; internal set; }
    public MarketDataInbox? MdInbox { get; internal set; }
    public JournalChannel? Journal { get; internal set; }
    public EventLoopThread? Thread { get; internal set; }
    public InstrumentId[] Instruments { get; internal set; } = [];
    public int Core { get; internal set; } = -1;
    public bool Finished { get; private set; }
    public bool HungReported { get; internal set; }
    public DateTime DeployedUtc { get; } = DateTime.UtcNow;

    public StrategyStatus Status => Finished ? StrategyStatus.Stopped : Engine?.Status ?? StrategyStatus.Created;
    public int ControlDepth => _control.Count + Volatile.Read(ref _overflowCount);

    /// <summary>Raised on the loop thread when the loop has finished (status Stopped).</summary>
    internal Action<StrategySlot>? OnFinished { get; set; }

    /// <summary>Any thread. Never drops while the strategy is alive.</summary>
    public bool PostControl(in HostEvent evt)
    {
        if (Finished) return false;
        if (Volatile.Read(ref _overflowCount) == 0 && _control.TryEnqueue(evt)) return true;
        lock (_overflowGate)
        {
            _overflow.Enqueue(evt);
            _overflowCount++;
        }
        // The loop is not keeping up with its own execution reports: kill-switch it (once).
        if (Interlocked.Exchange(ref _slowRequested, 1) == 0)
        {
            var slow = new HostEvent
            {
                Type = EventType.Control, Seq = _sequencer.Next(), TimestampNs = _clock.NowNs, A = (long)ControlCode.MarkSlow,
            };
            lock (_overflowGate)
            {
                _overflow.Enqueue(slow);
                _overflowCount++;
            }
        }
        return true;
    }

    public int PollOnce()
    {
        StrategyEngine engine = Engine!;
        int work = 0;

        while (work < 64 && TryDequeueControl(out HostEvent e))
        {
            engine.Process(ref e);
            work++;
        }

        long now = _clock.NowNs;
        if (engine.Timers.Count > 0 && engine.Timers.TryPopDue(now, out long timerId))
        {
            var t = new HostEvent { Type = EventType.Timer, Seq = _sequencer.Next(), TimestampNs = now, A = timerId };
            engine.Process(ref t);
            work++;
        }

        MarketDataInbox md = MdInbox!;
        for (int i = 0; i < 64 && md.TryDequeue(out HostEvent e); i++)
        {
            engine.Process(ref e);
            work++;
        }

        if (engine.Status == StrategyStatus.Stopping &&
            (engine.Oms.LiveOrderCount == 0 || now - engine.StopRequestedNs > _stopTimeoutNs))
        {
            engine.MarkStopped();
            Finished = true;
            Thread?.RequestStop();
            OnFinished?.Invoke(this);
        }
        return work;
    }

    private bool TryDequeueControl(out HostEvent e)
    {
        if (_control.TryDequeue(out e)) return true;
        if (Volatile.Read(ref _overflowCount) == 0) return false;
        lock (_overflowGate)
        {
            if (_overflow.Count == 0) return false;
            // Everything already in the ring is older than the overflow, and the ring is empty now.
            e = _overflow.Dequeue();
            _overflowCount--;
            return true;
        }
    }
}

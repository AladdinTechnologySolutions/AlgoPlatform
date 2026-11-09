using AlgoPlatform.Core.Collections;
using AlgoPlatform.Core.Events;

namespace AlgoPlatform.MarketData;

/// <summary>
/// One subscriber's market-data inbox: an SPSC ring from the feed handler thread to the strategy loop,
/// plus the producer-side backpressure policy (spec 6.11 / FR-3):
/// <list type="bullet">
/// <item>Ring has room and no backlog: enqueue directly.</item>
/// <item>Ring full, quote-only tick: keep only the latest quote per instrument (conflation).</item>
/// <item>Ring full, trade tick or status event: append to an overflow FIFO; trades are never conflated or dropped.</item>
/// <item>Overflow FIFO full: the subscriber is too slow; it is detached and <see cref="OnOverflow"/> fires (kill switch).</item>
/// </list>
/// Per-instrument order is preserved: a pending conflated quote is moved into the FIFO before a later
/// trade on the same instrument, and the FIFO is flushed before conflated quotes.
/// Everything except <see cref="TryDequeue"/> runs on the feed handler thread.
/// </summary>
public sealed class MarketDataInbox
{
    private readonly SpscRing<HostEvent> _ring;

    // Producer-side (feed thread) backlog.
    private readonly HostEvent[] _overflow;
    private readonly int _overflowMask;
    private long _overflowHead;
    private long _overflowTail;
    private readonly HostEvent[] _pendingQuote;
    private readonly bool[] _dirty;
    private readonly int[] _dirtyList;
    private int _dirtyCount;

    private long _published;
    private long _conflated;
    private long _overflowed;
    private volatile bool _detached;

    public MarketDataInbox(int ringCapacity = 4096, int overflowCapacity = 65536, int maxInstrumentId = 4095)
    {
        _ring = new SpscRing<HostEvent>(ringCapacity);
        _overflow = new HostEvent[overflowCapacity];
        _overflowMask = overflowCapacity - 1;
        if ((overflowCapacity & _overflowMask) != 0) throw new ArgumentException("overflowCapacity must be a power of two");
        _pendingQuote = new HostEvent[maxInstrumentId + 1];
        _dirty = new bool[maxInstrumentId + 1];
        _dirtyList = new int[maxInstrumentId + 1];
    }

    public string Name { get; init; } = "";

    /// <summary>Fired once, on the feed thread, when the overflow FIFO is exhausted.</summary>
    public Action<MarketDataInbox>? OnOverflow { get; set; }

    public bool Detached => _detached;
    public int Depth => _ring.Count;
    public int Backlog => (int)(_overflowTail - _overflowHead) + _dirtyCount;
    public bool HasBacklog => _overflowTail != _overflowHead || _dirtyCount > 0;
    public long Published => Volatile.Read(ref _published);
    public long Conflated => Volatile.Read(ref _conflated);
    public long Overflowed => Volatile.Read(ref _overflowed);

    /// <summary>Consumer (strategy loop) side.</summary>
    public bool TryDequeue(out HostEvent evt) => _ring.TryDequeue(out evt);

    /// <summary>Stop delivering; used when the subscriber is killed or stopped.</summary>
    public void Detach() => _detached = true;

    /// <summary>Feed thread. Never blocks.</summary>
    public void Publish(in HostEvent evt)
    {
        if (_detached) return;
        Volatile.Write(ref _published, _published + 1);
        if (HasBacklog) Flush();
        if (!HasBacklog && _ring.TryEnqueue(evt)) return;

        bool conflatable = evt.Type == EventType.Tick && !evt.Tick.IsTrade;
        int inst = evt.Type == EventType.Tick ? evt.Tick.Instrument.Value : (int)evt.A;
        if (conflatable)
        {
            if (_dirty[inst]) Volatile.Write(ref _conflated, _conflated + 1);
            else
            {
                _dirty[inst] = true;
                _dirtyList[_dirtyCount++] = inst;
            }
            _pendingQuote[inst] = evt;
            return;
        }
        if (_dirty[inst])
        {
            // Keep per-instrument order: the older pending quote goes ahead of this trade/status.
            if (!AppendOverflow(_pendingQuote[inst])) return;
            RemoveDirty(inst);
        }
        AppendOverflow(evt);
    }

    /// <summary>Feed thread. Moves as much backlog as fits into the ring. Returns items moved.</summary>
    public int Flush()
    {
        if (_detached) return 0;
        int moved = 0;
        while (_overflowHead != _overflowTail)
        {
            if (!_ring.TryEnqueue(_overflow[_overflowHead & _overflowMask])) return moved;
            _overflow[_overflowHead & _overflowMask] = default;
            _overflowHead++;
            moved++;
        }
        while (_dirtyCount > 0)
        {
            int inst = _dirtyList[0];
            if (!_ring.TryEnqueue(_pendingQuote[inst])) return moved;
            RemoveDirty(inst);
            moved++;
        }
        return moved;
    }

    private bool AppendOverflow(in HostEvent evt)
    {
        if (_overflowTail - _overflowHead >= _overflow.Length)
        {
            _detached = true;
            OnOverflow?.Invoke(this);
            return false;
        }
        _overflow[_overflowTail & _overflowMask] = evt;
        _overflowTail++;
        Volatile.Write(ref _overflowed, _overflowed + 1);
        return true;
    }

    private void RemoveDirty(int inst)
    {
        _dirty[inst] = false;
        _pendingQuote[inst] = default;
        for (int i = 0; i < _dirtyCount; i++)
        {
            if (_dirtyList[i] != inst) continue;
            Array.Copy(_dirtyList, i + 1, _dirtyList, i, _dirtyCount - i - 1);
            _dirtyCount--;
            return;
        }
    }
}

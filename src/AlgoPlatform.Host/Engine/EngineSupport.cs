using AlgoPlatform.Core.Collections;
using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Timing;

namespace AlgoPlatform.Host.Engine;

public enum StrategyStatus { Created = 0, Running = 1, Killed = 2, Slow = 3, Faulted = 4, Stopping = 5, Stopped = 6 }

/// <summary>Where accepted order commands go: the gateway's outbound ring in a host, nowhere in replay.</summary>
public interface IOrderSink
{
    void Send(in OrderCommand cmd);
}

public sealed class NullOrderSink : IOrderSink
{
    public static readonly NullOrderSink Instance = new();
    public void Send(in OrderCommand cmd) { }
}

/// <summary>
/// Pushes into the gateway's MPSC ring. If the ring is full it spins instead of rejecting, because a
/// reject that depends on gateway speed would make the strategy's behaviour non-reproducible.
/// </summary>
public sealed class RingOrderSink : IOrderSink
{
    private readonly MpscRing<OrderCommand> _ring;
    private long _stalls;

    public RingOrderSink(MpscRing<OrderCommand> ring) => _ring = ring;

    public long Stalls => Volatile.Read(ref _stalls);

    public void Send(in OrderCommand cmd)
    {
        if (_ring.TryEnqueue(cmd)) return;
        Interlocked.Increment(ref _stalls);
        var spin = new SpinWait();
        while (!_ring.TryEnqueue(cmd)) spin.SpinOnce(-1);
    }
}

/// <summary>Per-strategy timers. Small fixed table; linear scan is cheaper than a heap at this size.</summary>
public sealed class TimerTable
{
    private readonly long[] _ids;
    private readonly long[] _deadlines;
    private int _count;

    public TimerTable(int capacity = 64)
    {
        _ids = new long[capacity];
        _deadlines = new long[capacity];
    }

    public int Count => _count;

    /// <summary>Sets or replaces a timer. Returns false if the table is full.</summary>
    public bool Set(long id, long deadlineNs)
    {
        for (int i = 0; i < _count; i++)
        {
            if (_ids[i] != id) continue;
            _deadlines[i] = deadlineNs;
            return true;
        }
        if (_count == _ids.Length) return false;
        _ids[_count] = id;
        _deadlines[_count] = deadlineNs;
        _count++;
        return true;
    }

    public void Cancel(long id)
    {
        for (int i = 0; i < _count; i++)
        {
            if (_ids[i] != id) continue;
            RemoveAt(i);
            return;
        }
    }

    /// <summary>Removes and returns the earliest timer due at or before <paramref name="nowNs"/>.</summary>
    public bool TryPopDue(long nowNs, out long id)
    {
        int best = -1;
        for (int i = 0; i < _count; i++)
            if (_deadlines[i] <= nowNs && (best < 0 || _deadlines[i] < _deadlines[best])) best = i;
        if (best < 0)
        {
            id = 0;
            return false;
        }
        id = _ids[best];
        RemoveAt(best);
        return true;
    }

    private void RemoveAt(int i)
    {
        _count--;
        _ids[i] = _ids[_count];
        _deadlines[i] = _deadlines[_count];
    }
}

/// <summary>Counters and histograms for one strategy. Written by its loop thread only; read by scrapes.</summary>
public sealed class StrategyMetrics
{
    public readonly LatencyHistogram HandlerTime = new();
    public readonly LatencyHistogram TickToOrder = new();
    public readonly LatencyHistogram InboxLatency = new();
    public long Events;
    public long Ticks;
    public long ExecutionReports;
    public long OrdersSent;
    public long OrdersRejected;
    public long CancelsSent;
    public long ReplacesSent;
    public long Fills;
    public long DuplicateReports;
    public long BudgetBreaches;
    public long Faults;
}

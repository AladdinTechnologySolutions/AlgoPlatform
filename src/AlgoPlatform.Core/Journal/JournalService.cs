using AlgoPlatform.Core.Collections;
using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Threading;

namespace AlgoPlatform.Core.Journal;

/// <summary>Where a strategy loop records its inputs (before handling) and outputs.</summary>
public interface IJournalSink
{
    void Input(in HostEvent evt);
    void Output(in OrderCommand cmd);
}

public sealed class NullJournalSink : IJournalSink
{
    public static readonly NullJournalSink Instance = new();
    public void Input(in HostEvent evt) { }
    public void Output(in OrderCommand cmd) { }
}

public struct JournalItem
{
    public bool IsOutput;
    public HostEvent Event;
    public OrderCommand Command;
}

/// <summary>
/// One strategy's journal: an SPSC ring from the loop thread to the journal thread, plus the file.
/// The loop never does I/O; if the ring is full it spins (and counts the stall) rather than dropping,
/// because a lossy journal would break deterministic replay.
/// </summary>
public sealed class JournalChannel : IJournalSink
{
    internal readonly SpscRing<JournalItem> Ring;
    internal readonly JournalWriter? Writer;   // null: discard (benchmarks, allocation tests)
    private JournalItem _scratch;
    private volatile bool _closed;
    private long _stalls;

    internal JournalChannel(string? path, int ringCapacity)
    {
        Ring = new SpscRing<JournalItem>(ringCapacity);
        Writer = path is null ? null : new JournalWriter(path);
    }

    public string? Path => Writer?.Path;
    public long Stalls => Volatile.Read(ref _stalls);
    public bool Closed => _closed;
    internal bool Disposed { get; set; }

    public void Input(in HostEvent evt)
    {
        _scratch.IsOutput = false;
        _scratch.Event = evt;
        Enqueue();
    }

    public void Output(in OrderCommand cmd)
    {
        _scratch.IsOutput = true;
        _scratch.Event = default;
        _scratch.Command = cmd;
        Enqueue();
    }

    private void Enqueue()
    {
        if (Ring.TryEnqueue(_scratch)) return;
        Interlocked.Increment(ref _stalls);
        var spin = new SpinWait();
        while (!Ring.TryEnqueue(_scratch)) spin.SpinOnce(-1);
    }

    /// <summary>Marks the channel finished; the journal thread drains and closes the file.</summary>
    public void Close() => _closed = true;
}

/// <summary>The journal thread: drains every strategy's channel into its memory-mapped file.</summary>
public sealed class JournalService : ILoopWork, IDisposable
{
    private readonly object _gate = new();
    private readonly int _ringCapacity;
    private JournalChannel[] _channels = [];
    private readonly byte[] _scratch = new byte[64 * 1024];
    private EventLoopThread? _thread;

    public JournalService(int ringCapacity = 8192) => _ringCapacity = ringCapacity;

    /// <summary>Opens a channel writing to <paramref name="path"/>; null drains and discards (benchmarks only).</summary>
    public JournalChannel Open(string? path)
    {
        var ch = new JournalChannel(path, _ringCapacity);
        lock (_gate) _channels = [.. _channels, ch];
        return ch;
    }

    public void Start(IdleMode idle = IdleMode.Backoff)
    {
        _thread = new EventLoopThread("journal", this, idle);
        _thread.Start();
    }

    /// <summary>Blocks until a closed channel's file has been fully written and released.</summary>
    public bool WaitClosed(JournalChannel ch, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!ch.Disposed)
        {
            if (_thread is null || !_thread.IsRunning) PollOnce();
            if (DateTime.UtcNow > deadline) return false;
            Thread.Sleep(1);
        }
        return true;
    }

    public int PollOnce()
    {
        int work = 0;
        JournalChannel[] channels = Volatile.Read(ref _channels);
        for (int i = 0; i < channels.Length; i++)
        {
            JournalChannel ch = channels[i];
            if (ch.Disposed) continue;
            bool closed = ch.Closed;   // read before draining so nothing enqueued before Close is missed
            int n = 0;
            while (n < 4096 && ch.Ring.TryDequeue(out JournalItem item))
            {
                if (ch.Writer is not null) Write(ch.Writer, in item);
                n++;
            }
            work += n;
            if (closed && n == 0)
            {
                lock (_gate)
                {
                    ch.Writer?.Dispose();
                    ch.Disposed = true;
                    _channels = Array.FindAll(_channels, c => c != ch);
                }
            }
        }
        return work;
    }

    private void Write(JournalWriter w, in JournalItem item)
    {
        if (item.IsOutput)
        {
            int len = JournalCodec.EncodeOutput(item.Command, _scratch);
            w.Append(item.Command.CauseSeq, item.Command.TimestampNs, JournalCodec.OutputType(item.Command.Type), _scratch.AsSpan(0, len));
            return;
        }
        int fixedLen = JournalCodec.EncodeInput(item.Event, _scratch);
        byte[]? blob = JournalCodec.BlobOf(item.Event);
        if (blob is null)
        {
            w.Append(item.Event.Seq, item.Event.TimestampNs, (int)item.Event.Type, _scratch.AsSpan(0, fixedLen));
            return;
        }
        Span<byte> dst = w.Reserve(fixedLen + blob.Length);
        _scratch.AsSpan(0, fixedLen).CopyTo(dst);
        blob.CopyTo(dst[fixedLen..]);
        w.Commit(item.Event.Seq, item.Event.TimestampNs, (int)item.Event.Type, fixedLen + blob.Length);
    }

    public void Dispose()
    {
        _thread?.RequestStop();
        _thread?.Join(TimeSpan.FromSeconds(2));
        PollOnce();
        lock (_gate)
        {
            foreach (var ch in _channels)
            {
                while (ch.Ring.TryDequeue(out JournalItem item))
                    if (ch.Writer is not null) Write(ch.Writer, in item);
                ch.Writer?.Dispose();
                ch.Disposed = true;
            }
            _channels = [];
        }
    }
}

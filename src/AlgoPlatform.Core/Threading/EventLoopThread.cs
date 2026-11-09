namespace AlgoPlatform.Core.Threading;

/// <summary>Body of a run-to-completion loop. PollOnce must never block; it returns the amount of work done.</summary>
public interface ILoopWork
{
    int PollOnce();
}

/// <summary>
/// A dedicated (optionally pinned) thread that runs <c>while (running) { work.PollOnce() or idle }</c>.
/// Used for strategy loops, the feed handler, the gateway and the journal writer.
/// </summary>
public sealed class EventLoopThread
{
    private readonly ILoopWork _work;
    private readonly IThreadAffinity _affinity;
    private readonly int _core;
    private readonly Thread _thread;
    private IdleStrategy _idle;
    private volatile bool _running;

    public EventLoopThread(string name, ILoopWork work, IdleMode idleMode, int core = -1, IThreadAffinity? affinity = null)
    {
        _work = work;
        _core = core;
        _affinity = affinity ?? NoOpAffinity.Instance;
        _idle = new IdleStrategy(idleMode);
        _thread = new Thread(Run) { IsBackground = true, Name = name };
    }

    public bool IsRunning => _running;
    public int ManagedThreadId => _thread.ManagedThreadId;
    public bool Pinned { get; private set; }

    /// <summary>Called on the loop thread once, before the first poll.</summary>
    public Action? OnThreadStart { get; set; }

    /// <summary>Called on the loop thread if PollOnce throws. Default: rethrow (kills the process).</summary>
    public Action<Exception>? OnUnhandled { get; set; }

    public void Start()
    {
        _running = true;
        _thread.Start();
    }

    public void RequestStop() => _running = false;

    public bool Join(TimeSpan timeout) => !_thread.IsAlive || _thread.Join(timeout);

    private void Run()
    {
        if (_core >= 0) Pinned = _affinity.PinCurrentThread(_core);
        OnThreadStart?.Invoke();
        while (_running)
        {
            int done;
            try
            {
                done = _work.PollOnce();
            }
            catch (Exception ex) when (OnUnhandled is not null)
            {
                OnUnhandled(ex);
                done = 1;
            }
            if (done > 0) _idle.Reset();
            else _idle.Idle();
        }
    }
}

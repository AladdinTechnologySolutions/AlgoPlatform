using System.Runtime.CompilerServices;

namespace AlgoPlatform.Core.Threading;

public enum IdleMode
{
    /// <summary>Never gives up the core. Lowest latency; requires a dedicated core per loop.</summary>
    BusySpin,
    /// <summary>Spin briefly, then Thread.Yield. Good when loops slightly outnumber cores.</summary>
    SpinYield,
    /// <summary>Spin, yield, Sleep(0), then Sleep(1) after a long idle period. For shared/sandbox hosts and tests.</summary>
    Backoff,
}

/// <summary>What a loop does when a poll found no work. Mutable struct: keep it in a field, not a copy.</summary>
public struct IdleStrategy
{
    private readonly IdleMode _mode;
    private int _idle;

    public IdleStrategy(IdleMode mode)
    {
        _mode = mode;
        _idle = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Reset() => _idle = 0;

    public void Idle()
    {
        int n = ++_idle;
        switch (_mode)
        {
            case IdleMode.BusySpin:
                Thread.SpinWait(1);
                break;
            case IdleMode.SpinYield:
                if (n < 100) Thread.SpinWait(4);
                else Thread.Yield();
                break;
            default:
                if (n < 100) Thread.SpinWait(4);
                else if (n < 200) Thread.Yield();
                else if (n < 2_000) Thread.Sleep(0);
                else Thread.Sleep(1);
                break;
        }
    }
}

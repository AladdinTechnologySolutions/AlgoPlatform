using System.Diagnostics;

namespace AlgoPlatform.Core.Timing;

public interface IClock
{
    /// <summary>Monotonic nanoseconds. Not wall-clock time.</summary>
    long NowNs { get; }
}

/// <summary>Stopwatch/QPC based monotonic clock. Wall clock is used only for logging (spec section 10).</summary>
public sealed class MonotonicClock : IClock
{
    public static readonly MonotonicClock Instance = new();

    private static readonly long s_nsPerTick =
        Stopwatch.Frequency <= 1_000_000_000 && 1_000_000_000 % Stopwatch.Frequency == 0 ? 1_000_000_000 / Stopwatch.Frequency : 0;
    private static readonly double s_nsPerTickD = 1e9 / Stopwatch.Frequency;

    public long NowNs => Now();

    public static long Now()
    {
        long t = Stopwatch.GetTimestamp();
        return s_nsPerTick != 0 ? t * s_nsPerTick : (long)(t * s_nsPerTickD);
    }

    /// <summary>Resolution of the underlying counter in nanoseconds (100 ns on typical Windows QPC).</summary>
    public static double ResolutionNs => s_nsPerTickD;
}

/// <summary>Clock driven explicitly; used by replay and tests so time never depends on the machine.</summary>
public sealed class VirtualClock : IClock
{
    private long _now;
    public VirtualClock(long startNs = 0) => _now = startNs;
    public long NowNs => Volatile.Read(ref _now);
    public void Set(long ns) => Volatile.Write(ref _now, ns);
    public void Advance(long ns) => Interlocked.Add(ref _now, ns);
}

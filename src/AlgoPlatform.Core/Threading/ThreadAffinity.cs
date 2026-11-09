using System.Runtime.InteropServices;

namespace AlgoPlatform.Core.Threading;

/// <summary>Pins the calling thread to one logical core. Behind an interface so tests run anywhere.</summary>
public interface IThreadAffinity
{
    bool PinCurrentThread(int core);
}

public sealed class NoOpAffinity : IThreadAffinity
{
    public static readonly NoOpAffinity Instance = new();
    public bool PinCurrentThread(int core) => false;
}

public static class ThreadAffinity
{
    /// <summary>OS implementation when enabled and supported, otherwise the no-op fallback.</summary>
    public static IThreadAffinity Create(bool enabled)
    {
        if (!enabled) return NoOpAffinity.Instance;
        if (OperatingSystem.IsWindows()) return new WindowsAffinity();
        if (OperatingSystem.IsLinux()) return new LinuxAffinity();
        return NoOpAffinity.Instance;
    }
}

internal sealed partial class WindowsAffinity : IThreadAffinity
{
    [LibraryImport("kernel32.dll")]
    private static partial nint GetCurrentThread();

    [LibraryImport("kernel32.dll", SetLastError = true)]
    private static partial nuint SetThreadAffinityMask(nint thread, nuint mask);

    public bool PinCurrentThread(int core)
    {
        if (core < 0 || core >= 64 || core >= Environment.ProcessorCount) return false;
        Thread.BeginThreadAffinity();   // keep the managed thread on this OS thread
        return SetThreadAffinityMask(GetCurrentThread(), (nuint)1 << core) != 0;
    }
}

internal sealed partial class LinuxAffinity : IThreadAffinity
{
    [LibraryImport("libc", SetLastError = true)]
    private static partial int sched_setaffinity(int pid, nint cpusetsize, ref ulong mask);

    public bool PinCurrentThread(int core)
    {
        if (core < 0 || core >= 64 || core >= Environment.ProcessorCount) return false;
        ulong mask = 1UL << core;
        return sched_setaffinity(0, sizeof(ulong), ref mask) == 0;   // pid 0 = calling thread
    }
}

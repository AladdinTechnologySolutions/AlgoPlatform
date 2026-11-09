using AlgoPlatform.Core.Collections;
using AlgoPlatform.Core.Threading;
using AlgoPlatform.Core.Timing;

namespace AlgoPlatform.Tests.Core;

public class LatencyHistogramTests
{
    [Fact]
    public void Percentiles_are_within_bucket_precision()
    {
        var h = new LatencyHistogram();
        for (long v = 1; v <= 1_000_000; v++) h.Record(v);
        Assert.Equal(1_000_000, h.Count);
        Assert.Equal(1_000_000, h.Max);
        Assert.Equal(1, h.Min);
        foreach (double p in new[] { 50.0, 90, 99, 99.9 })
        {
            double exact = p / 100 * 1_000_000;
            double got = h.Percentile(p);
            Assert.InRange(got, exact, exact * 1.035);   // upper bucket bound, <= ~3% above
        }
        Assert.Equal(1_000_000, h.Percentile(100));
    }

    [Fact]
    public void Small_values_are_exact_and_reset_clears()
    {
        var h = new LatencyHistogram();
        for (int i = 0; i < 10; i++) h.Record(42);
        Assert.Equal(42, h.Percentile(50));
        Assert.Equal(42, h.Percentile(99.9));
        h.Reset();
        Assert.Equal(0, h.Count);
        Assert.Equal(0, h.Percentile(99));
    }

    [Fact]
    public void Add_merges_histograms()
    {
        var a = new LatencyHistogram();
        var b = new LatencyHistogram();
        a.Record(100);
        b.Record(1_000_000);
        a.Add(b);
        Assert.Equal(2, a.Count);
        Assert.Equal(1_000_000, a.Max);
    }
}

public class LongMapTests
{
    [Fact]
    public void Matches_dictionary_under_random_operations()
    {
        var map = new LongMap<long>(64);
        var reference = new Dictionary<long, long>();
        var rng = new Random(7);
        for (int i = 0; i < 200_000; i++)
        {
            long key = rng.Next(0, 5_000) * 1_000_003L;
            switch (rng.Next(3))
            {
                case 0:
                    Assert.Equal(reference.TryAdd(key, i), map.TryAdd(key, i));
                    break;
                case 1:
                    Assert.Equal(reference.Remove(key), map.Remove(key));
                    break;
                default:
                    Assert.Equal(reference.TryGetValue(key, out long rv), map.TryGetValue(key, out long mv));
                    Assert.Equal(rv, mv);
                    break;
            }
            Assert.Equal(reference.Count, map.Count);
        }
        int seen = 0;
        for (int s = 0; s < map.SlotCount; s++)
        {
            if (!map.IsUsed(s)) continue;
            seen++;
            Assert.Equal(reference[map.KeyAt(s)], map.ValueAt(s));
        }
        Assert.Equal(reference.Count, seen);
    }

    [Fact]
    public void Presized_map_does_not_resize_or_allocate_in_steady_state()
    {
        var map = new LongMap<long>(1024);
        for (int i = 0; i < 1000; i++) map.TryAdd(i, i);
        for (int i = 0; i < 1000; i++) map.Remove(i);
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int round = 0; round < 1000; round++)
        {
            for (int i = 0; i < 500; i++) map.TryAdd(round * 1000 + i, i);
            for (int i = 0; i < 500; i++) map.Remove(round * 1000 + i);
        }
        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
        Assert.Equal(0, map.Resizes);
    }
}

public class ClockAndLoopTests
{
    [Fact]
    public void Monotonic_clock_never_goes_backwards_and_virtual_clock_is_explicit()
    {
        long prev = MonotonicClock.Now();
        for (int i = 0; i < 100_000; i++)
        {
            long now = MonotonicClock.Now();
            Assert.True(now >= prev);
            prev = now;
        }
        var v = new VirtualClock(100);
        Assert.Equal(100, v.NowNs);
        v.Advance(50);
        v.Set(1_000);
        Assert.Equal(1_000, v.NowNs);
    }

    private sealed class CountingWork(SpscRing<long> ring) : ILoopWork
    {
        public long Sum;
        public long Count;
        public int PollOnce()
        {
            int n = 0;
            while (ring.TryDequeue(out long v))
            {
                Sum += v;
                Count++;
                n++;
            }
            return n;
        }
    }

    [Theory]
    [InlineData(IdleMode.BusySpin)]
    [InlineData(IdleMode.SpinYield)]
    [InlineData(IdleMode.Backoff)]
    public void Event_loop_thread_processes_everything_with_each_idle_mode(IdleMode mode)
    {
        var ring = new SpscRing<long>(1024);
        var work = new CountingWork(ring);
        var loop = new EventLoopThread("test", work, mode, core: 0, affinity: ThreadAffinity.Create(enabled: true));
        loop.Start();
        long expectedSum = 0;
        for (long i = 1; i <= 100_000; i++)
        {
            while (!ring.TryEnqueue(i)) Thread.SpinWait(1);
            expectedSum += i;
        }
        Assert.True(TestSupport.WaitUntil(() => Volatile.Read(ref work.Count) == 100_000, TimeSpan.FromSeconds(10)));
        loop.RequestStop();
        Assert.True(loop.Join(TimeSpan.FromSeconds(2)));
        Assert.Equal(expectedSum, work.Sum);
    }
}

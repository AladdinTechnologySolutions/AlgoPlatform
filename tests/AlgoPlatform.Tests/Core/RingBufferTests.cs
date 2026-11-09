using AlgoPlatform.Core.Collections;

namespace AlgoPlatform.Tests.Core;

public class RingBufferTests
{
    private struct Item
    {
        public int Producer;
        public long Seq;
    }

    [Fact]
    public void Capacity_must_be_a_power_of_two()
    {
        Assert.Throws<ArgumentException>(() => new SpscRing<long>(100));
        Assert.Throws<ArgumentException>(() => new MpscRing<long>(3));
    }

    [Fact]
    public void Spsc_preserves_order_across_many_wraparounds_and_reports_full()
    {
        var ring = new SpscRing<long>(8);
        long next = 0, expected = 0;
        for (int round = 0; round < 10_000; round++)
        {
            int toWrite = round % 9;
            for (int i = 0; i < toWrite; i++)
                if (ring.TryEnqueue(next)) next++;
            Assert.True(ring.Count <= 8);
            int toRead = (round * 7) % 9;
            for (int i = 0; i < toRead && ring.TryDequeue(out long v); i++)
            {
                Assert.Equal(expected, v);
                expected++;
            }
        }
        while (ring.TryEnqueue(next)) next++;
        Assert.Equal(8, ring.Count);
        Assert.False(ring.TryEnqueue(-1));   // full
        while (ring.TryDequeue(out long v)) Assert.Equal(expected++, v);
        Assert.Equal(next, expected);
    }

    [Fact]
    public void Spsc_two_threads_five_million_items_no_loss_in_order()
    {
        const long n = 5_000_000;
        var ring = new SpscRing<long>(1024);
        var producer = new Thread(() =>
        {
            for (long i = 0; i < n; i++)
                while (!ring.TryEnqueue(i)) Thread.SpinWait(1);
        });
        producer.Start();
        long expected = 0;
        while (expected < n)
        {
            if (ring.TryDequeue(out long v))
            {
                Assert.Equal(expected, v);
                expected++;
            }
        }
        producer.Join();
        Assert.False(ring.TryDequeue(out _));
    }

    /// <summary>Acceptance test 1: 4 producers x 1 million events to one consumer, no loss, per-producer order preserved.</summary>
    [Fact]
    public void Mpsc_four_producers_one_million_each_no_loss_per_producer_order()
    {
        const int producers = 4;
        const long perProducer = 1_000_000;
        var ring = new MpscRing<Item>(1 << 16);
        var start = new ManualResetEventSlim();
        var threads = new Thread[producers];
        for (int p = 0; p < producers; p++)
        {
            int id = p;
            threads[p] = new Thread(() =>
            {
                start.Wait();
                for (long s = 0; s < perProducer; s++)
                {
                    var item = new Item { Producer = id, Seq = s };
                    while (!ring.TryEnqueue(item)) Thread.SpinWait(1);
                }
            });
            threads[p].Start();
        }
        start.Set();

        var next = new long[producers];
        long received = 0;
        while (received < producers * perProducer)
        {
            if (!ring.TryDequeue(out Item item)) continue;
            Assert.Equal(next[item.Producer], item.Seq);   // in order per producer, nothing skipped
            next[item.Producer]++;
            received++;
        }
        foreach (var t in threads) t.Join();
        Assert.All(next, c => Assert.Equal(perProducer, c));
        Assert.False(ring.TryDequeue(out _));   // and nothing duplicated
    }

    [Fact]
    public void Mpsc_reports_full_and_recovers()
    {
        var ring = new MpscRing<int>(4);
        for (int i = 0; i < 4; i++) Assert.True(ring.TryEnqueue(i));
        Assert.False(ring.TryEnqueue(99));
        Assert.True(ring.TryDequeue(out int first));
        Assert.Equal(0, first);
        Assert.True(ring.TryEnqueue(4));
        for (int i = 1; i <= 4; i++)
        {
            Assert.True(ring.TryDequeue(out int v));
            Assert.Equal(i, v);
        }
    }

    [Fact]
    public void Rings_release_references_after_dequeue()
    {
        var ring = new SpscRing<(int, object?)>(2);
        var weak = Fill(ring);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        Assert.False(weak.IsAlive);

        static WeakReference Fill(SpscRing<(int, object?)> r)
        {
            var o = new object();
            r.TryEnqueue((1, o));
            r.TryDequeue(out _);
            return new WeakReference(o);
        }
    }
}

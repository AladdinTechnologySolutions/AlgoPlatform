using System.Numerics;
using System.Runtime.CompilerServices;

namespace AlgoPlatform.Core.Timing;

/// <summary>
/// HdrHistogram-style log-linear histogram with fixed buckets: exact below 64 ns, then 32 linear
/// sub-buckets per power of two (worst-case relative error about 3%). Record is O(1) and allocation-free.
/// Single writer; readers on other threads see an approximately consistent view, which is fine for metrics.
/// </summary>
public sealed class LatencyHistogram
{
    private const int LinearLimit = 64;
    private const int SubBuckets = 32;
    private const int BucketCount = LinearLimit + 58 * SubBuckets;

    private readonly long[] _counts = new long[BucketCount];
    private long _count;
    private long _sum;
    private long _min = long.MaxValue;
    private long _max;

    public long Count => Volatile.Read(ref _count);
    public long Max => Volatile.Read(ref _max);
    public long Min => _count == 0 ? 0 : _min;
    public double Mean => _count == 0 ? 0 : (double)_sum / _count;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Record(long valueNs)
    {
        if (valueNs < 0) valueNs = 0;
        _counts[IndexOf(valueNs)]++;
        _sum += valueNs;
        if (valueNs > _max) _max = valueNs;
        if (valueNs < _min) _min = valueNs;
        Volatile.Write(ref _count, _count + 1);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int IndexOf(long v)
    {
        if (v < LinearLimit) return (int)v;
        int msb = 63 - BitOperations.LeadingZeroCount((ulong)v);   // >= 6
        int shift = msb - 5;                                       // keep the top 6 bits: sub in [32, 63]
        int sub = (int)(v >> shift);
        int idx = LinearLimit + (shift - 1) * SubBuckets + (sub - SubBuckets);
        return idx < BucketCount ? idx : BucketCount - 1;
    }

    private static long UpperBoundOf(int idx)
    {
        if (idx < LinearLimit) return idx;
        int rel = idx - LinearLimit;
        int shift = rel / SubBuckets + 1;
        long sub = rel % SubBuckets + SubBuckets;
        return ((sub + 1) << shift) - 1;
    }

    /// <summary>Value at the percentile (0-100), reported as the bucket's upper bound capped at Max.</summary>
    public long Percentile(double percentile)
    {
        long total = Count;
        if (total == 0) return 0;
        long target = (long)Math.Ceiling(percentile / 100.0 * total);
        if (target < 1) target = 1;
        long cumulative = 0;
        for (int i = 0; i < BucketCount; i++)
        {
            cumulative += _counts[i];
            if (cumulative >= target) return Math.Min(UpperBoundOf(i), Max);
        }
        return Max;
    }

    public void Reset()
    {
        Array.Clear(_counts);
        _sum = 0;
        _min = long.MaxValue;
        _max = 0;
        Volatile.Write(ref _count, 0);
    }

    public void Add(LatencyHistogram other)
    {
        for (int i = 0; i < BucketCount; i++) _counts[i] += other._counts[i];
        _count += other._count;
        _sum += other._sum;
        if (other._count > 0)
        {
            _max = Math.Max(_max, other._max);
            _min = Math.Min(_min, other._min);
        }
    }

    public string Summary(string unit = "us", double divisor = 1000.0) =>
        $"n={Count} p50={Percentile(50) / divisor:F2}{unit} p99={Percentile(99) / divisor:F2}{unit} " +
        $"p99.9={Percentile(99.9) / divisor:F2}{unit} max={Max / divisor:F2}{unit}";
}

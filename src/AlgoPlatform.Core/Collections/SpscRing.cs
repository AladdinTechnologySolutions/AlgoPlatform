using System.Numerics;
using System.Runtime.CompilerServices;

namespace AlgoPlatform.Core.Collections;

/// <summary>
/// Bounded single-producer / single-consumer ring buffer. Capacity is a power of two, so the slot
/// index is <c>sequence &amp; mask</c>. Producer and consumer cursors live on separate cache lines and
/// each side caches the other side's cursor, so the common case touches no shared line at all.
/// Lock-free and allocation-free after construction.
/// </summary>
public sealed class SpscRing<T> where T : struct
{
    private readonly T[] _buffer;
    private readonly long _mask;
    private ProducerCursor _producer;
    private ConsumerCursor _consumer;

    public SpscRing(int capacity)
    {
        if (capacity < 2 || !BitOperations.IsPow2(capacity))
            throw new ArgumentException("Capacity must be a power of two >= 2", nameof(capacity));
        _buffer = new T[capacity];
        _mask = capacity - 1;
    }

    public int Capacity => _buffer.Length;

    /// <summary>Approximate number of queued items (exact when called from either endpoint thread).</summary>
    public int Count => (int)(Volatile.Read(ref _producer.Tail) - Volatile.Read(ref _consumer.Head));

    /// <summary>Producer only.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(in T item)
    {
        long tail = _producer.Tail;
        if (tail - _producer.CachedHead >= _buffer.Length)
        {
            _producer.CachedHead = Volatile.Read(ref _consumer.Head);
            if (tail - _producer.CachedHead >= _buffer.Length) return false;
        }
        _buffer[tail & _mask] = item;
        Volatile.Write(ref _producer.Tail, tail + 1);   // release: publishes the slot write
        return true;
    }

    /// <summary>Consumer only.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(out T item)
    {
        long head = _consumer.Head;
        if (head >= _consumer.CachedTail)
        {
            _consumer.CachedTail = Volatile.Read(ref _producer.Tail);   // acquire
            if (head >= _consumer.CachedTail) { item = default; return false; }
        }
        ref T slot = ref _buffer[head & _mask];
        item = slot;
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>()) slot = default;   // do not keep payloads alive
        Volatile.Write(ref _consumer.Head, head + 1);
        return true;
    }
}

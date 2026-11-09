using System.Numerics;
using System.Runtime.CompilerServices;

namespace AlgoPlatform.Core.Collections;

/// <summary>
/// Bounded multi-producer / single-consumer ring (Vyukov-style). Producers claim a slot with a CAS on
/// the shared tail and publish it by writing the slot's sequence number; the consumer only reads slots
/// whose sequence says "published", so per-producer ordering is preserved and nothing is lost.
/// </summary>
public sealed class MpscRing<T> where T : struct
{
    private struct Slot
    {
        public long Sequence;
        public T Item;
    }

    private readonly Slot[] _slots;
    private readonly long _mask;
    private PaddedLong _tail;   // shared by producers
    private PaddedLong _head;   // written by the consumer only

    public MpscRing(int capacity)
    {
        if (capacity < 2 || !BitOperations.IsPow2(capacity))
            throw new ArgumentException("Capacity must be a power of two >= 2", nameof(capacity));
        _slots = new Slot[capacity];
        for (int i = 0; i < capacity; i++) _slots[i].Sequence = i;
        _mask = capacity - 1;
    }

    public int Capacity => _slots.Length;

    public int Count => (int)Math.Max(0, Volatile.Read(ref _tail.Value) - Volatile.Read(ref _head.Value));

    /// <summary>Any thread. Returns false when the ring is full.</summary>
    public bool TryEnqueue(in T item)
    {
        long pos = Volatile.Read(ref _tail.Value);
        while (true)
        {
            ref Slot slot = ref _slots[pos & _mask];
            long seq = Volatile.Read(ref slot.Sequence);
            long dif = seq - pos;
            if (dif == 0)
            {
                long seen = Interlocked.CompareExchange(ref _tail.Value, pos + 1, pos);
                if (seen == pos)
                {
                    slot.Item = item;
                    Volatile.Write(ref slot.Sequence, pos + 1);   // publish
                    return true;
                }
                pos = seen;
            }
            else if (dif < 0)
            {
                return false;   // slot still holds an unconsumed item from the previous lap: full
            }
            else
            {
                pos = Volatile.Read(ref _tail.Value);
            }
        }
    }

    /// <summary>Single consumer only.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(out T item)
    {
        long pos = _head.Value;
        ref Slot slot = ref _slots[pos & _mask];
        if (Volatile.Read(ref slot.Sequence) != pos + 1) { item = default; return false; }
        item = slot.Item;
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>()) slot.Item = default;
        Volatile.Write(ref slot.Sequence, pos + _slots.Length);   // free the slot for the next lap
        Volatile.Write(ref _head.Value, pos + 1);
        return true;
    }
}

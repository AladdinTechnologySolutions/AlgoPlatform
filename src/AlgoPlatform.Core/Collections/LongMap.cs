using System.Numerics;
using System.Runtime.CompilerServices;

namespace AlgoPlatform.Core.Collections;

/// <summary>
/// Open-addressing hash map from long to a struct value: linear probing, backward-shift delete.
/// Pre-sized at construction so steady-state use allocates nothing; it doubles (and therefore
/// allocates) only past 50% load, which is counted in <see cref="Resizes"/>. Single-threaded.
/// </summary>
public sealed class LongMap<TValue> where TValue : struct
{
    private long[] _keys;
    private TValue[] _values;
    private bool[] _used;
    private int _mask;
    private int _shift;

    public LongMap(int initialCapacity = 1024)
    {
        int cap = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, initialCapacity * 2));
        _keys = new long[cap];
        _values = new TValue[cap];
        _used = new bool[cap];
        _mask = cap - 1;
        _shift = 64 - BitOperations.Log2((uint)cap);
    }

    public int Count { get; private set; }
    public int Resizes { get; private set; }
    public int SlotCount => _keys.Length;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int Home(long key) => (int)(((ulong)key * 0x9E3779B97F4A7C15UL) >> _shift);

    private int Find(long key)
    {
        int i = Home(key);
        while (_used[i])
        {
            if (_keys[i] == key) return i;
            i = (i + 1) & _mask;
        }
        return -1;
    }

    public bool ContainsKey(long key) => Find(key) >= 0;

    public bool TryGetValue(long key, out TValue value)
    {
        int i = Find(key);
        if (i < 0) { value = default; return false; }
        value = _values[i];
        return true;
    }

    /// <summary>Reference to the value, or a null ref when absent (check with Unsafe.IsNullRef).</summary>
    public ref TValue GetValueRefOrNullRef(long key)
    {
        int i = Find(key);
        if (i < 0) return ref Unsafe.NullRef<TValue>();
        return ref _values[i];
    }

    /// <summary>Adds the key if absent. Returns false if it already existed.</summary>
    public bool TryAdd(long key, in TValue value)
    {
        if ((Count + 1) * 2 > _keys.Length) Grow();
        int i = Home(key);
        while (_used[i])
        {
            if (_keys[i] == key) return false;
            i = (i + 1) & _mask;
        }
        _used[i] = true;
        _keys[i] = key;
        _values[i] = value;
        Count++;
        return true;
    }

    public bool Remove(long key)
    {
        int i = Find(key);
        if (i < 0) return false;
        // Backward-shift deletion keeps probe chains intact without tombstones.
        int j = i;
        while (true)
        {
            j = (j + 1) & _mask;
            if (!_used[j]) break;
            int home = Home(_keys[j]);
            bool stays = i <= j ? (i < home && home <= j) : (i < home || home <= j);
            if (stays) continue;
            _keys[i] = _keys[j];
            _values[i] = _values[j];
            i = j;
        }
        _used[i] = false;
        _keys[i] = 0;
        _values[i] = default;
        Count--;
        return true;
    }

    // Allocation-free iteration: for (int i = 0; i < map.SlotCount; i++) if (map.IsUsed(i)) ...
    public bool IsUsed(int slot) => _used[slot];
    public long KeyAt(int slot) => _keys[slot];
    public ref TValue ValueAt(int slot) => ref _values[slot];

    private void Grow()
    {
        long[] oldKeys = _keys;
        TValue[] oldValues = _values;
        bool[] oldUsed = _used;
        int cap = oldKeys.Length * 2;
        _keys = new long[cap];
        _values = new TValue[cap];
        _used = new bool[cap];
        _mask = cap - 1;
        _shift = 64 - BitOperations.Log2((uint)cap);
        Count = 0;
        Resizes++;
        for (int i = 0; i < oldKeys.Length; i++)
            if (oldUsed[i]) TryAdd(oldKeys[i], oldValues[i]);
    }
}

/// <summary>Set of longs built on <see cref="LongMap{TValue}"/>.</summary>
public sealed class LongSet
{
    private readonly LongMap<byte> _map;
    public LongSet(int initialCapacity = 1024) => _map = new LongMap<byte>(initialCapacity);
    public int Count => _map.Count;
    public bool Add(long key) => _map.TryAdd(key, 0);
    public bool Contains(long key) => _map.ContainsKey(key);
}

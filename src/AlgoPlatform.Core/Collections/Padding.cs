using System.Runtime.InteropServices;

namespace AlgoPlatform.Core.Collections;

/// <summary>A long that sits alone on its own cache line(s). 64 bytes of padding either side also
/// covers the adjacent-line prefetcher on Intel (128-byte effective false-sharing distance).</summary>
[StructLayout(LayoutKind.Explicit, Size = 192)]
public struct PaddedLong
{
    [FieldOffset(64)] public long Value;
}

/// <summary>Producer-owned cursor plus the producer's cached copy of the consumer cursor.</summary>
[StructLayout(LayoutKind.Explicit, Size = 192)]
internal struct ProducerCursor
{
    [FieldOffset(64)] public long Tail;
    [FieldOffset(72)] public long CachedHead;
}

/// <summary>Consumer-owned cursor plus the consumer's cached copy of the producer cursor.</summary>
[StructLayout(LayoutKind.Explicit, Size = 192)]
internal struct ConsumerCursor
{
    [FieldOffset(64)] public long Head;
    [FieldOffset(72)] public long CachedTail;
}

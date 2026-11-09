using System.Buffers.Binary;
using System.IO.MemoryMappedFiles;

namespace AlgoPlatform.Core.Journal;

/// <summary>
/// Append-only binary journal backed by a memory-mapped file.
/// File: 16-byte header ("APJ1", version, reserved) then records of
/// [Seq:8][TimestampNs:8][Type:4][Length:4][payload:Length]. All integers little-endian.
/// The mapped region past the last record is zero, so a reader stops at the first Type == 0.
/// Single writer (the journal thread); never called from a strategy loop.
/// </summary>
public sealed unsafe class JournalWriter : IDisposable
{
    public const int Magic = 0x314A5041;   // "APJ1"
    public const int Version = 1;
    public const int FileHeaderSize = 16;
    public const int RecordHeaderSize = 24;

    private readonly FileStream _file;
    private MemoryMappedFile? _map;
    private MemoryMappedViewAccessor? _view;
    private byte* _base;
    private long _capacity;
    private long _position;
    private bool _disposed;

    public JournalWriter(string path, long initialCapacity = 4 * 1024 * 1024)
    {
        Path = path;
        Directory.CreateDirectory(System.IO.Path.GetDirectoryName(System.IO.Path.GetFullPath(path))!);
        _file = new FileStream(path, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
        Map(Math.Max(initialCapacity, 64 * 1024));
        var header = new Span<byte>(_base, FileHeaderSize);
        BinaryPrimitives.WriteInt32LittleEndian(header, Magic);
        BinaryPrimitives.WriteInt32LittleEndian(header[4..], Version);
        _position = FileHeaderSize;
    }

    public string Path { get; }
    public long Position => _position;
    public long RecordCount { get; private set; }

    public void Append(long seq, long timestampNs, int type, ReadOnlySpan<byte> payload)
    {
        Span<byte> dst = Reserve(payload.Length);
        payload.CopyTo(dst);
        Commit(seq, timestampNs, type, payload.Length);
    }

    /// <summary>Returns a span for the payload of the next record; follow with <see cref="Commit"/>.</summary>
    public Span<byte> Reserve(int maxPayload)
    {
        long needed = _position + RecordHeaderSize + maxPayload + 1;
        if (needed > _capacity) Remap(Math.Max(_capacity * 2, needed * 2));
        return new Span<byte>(_base + _position + RecordHeaderSize, maxPayload);
    }

    public void Commit(long seq, long timestampNs, int type, int payloadLength)
    {
        if (type == 0) throw new ArgumentException("Record type 0 is reserved as the end marker");
        var h = new Span<byte>(_base + _position, RecordHeaderSize);
        BinaryPrimitives.WriteInt64LittleEndian(h, seq);
        BinaryPrimitives.WriteInt64LittleEndian(h[8..], timestampNs);
        BinaryPrimitives.WriteInt32LittleEndian(h[20..], payloadLength);
        // Type last so a concurrent reader never sees a type with a half-written header.
        Volatile.Write(ref *(int*)(_base + _position + 16), type);
        _position += RecordHeaderSize + payloadLength;
        RecordCount++;
    }

    public void Flush() => _view?.Flush();

    private void Map(long capacity)
    {
        _file.SetLength(capacity);
        _map = MemoryMappedFile.CreateFromFile(_file, null, capacity, MemoryMappedFileAccess.ReadWrite,
            HandleInheritability.None, leaveOpen: true);
        _view = _map.CreateViewAccessor(0, capacity, MemoryMappedFileAccess.ReadWrite);
        byte* ptr = null;
        _view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
        _base = ptr + _view.PointerOffset;
        _capacity = capacity;
    }

    private void Unmap()
    {
        if (_view is null) return;
        _view.Flush();
        _view.SafeMemoryMappedViewHandle.ReleasePointer();
        _view.Dispose();
        _map!.Dispose();
        _view = null;
        _map = null;
        _base = null;
    }

    private void Remap(long capacity)
    {
        Unmap();
        Map(capacity);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        Unmap();
        _file.SetLength(_position);   // trim the unused mapped tail
        _file.Dispose();
    }
}

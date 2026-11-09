using System.Buffers.Binary;
using AlgoPlatform.Core.Events;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Core.Journal;

/// <summary>Payload objects that know their own journal encoding (e.g. reference data snapshots).</summary>
public interface IJournalBlob
{
    byte[] ToJournalBytes();
}

/// <summary>
/// Explicit field-by-field encoding of events and commands. Struct memory is never copied raw, so
/// padding bytes cannot leak into the journal and outputs are byte-for-byte comparable (FR-9).
/// Record type = EventType for inputs, OutputBase + CommandType for outputs.
/// </summary>
public static class JournalCodec
{
    public const int OutputBase = 100;
    public const int MaxFixedPayload = 128;

    public static int OutputType(CommandType t) => OutputBase + (int)t;

    /// <summary>Encodes the fixed part of an input event. Blob payloads (ref data, async, session) are appended by the caller.</summary>
    public static int EncodeInput(in HostEvent e, Span<byte> dst)
    {
        switch (e.Type)
        {
            case EventType.Tick:
            {
                ref readonly Tick t = ref e.Tick;
                W64(dst, 0, t.Seq); W64(dst, 8, t.TimestampNs); W32(dst, 16, t.Instrument.Value);
                W64(dst, 20, t.BidPx); W64(dst, 28, t.BidQty); W64(dst, 36, t.AskPx); W64(dst, 44, t.AskQty);
                W64(dst, 52, t.LastPx); W64(dst, 60, t.LastQty);
                return 68;
            }
            case EventType.Execution:
            {
                ref readonly ExecutionReport r = ref e.Exec;
                W64(dst, 0, r.Seq); W64(dst, 8, r.OrderId); W64(dst, 16, r.ExecId); W32(dst, 24, r.Instrument.Value);
                dst[28] = (byte)r.Side; dst[29] = (byte)r.Status;
                W64(dst, 30, r.LastPx); W64(dst, 38, r.LastQty); W64(dst, 46, r.CumQty); W64(dst, 54, r.LeavesQty);
                W32(dst, 62, r.RejectCode);
                return 66;
            }
            case EventType.SessionStart:
                return 0;
            default:
                W64(dst, 0, e.A); W64(dst, 8, e.B);
                return 16;
        }
    }

    public static byte[]? BlobOf(in HostEvent e) => e.Payload switch
    {
        byte[] b => b,
        IJournalBlob j => j.ToJournalBytes(),
        _ => null,
    };

    /// <summary>Decodes an input record. <paramref name="decodeBlob"/> turns a ref-data blob back into a snapshot object.</summary>
    public static HostEvent DecodeInput(in JournalRecord rec, Func<EventType, byte[], object?>? decodeBlob = null)
    {
        var s = rec.Payload.Span;
        var e = new HostEvent { Type = (EventType)rec.Type, Seq = rec.Seq, TimestampNs = rec.TimestampNs };
        switch (e.Type)
        {
            case EventType.Tick:
                e.Tick = new Tick(R64(s, 0), R64(s, 8), new InstrumentId(R32(s, 16)), R64(s, 20), R64(s, 28),
                    R64(s, 36), R64(s, 44), R64(s, 52), R64(s, 60));
                break;
            case EventType.Execution:
                e.Exec = new ExecutionReport(R64(s, 0), R64(s, 8), R64(s, 16), new InstrumentId(R32(s, 24)),
                    (Side)s[28], (OrdStatus)s[29], R64(s, 30), R64(s, 38), R64(s, 46), R64(s, 54), R32(s, 62));
                break;
            case EventType.SessionStart:
                e.Payload = s.ToArray();
                break;
            default:
                e.A = R64(s, 0);
                e.B = R64(s, 8);
                if (s.Length > 16)
                {
                    byte[] blob = s[16..].ToArray();
                    e.Payload = decodeBlob is null ? blob : decodeBlob(e.Type, blob);
                }
                break;
        }
        return e;
    }

    public static int EncodeOutput(in OrderCommand c, Span<byte> dst)
    {
        dst[0] = (byte)c.Type;
        W32(dst, 1, c.StrategyId); W64(dst, 5, c.OrderId); W32(dst, 13, c.Instrument.Value);
        dst[17] = (byte)c.Side; dst[18] = (byte)c.OrdType; dst[19] = (byte)c.Tif;
        W64(dst, 20, c.Px); W64(dst, 28, c.Qty); W32(dst, 36, c.RejectCode);
        return 40;
    }

    public static OrderCommand DecodeOutput(in JournalRecord rec)
    {
        var s = rec.Payload.Span;
        return new OrderCommand
        {
            Type = (CommandType)s[0], StrategyId = R32(s, 1), OrderId = R64(s, 5), Instrument = new InstrumentId(R32(s, 13)),
            Side = (Side)s[17], OrdType = (OrdType)s[18], Tif = (Tif)s[19], Px = R64(s, 20), Qty = R64(s, 28),
            RejectCode = R32(s, 36), CauseSeq = rec.Seq, TimestampNs = rec.TimestampNs,
        };
    }

    private static void W64(Span<byte> d, int o, long v) => BinaryPrimitives.WriteInt64LittleEndian(d[o..], v);
    private static void W32(Span<byte> d, int o, int v) => BinaryPrimitives.WriteInt32LittleEndian(d[o..], v);
    private static long R64(ReadOnlySpan<byte> s, int o) => BinaryPrimitives.ReadInt64LittleEndian(s[o..]);
    private static int R32(ReadOnlySpan<byte> s, int o) => BinaryPrimitives.ReadInt32LittleEndian(s[o..]);
}

using System.Buffers.Text;
using System.Text;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Gateway.Fix;

/// <summary>Tags and message types of the v1 FIX-like protocol (spec section 9).</summary>
public static class FixTags
{
    public const byte Soh = 0x01;

    public const int BeginString = 8, BodyLength = 9, CheckSum = 10, MsgType = 35, SenderCompId = 49, TargetCompId = 56,
        MsgSeqNum = 34, PossDupFlag = 43, ClOrdId = 11, OrigClOrdId = 41, OrderId = 37, ExecId = 17, Symbol = 55,
        Side = 54, OrderQty = 38, OrdType = 40, Price = 44, TimeInForce = 59, OrdStatus = 39, ExecType = 150,
        CumQty = 14, LeavesQty = 151, LastPx = 31, LastQty = 32, BeginSeqNo = 7, EndSeqNo = 16, NewSeqNo = 36,
        GapFillFlag = 123, HeartBtInt = 108, TestReqId = 112, CxlRejResponseTo = 434, OrdRejReason = 103, Text = 58;
}

public static class FixMsgType
{
    public const byte Heartbeat = (byte)'0', TestRequest = (byte)'1', ResendRequest = (byte)'2', Reject = (byte)'3',
        SequenceReset = (byte)'4', Logout = (byte)'5', ExecutionReport = (byte)'8', CancelReject = (byte)'9',
        Logon = (byte)'A', NewOrderSingle = (byte)'D', CancelRequest = (byte)'F', ReplaceRequest = (byte)'G',
        OrderStatusRequest = (byte)'H';
}

/// <summary>OrdRejReason values used by the simulator.</summary>
public static class FixRejectReason
{
    public const int UnknownOrder = 5;
    public const int DuplicateOrder = 6;
    public const int Other = 99;
}

/// <summary>Conversions between internal enums and FIX characters.</summary>
public static class FixEnums
{
    public static byte SideChar(Side s) => s == Side.Buy ? (byte)'1' : (byte)'2';
    public static Side ParseSide(byte c) => c == (byte)'2' ? Side.Sell : Side.Buy;
    public static byte OrdTypeChar(OrdType t) => t == OrdType.Market ? (byte)'1' : (byte)'2';
    public static OrdType ParseOrdType(byte c) => c == (byte)'1' ? OrdType.Market : OrdType.Limit;
    public static byte TifChar(Tif t) => t switch { Tif.Ioc => (byte)'3', Tif.Fok => (byte)'4', _ => (byte)'0' };
    public static Tif ParseTif(byte c) => c switch { (byte)'3' => Tif.Ioc, (byte)'4' => Tif.Fok, _ => Tif.Day };

    /// <summary>Exchange OrdStatus (tag 39) to the internal status.</summary>
    public static OrdStatus ParseOrdStatus(byte c) => c switch
    {
        (byte)'1' => OrdStatus.PartiallyFilled,
        (byte)'2' => OrdStatus.Filled,
        (byte)'4' => OrdStatus.Cancelled,
        (byte)'8' => OrdStatus.Rejected,
        _ => OrdStatus.Working,   // 0 New, 6 PendingCancel, E PendingReplace, A PendingNew: all live
    };
}

/// <summary>
/// Builds one message at a time into pre-allocated buffers: body first, then BeginString, BodyLength
/// and CheckSum around it. Integers and prices are formatted with Utf8Formatter; nothing allocates.
/// </summary>
public sealed class FixWriter
{
    private static readonly byte[] s_begin = "8=FIX.4.4\u00019="u8.ToArray();
    private readonly byte[] _body = new byte[4096];
    private readonly byte[] _msg = new byte[4096 + 64];
    private int _len;

    public FixWriter Begin(byte msgType)
    {
        _len = 0;
        return AddChar(FixTags.MsgType, msgType);
    }

    public FixWriter Add(int tag, long value)
    {
        WriteTag(tag);
        Utf8Formatter.TryFormat(value, _body.AsSpan(_len), out int w);
        _len += w;
        _body[_len++] = FixTags.Soh;
        return this;
    }

    public FixWriter AddChar(int tag, byte value)
    {
        WriteTag(tag);
        _body[_len++] = value;
        _body[_len++] = FixTags.Soh;
        return this;
    }

    public FixWriter Add(int tag, ReadOnlySpan<byte> value)
    {
        WriteTag(tag);
        value.CopyTo(_body.AsSpan(_len));
        _len += value.Length;
        _body[_len++] = FixTags.Soh;
        return this;
    }

    /// <summary>Fixed-point price (scale 10^4) written as a decimal with four places, e.g. 101.2500.</summary>
    public FixWriter AddPrice(int tag, long px)
    {
        WriteTag(tag);
        if (px < 0)
        {
            _body[_len++] = (byte)'-';
            px = -px;
        }
        Utf8Formatter.TryFormat(px / Price.Scale, _body.AsSpan(_len), out int w);
        _len += w;
        _body[_len++] = (byte)'.';
        long frac = px % Price.Scale;
        for (long d = Price.Scale / 10; d > 0; d /= 10)
        {
            _body[_len++] = (byte)('0' + frac / d);
            frac %= d;
        }
        _body[_len++] = FixTags.Soh;
        return this;
    }

    /// <summary>Completes the message; the returned span is valid until the next Begin.</summary>
    public ReadOnlySpan<byte> Finish()
    {
        int p = 0;
        s_begin.CopyTo(_msg, 0);
        p += s_begin.Length;
        Utf8Formatter.TryFormat(_len, _msg.AsSpan(p), out int w);
        p += w;
        _msg[p++] = FixTags.Soh;
        Buffer.BlockCopy(_body, 0, _msg, p, _len);
        p += _len;
        int sum = 0;
        for (int i = 0; i < p; i++) sum += _msg[i];
        sum &= 0xFF;
        _msg[p++] = (byte)'1';
        _msg[p++] = (byte)'0';
        _msg[p++] = (byte)'=';
        _msg[p++] = (byte)('0' + sum / 100);
        _msg[p++] = (byte)('0' + sum / 10 % 10);
        _msg[p++] = (byte)('0' + sum % 10);
        _msg[p++] = FixTags.Soh;
        return _msg.AsSpan(0, p);
    }

    private void WriteTag(int tag)
    {
        Utf8Formatter.TryFormat(tag, _body.AsSpan(_len), out int w);
        _len += w;
        _body[_len++] = (byte)'=';
    }
}

/// <summary>A parsed message: a copy of the bytes plus an index of (tag, value offset, value length).</summary>
public sealed class FixMessage
{
    private const int MaxFields = 128;
    private readonly byte[] _data = new byte[8192];
    private readonly int[] _tags = new int[MaxFields];
    private readonly int[] _starts = new int[MaxFields];
    private readonly int[] _lens = new int[MaxFields];
    private int _count;
    private int _length;

    public byte MsgType { get; private set; }
    public int FieldCount => _count;
    public ReadOnlySpan<byte> Raw => _data.AsSpan(0, _length);

    public bool Load(ReadOnlySpan<byte> raw)
    {
        if (raw.Length > _data.Length) return false;
        raw.CopyTo(_data);
        _length = raw.Length;
        _count = 0;
        MsgType = 0;
        int i = 0;
        while (i < _length && _count < MaxFields)
        {
            int tag = 0;
            while (i < _length && _data[i] != (byte)'=')
            {
                tag = tag * 10 + (_data[i] - '0');
                i++;
            }
            i++;
            int start = i;
            while (i < _length && _data[i] != FixTags.Soh) i++;
            _tags[_count] = tag;
            _starts[_count] = start;
            _lens[_count] = i - start;
            if (tag == FixTags.MsgType && i > start) MsgType = _data[start];
            _count++;
            i++;
        }
        return MsgType != 0;
    }

    private int IndexOf(int tag)
    {
        for (int i = 0; i < _count; i++) if (_tags[i] == tag) return i;
        return -1;
    }

    public bool Has(int tag) => IndexOf(tag) >= 0;

    public ReadOnlySpan<byte> GetBytes(int tag)
    {
        int i = IndexOf(tag);
        return i < 0 ? default : _data.AsSpan(_starts[i], _lens[i]);
    }

    public bool TryGetLong(int tag, out long value)
    {
        value = 0;
        int i = IndexOf(tag);
        return i >= 0 && Utf8Parser.TryParse(_data.AsSpan(_starts[i], _lens[i]), out value, out _);
    }

    public long GetLong(int tag, long defaultValue = 0) => TryGetLong(tag, out long v) ? v : defaultValue;

    public byte GetChar(int tag, byte defaultValue = 0)
    {
        int i = IndexOf(tag);
        return i < 0 || _lens[i] == 0 ? defaultValue : _data[_starts[i]];
    }

    /// <summary>Parses a decimal price into fixed point (scale 10^4), truncating extra decimals.</summary>
    public bool TryGetPrice(int tag, out long px)
    {
        px = 0;
        int i = IndexOf(tag);
        if (i < 0) return false;
        return TryParsePrice(_data.AsSpan(_starts[i], _lens[i]), out px);
    }

    public static bool TryParsePrice(ReadOnlySpan<byte> s, out long px)
    {
        px = 0;
        if (s.IsEmpty) return false;
        int i = 0;
        bool neg = s[0] == (byte)'-';
        if (neg) i++;
        long ip = 0, frac = 0;
        int fracDigits = 0;
        bool any = false;
        for (; i < s.Length && s[i] != (byte)'.'; i++)
        {
            if ((uint)(s[i] - '0') > 9) return false;
            ip = ip * 10 + (s[i] - '0');
            any = true;
        }
        if (i < s.Length) i++;
        for (; i < s.Length; i++)
        {
            if ((uint)(s[i] - '0') > 9) return false;
            if (fracDigits < 4)
            {
                frac = frac * 10 + (s[i] - '0');
                fracDigits++;
            }
            any = true;
        }
        for (; fracDigits < 4; fracDigits++) frac *= 10;
        px = ip * Price.Scale + frac;
        if (neg) px = -px;
        return any;
    }

    /// <summary>All fields as strings (simulator and diagnostics only; allocates).</summary>
    public List<(int Tag, string Value)> Fields()
    {
        var list = new List<(int, string)>(_count);
        for (int i = 0; i < _count; i++) list.Add((_tags[i], Encoding.ASCII.GetString(_data, _starts[i], _lens[i])));
        return list;
    }

    public override string ToString() => Encoding.ASCII.GetString(_data, 0, _length).Replace('\u0001', '|');
}

/// <summary>Splits a TCP byte stream into whole messages using BodyLength (tag 9).</summary>
public sealed class FixFramer
{
    private readonly byte[] _buf = new byte[1 << 20];
    private int _start;
    private int _end;

    public int Buffered => _end - _start;

    public void Reset()
    {
        _start = 0;
        _end = 0;
    }

    /// <summary>Space to receive into. Compacts when the tail runs out.</summary>
    public Span<byte> WritableSpan()
    {
        if (_buf.Length - _end < 64 * 1024 && _start > 0)
        {
            Buffer.BlockCopy(_buf, _start, _buf, 0, _end - _start);
            _end -= _start;
            _start = 0;
        }
        return _buf.AsSpan(_end);
    }

    public void Advance(int n) => _end += n;

    public bool TryRead(FixMessage msg)
    {
        while (true)
        {
            var s = _buf.AsSpan(_start, _end - _start);
            if (s.Length < 20) return false;
            if (s[0] != (byte)'8' || s[1] != (byte)'=')
            {
                // Resynchronise on the next BeginString.
                int next = s[1..].IndexOf("8=FIX"u8);
                if (next < 0)
                {
                    _start = _end - 4;
                    return false;
                }
                _start += next + 1;
                continue;
            }
            int soh = s.IndexOf(FixTags.Soh);
            if (soh < 0) return false;
            if (s.Length < soh + 3 || s[soh + 1] != (byte)'9' || s[soh + 2] != (byte)'=')
            {
                _start++;
                continue;
            }
            int lenStart = soh + 3;
            int lenEnd = s[lenStart..].IndexOf(FixTags.Soh);
            if (lenEnd < 0) return false;
            if (!Utf8Parser.TryParse(s.Slice(lenStart, lenEnd), out int bodyLen, out _))
            {
                _start++;
                continue;
            }
            int total = lenStart + lenEnd + 1 + bodyLen + 7;   // + "10=nnn<SOH>"
            if (s.Length < total) return false;
            bool ok = msg.Load(s[..total]);
            _start += total;
            if (ok) return true;
        }
    }
}

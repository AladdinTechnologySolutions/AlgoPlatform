using System.Runtime.InteropServices;
using AlgoPlatform.Core.Collections;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Core.Events;

/// <summary>Kinds of events that enter a strategy loop. Values are part of the journal format.</summary>
public enum EventType : byte
{
    None = 0,
    Tick = 1,
    Execution = 2,
    Timer = 3,
    ReferenceData = 4,
    AsyncResult = 5,
    Control = 6,
    InstrumentStatus = 7,
    ExchangeStatus = 8,
    SessionStart = 9,
}

/// <summary>Control commands delivered through the inbox (and therefore journalled).</summary>
public enum ControlCode : byte
{
    None = 0,
    Kill = 1,              // stop new orders, cancel working orders
    KillAndFlatten = 2,    // as Kill, then send market orders to flatten positions
    Stop = 3,              // OnStop, cancel working orders, exit the loop
    MarkSlow = 4,          // handler budget breached repeatedly or inbox overflow: kill and stop callbacks
}

/// <summary>
/// Fixed-size event passed through the inbox rings. One struct for every kind keeps the rings
/// monomorphic; only the fields relevant to <see cref="Type"/> are meaningful. Tick and Exec share
/// storage (an event is never both), which keeps the struct small: it is copied once per subscriber
/// on fan-out, so its size is the main cost of the feed handler (see PERFORMANCE.md).
/// </summary>
[StructLayout(LayoutKind.Explicit)]
public struct HostEvent
{
    /// <summary>Set on events a loop generates for itself deterministically (e.g. a risk reject of a Replace).
    /// They are re-generated on replay, so they are not journalled.</summary>
    public const byte FlagInternal = 1;

    [FieldOffset(0)] public object? Payload;    // RefData: snapshot | Async: byte[] | SessionStart: byte[] (session json)
    [FieldOffset(8)] public EventType Type;
    [FieldOffset(9)] public byte Flags;
    [FieldOffset(16)] public long Seq;          // host sequencer number (stamped at the host boundary)
    [FieldOffset(24)] public long TimestampNs;  // event time: monotonic clock at sequencing; becomes ctx.NowNs
    [FieldOffset(32)] public long PublishNs;    // latency probe only: when the producer published (or intended to). Never journalled.
    [FieldOffset(40)] public long A;            // Timer: id | Control: code | InstrumentStatus: instrument | ExchangeStatus: 1 up / 0 down | Async: request id | RefData: version
    [FieldOffset(48)] public long B;            // Control: reason | InstrumentStatus: 1 stale / 0 ok
    [FieldOffset(56)] public Tick Tick;
    [FieldOffset(56)] public ExecutionReport Exec;
}

/// <summary>What a strategy asks the gateway to do. Also the "output" record in the journal.</summary>
public enum CommandType : byte
{
    New = 1,
    Cancel = 2,
    Replace = 3,
    CancelAllForStrategy = 4,   // issued by the watchdog when a loop is hung
}

public struct OrderCommand
{
    public CommandType Type;
    public int StrategyId;
    public long OrderId;
    public InstrumentId Instrument;
    public Side Side;
    public OrdType OrdType;
    public Tif Tif;
    public long Px;
    public long Qty;
    public int RejectCode;      // non-zero: rejected by pre-trade risk, journalled but never sent
    public long CauseSeq;       // Seq of the input event being handled when the command was created
    public long TimestampNs;    // event time when created
}

/// <summary>Order ids are (strategyId &lt;&lt; 40) | per-strategy counter, so they are deterministic per
/// strategy and the gateway can route reports without a lookup.</summary>
public static class OrderIds
{
    public const int StrategyShift = 40;
    public static long Make(int strategyId, long counter) => ((long)strategyId << StrategyShift) | counter;
    public static int StrategyOf(long orderId) => (int)(orderId >> StrategyShift);
}

/// <summary>Host-wide sequence number source (spec 8.3): one atomic counter at the host boundary.</summary>
public sealed class Sequencer
{
    private PaddedLong _seq;
    public long Next() => Interlocked.Increment(ref _seq.Value);
    public long Current => Volatile.Read(ref _seq.Value);
}

/// <summary>Anything that can deliver events into a strategy loop.</summary>
public interface IEventTarget
{
    int StrategyId { get; }
    /// <summary>Exec reports, control, async results. Never drops: returns false only if the strategy is gone.</summary>
    bool PostControl(in HostEvent evt);
}

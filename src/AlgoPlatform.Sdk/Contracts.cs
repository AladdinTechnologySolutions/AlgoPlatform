namespace AlgoPlatform.Sdk;

// Public SDK contracts (spec section 7). Changes here need a note in docs/DESIGN_DECISIONS.md.

public readonly record struct InstrumentId(int Value);

public enum Side : byte { Buy = 1, Sell = 2 }

public enum OrdType : byte { Market = 1, Limit = 2 }

public enum Tif : byte { Day = 0, Ioc = 3, Fok = 4 }

// PendingNew appended (D-03); existing values unchanged.
public enum OrdStatus : byte { New, Working, PartiallyFilled, Filled, Cancelled, Rejected, PendingCancel, PendingReplace, PendingNew }

public enum LogLevel : byte { Trace, Debug, Info, Warn, Error }

/// <summary>Top of book plus last trade. Prices are fixed-point integers (price * 10^4), see <see cref="Price"/>.
/// A tick with LastQty &gt; 0 carries a trade and is never conflated.</summary>
public readonly record struct Tick(long Seq, long TimestampNs, InstrumentId Instrument, long BidPx, long BidQty, long AskPx, long AskQty, long LastPx, long LastQty)
{
    public bool IsTrade => LastQty > 0;
}

public readonly record struct ExecutionReport(
    long Seq, long OrderId, long ExecId, InstrumentId Instrument, Side Side,
    OrdStatus Status, long LastPx, long LastQty, long CumQty, long LeavesQty, int RejectCode);

public readonly record struct NewOrder(InstrumentId Instrument, Side Side, OrdType Type, Tif Tif, long Px, long Qty);

public interface IStrategy
{
    void OnStart(IStrategyContext ctx);
    void OnTick(in Tick tick);
    void OnExecution(in ExecutionReport report);
    void OnTimer(long timerId);
    void OnReferenceDataChanged(IReferenceData snapshot);
    void OnAsyncResult(long requestId, ReadOnlySpan<byte> payload);
    void OnStop();
}

public interface IStrategyContext
{
    long NowNs { get; }                                   // time of the event being handled; virtual clock in replay
    IReferenceData Reference { get; }                     // current immutable snapshot
    Position GetPosition(InstrumentId id);
    OrderHandle Submit(in NewOrder order);                // runs pre-trade risk inline; returns Rejected handle on failure
    void Cancel(long orderId);
    void Replace(long orderId, long newPx, long newQty);
    void SetTimer(long timerId, long afterNs);            // afterNs < 0 cancels the timer
    long RunAsync(Func<byte[]> work);                     // off-loop; result returns via OnAsyncResult
    void Log(LogLevel level, string template, long a = 0, long b = 0);   // non-allocating, buffered

    // Additions (D-04): manifest data the strategy needs without touching files.
    IReadOnlyList<InstrumentId> Instruments { get; }
    double GetParameter(string name, double defaultValue);
}

public interface IReferenceData
{
    int Version { get; }
    bool TryGet(InstrumentId id, out InstrumentInfo info);
    bool TryGetBySymbol(string symbol, out InstrumentInfo info);   // addition (D-04)
}

public readonly record struct InstrumentInfo(InstrumentId Id, string Symbol, long TickSize, long LotSize, long MinPx, long MaxPx, bool Tradable);

public readonly record struct Position(long NetQty, long AvgPx, long RealisedPnl);

public readonly record struct OrderHandle(long OrderId, bool Accepted, int RejectCode);

/// <summary>Fixed-point price helpers. All prices on the hot path are long = price * Scale.</summary>
public static class Price
{
    public const long Scale = 10_000;
    public static long FromDouble(double px) => (long)Math.Round(px * Scale);
    public static double ToDouble(long px) => (double)px / Scale;
}

/// <summary>Reject codes returned in <see cref="OrderHandle.RejectCode"/> and <see cref="ExecutionReport.RejectCode"/>.</summary>
public static class RejectCodes
{
    public const int None = 0;
    public const int KillSwitch = 1;
    public const int MaxOrderQty = 2;
    public const int MaxNotional = 3;
    public const int MaxPosition = 4;
    public const int MaxStrategyPosition = 5;
    public const int PriceBand = 6;
    public const int RateLimit = 7;
    public const int FatFinger = 8;
    public const int MissingReferenceData = 9;
    public const int NotTradable = 10;
    public const int StaleMarketData = 11;
    public const int ExchangeDown = 12;
    public const int InvalidOrder = 13;
    public const int UnknownOrder = 14;
    public const int ExchangeReject = 15;
    public const int UnknownAtExchange = 16;
    public const int StrategyNotRunning = 17;

    public static string Name(int code) => code switch
    {
        None => "None", KillSwitch => "KillSwitch", MaxOrderQty => "MaxOrderQty", MaxNotional => "MaxNotional",
        MaxPosition => "MaxPosition", MaxStrategyPosition => "MaxStrategyPosition", PriceBand => "PriceBand",
        RateLimit => "RateLimit", FatFinger => "FatFinger", MissingReferenceData => "MissingReferenceData",
        NotTradable => "NotTradable", StaleMarketData => "StaleMarketData", ExchangeDown => "ExchangeDown",
        InvalidOrder => "InvalidOrder", UnknownOrder => "UnknownOrder", ExchangeReject => "ExchangeReject",
        UnknownAtExchange => "UnknownAtExchange", StrategyNotRunning => "StrategyNotRunning", _ => "Code" + code,
    };
}

/// <summary>Convenience base class so strategies only override what they need.</summary>
public abstract class StrategyBase : IStrategy
{
    protected IStrategyContext Ctx { get; private set; } = null!;
    public virtual void OnStart(IStrategyContext ctx) => Ctx = ctx;
    public virtual void OnTick(in Tick tick) { }
    public virtual void OnExecution(in ExecutionReport report) { }
    public virtual void OnTimer(long timerId) { }
    public virtual void OnReferenceDataChanged(IReferenceData snapshot) { }
    public virtual void OnAsyncResult(long requestId, ReadOnlySpan<byte> payload) { }
    public virtual void OnStop() { }
}

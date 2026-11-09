using System.Text.Json;
using AlgoPlatform.Core.Collections;
using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Journal;
using AlgoPlatform.Core.Logging;
using AlgoPlatform.Core.Timing;
using AlgoPlatform.Oms;
using AlgoPlatform.Risk;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Host.Engine;

public sealed class EngineOptions
{
    /// <summary>Replay: timers and async work are not executed (their results come from the journal) and the
    /// handler budget is not enforced (a budget kill in the original run is itself a journalled event).</summary>
    public bool ReplayMode { get; init; }
    public long HandlerBudgetNs { get; init; } = 20_000;
    public int MaxConsecutiveBreaches { get; init; } = 10;
    public bool RetainTerminalOrders { get; init; }
    /// <summary>Real monotonic clock, used only for metrics and the budget. Never visible to the strategy.</summary>
    public IClock Clock { get; init; } = MonotonicClock.Instance;
    public LogBuffer? Log { get; init; }
    /// <summary>Runs RunAsync work off the loop; (strategyId, requestId, work).</summary>
    public Action<int, long, Func<byte[]>>? AsyncRunner { get; init; }
}

/// <summary>
/// Everything that happens for one strategy on its loop thread: the <see cref="IStrategyContext"/> it
/// sees, with pre-trade risk and the OMS inline, plus event dispatch, fault isolation, kill handling and
/// journalling. The same class runs live in a host and in replay; only the sinks and options differ.
///
/// Determinism rule: everything that can change an output is either an input event (journalled before
/// it is handled) or a pure function of earlier inputs. ctx.NowNs is the event's timestamp, not the
/// wall clock, and decisions the loop makes for timing reasons (timers, budget kills) are injected as
/// events so they are journalled too.
/// </summary>
public sealed class StrategyEngine : IStrategyContext
{
    public const int MaxInstrumentId = 4095;

    private readonly IStrategy _strategy;
    private readonly IOrderSink _sink;
    private readonly IJournalSink _journal;
    private readonly EngineOptions _options;
    private readonly IClock _clock;
    private readonly SpscRing<HostEvent> _self = new(1024);
    private readonly long[] _liveBuffer = new long[4096];
    private InstrumentId[] _instruments = [];

    private IReferenceData? _ref;
    private long _now;
    private long _currentSeq;
    private long _currentPublishNs;
    private long _handlerStartNs;
    private long _asyncCounter;
    private int _consecutiveBreaches;
    private int _status;

    public StrategyEngine(int strategyId, StrategyManifest manifest, IStrategy strategy, IOrderSink sink, IJournalSink journal,
        EngineOptions? options = null)
    {
        StrategyId = strategyId;
        Manifest = manifest;
        _strategy = strategy;
        _sink = sink;
        _journal = journal;
        _options = options ?? new EngineOptions();
        _clock = _options.Clock;
        Oms = new OrderManager(strategyId, MaxInstrumentId) { RetainTerminalOrders = _options.RetainTerminalOrders };
        Risk = new PreTradeRisk(manifest.Limits, manifest.Instruments.Length, MaxInstrumentId);
    }

    public int StrategyId { get; }
    public StrategyManifest Manifest { get; }
    public OrderManager Oms { get; }
    public PreTradeRisk Risk { get; }
    public TimerTable Timers { get; } = new();
    public StrategyMetrics Metrics { get; } = new();
    public StrategyStatus Status => (StrategyStatus)Volatile.Read(ref _status);
    public bool CallbacksEnabled { get; private set; }
    public string? LastError { get; private set; }
    public long StopRequestedNs { get; private set; }

    /// <summary>Monotonic time the current handler started, 0 when idle. Read by the watchdog thread.</summary>
    public long HandlerStartNs => Volatile.Read(ref _handlerStartNs);

    /// <summary>Raised on the loop thread when the status changes.</summary>
    public Action<StrategyEngine, StrategyStatus>? StatusChanged { get; set; }

    // ------------------------------------------------------------------ event processing

    /// <summary>Handles one inbox event, then any internal events it caused, before returning.</summary>
    public void Process(ref HostEvent e)
    {
        ProcessOne(ref e);
        while (_self.TryDequeue(out HostEvent s)) ProcessOne(ref s);
    }

    private void ProcessOne(ref HostEvent e)
    {
        if ((e.Flags & HostEvent.FlagInternal) == 0) _journal.Input(in e);
        Metrics.Events++;
        _now = e.TimestampNs;
        _currentSeq = e.Seq;
        _currentPublishNs = e.Type == EventType.Tick ? e.PublishNs : 0;

        long t0 = _clock.NowNs;
        Volatile.Write(ref _handlerStartNs, t0);
        bool invoked = false;
        try
        {
            switch (e.Type)
            {
                case EventType.Tick:
                    Metrics.Ticks++;
                    Risk.OnTick(in e.Tick);
                    if (e.PublishNs != 0) Metrics.InboxLatency.Record(t0 - e.PublishNs);
                    if (CallbacksEnabled)
                    {
                        invoked = true;
                        _strategy.OnTick(in e.Tick);
                    }
                    break;
                case EventType.Execution:
                    Metrics.ExecutionReports++;
                    // Internal reports (local replace rejects) only inform the strategy; the order itself did not change.
                    if ((e.Flags & HostEvent.FlagInternal) == 0 && Oms.Apply(in e.Exec) != ApplyResult.Applied)
                    {
                        Metrics.DuplicateReports++;
                        break;
                    }
                    if (e.Exec.LastQty > 0) Metrics.Fills++;
                    if (CallbacksEnabled)
                    {
                        invoked = true;
                        _strategy.OnExecution(in e.Exec);
                    }
                    break;
                case EventType.Timer:
                    if (CallbacksEnabled)
                    {
                        invoked = true;
                        _strategy.OnTimer(e.A);
                    }
                    break;
                case EventType.ReferenceData:
                    _ref = e.Payload as IReferenceData;
                    if (CallbacksEnabled && _ref is not null)
                    {
                        invoked = true;
                        _strategy.OnReferenceDataChanged(_ref);
                    }
                    break;
                case EventType.AsyncResult:
                    if (CallbacksEnabled)
                    {
                        invoked = true;
                        _strategy.OnAsyncResult(e.A, e.Payload as byte[] ?? []);
                    }
                    break;
                case EventType.InstrumentStatus:
                    Risk.SetStale(new InstrumentId((int)e.A), e.B == 1);
                    break;
                case EventType.ExchangeStatus:
                    Risk.SetExchangeUp(e.A == 1);
                    // Cancels refused while the exchange was down are re-issued once it is back.
                    if (e.A == 1 && Risk.Killed) CancelAllLive();
                    break;
                case EventType.Control:
                    HandleControl((ControlCode)e.A);
                    break;
                case EventType.SessionStart:
                    invoked = StartSession();
                    break;
            }
        }
        catch (Exception ex)
        {
            Fault(ex);
        }
        Volatile.Write(ref _handlerStartNs, 0);

        if (invoked)
        {
            long dt = _clock.NowNs - t0;
            Metrics.HandlerTime.Record(dt);
            CheckBudget(dt);
        }
    }

    private bool StartSession()
    {
        var ids = new List<InstrumentId>(Manifest.Instruments.Length);
        foreach (string symbol in Manifest.Instruments)
        {
            if (_ref is not null && _ref.TryGetBySymbol(symbol, out InstrumentInfo info)) ids.Add(info.Id);
            else Log(LogLevel.Warn, "unknown instrument in manifest, ignored");
        }
        _instruments = ids.ToArray();
        SetStatus(StrategyStatus.Running);
        CallbacksEnabled = true;
        _strategy.OnStart(this);
        return true;
    }

    private void HandleControl(ControlCode code)
    {
        switch (code)
        {
            case ControlCode.Kill:
            case ControlCode.KillAndFlatten:
                Risk.Kill();
                if (Status == StrategyStatus.Running) SetStatus(StrategyStatus.Killed);
                CancelAllLive();
                if (code == ControlCode.KillAndFlatten) Flatten();
                Log(LogLevel.Warn, "kill switch: working orders cancelled", Oms.LiveOrderCount);
                break;
            case ControlCode.MarkSlow:
                Risk.Kill();
                CallbacksEnabled = false;
                SetStatus(StrategyStatus.Slow);
                CancelAllLive();
                Log(LogLevel.Error, "strategy too slow: kill-switched and stopped", Metrics.BudgetBreaches);
                break;
            case ControlCode.Stop:
                Risk.Kill();   // OnStop may cancel, but cannot open anything new
                if (CallbacksEnabled)
                {
                    try { _strategy.OnStop(); }
                    catch (Exception ex) { LastError = ex.ToString(); }
                }
                CallbacksEnabled = false;
                CancelAllLive();
                StopRequestedNs = _clock.NowNs;
                SetStatus(StrategyStatus.Stopping);
                break;
        }
    }

    private void Fault(Exception ex)
    {
        Metrics.Faults++;
        LastError = ex.ToString();
        CallbacksEnabled = false;
        Risk.Kill();
        SetStatus(StrategyStatus.Faulted);
        CancelAllLive();
        _options.Log?.Log(StrategyId, LogLevel.Error, "strategy threw; faulted: " + ex.GetType().Name + ": " + ex.Message);
    }

    private void CheckBudget(long dt)
    {
        if (_options.ReplayMode) return;
        if (dt <= _options.HandlerBudgetNs)
        {
            _consecutiveBreaches = 0;
            return;
        }
        Metrics.BudgetBreaches++;
        _consecutiveBreaches++;
        Log(LogLevel.Warn, "handler budget breached (ns, consecutive)", dt, _consecutiveBreaches);
        if (_consecutiveBreaches == _options.MaxConsecutiveBreaches && Status is StrategyStatus.Running or StrategyStatus.Killed)
        {
            // A timing decision: inject it as a real (journalled) event so replay sees the same thing.
            var evt = new HostEvent { Type = EventType.Control, Seq = _currentSeq, TimestampNs = _now, A = (long)ControlCode.MarkSlow };
            _self.TryEnqueue(evt);
        }
    }

    /// <summary>Called by the loop when a stopping strategy has no live orders left (or timed out).</summary>
    public void MarkStopped() => SetStatus(StrategyStatus.Stopped);

    private void SetStatus(StrategyStatus s)
    {
        if (Status == s) return;
        Volatile.Write(ref _status, (int)s);
        StatusChanged?.Invoke(this, s);
    }

    // ------------------------------------------------------------------ IStrategyContext

    public long NowNs => _now;
    public IReferenceData Reference => _ref!;
    public IReadOnlyList<InstrumentId> Instruments => _instruments;
    public double GetParameter(string name, double defaultValue) => Manifest.GetParameter(name, defaultValue);
    public Position GetPosition(InstrumentId id) => Oms.GetPosition(id);

    public OrderHandle Submit(in NewOrder order)
    {
        int code = Status == StrategyStatus.Running ? Risk.CheckNew(in order, _ref, Oms, _now)
            : Risk.Killed ? RejectCodes.KillSwitch : RejectCodes.StrategyNotRunning;
        var cmd = new OrderCommand
        {
            Type = CommandType.New, StrategyId = StrategyId, Instrument = order.Instrument, Side = order.Side,
            OrdType = order.Type, Tif = order.Tif, Px = order.Px, Qty = order.Qty, CauseSeq = _currentSeq, TimestampNs = _now,
        };
        if (code != RejectCodes.None)
        {
            cmd.RejectCode = code;
            _journal.Output(in cmd);
            Metrics.OrdersRejected++;
            return new OrderHandle(0, false, code);
        }
        return new OrderHandle(SendNew(ref cmd, in order), true, RejectCodes.None);
    }

    private long SendNew(ref OrderCommand cmd, in NewOrder order)
    {
        long id = Oms.NextOrderId();
        cmd.OrderId = id;
        Oms.Create(id, in order);
        Risk.RecordSent(_now);
        _journal.Output(in cmd);
        _sink.Send(in cmd);
        Metrics.OrdersSent++;
        if (_currentPublishNs != 0) Metrics.TickToOrder.Record(_clock.NowNs - _currentPublishNs);
        return id;
    }

    public void Cancel(long orderId)
    {
        if (!Oms.RequestCancel(orderId)) return;
        Oms.TryGet(orderId, out OrderState o);
        var cmd = new OrderCommand
        {
            Type = CommandType.Cancel, StrategyId = StrategyId, OrderId = orderId, Instrument = o.Instrument, Side = o.Side,
            OrdType = o.Type, Tif = o.Tif, Px = o.Px, Qty = o.OrderQty, CauseSeq = _currentSeq, TimestampNs = _now,
        };
        _journal.Output(in cmd);
        _sink.Send(in cmd);
        Metrics.CancelsSent++;
    }

    public void Replace(long orderId, long newPx, long newQty)
    {
        if (!Oms.TryGet(orderId, out OrderState o)) return;
        var cmd = new OrderCommand
        {
            Type = CommandType.Replace, StrategyId = StrategyId, OrderId = orderId, Instrument = o.Instrument, Side = o.Side,
            OrdType = o.Type, Tif = o.Tif, Px = newPx, Qty = newQty, CauseSeq = _currentSeq, TimestampNs = _now,
        };
        int code;
        if (Status != StrategyStatus.Running) code = Risk.Killed ? RejectCodes.KillSwitch : RejectCodes.StrategyNotRunning;
        else if (!Oms.CanReplace(orderId, newQty, out _)) code = RejectCodes.InvalidOrder;
        else code = Risk.CheckReplace(in o, newPx, newQty, _ref, Oms, _now);

        if (code != RejectCodes.None)
        {
            cmd.RejectCode = code;
            _journal.Output(in cmd);
            Metrics.OrdersRejected++;
            // Tell the strategy through the normal path, after the current handler returns.
            var reject = new HostEvent
            {
                Type = EventType.Execution, Flags = HostEvent.FlagInternal, Seq = _currentSeq, TimestampNs = _now,
                Exec = new ExecutionReport(_currentSeq, orderId, 0, o.Instrument, o.Side, o.ExternalStatus, 0, 0, o.CumQty, o.LeavesQty, code),
            };
            _self.TryEnqueue(reject);
            return;
        }
        Oms.RequestReplace(orderId, newPx, newQty);
        Risk.RecordSent(_now);
        _journal.Output(in cmd);
        _sink.Send(in cmd);
        Metrics.ReplacesSent++;
    }

    public void SetTimer(long timerId, long afterNs)
    {
        if (afterNs < 0) Timers.Cancel(timerId);
        else if (!Timers.Set(timerId, _now + afterNs)) Log(LogLevel.Warn, "timer table full", timerId);
    }

    public long RunAsync(Func<byte[]> work)
    {
        long id = ++_asyncCounter;
        if (!_options.ReplayMode) _options.AsyncRunner?.Invoke(StrategyId, id, work);
        return id;
    }

    public void Log(LogLevel level, string template, long a = 0, long b = 0) => _options.Log?.Log(StrategyId, level, template, a, b);

    // ------------------------------------------------------------------ kill helpers

    private void CancelAllLive()
    {
        int n = Oms.CollectLiveOrders(_liveBuffer);
        for (int i = 0; i < n; i++) Cancel(_liveBuffer[i]);
    }

    /// <summary>Market IOC orders that take every position back to flat. Bypasses limits (it only reduces risk),
    /// but still needs the exchange to be up.</summary>
    private void Flatten()
    {
        if (!Risk.ExchangeUp) return;
        foreach (InstrumentId id in _instruments)
        {
            long net = Oms.GetPosition(id).NetQty;
            if (net == 0) continue;
            var order = new NewOrder(id, net > 0 ? Side.Sell : Side.Buy, OrdType.Market, Tif.Ioc, 0, Math.Abs(net));
            var cmd = new OrderCommand
            {
                Type = CommandType.New, StrategyId = StrategyId, Instrument = id, Side = order.Side, OrdType = OrdType.Market,
                Tif = Tif.Ioc, Qty = order.Qty, CauseSeq = _currentSeq, TimestampNs = _now,
            };
            SendNew(ref cmd, in order);
        }
    }

    // ------------------------------------------------------------------ session record

    /// <summary>Payload of the SessionStart journal record: enough for a replay to rebuild this engine.</summary>
    public static byte[] SessionPayload(int strategyId, string hostId, StrategyManifest manifest) =>
        JsonSerializer.SerializeToUtf8Bytes(new SessionInfo { StrategyId = strategyId, HostId = hostId, Manifest = manifest },
            StrategyManifest.JsonOptions);

    public static SessionInfo ParseSession(byte[] payload) =>
        JsonSerializer.Deserialize<SessionInfo>(payload, StrategyManifest.JsonOptions) ?? throw new InvalidDataException("Bad session record");
}

public sealed class SessionInfo
{
    public int StrategyId { get; set; }
    public string HostId { get; set; } = "";
    public StrategyManifest Manifest { get; set; } = new();
}

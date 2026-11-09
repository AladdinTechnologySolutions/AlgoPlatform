using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using AlgoPlatform.Core.Collections;
using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Logging;
using AlgoPlatform.Core.Threading;
using AlgoPlatform.Core.Timing;
using AlgoPlatform.Gateway.Fix;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Gateway;

public enum SessionState { Disconnected = 0, LoggingOn = 1, Resending = 2, Reconciling = 3, Up = 4 }

public sealed class GatewayOptions
{
    public string Host { get; set; } = "127.0.0.1";
    public int Port { get; set; }
    public string SenderCompId { get; set; } = "ALGO";
    public string TargetCompId { get; set; } = "SIMX";
    public int HeartbeatIntervalMs { get; set; } = 1000;
    public int ReconnectDelayMs { get; set; } = 100;
    public int RecoveryTimeoutMs { get; set; } = 5000;
    /// <summary>Exchange throttle; 0 = unlimited. Excess commands wait in the outbound ring.</summary>
    public int MaxMessagesPerSecond { get; set; }
    public int OutboundCapacity { get; set; } = 1 << 16;
    public int MaxStrategies { get; set; } = 1024;
}

/// <summary>
/// The exchange gateway (FR-7). One thread owns the TCP session, sequence numbers, heartbeats, encoding
/// and throttling: strategy loops hand it <see cref="OrderCommand"/>s through an MPSC ring, and it hands
/// back execution reports, stamped by the host sequencer, through each strategy's control inbox.
///
/// Recovery (spec 8.3): on disconnect it broadcasts "exchange down" (risk then fails closed), reconnects,
/// logs on, asks the exchange to resend anything it missed, drops duplicates by ExecId, then sends an
/// order status request for every order it still believes is open and only goes Up when every one is
/// answered. State is rebuilt from exchange reports; an order the exchange has never heard of is
/// rejected (UnknownAtExchange), never guessed. Outgoing gaps are gap-filled: orders are never re-sent.
/// </summary>
public sealed class ExchangeGateway : ILoopWork, IDisposable
{
    private struct GwOrder
    {
        public int StrategyId;
        public InstrumentId Instrument;
        public Side Side;
        public OrdType OrdType;
        public Tif Tif;
        public long Px;
        public OrdStatus Status;
        public long OrderQty;
        public long CumQty;
        public long LeavesQty;
        public bool AwaitingStatus;
        public long LastExecId;
    }

    private readonly GatewayOptions _options;
    private readonly Sequencer _sequencer;
    private readonly IClock _clock;
    private readonly LogBuffer? _log;
    private readonly byte[] _sender;
    private readonly byte[] _target;
    private readonly long _heartbeatNs;

    private readonly IEventTarget?[] _targets;
    private readonly bool[] _blocked;
    private readonly bool[] _cancelAllPending;
    private readonly ConcurrentQueue<(IEventTarget Target, bool Add)> _registrations = new();
    private byte[][] _symbols = [];

    private readonly FixFramer _framer = new();
    private readonly FixMessage _msg = new();
    private readonly FixWriter _w = new();
    private readonly LongMap<GwOrder> _orders = new(1 << 14);

    private Socket? _socket;
    private int _state;
    private int _nextOutSeq = 1;
    private long _expectedInSeq = 1;
    private long _resendEnd;
    private long _lastSentNs;
    private long _lastRecvNs;
    private long _nextConnectNs;
    private long _deadlineNs;
    private int _awaitingStatus;
    private bool _broadcastUp;
    private double _tokens;
    private long _lastRefillNs;
    private EventLoopThread? _thread;

    private long _ordersSent, _cancelsSent, _replacesSent, _reports, _duplicates, _unknownReports, _disconnects, _rejectedWhileDown, _logons;

    public ExchangeGateway(GatewayOptions options, Sequencer sequencer, IClock clock, LogBuffer? log = null)
    {
        _options = options;
        _sequencer = sequencer;
        _clock = clock;
        _log = log;
        _sender = Encoding.ASCII.GetBytes(options.SenderCompId);
        _target = Encoding.ASCII.GetBytes(options.TargetCompId);
        _heartbeatNs = options.HeartbeatIntervalMs * 1_000_000L;
        _targets = new IEventTarget?[options.MaxStrategies];
        _blocked = new bool[options.MaxStrategies];
        _cancelAllPending = new bool[options.MaxStrategies];
        Outbound = new MpscRing<OrderCommand>(options.OutboundCapacity);
        _tokens = options.MaxMessagesPerSecond;
    }

    /// <summary>Strategy loops enqueue here (multi-producer). The gateway thread is the only consumer.</summary>
    public MpscRing<OrderCommand> Outbound { get; }

    public SessionState State => (SessionState)Volatile.Read(ref _state);
    public bool IsUp => State == SessionState.Up;
    public int OpenOrders => _orders.Count;
    public long OrdersSent => Volatile.Read(ref _ordersSent);
    public long CancelsSent => Volatile.Read(ref _cancelsSent);
    public long ReplacesSent => Volatile.Read(ref _replacesSent);
    public long ReportsReceived => Volatile.Read(ref _reports);
    public long DuplicateReports => Volatile.Read(ref _duplicates);
    public long UnknownReports => Volatile.Read(ref _unknownReports);
    public long Disconnects => Volatile.Read(ref _disconnects);
    public long Logons => Volatile.Read(ref _logons);
    public long RejectedWhileDown => Volatile.Read(ref _rejectedWhileDown);

    public void Start(IdleMode idle, int core = -1, IThreadAffinity? affinity = null)
    {
        _thread = new EventLoopThread("gateway", this, idle, core, affinity);
        _thread.Start();
    }

    /// <summary>Symbol bytes indexed by instrument id, used for encoding tag 55. Swapped atomically.</summary>
    public void SetSymbols(IEnumerable<(InstrumentId Id, string Symbol)> symbols)
    {
        var list = symbols.ToList();
        int max = list.Count == 0 ? 0 : list.Max(s => s.Id.Value);
        var arr = new byte[max + 1][];
        for (int i = 0; i < arr.Length; i++) arr[i] = [];
        foreach (var (id, sym) in list) arr[id.Value] = Encoding.ASCII.GetBytes(sym);
        Volatile.Write(ref _symbols, arr);
    }

    /// <summary>Any thread. Applied on the gateway thread, which then posts the current exchange status to the target,
    /// so a strategy can never see a stale status after a newer one.</summary>
    public void Register(IEventTarget target) => _registrations.Enqueue((target, true));

    public void Unregister(IEventTarget target) => _registrations.Enqueue((target, false));

    /// <summary>Watchdog path: refuse further commands from a hung strategy and cancel its open orders.</summary>
    public void BlockAndCancelAll(int strategyId)
    {
        Volatile.Write(ref _blocked[strategyId], true);
        var cmd = new OrderCommand { Type = CommandType.CancelAllForStrategy, StrategyId = strategyId };
        while (!Outbound.TryEnqueue(cmd)) Thread.SpinWait(16);
    }

    public int PollOnce()
    {
        int work = ProcessRegistrations();
        long now = _clock.NowNs;
        if (State == SessionState.Disconnected)
        {
            if (now >= _nextConnectNs) Connect(now);
            if (State == SessionState.Disconnected) return work + DrainOutbound(now);
        }
        work += Receive(now);
        if (State != SessionState.Disconnected) work += DrainOutbound(now);
        if (State != SessionState.Disconnected) Housekeeping(now);
        return work;
    }

    // ---------------------------------------------------------------- connection

    private void Connect(long now)
    {
        try
        {
            var s = new Socket(AddressFamilyFor(_options.Host), SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
            if (!s.ConnectAsync(_options.Host, _options.Port).Wait(TimeSpan.FromSeconds(1)))
            {
                s.Dispose();
                _nextConnectNs = now + _options.ReconnectDelayMs * 1_000_000L;
                return;
            }
            s.Blocking = false;
            _socket = s;
        }
        catch (Exception)
        {
            _nextConnectNs = now + _options.ReconnectDelayMs * 1_000_000L;
            return;
        }
        _framer.Reset();
        SetState(SessionState.LoggingOn);
        _lastRecvNs = now;
        _deadlineNs = now + _options.RecoveryTimeoutMs * 1_000_000L;
        Start(FixMsgType.Logon).Add(98, 0).Add(FixTags.HeartBtInt, Math.Max(1, _options.HeartbeatIntervalMs / 1000));
        Flush(now);
        _log?.Host(LogLevel.Info, "gateway: connected, logging on");
    }

    private static AddressFamily AddressFamilyFor(string host) =>
        System.Net.IPAddress.TryParse(host, out var ip) ? ip.AddressFamily : AddressFamily.InterNetwork;

    private void Disconnect(long now, string reason)
    {
        if (_socket is not null)
        {
            try { _socket.Dispose(); } catch (Exception) { }
            _socket = null;
        }
        if (State != SessionState.Disconnected)
        {
            Interlocked.Increment(ref _disconnects);
            _log?.Host(LogLevel.Warn, "gateway: disconnected: " + reason);
        }
        SetState(SessionState.Disconnected);
        _nextConnectNs = now + _options.ReconnectDelayMs * 1_000_000L;
        _awaitingStatus = 0;
        for (int i = 0; i < _orders.SlotCount; i++)
            if (_orders.IsUsed(i)) _orders.ValueAt(i).AwaitingStatus = false;
        if (_broadcastUp) Broadcast(now, up: false);
    }

    private void SetState(SessionState s) => Volatile.Write(ref _state, (int)s);

    // ---------------------------------------------------------------- inbound

    private int Receive(long now)
    {
        Socket? s = _socket;
        if (s is null) return 0;
        int n = s.Receive(_framer.WritableSpan(), SocketFlags.None, out SocketError err);
        if (err == SocketError.WouldBlock) return 0;
        if (err != SocketError.Success || n == 0)
        {
            Disconnect(now, err == SocketError.Success ? "closed by peer" : err.ToString());
            return 1;
        }
        _framer.Advance(n);
        int handled = 0;
        while (_socket is not null && _framer.TryRead(_msg))
        {
            Handle(now);
            handled++;
        }
        return handled + 1;
    }

    private void Handle(long now)
    {
        _lastRecvNs = now;
        byte type = _msg.MsgType;
        long seq = _msg.GetLong(FixTags.MsgSeqNum);
        bool possDup = _msg.GetChar(FixTags.PossDupFlag) == (byte)'Y';
        if (!possDup && type != FixMsgType.Logon && type != FixMsgType.SequenceReset && seq >= _expectedInSeq)
            _expectedInSeq = seq + 1;

        switch (type)
        {
            case FixMsgType.Logon:
                OnLogon(now, seq);
                break;
            case FixMsgType.TestRequest:
                Start(FixMsgType.Heartbeat).Add(FixTags.TestReqId, _msg.GetBytes(FixTags.TestReqId));
                Flush(now);
                break;
            case FixMsgType.ResendRequest:
                // We never re-send orders: gap-fill the whole range the exchange asked for.
                Start(FixMsgType.SequenceReset, (int)_msg.GetLong(FixTags.BeginSeqNo), possDup: true)
                    .AddChar(FixTags.GapFillFlag, (byte)'Y').Add(FixTags.NewSeqNo, _nextOutSeq);
                Flush(now);
                break;
            case FixMsgType.SequenceReset:
                long newSeq = _msg.GetLong(FixTags.NewSeqNo);
                if (newSeq > _expectedInSeq) _expectedInSeq = newSeq;
                CheckResendDone(now, newSeq - 1);
                break;
            case FixMsgType.Logout:
                Disconnect(now, "logout from exchange");
                break;
            case FixMsgType.ExecutionReport:
                OnExecutionReport(now);
                if (possDup) CheckResendDone(now, seq);
                break;
            case FixMsgType.CancelReject:
                OnCancelReject(now);
                if (possDup) CheckResendDone(now, seq);
                break;
        }
    }

    private void OnLogon(long now, long exchangeSeq)
    {
        Interlocked.Increment(ref _logons);
        if (exchangeSeq > _expectedInSeq)
        {
            // We missed messages while disconnected: ask for them before trusting any state.
            _resendEnd = exchangeSeq - 1;
            Start(FixMsgType.ResendRequest).Add(FixTags.BeginSeqNo, _expectedInSeq).Add(FixTags.EndSeqNo, _resendEnd);
            _expectedInSeq = exchangeSeq + 1;
            SetState(SessionState.Resending);
            _deadlineNs = now + _options.RecoveryTimeoutMs * 1_000_000L;
            Flush(now);
        }
        else
        {
            _expectedInSeq = exchangeSeq + 1;
            StartReconcile(now);
        }
    }

    private void CheckResendDone(long now, long seqProcessed)
    {
        if (State == SessionState.Resending && seqProcessed >= _resendEnd) StartReconcile(now);
    }

    private void StartReconcile(long now)
    {
        SetState(SessionState.Reconciling);
        _awaitingStatus = 0;
        byte[][] symbols = Volatile.Read(ref _symbols);
        for (int i = 0; i < _orders.SlotCount && _socket is not null; i++)
        {
            if (!_orders.IsUsed(i)) continue;
            ref GwOrder o = ref _orders.ValueAt(i);
            o.AwaitingStatus = true;
            _awaitingStatus++;
            Start(FixMsgType.OrderStatusRequest).Add(FixTags.ClOrdId, _orders.KeyAt(i))
                .Add(FixTags.Symbol, SymbolOf(symbols, o.Instrument)).AddChar(FixTags.Side, FixEnums.SideChar(o.Side));
            Flush(now);
        }
        _deadlineNs = now + _options.RecoveryTimeoutMs * 1_000_000L;
        if (_awaitingStatus == 0 && _socket is not null) GoUp(now);
    }

    private void GoUp(long now)
    {
        SetState(SessionState.Up);
        Broadcast(now, up: true);
        for (int sid = 0; sid < _cancelAllPending.Length; sid++)
        {
            if (!_cancelAllPending[sid]) continue;
            _cancelAllPending[sid] = false;
            CancelAllFor(now, sid);
        }
        _log?.Host(LogLevel.Info, "gateway: session up");
    }

    private void OnExecutionReport(long now)
    {
        Interlocked.Increment(ref _reports);
        long orderId = _msg.GetLong(FixTags.ClOrdId);
        ref GwOrder o = ref _orders.GetValueRefOrNullRef(orderId);
        if (Unsafe.IsNullRef(ref o))
        {
            Interlocked.Increment(ref _unknownReports);   // e.g. a re-sent report for an order already finished
            return;
        }
        byte execType = _msg.GetChar(FixTags.ExecType);
        byte ordStatus = _msg.GetChar(FixTags.OrdStatus);
        long execId = _msg.GetLong(FixTags.ExecId);
        long cum = _msg.GetLong(FixTags.CumQty);
        long leaves = _msg.GetLong(FixTags.LeavesQty);
        long lastQty = _msg.GetLong(FixTags.LastQty);
        _msg.TryGetPrice(FixTags.LastPx, out long lastPx);

        if (execType != (byte)'I' && execId != 0)
        {
            // Re-sent reports (PossDup after a resend request) are dropped here; the OMS dedupes again.
            if (execId <= o.LastExecId)
            {
                Interlocked.Increment(ref _duplicates);
                return;
            }
            o.LastExecId = execId;
        }

        OrdStatus status = FixEnums.ParseOrdStatus(ordStatus);
        int rejectCode = 0;
        if (execType == (byte)'I')
        {
            // Order status reply during reconciliation.
            if (o.AwaitingStatus)
            {
                o.AwaitingStatus = false;
                _awaitingStatus--;
            }
            if (ordStatus == (byte)'8' && _msg.GetLong(FixTags.OrdRejReason) == FixRejectReason.UnknownOrder)
            {
                rejectCode = RejectCodes.UnknownAtExchange;
                cum = o.CumQty;
                leaves = 0;
            }
            execId = 0;
            lastQty = 0;
        }
        else if (execType == (byte)'8')
        {
            rejectCode = RejectCodes.ExchangeReject;
        }

        o.Status = status;
        o.CumQty = cum;
        o.LeavesQty = leaves;
        o.OrderQty = cum + leaves;
        var report = new ExecutionReport(0, orderId, execId, o.Instrument, o.Side, status, lastPx, lastQty, cum, leaves, rejectCode);
        Route(now, o.StrategyId, in report);
        if (status is OrdStatus.Filled or OrdStatus.Cancelled or OrdStatus.Rejected) _orders.Remove(orderId);

        if (State == SessionState.Reconciling && _awaitingStatus <= 0) GoUp(now);
    }

    private void OnCancelReject(long now)
    {
        Interlocked.Increment(ref _reports);
        long orderId = _msg.GetLong(FixTags.ClOrdId);
        ref GwOrder o = ref _orders.GetValueRefOrNullRef(orderId);
        if (Unsafe.IsNullRef(ref o))
        {
            Interlocked.Increment(ref _unknownReports);
            return;
        }
        var report = new ExecutionReport(0, orderId, 0, o.Instrument, o.Side, o.Status, 0, 0, o.CumQty, o.LeavesQty,
            RejectCodes.ExchangeReject);
        Route(now, o.StrategyId, in report);
    }

    private void Route(long now, int strategyId, in ExecutionReport r)
    {
        IEventTarget? target = (uint)strategyId < (uint)_targets.Length ? _targets[strategyId] : null;
        if (target is null) return;
        long seq = _sequencer.Next();
        var evt = new HostEvent
        {
            Type = EventType.Execution,
            Seq = seq,
            TimestampNs = now,
            Exec = r with { Seq = seq },
        };
        target.PostControl(in evt);
    }

    private void Broadcast(long now, bool up)
    {
        _broadcastUp = up;
        for (int i = 0; i < _targets.Length; i++)
        {
            IEventTarget? t = _targets[i];
            if (t is null) continue;
            var evt = new HostEvent { Type = EventType.ExchangeStatus, Seq = _sequencer.Next(), TimestampNs = now, A = up ? 1 : 0 };
            t.PostControl(in evt);
        }
    }

    private int ProcessRegistrations()
    {
        int n = 0;
        while (_registrations.TryDequeue(out var reg))
        {
            n++;
            int id = reg.Target.StrategyId;
            if (!reg.Add)
            {
                if (_targets[id] == reg.Target) _targets[id] = null;
                continue;
            }
            _targets[id] = reg.Target;
            Volatile.Write(ref _blocked[id], false);
            var evt = new HostEvent
            {
                Type = EventType.ExchangeStatus, Seq = _sequencer.Next(), TimestampNs = _clock.NowNs, A = _broadcastUp ? 1 : 0,
            };
            reg.Target.PostControl(in evt);
        }
        return n;
    }

    // ---------------------------------------------------------------- outbound

    private int DrainOutbound(long now)
    {
        int n = 0;
        bool throttled = _options.MaxMessagesPerSecond > 0;
        if (throttled) Refill(now);
        while (n < 128)
        {
            if (throttled && _tokens < 1) break;
            if (!Outbound.TryDequeue(out OrderCommand cmd)) break;
            n++;
            if (cmd.Type != CommandType.CancelAllForStrategy && Volatile.Read(ref _blocked[cmd.StrategyId]))
            {
                RejectBack(now, in cmd, RejectCodes.KillSwitch);
                continue;
            }
            if (State != SessionState.Up)
            {
                Interlocked.Increment(ref _rejectedWhileDown);
                if (cmd.Type == CommandType.CancelAllForStrategy) _cancelAllPending[cmd.StrategyId] = true;
                else RejectBack(now, in cmd, RejectCodes.ExchangeDown);
                continue;
            }
            if (throttled) _tokens -= 1;
            switch (cmd.Type)
            {
                case CommandType.New: SendNew(now, in cmd); break;
                case CommandType.Cancel: SendCancel(now, cmd.OrderId); break;
                case CommandType.Replace: SendReplace(now, in cmd); break;
                case CommandType.CancelAllForStrategy: CancelAllFor(now, cmd.StrategyId); break;
            }
            if (_socket is null) break;
        }
        return n;
    }

    private void Refill(long now)
    {
        if (_lastRefillNs == 0) _lastRefillNs = now;
        _tokens = Math.Min(_options.MaxMessagesPerSecond, _tokens + (now - _lastRefillNs) * _options.MaxMessagesPerSecond / 1e9);
        _lastRefillNs = now;
    }

    private void SendNew(long now, in OrderCommand c)
    {
        var o = new GwOrder
        {
            StrategyId = c.StrategyId, Instrument = c.Instrument, Side = c.Side, OrdType = c.OrdType, Tif = c.Tif, Px = c.Px,
            Status = OrdStatus.PendingNew, OrderQty = c.Qty, LeavesQty = c.Qty,
        };
        _orders.TryAdd(c.OrderId, o);   // recorded before sending, so a send that dies on the wire is reconciled
        FixWriter w = Start(FixMsgType.NewOrderSingle).Add(FixTags.ClOrdId, c.OrderId)
            .Add(FixTags.Symbol, SymbolOf(Volatile.Read(ref _symbols), c.Instrument))
            .AddChar(FixTags.Side, FixEnums.SideChar(c.Side)).Add(FixTags.OrderQty, c.Qty)
            .AddChar(FixTags.OrdType, FixEnums.OrdTypeChar(c.OrdType));
        if (c.OrdType == OrdType.Limit) w.AddPrice(FixTags.Price, c.Px);
        w.AddChar(FixTags.TimeInForce, FixEnums.TifChar(c.Tif));
        Flush(now);
        Interlocked.Increment(ref _ordersSent);
    }

    private void SendCancel(long now, long orderId)
    {
        ref GwOrder o = ref _orders.GetValueRefOrNullRef(orderId);
        if (Unsafe.IsNullRef(ref o))
        {
            // Already finished: tell the strategy so its OMS clears the pending cancel.
            var cmd = new OrderCommand { Type = CommandType.Cancel, OrderId = orderId, StrategyId = OrderIds.StrategyOf(orderId) };
            RejectBack(now, in cmd, RejectCodes.UnknownOrder);
            return;
        }
        Start(FixMsgType.CancelRequest).Add(FixTags.ClOrdId, orderId).Add(FixTags.OrigClOrdId, orderId)
            .Add(FixTags.Symbol, SymbolOf(Volatile.Read(ref _symbols), o.Instrument))
            .AddChar(FixTags.Side, FixEnums.SideChar(o.Side)).Add(FixTags.OrderQty, o.OrderQty);
        Flush(now);
        Interlocked.Increment(ref _cancelsSent);
    }

    private void SendReplace(long now, in OrderCommand c)
    {
        ref GwOrder o = ref _orders.GetValueRefOrNullRef(c.OrderId);
        if (Unsafe.IsNullRef(ref o))
        {
            RejectBack(now, in c, RejectCodes.UnknownOrder);
            return;
        }
        FixWriter w = Start(FixMsgType.ReplaceRequest).Add(FixTags.ClOrdId, c.OrderId).Add(FixTags.OrigClOrdId, c.OrderId)
            .Add(FixTags.Symbol, SymbolOf(Volatile.Read(ref _symbols), o.Instrument))
            .AddChar(FixTags.Side, FixEnums.SideChar(o.Side)).Add(FixTags.OrderQty, c.Qty)
            .AddChar(FixTags.OrdType, FixEnums.OrdTypeChar(o.OrdType));
        if (o.OrdType == OrdType.Limit) w.AddPrice(FixTags.Price, c.Px);
        w.AddChar(FixTags.TimeInForce, FixEnums.TifChar(o.Tif));
        Flush(now);
        Interlocked.Increment(ref _replacesSent);
    }

    private void CancelAllFor(long now, int strategyId)
    {
        for (int i = 0; i < _orders.SlotCount && _socket is not null; i++)
        {
            if (_orders.IsUsed(i) && _orders.ValueAt(i).StrategyId == strategyId) SendCancel(now, _orders.KeyAt(i));
        }
    }

    /// <summary>Answers a command locally: a reject for a new order, a cancel-reject for cancel/replace.</summary>
    private void RejectBack(long now, in OrderCommand c, int rejectCode)
    {
        ExecutionReport r;
        if (c.Type == CommandType.New)
        {
            r = new ExecutionReport(0, c.OrderId, 0, c.Instrument, c.Side, OrdStatus.Rejected, 0, 0, 0, 0, rejectCode);
        }
        else
        {
            _orders.TryGetValue(c.OrderId, out GwOrder o);
            OrdStatus st = _orders.ContainsKey(c.OrderId) ? o.Status : OrdStatus.Working;
            r = new ExecutionReport(0, c.OrderId, 0, o.Instrument, o.Side, st, 0, 0, o.CumQty, o.LeavesQty, rejectCode);
        }
        Route(now, c.StrategyId, in r);
    }

    // ---------------------------------------------------------------- session plumbing

    private FixWriter Start(byte msgType, int seq = -1, bool possDup = false)
    {
        _w.Begin(msgType).Add(FixTags.SenderCompId, _sender).Add(FixTags.TargetCompId, _target)
            .Add(FixTags.MsgSeqNum, seq >= 0 ? seq : _nextOutSeq++);
        if (possDup) _w.AddChar(FixTags.PossDupFlag, (byte)'Y');
        return _w;
    }

    private void Flush(long now)
    {
        Socket? s = _socket;
        if (s is null) return;
        ReadOnlySpan<byte> data = _w.Finish();
        var spin = new SpinWait();
        while (!data.IsEmpty)
        {
            int sent = s.Send(data, SocketFlags.None, out SocketError err);
            if (err == SocketError.WouldBlock)
            {
                spin.SpinOnce(-1);
                continue;
            }
            if (err != SocketError.Success)
            {
                Disconnect(now, "send failed: " + err);
                return;
            }
            data = data[sent..];
        }
        _lastSentNs = now;
    }

    private void Housekeeping(long now)
    {
        if (now - _lastSentNs > _heartbeatNs)
        {
            Start(FixMsgType.Heartbeat);
            Flush(now);
        }
        if (now - _lastRecvNs > 3 * _heartbeatNs)
        {
            Disconnect(now, "heartbeat timeout");
            return;
        }
        if (State != SessionState.Up && now > _deadlineNs) Disconnect(now, "recovery timeout in " + State);
    }

    private static ReadOnlySpan<byte> SymbolOf(byte[][] symbols, InstrumentId id) =>
        (uint)id.Value < (uint)symbols.Length ? symbols[id.Value] : default;

    public void Dispose()
    {
        _thread?.RequestStop();
        _thread?.Join(TimeSpan.FromSeconds(2));
        if (_socket is not null && State != SessionState.Disconnected)
        {
            try
            {
                Start(FixMsgType.Logout);
                Flush(_clock.NowNs);
            }
            catch (Exception) { }
        }
        _socket?.Dispose();
        _socket = null;
        SetState(SessionState.Disconnected);
    }
}

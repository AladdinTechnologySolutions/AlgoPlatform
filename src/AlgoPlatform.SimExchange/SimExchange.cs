using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using AlgoPlatform.Gateway.Fix;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.SimExchange;

public sealed class SimExchangeOptions
{
    public int Port { get; set; }
    public string CompId { get; set; } = "SIMX";
    public int HeartbeatIntervalMs { get; set; } = 1000;
    /// <summary>Synthetic quote used for symbols that never received <see cref="SimExchange.SetQuote"/>.</summary>
    public long DefaultBid { get; set; } = Price.FromDouble(100.00);
    public long DefaultAsk { get; set; } = Price.FromDouble(100.02);
    /// <summary>Largest quantity per execution; 0 = fill the whole order at once. Smaller values produce partial fills.</summary>
    public long MaxFillQty { get; set; }
    /// <summary>Fault injection: after receiving this many new orders, swallow the responses to the last one and drop
    /// the connection. 0 = never. One-shot.</summary>
    public int DropAfterOrders { get; set; }
}

public readonly record struct SimOrderView(long ClOrdId, string Symbol, Side Side, char Status, long OrderQty, long CumQty, long LeavesQty, long Px);

/// <summary>
/// TCP simulated exchange (spec 8.2): matches against a synthetic top of book, keeps a persistent FIX
/// session per SenderCompID across reconnects, stores every outbound message for resend, answers
/// order status requests, and can inject disconnects. Simulator code: it uses locks and allocates freely.
/// </summary>
public sealed class SimExchange : IDisposable
{
    private sealed class StoredMessage
    {
        public int Seq;
        public byte MsgType;
        public List<(int Tag, string Value)> Body = new();
    }

    private sealed class Session
    {
        public required string CompId;
        public int NextOutSeq = 1;
        public long ExpectedIn = 1;
        public readonly List<StoredMessage> Store = new();
        public Connection? Conn;
        public bool Suppress;
    }

    private sealed class Connection
    {
        public required Socket Socket;
        public Session? Session;
        public volatile bool Closed;
    }

    private sealed class SimOrder
    {
        public long ClOrdId;
        public long ExchOrderId;
        public string Symbol = "";
        public Side Side;
        public OrdType Type;
        public Tif Tif;
        public long Px;
        public long OrderQty;
        public long CumQty;
        public long LeavesQty;
        public char Status = '0';
        public Session? Owner;   // the FIX session that entered the order; reports go there even after a reconnect
        public bool IsLive => Status is '0' or '1';
    }

    private readonly SimExchangeOptions _options;
    private readonly object _gate = new();
    private readonly Dictionary<string, Session> _sessions = new();
    private readonly Dictionary<long, SimOrder> _orders = new();
    private readonly Dictionary<string, (long Bid, long Ask)> _quotes = new();
    private readonly FixWriter _w = new();
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _cts = new();
    private long _nextExecId = 1_000_000;
    private long _nextExchOrderId = 1;
    private int _dropAfter;

    public SimExchange(SimExchangeOptions options)
    {
        _options = options;
        _dropAfter = options.DropAfterOrders;
        _listener = new TcpListener(IPAddress.Loopback, options.Port);
    }

    public int Port { get; private set; }
    public long OrdersReceived { get; private set; }
    public long DuplicateClOrdIds { get; private set; }
    public long InjectedDrops { get; private set; }
    public long Logons { get; private set; }
    public long ResendRequestsServed { get; private set; }
    public long GapFillsReceived { get; private set; }
    public long StatusRequests { get; private set; }

    public void Start()
    {
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        new Thread(AcceptLoop) { IsBackground = true, Name = "simx-accept" }.Start();
        new Thread(HeartbeatLoop) { IsBackground = true, Name = "simx-heartbeat" }.Start();
    }

    /// <summary>Re-arms the one-shot disconnect injection.</summary>
    public void DropAfterOrders(int totalOrders)
    {
        lock (_gate) _dropAfter = totalOrders;
    }

    public void DropConnections()
    {
        lock (_gate)
        {
            foreach (var s in _sessions.Values)
                if (s.Conn is not null) Close(s.Conn);
        }
    }

    /// <summary>Moves the synthetic book; resting orders that become marketable fill.</summary>
    public void SetQuote(string symbol, long bid, long ask)
    {
        lock (_gate)
        {
            _quotes[symbol] = (bid, ask);
            foreach (var o in _orders.Values)
                if (o.IsLive && o.Symbol == symbol) Match(o);
        }
    }

    public List<SimOrderView> GetOrders()
    {
        lock (_gate)
        {
            var list = new List<SimOrderView>(_orders.Count);
            foreach (var o in _orders.Values)
                list.Add(new SimOrderView(o.ClOrdId, o.Symbol, o.Side, o.Status, o.OrderQty, o.CumQty, o.LeavesQty, o.Px));
            return list;
        }
    }

    // ---------------------------------------------------------------- networking

    private void AcceptLoop()
    {
        while (!_cts.IsCancellationRequested)
        {
            Socket s;
            try { s = _listener.AcceptSocket(); }
            catch (Exception) { return; }
            s.NoDelay = true;
            var conn = new Connection { Socket = s };
            new Thread(() => ReadLoop(conn)) { IsBackground = true, Name = "simx-conn" }.Start();
        }
    }

    private void ReadLoop(Connection conn)
    {
        var framer = new FixFramer();
        var msg = new FixMessage();
        try
        {
            while (!conn.Closed)
            {
                int n = conn.Socket.Receive(framer.WritableSpan());
                if (n == 0) break;
                framer.Advance(n);
                while (framer.TryRead(msg))
                {
                    lock (_gate)
                    {
                        if (conn.Closed) return;
                        Handle(conn, msg);
                    }
                }
            }
        }
        catch (Exception) { }
        finally
        {
            lock (_gate) Close(conn);
        }
    }

    private void HeartbeatLoop()
    {
        while (!_cts.Token.WaitHandle.WaitOne(_options.HeartbeatIntervalMs))
        {
            lock (_gate)
            {
                foreach (var s in _sessions.Values)
                    if (s.Conn is not null) Send(s, FixMsgType.Heartbeat, new());
            }
        }
    }

    private void Close(Connection conn)
    {
        if (conn.Closed) return;
        conn.Closed = true;
        try { conn.Socket.Shutdown(SocketShutdown.Both); } catch (Exception) { }
        conn.Socket.Dispose();
        if (conn.Session is not null && conn.Session.Conn == conn) conn.Session.Conn = null;
    }

    // ---------------------------------------------------------------- session layer (called under _gate)

    private void Handle(Connection conn, FixMessage msg)
    {
        byte type = msg.MsgType;
        if (type == FixMsgType.Logon)
        {
            OnLogon(conn, msg);
            return;
        }
        Session? session = conn.Session;
        if (session is null) return;   // must log on first

        long seq = msg.GetLong(FixTags.MsgSeqNum);
        bool possDup = msg.GetChar(FixTags.PossDupFlag) == (byte)'Y';
        switch (type)
        {
            case FixMsgType.SequenceReset:
                if (msg.GetChar(FixTags.GapFillFlag) == (byte)'Y') GapFillsReceived++;
                session.ExpectedIn = Math.Max(session.ExpectedIn, msg.GetLong(FixTags.NewSeqNo));
                return;
            case FixMsgType.ResendRequest:
                Resend(session, msg.GetLong(FixTags.BeginSeqNo), msg.GetLong(FixTags.EndSeqNo));
                return;
            case FixMsgType.TestRequest:
                Send(session, FixMsgType.Heartbeat, new() { (FixTags.TestReqId, Encoding.ASCII.GetString(msg.GetBytes(FixTags.TestReqId))) });
                return;
            case FixMsgType.Logout:
                Send(session, FixMsgType.Logout, new());
                Close(conn);
                return;
        }

        if (seq < session.ExpectedIn && !possDup) return;   // stale duplicate from the client
        if (seq >= session.ExpectedIn) session.ExpectedIn = seq + 1;

        switch (type)
        {
            case FixMsgType.NewOrderSingle: OnNewOrder(session, msg); break;
            case FixMsgType.CancelRequest: OnCancel(session, msg); break;
            case FixMsgType.ReplaceRequest: OnReplace(session, msg); break;
            case FixMsgType.OrderStatusRequest: OnStatus(session, msg); break;
        }
    }

    private void OnLogon(Connection conn, FixMessage msg)
    {
        string compId = Encoding.ASCII.GetString(msg.GetBytes(FixTags.SenderCompId));
        if (!_sessions.TryGetValue(compId, out Session? session))
        {
            session = new Session { CompId = compId };
            _sessions[compId] = session;
        }
        if (session.Conn is not null && session.Conn != conn) Close(session.Conn);
        session.Conn = conn;
        conn.Session = session;
        Logons++;
        long seq = msg.GetLong(FixTags.MsgSeqNum);
        Send(session, FixMsgType.Logon, new() { (98, "0"), (FixTags.HeartBtInt, "1") });
        if (seq > session.ExpectedIn)
            Send(session, FixMsgType.ResendRequest, new() { (FixTags.BeginSeqNo, Num(session.ExpectedIn)), (FixTags.EndSeqNo, "0") });
        else
            session.ExpectedIn = seq + 1;
    }

    private void Resend(Session session, long begin, long end)
    {
        ResendRequestsServed++;
        if (end == 0 || end >= session.NextOutSeq) end = session.NextOutSeq - 1;
        foreach (var m in session.Store)
        {
            if (m.Seq < begin || m.Seq > end) continue;
            if (m.MsgType is FixMsgType.ExecutionReport or FixMsgType.CancelReject)
                Write(session, m.MsgType, m.Seq, possDup: true, m.Body);
            else
                Write(session, FixMsgType.SequenceReset, m.Seq, possDup: true,
                    new() { (FixTags.GapFillFlag, "Y"), (FixTags.NewSeqNo, Num(m.Seq + 1)) });
        }
    }

    private void Send(Session session, byte msgType, List<(int, string)> body)
    {
        int seq = session.NextOutSeq++;
        session.Store.Add(new StoredMessage { Seq = seq, MsgType = msgType, Body = body });
        if (!session.Suppress) Write(session, msgType, seq, possDup: false, body);
    }

    private void Write(Session session, byte msgType, int seq, bool possDup, List<(int Tag, string Value)> body)
    {
        Connection? conn = session.Conn;
        if (conn is null || conn.Closed) return;
        _w.Begin(msgType).Add(FixTags.SenderCompId, Encoding.ASCII.GetBytes(_options.CompId))
            .Add(FixTags.TargetCompId, Encoding.ASCII.GetBytes(session.CompId)).Add(FixTags.MsgSeqNum, seq);
        if (possDup) _w.AddChar(FixTags.PossDupFlag, (byte)'Y');
        foreach (var (tag, value) in body) _w.Add(tag, Encoding.ASCII.GetBytes(value));
        try
        {
            conn.Socket.Send(_w.Finish());
        }
        catch (Exception)
        {
            Close(conn);
        }
    }

    // ---------------------------------------------------------------- order handling (called under _gate)

    private void OnNewOrder(Session session, FixMessage msg)
    {
        long id = msg.GetLong(FixTags.ClOrdId);
        OrdersReceived++;
        bool drop = _dropAfter > 0 && OrdersReceived >= _dropAfter;
        if (drop)
        {
            // The order is accepted and its responses stored, but none of them reach the client.
            _dropAfter = 0;
            session.Suppress = true;
        }
        try
        {
            if (_orders.ContainsKey(id))
            {
                DuplicateClOrdIds++;
                Send(session, FixMsgType.ExecutionReport, new()
                {
                    (FixTags.ClOrdId, Num(id)), (FixTags.ExecId, Num(_nextExecId++)), (FixTags.ExecType, "8"),
                    (FixTags.OrdStatus, "8"), (FixTags.OrdRejReason, Num(FixRejectReason.DuplicateOrder)),
                });
                return;
            }
            msg.TryGetPrice(FixTags.Price, out long px);
            var o = new SimOrder
            {
                ClOrdId = id, ExchOrderId = _nextExchOrderId++, Owner = session,
                Symbol = Encoding.ASCII.GetString(msg.GetBytes(FixTags.Symbol)),
                Side = FixEnums.ParseSide(msg.GetChar(FixTags.Side)),
                Type = FixEnums.ParseOrdType(msg.GetChar(FixTags.OrdType)),
                Tif = FixEnums.ParseTif(msg.GetChar(FixTags.TimeInForce)),
                Px = px, OrderQty = msg.GetLong(FixTags.OrderQty),
            };
            o.LeavesQty = o.OrderQty;
            _orders[id] = o;
            if (o.OrderQty <= 0 || (o.Type == OrdType.Limit && o.Px <= 0))
            {
                o.Status = '8';
                o.LeavesQty = 0;
                Report(session, o, '8', 0, 0, FixRejectReason.Other);
                return;
            }
            Report(session, o, '0', 0, 0);
            Match(o);
        }
        finally
        {
            if (drop)
            {
                session.Suppress = false;
                InjectedDrops++;
                if (session.Conn is not null) Close(session.Conn);
            }
        }
    }

    private void OnCancel(Session session, FixMessage msg)
    {
        long id = msg.GetLong(FixTags.OrigClOrdId);
        if (!_orders.TryGetValue(id, out SimOrder? o) || !o.IsLive)
        {
            CancelReject(session, id, o, "1");
            return;
        }
        o.Status = '4';
        o.LeavesQty = 0;
        Report(session, o, '4', 0, 0);
    }

    private void OnReplace(Session session, FixMessage msg)
    {
        long id = msg.GetLong(FixTags.OrigClOrdId);
        long newQty = msg.GetLong(FixTags.OrderQty);
        if (!_orders.TryGetValue(id, out SimOrder? o) || !o.IsLive || newQty <= o.CumQty)
        {
            CancelReject(session, id, o, "2");
            return;
        }
        if (msg.TryGetPrice(FixTags.Price, out long px)) o.Px = px;
        o.OrderQty = newQty;
        o.LeavesQty = newQty - o.CumQty;
        Report(session, o, '5', 0, 0);
        Match(o);
    }

    private void OnStatus(Session session, FixMessage msg)
    {
        StatusRequests++;
        long id = msg.GetLong(FixTags.ClOrdId);
        if (!_orders.TryGetValue(id, out SimOrder? o))
        {
            Send(session, FixMsgType.ExecutionReport, new()
            {
                (FixTags.ClOrdId, Num(id)), (FixTags.ExecId, "0"), (FixTags.ExecType, "I"), (FixTags.OrdStatus, "8"),
                (FixTags.OrdRejReason, Num(FixRejectReason.UnknownOrder)), (FixTags.CumQty, "0"), (FixTags.LeavesQty, "0"),
            });
            return;
        }
        Report(session, o, 'I', 0, 0, execId: 0);
    }

    private void CancelReject(Session session, long id, SimOrder? o, string responseTo)
    {
        Send(session, FixMsgType.CancelReject, new()
        {
            (FixTags.ClOrdId, Num(id)), (FixTags.OrigClOrdId, Num(id)), (FixTags.OrdStatus, o is null ? "8" : o.Status.ToString()),
            (FixTags.CxlRejResponseTo, responseTo),
        });
    }

    private void Match(SimOrder o)
    {
        if (!o.IsLive) return;
        (long bid, long ask) = _quotes.TryGetValue(o.Symbol, out var q) ? q : (_options.DefaultBid, _options.DefaultAsk);
        bool marketable = o.Type == OrdType.Market || (o.Side == Side.Buy ? o.Px >= ask : o.Px <= bid);
        if (marketable)
        {
            long fillPx = o.Side == Side.Buy ? ask : bid;
            while (o.LeavesQty > 0)
            {
                long qty = _options.MaxFillQty > 0 ? Math.Min(_options.MaxFillQty, o.LeavesQty) : o.LeavesQty;
                o.CumQty += qty;
                o.LeavesQty -= qty;
                o.Status = o.LeavesQty == 0 ? '2' : '1';
                Report(o.Owner, o, 'F', fillPx, qty);
            }
        }
        else if (o.Tif is Tif.Ioc or Tif.Fok)
        {
            o.Status = '4';
            o.LeavesQty = 0;
            Report(o.Owner, o, '4', 0, 0);
        }
    }

    private void Report(Session? session, SimOrder o, char execType, long lastPx, long lastQty, int rejectReason = 0, long? execId = null)
    {
        if (session is null) return;
        var body = new List<(int, string)>
        {
            (FixTags.OrderId, Num(o.ExchOrderId)), (FixTags.ClOrdId, Num(o.ClOrdId)),
            (FixTags.ExecId, Num(execId ?? _nextExecId++)), (FixTags.ExecType, execType.ToString()),
            (FixTags.OrdStatus, o.Status.ToString()), (FixTags.Symbol, o.Symbol),
            (FixTags.Side, o.Side == Side.Buy ? "1" : "2"), (FixTags.OrderQty, Num(o.OrderQty)),
            (FixTags.Price, Px(o.Px)), (FixTags.CumQty, Num(o.CumQty)), (FixTags.LeavesQty, Num(o.LeavesQty)),
            (FixTags.LastPx, Px(lastPx)), (FixTags.LastQty, Num(lastQty)),
        };
        if (rejectReason != 0) body.Add((FixTags.OrdRejReason, Num(rejectReason)));
        Send(session, FixMsgType.ExecutionReport, body);
    }

    private static string Num(long v) => v.ToString(CultureInfo.InvariantCulture);
    private static string Px(long px) => (px / (decimal)Price.Scale).ToString("0.0000", CultureInfo.InvariantCulture);

    public void Dispose()
    {
        _cts.Cancel();
        try { _listener.Stop(); } catch (Exception) { }
        lock (_gate)
        {
            foreach (var s in _sessions.Values)
                if (s.Conn is not null) Close(s.Conn);
        }
    }
}

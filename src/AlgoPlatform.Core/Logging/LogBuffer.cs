using System.Text;
using System.Text.Json;
using AlgoPlatform.Core.Collections;
using AlgoPlatform.Core.Threading;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Core.Logging;

public struct LogEntry
{
    public long TimestampNs;
    public int StrategyId;
    public LogLevel Level;
    public string Template;
    public long A;
    public long B;
}

/// <summary>
/// Non-allocating structured log. Hot-path callers enqueue a struct holding a reference to a constant
/// template string and two numbers; a housekeeping thread formats and writes JSON lines. If the ring
/// is full the entry is dropped and counted: logging must never slow a strategy down.
/// </summary>
public sealed class LogBuffer : ILoopWork, IDisposable
{
    private readonly MpscRing<LogEntry> _ring;
    private readonly TextWriter _out;
    private readonly Func<int, string> _strategyName;
    private readonly StringBuilder _sb = new(256);
    private readonly long _startTicks = DateTime.UtcNow.Ticks;
    private readonly long _startNs = Timing.MonotonicClock.Now();
    private EventLoopThread? _thread;
    private long _dropped;
    private long _written;

    public LogBuffer(TextWriter output, Func<int, string>? strategyName = null, int capacity = 1 << 16)
    {
        _ring = new MpscRing<LogEntry>(capacity);
        _out = output;
        _strategyName = strategyName ?? (id => id.ToString());
    }

    public long Dropped => Volatile.Read(ref _dropped);
    public long Written => Volatile.Read(ref _written);
    public LogLevel MinLevel { get; set; } = LogLevel.Info;

    public void Start()
    {
        _thread = new EventLoopThread("log-drain", this, IdleMode.Backoff);
        _thread.Start();
    }

    public void Log(int strategyId, LogLevel level, string template, long a = 0, long b = 0)
    {
        if (level < MinLevel) return;
        var e = new LogEntry
        {
            TimestampNs = Timing.MonotonicClock.Now(), StrategyId = strategyId, Level = level, Template = template, A = a, B = b,
        };
        if (!_ring.TryEnqueue(e)) Interlocked.Increment(ref _dropped);
    }

    /// <summary>Control-path convenience for host messages (strategy id 0).</summary>
    public void Host(LogLevel level, string message) => Log(0, level, message);

    public int PollOnce()
    {
        int n = 0;
        while (n < 1024 && _ring.TryDequeue(out LogEntry e))
        {
            WriteLine(in e);
            n++;
        }
        if (n > 0)
        {
            _out.Flush();
            Interlocked.Add(ref _written, n);
        }
        return n;
    }

    private void WriteLine(in LogEntry e)
    {
        var wall = new DateTime(_startTicks + (e.TimestampNs - _startNs) / 100, DateTimeKind.Utc);
        _sb.Clear();
        _sb.Append("{\"ts\":\"").Append(wall.ToString("O")).Append("\",\"level\":\"").Append(e.Level)
           .Append("\",\"strategy\":").Append(JsonSerializer.Serialize(e.StrategyId == 0 ? "host" : _strategyName(e.StrategyId)))
           .Append(",\"msg\":").Append(JsonSerializer.Serialize(e.Template))
           .Append(",\"a\":").Append(e.A).Append(",\"b\":").Append(e.B).Append('}');
        _out.WriteLine(_sb.ToString());
    }

    public void Dispose()
    {
        _thread?.RequestStop();
        _thread?.Join(TimeSpan.FromSeconds(1));
        while (PollOnce() > 0) { }
    }
}

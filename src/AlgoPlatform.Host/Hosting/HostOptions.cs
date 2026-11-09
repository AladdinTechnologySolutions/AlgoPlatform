using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Core.Threading;
using AlgoPlatform.Gateway;
using AlgoPlatform.MarketData;
using AlgoPlatform.ReferenceData;
using AlgoPlatform.SimExchange;

namespace AlgoPlatform.Host.Hosting;

public sealed class HostOptions
{
    public string HostId { get; set; } = "host-1";

    /// <summary>Live: gateway connects to <see cref="Gateway"/> Host:Port. Paper: an embedded SimExchange is started and
    /// driven by this host's market data. (Replay runs through AlgoPlatform.Replay, not a host.)</summary>
    public RunMode Mode { get; set; } = RunMode.Paper;

    /// <summary>A sandbox host runs untrusted/"shared" strategies: unpinned loops with a backing-off idle strategy.</summary>
    public bool Sandbox { get; set; }
    public int MaxSandboxStrategies { get; set; } = 64;

    public bool PinThreads { get; set; }
    /// <summary>Cores this host may use. First two go to the feed handler and the gateway, the rest to strategy loops.
    /// Empty = all logical processors.</summary>
    public int[] Cores { get; set; } = [];
    public IdleMode StrategyIdle { get; set; } = IdleMode.SpinYield;
    public IdleMode InfrastructureIdle { get; set; } = IdleMode.SpinYield;

    public string DataDirectory { get; set; } = Path.Combine("data", "host-1");
    public bool EnableJournal { get; set; } = true;
    public ReferenceDataSnapshot? ReferenceData { get; set; }
    public string? ReferenceDataPath { get; set; }

    /// <summary>Override the market data source (tests, benchmarks). Default: a seeded SimulatedFeed over all instruments.</summary>
    public IFeedSource? FeedSource { get; set; }
    public ulong FeedSeed { get; set; } = 42;
    public double FeedMessagesPerSecond { get; set; } = 2_000;

    public GatewayOptions Gateway { get; set; } = new();
    public SimExchangeOptions EmbeddedExchange { get; set; } = new();
    /// <summary>Test hook: false starts the host with no gateway at all (orders go nowhere).</summary>
    public bool EnableGateway { get; set; } = true;

    public long HandlerBudgetNs { get; set; } = 20_000;
    public int MaxConsecutiveBreaches { get; set; } = 10;
    public int HungThresholdMs { get; set; } = 1_000;

    public int MarketDataRingCapacity { get; set; } = 4096;
    public int MarketDataOverflowCapacity { get; set; } = 65536;
    public int ControlRingCapacity { get; set; } = 16384;
    public int JournalRingCapacity { get; set; } = 8192;
    public bool RetainTerminalOrders { get; set; }

    public TextWriter? LogOutput { get; set; }

    public int[] EffectiveCores() => Cores.Length > 0 ? Cores : Enumerable.Range(0, Environment.ProcessorCount).ToArray();
}

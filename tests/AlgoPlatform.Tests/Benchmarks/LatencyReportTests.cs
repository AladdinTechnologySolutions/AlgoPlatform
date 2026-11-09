using AlgoPlatform.Benchmarks;
using AlgoPlatform.Core.Threading;

namespace AlgoPlatform.Tests.Benchmarks;

/// <summary>
/// Acceptance test 9: the latency harness produces a tick-to-order report (p50/p99/p99.9/max) from real
/// measurements. This runs a short version; the full 1/10/50-strategy run writes docs/PERFORMANCE.results.md.
/// </summary>
public class LatencyReportTests
{
    [Fact]
    public void Harness_measures_tick_to_order_and_renders_the_report()
    {
        var row = LatencyHarness.RunHost(2, IdleMode.SpinYield, 5_000, TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(1));
        Assert.True(row.Samples > 50, $"only {row.Samples} samples");
        Assert.True(row.TickToOrder.Percentile(50) > 0);
        Assert.True(row.TickToOrder.Percentile(99.9) <= row.TickToOrder.Max);
        var fanout = LatencyHarness.RunFanout(4, 100_000, 2);
        Assert.Equal(fanout.TradesPublished, fanout.TradesReceived);
        string md = LatencyHarness.Render([row], [fanout], (1, 1, 0, 1), LatencyHarness.MachineDescription(), TimeSpan.FromSeconds(1));
        Assert.Contains("| 2 | SpinYield |", md);
        Assert.Contains("intended", md);
    }
}

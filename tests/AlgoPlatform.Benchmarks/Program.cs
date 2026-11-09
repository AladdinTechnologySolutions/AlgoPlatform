using AlgoPlatform.Core.Threading;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;

namespace AlgoPlatform.Benchmarks;

/// <summary>
/// dotnet run -c Release --project tests/AlgoPlatform.Benchmarks -- report [seconds]   latency harness, writes docs/PERFORMANCE.results.md
/// dotnet run -c Release --project tests/AlgoPlatform.Benchmarks -- quick              short smoke run of every harness mode
/// dotnet run -c Release --project tests/AlgoPlatform.Benchmarks -- micro              BenchmarkDotNet micro-benchmarks
/// </summary>
public static class Program
{
    public static int Main(string[] args)
    {
        string mode = args.Length > 0 ? args[0] : "report";
        if (mode == "micro")
        {
            BenchmarkRunner.Run<MicroBenchmarks>(DefaultConfig.Instance, args[1..]);
            return 0;
        }

        if (mode == "alloc")
        {
            var a = LatencyHarness.RunAllocation(args.Length > 1 ? long.Parse(args[1]) : 10_000_000);
            Console.WriteLine($"allocation: {a.Bytes} bytes over {a.Ticks} ticks, {a.Orders} orders, {a.NsPerTick:F0} ns/tick");
            return 0;
        }

        bool quick = mode == "quick";
        var measure = TimeSpan.FromSeconds(quick ? 1 : args.Length > 1 ? double.Parse(args[1]) : 10);
        var warmup = TimeSpan.FromSeconds(quick ? 0.5 : 3);
        string machine = LatencyHarness.MachineDescription();
        Console.WriteLine(machine);

        var rows = new List<LatencyRow>();
        var configs = quick
            ? new[] { (1, IdleMode.BusySpin, 5_000.0) }
            : new[]
            {
                (1, IdleMode.BusySpin, 20_000.0),
                (10, IdleMode.SpinYield, 20_000.0),
                (50, IdleMode.SpinYield, 20_000.0),
                (50, IdleMode.Backoff, 20_000.0),
            };
        foreach (var (n, idle, rate) in configs)
        {
            var row = LatencyHarness.RunHost(n, idle, rate, warmup, measure);
            rows.Add(row);
            Console.WriteLine($"{n,3} strategies {idle,-9} tick-to-order {row.TickToOrder.Summary()} handler p99 {LatencyHarness.Us(row.Handler.Percentile(99))}us orders {row.Orders}");
        }

        var fanout = new List<LatencyHarness.FanoutResult>();
        foreach (int subs in quick ? new[] { 4 } : new[] { 1, 10, 50 })
        {
            var f = LatencyHarness.RunFanout(subs, quick ? 200_000 : 5_000_000, consumerThreads: Math.Min(subs, 6));
            fanout.Add(f);
            Console.WriteLine($"fan-out {subs,3} subscribers: {f.TicksPerSecond:N0} ticks/s, trades {f.TradesReceived}/{f.TradesPublished}, conflated {f.Conflated}");
        }

        var alloc = LatencyHarness.RunAllocation(quick ? 1_000_000 : 10_000_000);
        Console.WriteLine($"allocation: {alloc.Bytes} bytes over {alloc.Ticks} ticks, {alloc.NsPerTick:F0} ns/tick");

        string text = LatencyHarness.Render(rows, fanout, alloc, machine, measure);
        string path = args.Length > 2 ? args[2] : Path.Combine(FindRepoRoot(), "docs", quick ? "PERFORMANCE.quick.md" : "PERFORMANCE.results.md");
        File.WriteAllText(path, text);
        Console.WriteLine("wrote " + path);
        return 0;
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "AlgoPlatform.sln"))) dir = dir.Parent;
        return dir?.FullName ?? Directory.GetCurrentDirectory();
    }
}

using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AlgoPlatform.ControlPlane;
using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Core.Journal;
using AlgoPlatform.Host.Api;
using AlgoPlatform.Host.Hosting;
using AlgoPlatform.Host.Loading;
using AlgoPlatform.Strategies.Samples;
using AlgoPlatform.Tests.Host;

namespace AlgoPlatform.Tests.ControlPlane;

/// <summary>Acceptance test 10: deploy, start, kill, stop via REST against a running host.</summary>
public class ControlPlaneTests
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private static byte[] Package(StrategyManifest manifest) =>
        StrategyLoader.BuildPackage(manifest, typeof(MarketMaker).Assembly.Location);

    [Fact]
    public async Task Deploy_start_kill_stop_through_the_control_plane()
    {
        await using var cp = await ControlPlaneApp.StartAsync(new ControlPlaneOptions
        {
            DataDirectory = TestSupport.TempDir("cp"), HeartbeatIntervalMs = 200,
        });
        using var host = new StrategyHost(TestSupport.HostOptions("cp-host", feedRate: 5_000));
        host.Start();
        await using var hostServer = await HostServer.StartAsync(host, controlPlaneUrl: cp.Url);
        using var http = new HttpClient { BaseAddress = new Uri(cp.Url) };

        // Bad package: refused with the reasons.
        var badManifest = TestSupport.Manifest("bad");
        badManifest.Limits.MaxOrderQty = 0;
        var bad = await http.PostAsync("/packages", new ByteArrayContent(Package(badManifest)));
        Assert.Equal(HttpStatusCode.BadRequest, bad.StatusCode);
        Assert.Contains("maxOrderQty", await bad.Content.ReadAsStringAsync());

        // Upload, deploy, start.
        var manifest = TestSupport.Manifest("mm-rest", ["VOD.L", "BARC.L"]);
        var upload = await http.PostAsync("/packages", new ByteArrayContent(Package(manifest)));
        Assert.Equal(HttpStatusCode.Created, upload.StatusCode);
        var deploy = await http.PostAsync("/strategies/mm-rest/deploy", null);
        Assert.Equal(HttpStatusCode.OK, deploy.StatusCode);
        Assert.Contains("cp-host", await deploy.Content.ReadAsStringAsync());
        Assert.Equal(HttpStatusCode.OK, (await http.PostAsync("/strategies/mm-rest/start", null)).StatusCode);

        Assert.True(await TestSupport.WaitUntilAsync(async () =>
        {
            var info = await StrategyAsync(http, "mm-rest");
            return info is { Status: "Running", LiveOrders: >= 2 };
        }, TimeSpan.FromSeconds(15)), "strategy never quoted");

        string metrics = await http.GetStringAsync("/metrics");
        Assert.Contains("algo_strategy_orders_sent_total{host=\"cp-host\",strategy=\"mm-rest\"}", metrics);
        Assert.Contains("algo_strategy_tick_to_order_seconds", metrics);

        // Kill: working orders are cancelled, nothing new is sent.
        Assert.Equal(HttpStatusCode.OK, (await http.PostAsync("/strategies/mm-rest/kill", null)).StatusCode);
        Assert.True(await TestSupport.WaitUntilAsync(async () =>
            await StrategyAsync(http, "mm-rest") is { Status: "Killed", LiveOrders: 0 }, TimeSpan.FromSeconds(10)));

        var journal = await http.GetFromJsonAsync<List<JournalEntry>>("/strategies/mm-rest/journal", Json);
        Assert.Contains(journal!, e => e.Type == "In.Control" && e.Detail == "Kill");

        // Stop.
        Assert.Equal(HttpStatusCode.OK, (await http.PostAsync("/strategies/mm-rest/stop", null)).StatusCode);
        Assert.True(await TestSupport.WaitUntilAsync(async () =>
            await StrategyAsync(http, "mm-rest") is { Status: "Stopped" }, TimeSpan.FromSeconds(10)));

        // The kill took effect within one loop iteration: cancels for every live order directly follow it.
        var slot = host.GetSlot("mm-rest")!;
        Assert.True(host.Journal.WaitClosed(slot.Journal!, TimeSpan.FromSeconds(5)));
        KillJournalCheck.AssertCancelsFollowKillImmediately(JournalReader.ReadAll(slot.Journal!.Path!));

        // Replay through the control plane: the recorded session reproduces exactly.
        var replay = await http.PostAsJsonAsync("/replays", new ReplayRequest("mm-rest", null));
        Assert.Equal(HttpStatusCode.OK, replay.StatusCode);
        var record = await replay.Content.ReadFromJsonAsync<ReplayRecord>(Json);
        Assert.True(record!.MatchesOriginal, $"replay mismatch at {record.FirstMismatch}");
        Assert.True(record.Outputs > 0);
    }

    [Fact]
    public async Task Placement_rejects_when_no_host_has_capacity_and_heartbeats_mark_hosts_down()
    {
        await using var cp = await ControlPlaneApp.StartAsync(new ControlPlaneOptions
        {
            DataDirectory = TestSupport.TempDir("cp2"), HeartbeatIntervalMs = 100, MissedHeartbeatsForDown = 2,
        });
        var options = TestSupport.HostOptions("small-host");
        options.Cores = [0, 1, 2];   // one strategy core
        using var host = new StrategyHost(options);
        host.Start();
        var hostServer = await HostServer.StartAsync(host, controlPlaneUrl: cp.Url);
        using var http = new HttpClient { BaseAddress = new Uri(cp.Url) };

        await http.PostAsync("/packages", new ByteArrayContent(Package(TestSupport.Manifest("first"))));
        await http.PostAsync("/packages", new ByteArrayContent(Package(TestSupport.Manifest("second"))));
        await http.PostAsync("/packages", new ByteArrayContent(Package(TestSupport.Manifest("sandboxed", resourceClass: ResourceClasses.Shared))));
        Assert.Equal(HttpStatusCode.OK, (await http.PostAsync("/strategies/first/deploy", null)).StatusCode);

        var full = await http.PostAsync("/strategies/second/deploy", null);
        Assert.Equal(HttpStatusCode.Conflict, full.StatusCode);
        Assert.Contains("no capacity", await full.Content.ReadAsStringAsync());
        var noSandbox = await http.PostAsync("/strategies/sandboxed/deploy", null);
        Assert.Equal(HttpStatusCode.Conflict, noSandbox.StatusCode);

        // Redeploying the same strategy (hot reload) reuses its core.
        Assert.Equal(HttpStatusCode.OK, (await http.PostAsync("/strategies/first/deploy", null)).StatusCode);

        await hostServer.DisposeAsync();   // host stops answering
        Assert.True(await TestSupport.WaitUntilAsync(async () =>
        {
            var hosts = await http.GetFromJsonAsync<List<HostRecord>>("/hosts", Json);
            return hosts!.Single().Status == "Down";
        }, TimeSpan.FromSeconds(5)));
        var alerts = await http.GetStringAsync("/alerts");
        Assert.Contains("missed", alerts);
    }

    private static async Task<StrategyInfo?> StrategyAsync(HttpClient http, string name)
    {
        var list = await http.GetFromJsonAsync<List<JsonElement>>("/strategies", Json);
        foreach (var item in list!)
        {
            var s = item.GetProperty("strategy").Deserialize<StrategyInfo>(Json)!;
            if (s.Name == name) return s;
        }
        return null;
    }
}

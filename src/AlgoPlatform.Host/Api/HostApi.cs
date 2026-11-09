using System.Net.Http.Json;
using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Journal;
using AlgoPlatform.Host.Hosting;
using AlgoPlatform.Sdk;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AlgoPlatform.Host.Api;

public sealed record StrategyInfo(string Name, string Version, int StrategyId, string Status, string[] Instruments, int Core,
    long OrdersSent, long OrdersRejected, long Fills, long LiveOrders, string? LastError);

public sealed record HostHealth(string HostId, string Mode, bool Sandbox, string Gateway, int Strategies, int FreeDedicatedCores,
    int FreeSandboxSlots, int DedicatedCapacity);

public sealed record JournalEntry(long Seq, long TimestampNs, string Type, string Detail);

public sealed record HostRegistration(string Id, string Url, bool Sandbox, int DedicatedCores, int SandboxSlots);

/// <summary>Per-host control endpoint (control path only; nothing here runs on a strategy loop).</summary>
public static class HostApi
{
    public static void Map(IEndpointRouteBuilder app, StrategyHost host)
    {
        app.MapGet("/health", () => Health(host));
        app.MapGet("/metrics", () => Results.Text(host.MetricsText(), "text/plain; version=0.0.4"));
        app.MapGet("/strategies", () => host.Slots.Select(Info).ToList());
        app.MapGet("/strategies/{name}", (string name) =>
            host.GetSlot(name) is { } s ? Results.Ok(Info(s)) : Results.NotFound());

        app.MapPost("/strategies", async (HttpRequest req, bool? start) =>
        {
            using var ms = new MemoryStream();
            await req.Body.CopyToAsync(ms);
            try
            {
                StrategySlot slot = host.DeployPackage(ms.ToArray());
                if (start == true) host.Start(slot.Name);
                return Results.Ok(Info(host.GetSlot(slot.Name)!));
            }
            catch (CapacityException ex) { return Results.Conflict(new { error = ex.Message }); }
            catch (Exception ex) when (ex is InvalidDataException or InvalidOperationException or TypeLoadException or BadImageFormatException)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });

        app.MapPost("/strategies/{name}/start", (string name) => Act(host, name, () => host.Start(name)));
        app.MapPost("/strategies/{name}/stop", (string name) => Act(host, name, () => host.Stop(name, TimeSpan.FromSeconds(5))));
        app.MapPost("/strategies/{name}/kill", (string name, bool? flatten) => Act(host, name, () => host.Kill(name, flatten == true)));
        app.MapDelete("/strategies/{name}", (string name) => Act(host, name, () => host.Remove(name)));
        app.MapPost("/kill", (bool? flatten) =>
        {
            host.KillAll(flatten == true);
            return Results.Ok(new { killed = host.Slots.Count });
        });

        app.MapGet("/strategies/{name}/journal", (string name, long? from, long? to, int? limit) =>
        {
            string? path = host.GetSlot(name)?.Journal?.Path;
            if (path is null || !File.Exists(path)) return Results.NotFound();
            return Results.Ok(ReadJournal(path, from ?? 0, to ?? long.MaxValue, limit ?? 10_000));
        });
        app.MapGet("/strategies/{name}/journal/raw", (string name) =>
        {
            string? path = host.GetSlot(name)?.Journal?.Path;
            if (path is null || !File.Exists(path)) return Results.NotFound();
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            using var ms = new MemoryStream();
            fs.CopyTo(ms);
            return Results.File(ms.ToArray(), "application/octet-stream", Path.GetFileName(path));
        });
    }

    public static HostHealth Health(StrategyHost host) => new(host.Options.HostId, host.Options.Mode.ToString(), host.Options.Sandbox,
        host.Gateway?.State.ToString() ?? "None", host.Slots.Count, host.FreeDedicatedCores, host.FreeSandboxSlots, host.DedicatedCapacity);

    public static StrategyInfo Info(StrategySlot s)
    {
        var e = s.Engine;
        return new StrategyInfo(s.Name, s.Manifest.Version, s.StrategyId, s.Status.ToString(), s.Manifest.Instruments, s.Core,
            e?.Metrics.OrdersSent ?? 0, e?.Metrics.OrdersRejected ?? 0, e?.Metrics.Fills ?? 0, e?.Oms.LiveOrderCount ?? 0, e?.LastError);
    }

    private static IResult Act(StrategyHost host, string name, Action action)
    {
        if (host.GetSlot(name) is null) return Results.NotFound(new { error = $"no strategy '{name}'" });
        try
        {
            action();
        }
        catch (InvalidOperationException ex)
        {
            return Results.Conflict(new { error = ex.Message });
        }
        return host.GetSlot(name) is { } s ? Results.Ok(Info(s)) : Results.Ok();
    }

    public static List<JournalEntry> ReadJournal(string path, long from, long to, int limit)
    {
        var list = new List<JournalEntry>();
        foreach (JournalRecord r in JournalReader.ReadAll(path))
        {
            if (r.Seq < from || r.Seq > to) continue;
            list.Add(Describe(r));
            if (list.Count >= limit) break;
        }
        return list;
    }

    public static JournalEntry Describe(JournalRecord r)
    {
        if (r.IsOutput)
        {
            OrderCommand c = JournalCodec.DecodeOutput(r);
            string detail = $"order={c.OrderId} {c.Side} {c.OrdType} px={Price.ToDouble(c.Px)} qty={c.Qty} instrument={c.Instrument.Value}" +
                (c.RejectCode != 0 ? $" REJECTED {RejectCodes.Name(c.RejectCode)}" : "");
            return new JournalEntry(r.Seq, r.TimestampNs, "Out." + c.Type, detail);
        }
        HostEvent e = JournalCodec.DecodeInput(r);
        string d = e.Type switch
        {
            EventType.Tick => $"instrument={e.Tick.Instrument.Value} bid={Price.ToDouble(e.Tick.BidPx)}x{e.Tick.BidQty} ask={Price.ToDouble(e.Tick.AskPx)}x{e.Tick.AskQty} last={Price.ToDouble(e.Tick.LastPx)}x{e.Tick.LastQty}",
            EventType.Execution => $"order={e.Exec.OrderId} exec={e.Exec.ExecId} {e.Exec.Status} last={Price.ToDouble(e.Exec.LastPx)}x{e.Exec.LastQty} cum={e.Exec.CumQty} leaves={e.Exec.LeavesQty}" +
                (e.Exec.RejectCode != 0 ? $" reject={RejectCodes.Name(e.Exec.RejectCode)}" : ""),
            EventType.Control => ((ControlCode)e.A).ToString(),
            EventType.Timer => $"timer={e.A}",
            EventType.ExchangeStatus => e.A == 1 ? "exchange up" : "exchange down",
            EventType.InstrumentStatus => $"instrument={e.A} {(e.B == 1 ? "stale" : "ok")}",
            EventType.ReferenceData => $"version={e.A}",
            EventType.AsyncResult => $"request={e.A}",
            _ => "",
        };
        return new JournalEntry(r.Seq, r.TimestampNs, "In." + e.Type, d);
    }
}

/// <summary>Runs the host's HTTP endpoint on Kestrel (port 0 picks a free port).</summary>
public sealed class HostServer : IAsyncDisposable
{
    private readonly WebApplication _app;

    private HostServer(WebApplication app, string url)
    {
        _app = app;
        Url = url;
    }

    public string Url { get; }

    public static async Task<HostServer> StartAsync(StrategyHost host, string url = "http://127.0.0.1:0", string? controlPlaneUrl = null)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls(url);
        builder.Logging.ClearProviders();
        var app = builder.Build();
        HostApi.Map(app, host);
        await app.StartAsync();
        string bound = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        if (controlPlaneUrl is not null)
        {
            using var http = new HttpClient();
            var reg = new HostRegistration(host.Options.HostId, bound, host.Options.Sandbox, host.DedicatedCapacity,
                host.Options.Sandbox ? host.Options.MaxSandboxStrategies : 0);
            (await http.PostAsJsonAsync(controlPlaneUrl.TrimEnd('/') + "/hosts", reg)).EnsureSuccessStatusCode();
        }
        return new HostServer(app, bound);
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}

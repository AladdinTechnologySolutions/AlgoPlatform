using System.Net.Http.Json;
using AlgoPlatform.Host.Api;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AlgoPlatform.ControlPlane;

/// <summary>
/// Control plane REST API (FR-12): upload package, list strategies, deploy/start/stop/kill, metrics,
/// replay and journal access. It never touches the hot path; it talks to hosts over their HTTP endpoints.
/// </summary>
public sealed class ControlPlaneApp : IAsyncDisposable
{
    private readonly WebApplication _app;

    private ControlPlaneApp(WebApplication app, ControlPlaneState state, string url)
    {
        _app = app;
        State = state;
        Url = url;
    }

    public ControlPlaneState State { get; }
    public string Url { get; }

    public static async Task<ControlPlaneApp> StartAsync(ControlPlaneOptions options, string url = "http://127.0.0.1:0")
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls(url);
        builder.Logging.ClearProviders();
        var app = builder.Build();
        var state = new ControlPlaneState(options);
        Map(app, state);
        await app.StartAsync();
        state.StartHeartbeats();
        string bound = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new ControlPlaneApp(app, state, bound);
    }

    public static void Map(IEndpointRouteBuilder app, ControlPlaneState state)
    {
        // Hosts
        app.MapPost("/hosts", (HostRegistration reg) => Results.Ok(state.RegisterHost(reg)));
        app.MapGet("/hosts", () => state.Hosts.Values.OrderBy(h => h.Id));

        // Packages
        app.MapPost("/packages", async (HttpRequest req) =>
        {
            using var ms = new MemoryStream();
            await req.Body.CopyToAsync(ms);
            try
            {
                var info = state.SavePackage(ms.ToArray());
                return Results.Created($"/packages/{info.Name}/{info.Version}", info);
            }
            catch (PackageValidationException ex) { return Results.BadRequest(new { errors = ex.Errors }); }
        });
        app.MapGet("/packages", () => state.Packages);

        // Strategies
        app.MapPost("/strategies/{name}/deploy", async (string name, string? version, bool? start) =>
        {
            try
            {
                var (deployment, strategy) = await state.DeployAsync(name, version, start == true);
                return Results.Ok(new { deployment, strategy });
            }
            catch (KeyNotFoundException ex) { return Results.NotFound(new { error = ex.Message }); }
            catch (PlacementException ex) { return Results.Conflict(new { error = ex.Message }); }
            catch (InvalidOperationException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });
        app.MapPost("/strategies/{name}/start", (string name) => Forward(state, name, "start"));
        app.MapPost("/strategies/{name}/stop", (string name) => Forward(state, name, "stop"));
        app.MapPost("/strategies/{name}/kill", (string name, bool? flatten) => Forward(state, name, $"kill?flatten={(flatten == true ? "true" : "false")}"));
        app.MapGet("/strategies", async () =>
        {
            var all = new List<object>();
            foreach (var h in state.Hosts.Values.Where(h => h.Status == "Up"))
            {
                try
                {
                    var list = await state.Http.GetFromJsonAsync<List<StrategyInfo>>($"{h.Url}/strategies");
                    if (list is not null) all.AddRange(list.Select(s => new { host = h.Id, strategy = s }));
                }
                catch (Exception) { }
            }
            return Results.Ok(all);
        });
        app.MapGet("/strategies/{name}/journal", async (string name, long? from, long? to) =>
        {
            try
            {
                var host = state.HostOf(name);
                string q = $"?from={from ?? 0}&to={to ?? long.MaxValue}";
                var resp = await state.Http.GetAsync($"{host.Url}/strategies/{name}/journal{q}");
                return Results.Content(await resp.Content.ReadAsStringAsync(), "application/json", statusCode: (int)resp.StatusCode);
            }
            catch (KeyNotFoundException ex) { return Results.NotFound(new { error = ex.Message }); }
        });

        // Global kill switch
        app.MapPost("/kill", async (bool? flatten) =>
        {
            int hosts = 0;
            foreach (var h in state.Hosts.Values.Where(h => h.Status == "Up"))
            {
                try
                {
                    await state.Http.PostAsync($"{h.Url}/kill?flatten={(flatten == true ? "true" : "false")}", null);
                    hosts++;
                }
                catch (Exception ex) { state.Alert(h.Id, "global kill could not reach host: " + ex.Message); }
            }
            return Results.Ok(new { hosts });
        });

        // Observability
        app.MapGet("/metrics", async () => Results.Text(await state.AggregateMetricsAsync(), "text/plain; version=0.0.4"));
        app.MapGet("/alerts", () => state.Alerts.ToArray());

        // Replay
        app.MapPost("/replays", async (ReplayRequest req) =>
        {
            try { return Results.Ok(await state.ReplayAsync(req.Strategy, req.Version)); }
            catch (KeyNotFoundException ex) { return Results.NotFound(new { error = ex.Message }); }
        });
        app.MapGet("/replays/{id}", (string id) => state.Replays.TryGetValue(id, out var r) ? Results.Ok(r) : Results.NotFound());
    }

    private static async Task<IResult> Forward(ControlPlaneState state, string name, string action)
    {
        HostRecord host;
        try { host = state.HostOf(name); }
        catch (KeyNotFoundException ex) { return Results.NotFound(new { error = ex.Message }); }
        var resp = await state.Http.PostAsync($"{host.Url}/strategies/{name}/{action}", null);
        return Results.Content(await resp.Content.ReadAsStringAsync(), "application/json", statusCode: (int)resp.StatusCode);
    }

    public async ValueTask DisposeAsync()
    {
        State.Dispose();
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}

public sealed record ReplayRequest(string Strategy, string? Version);

using AlgoPlatform.ControlPlane;
using Microsoft.Extensions.Configuration;

// Control plane process. Example:
//   dotnet run --project src/AlgoPlatform.ControlPlane -- --Url=http://127.0.0.1:5100
// Hosts register themselves by passing --ControlPlane=http://127.0.0.1:5100 on start.

var config = new ConfigurationBuilder().AddCommandLine(args).AddEnvironmentVariables("ALGO_").Build();
var options = new ControlPlaneOptions
{
    DataDirectory = config["DataDirectory"] ?? Path.Combine("data", "control-plane"),
    HeartbeatIntervalMs = int.Parse(config["HeartbeatIntervalMs"] ?? "1000"),
};
await using var app = await ControlPlaneApp.StartAsync(options, config["Url"] ?? "http://127.0.0.1:5100");
Console.WriteLine($"control plane listening on {app.Url}. Ctrl+C to stop.");

var done = new TaskCompletionSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    done.TrySetResult();
};
await done.Task;

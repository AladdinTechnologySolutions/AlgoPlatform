using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Host.Api;
using AlgoPlatform.Host.Hosting;
using Microsoft.Extensions.Configuration;

// Strategy host process. Example:
//   dotnet run --project src/AlgoPlatform.Host -- --HostId=host-1 --Mode=Paper --Url=http://127.0.0.1:5101 --ControlPlane=http://127.0.0.1:5100
// Live mode connects to a running SimExchange:  --Mode=Live --ExchangeHost=127.0.0.1 --ExchangePort=9878

var config = new ConfigurationBuilder().AddCommandLine(args).AddEnvironmentVariables("ALGO_").Build();
string hostId = config["HostId"] ?? "host-1";
var options = new HostOptions
{
    HostId = hostId,
    Mode = Enum.Parse<RunMode>(config["Mode"] ?? "Paper", ignoreCase: true),
    Sandbox = bool.Parse(config["Sandbox"] ?? "false"),
    PinThreads = bool.Parse(config["PinThreads"] ?? "false"),
    Cores = (config["Cores"] ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries).Select(int.Parse).ToArray(),
    DataDirectory = config["DataDirectory"] ?? Path.Combine("data", hostId),
    ReferenceDataPath = config["ReferenceData"],
    FeedMessagesPerSecond = double.Parse(config["FeedRate"] ?? "2000", System.Globalization.CultureInfo.InvariantCulture),
    LogOutput = Console.Out,
};
if (options.Mode == RunMode.Live)
{
    options.Gateway.Host = config["ExchangeHost"] ?? "127.0.0.1";
    options.Gateway.Port = int.Parse(config["ExchangePort"] ?? "9878");
}

using var host = new StrategyHost(options);
host.Start();
await using var server = await HostServer.StartAsync(host, config["Url"] ?? "http://127.0.0.1:5101", config["ControlPlane"]);
Console.WriteLine($"host {hostId} ({options.Mode}) listening on {server.Url}. Ctrl+C to stop.");

var done = new TaskCompletionSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    done.TrySetResult();
};
await done.Task;

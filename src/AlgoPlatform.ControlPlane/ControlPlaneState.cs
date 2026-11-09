using System.Collections.Concurrent;
using System.IO.Compression;
using System.Net.Http.Json;
using System.Text;
using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Host.Api;
using AlgoPlatform.Host.Loading;
using AlgoPlatform.Replay;

namespace AlgoPlatform.ControlPlane;

public sealed class ControlPlaneOptions
{
    public string DataDirectory { get; set; } = Path.Combine("data", "control-plane");
    public int HeartbeatIntervalMs { get; set; } = 1000;
    public int MissedHeartbeatsForDown { get; set; } = 3;
}

public sealed record PackageInfo(string Name, string Version, string ResourceClass, string[] Instruments, string ZipPath, DateTime UploadedUtc);

public sealed class HostRecord
{
    public required string Id { get; init; }
    public required string Url { get; set; }
    public bool Sandbox { get; set; }
    public int DedicatedCores { get; set; }
    public int SandboxSlots { get; set; }
    public string Status { get; set; } = "Up";
    public DateTime LastSeenUtc { get; set; } = DateTime.UtcNow;
    public int MissedHeartbeats { get; set; }
}

public sealed record DeploymentRecord(string Name, string Version, string HostId, DateTime DeployedUtc);

public sealed record ReplayRecord(string Id, string Strategy, string Version, bool MatchesOriginal, int FirstMismatch, int InputEvents,
    int Outputs, int OriginalOutputs, string Digest, string FinalStatus, double ElapsedMs);

public sealed record Alert(DateTime TimeUtc, string Source, string Message);

public sealed class PackageValidationException(List<string> errors) : Exception(string.Join("; ", errors))
{
    public List<string> Errors { get; } = errors;
}

public sealed class PlacementException(string message) : Exception(message);

/// <summary>Everything the control plane knows. Control path only: allocation, locks and async are fine here.</summary>
public sealed class ControlPlaneState : IDisposable
{
    private readonly ControlPlaneOptions _options;
    private readonly ConcurrentDictionary<string, PackageInfo> _packages = new();
    private readonly object _placementGate = new();
    private Timer? _heartbeat;

    public ControlPlaneState(ControlPlaneOptions options)
    {
        _options = options;
        Directory.CreateDirectory(PackageRoot);
    }

    public HttpClient Http { get; } = new() { Timeout = TimeSpan.FromSeconds(30) };
    public ConcurrentDictionary<string, HostRecord> Hosts { get; } = new();
    public ConcurrentDictionary<string, DeploymentRecord> Deployments { get; } = new();
    public ConcurrentDictionary<string, ReplayRecord> Replays { get; } = new();
    public ConcurrentQueue<Alert> Alerts { get; } = new();
    public string PackageRoot => Path.Combine(_options.DataDirectory, "packages");

    // ---------------------------------------------------------------- packages

    /// <summary>Validates and stores an uploaded package (FR-2).</summary>
    public PackageInfo SavePackage(byte[] zip)
    {
        StrategyManifest manifest;
        using (var archive = OpenZip(zip))
        {
            var entry = archive.GetEntry(StrategyLoader.ManifestFile)
                ?? throw new PackageValidationException([$"package has no {StrategyLoader.ManifestFile} at its root"]);
            using (var reader = new StreamReader(entry.Open())) manifest = StrategyManifest.Parse(reader.ReadToEnd());
            var errors = manifest.Validate();
            if (errors.Count == 0)
            {
                string assembly = manifest.EntryType.Split(',', 2, StringSplitOptions.TrimEntries)[1] + ".dll";
                if (archive.GetEntry(assembly) is null) errors.Add($"entry assembly {assembly} is not in the package");
            }
            if (errors.Count > 0) throw new PackageValidationException(errors);
        }
        string dir = Path.Combine(PackageRoot, manifest.Name, manifest.Version);
        Directory.CreateDirectory(dir);
        string zipPath = Path.Combine(dir, "package.zip");
        File.WriteAllBytes(zipPath, zip);
        var info = new PackageInfo(manifest.Name, manifest.Version, manifest.ResourceClass, manifest.Instruments, zipPath, DateTime.UtcNow);
        _packages[Key(manifest.Name, manifest.Version)] = info;
        return info;
    }

    private static ZipArchive OpenZip(byte[] zip)
    {
        try { return new ZipArchive(new MemoryStream(zip), ZipArchiveMode.Read); }
        catch (InvalidDataException) { throw new PackageValidationException(["body is not a zip archive"]); }
    }

    public IEnumerable<PackageInfo> Packages => _packages.Values.OrderBy(p => p.Name).ThenBy(p => p.UploadedUtc);

    public PackageInfo? FindPackage(string name, string? version) =>
        version is not null
            ? _packages.GetValueOrDefault(Key(name, version))
            : _packages.Values.Where(p => p.Name == name).OrderByDescending(p => p.UploadedUtc).FirstOrDefault();

    private static string Key(string name, string version) => name + "@" + version;

    // ---------------------------------------------------------------- hosts and placement

    public HostRecord RegisterHost(HostRegistration reg)
    {
        var record = Hosts.AddOrUpdate(reg.Id,
            _ => new HostRecord { Id = reg.Id, Url = reg.Url, Sandbox = reg.Sandbox, DedicatedCores = reg.DedicatedCores, SandboxSlots = reg.SandboxSlots },
            (_, existing) =>
            {
                existing.Url = reg.Url;
                existing.Sandbox = reg.Sandbox;
                existing.DedicatedCores = reg.DedicatedCores;
                existing.SandboxSlots = reg.SandboxSlots;
                existing.Status = "Up";
                existing.MissedHeartbeats = 0;
                existing.LastSeenUtc = DateTime.UtcNow;
                return existing;
            });
        return record;
    }

    /// <summary>
    /// Placement (spec 8.3): dedicated-core strategies go to production hosts with a free core, "shared" ones to
    /// sandbox hosts with a free slot. A redeploy prefers the host it is already on (hot reload frees its core).
    /// No capacity is a clear error, never an overcommit.
    /// </summary>
    public async Task<(DeploymentRecord Deployment, StrategyInfo Strategy)> DeployAsync(string name, string? version, bool start)
    {
        PackageInfo package = FindPackage(name, version) ?? throw new KeyNotFoundException($"no package {name}{(version is null ? "" : "@" + version)}");
        byte[] zip = await File.ReadAllBytesAsync(package.ZipPath);
        bool sandbox = string.Equals(package.ResourceClass, ResourceClasses.Shared, StringComparison.OrdinalIgnoreCase);

        HostRecord target;
        lock (_placementGate)
        {
            var candidates = new List<(HostRecord Host, int Free)>();
            foreach (var h in Hosts.Values.Where(h => h.Status == "Up" && h.Sandbox == sandbox))
            {
                HostHealth? health = GetHealth(h);
                if (health is null) continue;
                bool alreadyThere = Deployments.TryGetValue(name, out var d) && d.HostId == h.Id;
                int free = sandbox ? health.FreeSandboxSlots : health.FreeDedicatedCores;
                if (free > 0 || alreadyThere) candidates.Add((h, alreadyThere ? int.MaxValue : free));
            }
            if (candidates.Count == 0)
                throw new PlacementException(sandbox
                    ? $"no capacity: no sandbox host has a free slot for '{name}'"
                    : $"no capacity: no production host has a free core for dedicated-core strategy '{name}'");
            target = candidates.OrderByDescending(c => c.Free).First().Host;
        }

        var content = new ByteArrayContent(zip);
        content.Headers.ContentType = new("application/zip");
        HttpResponseMessage resp = await Http.PostAsync($"{target.Url}/strategies?start={(start ? "true" : "false")}", content);
        if (!resp.IsSuccessStatusCode)
        {
            string body = await resp.Content.ReadAsStringAsync();
            if (resp.StatusCode == System.Net.HttpStatusCode.Conflict) throw new PlacementException($"host {target.Id} refused: {body}");
            throw new InvalidOperationException($"host {target.Id} rejected the deployment ({(int)resp.StatusCode}): {body}");
        }
        StrategyInfo info = (await resp.Content.ReadFromJsonAsync<StrategyInfo>())!;
        var deployment = new DeploymentRecord(package.Name, package.Version, target.Id, DateTime.UtcNow);
        Deployments[name] = deployment;
        return (deployment, info);
    }

    private HostHealth? GetHealth(HostRecord h)
    {
        try { return Http.GetFromJsonAsync<HostHealth>($"{h.Url}/health").GetAwaiter().GetResult(); }
        catch (Exception) { return null; }
    }

    public HostRecord HostOf(string strategy)
    {
        if (!Deployments.TryGetValue(strategy, out var d)) throw new KeyNotFoundException($"strategy '{strategy}' is not deployed");
        return Hosts.TryGetValue(d.HostId, out var h) ? h : throw new KeyNotFoundException($"host {d.HostId} is not registered");
    }

    // ---------------------------------------------------------------- heartbeats

    /// <summary>Polls every host's /health. After N misses the host is marked Down and an alert raised (spec section 10).</summary>
    public void StartHeartbeats() =>
        _heartbeat = new Timer(_ => CheckHosts(), null, _options.HeartbeatIntervalMs, _options.HeartbeatIntervalMs);

    public void CheckHosts()
    {
        foreach (var h in Hosts.Values)
        {
            bool ok;
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(Math.Max(250, _options.HeartbeatIntervalMs)));
                ok = Http.GetAsync($"{h.Url}/health", cts.Token).GetAwaiter().GetResult().IsSuccessStatusCode;
            }
            catch (Exception) { ok = false; }

            lock (h)
            {
                if (ok)
                {
                    if (h.Status == "Down") Alert(h.Id, "host is back; strategies must reconcile with the exchange before trading");
                    h.Status = "Up";
                    h.MissedHeartbeats = 0;
                    h.LastSeenUtc = DateTime.UtcNow;
                }
                else if (++h.MissedHeartbeats >= _options.MissedHeartbeatsForDown && h.Status != "Down")
                {
                    Alert(h.Id, $"host missed {h.MissedHeartbeats} heartbeats: marked Down (restart it in recover mode)");
                    h.Status = "Down";
                }
            }
        }
    }

    public void Alert(string source, string message)
    {
        Alerts.Enqueue(new Alert(DateTime.UtcNow, source, message));
        while (Alerts.Count > 1000) Alerts.TryDequeue(out _);
        Console.Error.WriteLine($"ALERT [{source}] {message}");
    }

    // ---------------------------------------------------------------- replay

    /// <summary>Fetches a strategy's journal from its host and replays it against a package version.</summary>
    public async Task<ReplayRecord> ReplayAsync(string name, string? version)
    {
        HostRecord host = HostOf(name);
        PackageInfo package = FindPackage(name, version ?? Deployments[name].Version)
            ?? throw new KeyNotFoundException($"no package {name}@{version}");
        HttpResponseMessage resp = await Http.GetAsync($"{host.Url}/strategies/{name}/journal/raw");
        if (!resp.IsSuccessStatusCode) throw new KeyNotFoundException($"host {host.Id} has no journal for {name}");
        byte[] journal = await resp.Content.ReadAsByteArrayAsync();

        string work = Path.Combine(_options.DataDirectory, "replays", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(work);
        string dir = StrategyLoader.Extract(await File.ReadAllBytesAsync(package.ZipPath), work);
        LoadedStrategy loaded = StrategyLoader.Load(dir);
        try
        {
            var records = Core.Journal.JournalReader.Parse(journal);
            ReplayResult result = JournalReplayer.Run(records, loaded.Factory);
            var rec = new ReplayRecord(Guid.NewGuid().ToString("N")[..12], name, package.Version, result.MatchesOriginal, result.FirstMismatch,
                result.InputEvents, result.Outputs.Count, result.OriginalOutputs.Count, result.Digest, result.FinalStatus.ToString(),
                result.Elapsed.TotalMilliseconds);
            Replays[rec.Id] = rec;
            return rec;
        }
        finally
        {
            loaded.Context.Unload();
        }
    }

    // ---------------------------------------------------------------- metrics

    /// <summary>Merges every host's Prometheus text into one exposition, one HELP/TYPE block per metric family.</summary>
    public async Task<string> AggregateMetricsAsync()
    {
        var families = new Dictionary<string, (List<string> Header, List<string> Samples)>();
        var order = new List<string>();
        foreach (var h in Hosts.Values.Where(h => h.Status == "Up"))
        {
            string text;
            try { text = await Http.GetStringAsync($"{h.Url}/metrics"); }
            catch (Exception) { continue; }
            string current = "";
            foreach (string line in text.Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                if (line.StartsWith("# HELP ") || line.StartsWith("# TYPE "))
                {
                    current = line.Split(' ')[2];
                    if (!families.ContainsKey(current))
                    {
                        families[current] = (new List<string>(), new List<string>());
                        order.Add(current);
                    }
                    if (families[current].Header.Count < 2 && !families[current].Header.Contains(line)) families[current].Header.Add(line);
                    continue;
                }
                if (current.Length > 0) families[current].Samples.Add(line);
            }
        }
        var sb = new StringBuilder();
        foreach (string name in order)
        {
            foreach (string l in families[name].Header) sb.Append(l).Append('\n');
            foreach (string l in families[name].Samples) sb.Append(l).Append('\n');
        }
        sb.Append("# HELP algo_controlplane_hosts Registered hosts by status\n# TYPE algo_controlplane_hosts gauge\n");
        foreach (var g in Hosts.Values.GroupBy(h => h.Status))
            sb.Append("algo_controlplane_hosts{status=\"").Append(g.Key).Append("\"} ").Append(g.Count()).Append('\n');
        return sb.ToString();
    }

    public void Dispose()
    {
        _heartbeat?.Dispose();
        Http.Dispose();
    }
}

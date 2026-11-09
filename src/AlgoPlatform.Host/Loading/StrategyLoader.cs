using System.IO.Compression;
using System.Reflection;
using System.Runtime.Loader;
using AlgoPlatform.Core.Deployment;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Host.Loading;

/// <summary>
/// One collectible load context per strategy package, so two developers can ship different versions of
/// the same dependency and a stopped strategy can be unloaded. The SDK is deliberately resolved from
/// the default context: the host and the strategy must agree on the identity of <see cref="IStrategy"/>.
/// </summary>
public sealed class StrategyLoadContext : AssemblyLoadContext
{
    private static readonly HashSet<string> s_shared = new(StringComparer.OrdinalIgnoreCase) { "AlgoPlatform.Sdk" };
    private readonly string _directory;

    public StrategyLoadContext(string directory, string name) : base(name, isCollectible: true) => _directory = directory;

    public string Directory => _directory;

    protected override Assembly? Load(AssemblyName assemblyName)
    {
        if (assemblyName.Name is null || s_shared.Contains(assemblyName.Name)) return null;
        string path = Path.Combine(_directory, assemblyName.Name + ".dll");
        return File.Exists(path) ? LoadFromAssemblyPath(path) : null;   // null: fall back to the default context
    }
}

public sealed record LoadedStrategy(StrategyManifest Manifest, Func<IStrategy> Factory, StrategyLoadContext Context);

public static class StrategyLoader
{
    public const string ManifestFile = "strategy.json";

    /// <summary>Reads and validates the manifest in a package directory.</summary>
    public static StrategyManifest ReadManifest(string directory)
    {
        string path = Path.Combine(directory, ManifestFile);
        if (!File.Exists(path)) throw new InvalidDataException($"Package has no {ManifestFile}");
        var manifest = StrategyManifest.Parse(File.ReadAllText(path));
        var errors = manifest.Validate();
        if (errors.Count > 0) throw new InvalidDataException("Invalid manifest: " + string.Join("; ", errors));
        return manifest;
    }

    /// <summary>Loads the entry type from an extracted package into its own load context.</summary>
    public static LoadedStrategy Load(string directory)
    {
        StrategyManifest manifest = ReadManifest(directory);
        string[] parts = manifest.EntryType.Split(',', 2, StringSplitOptions.TrimEntries);
        string typeName = parts[0];
        string assemblyFile = Path.Combine(directory, parts[1] + ".dll");
        if (!File.Exists(assemblyFile)) throw new InvalidDataException($"Entry assembly {parts[1]}.dll not found in package");

        var context = new StrategyLoadContext(Path.GetFullPath(directory), $"{manifest.Name}@{manifest.Version}");
        Assembly assembly = context.LoadFromAssemblyPath(Path.GetFullPath(assemblyFile));
        Type type = assembly.GetType(typeName, throwOnError: true)!;
        if (!typeof(IStrategy).IsAssignableFrom(type))
            throw new InvalidDataException($"{manifest.EntryType} does not implement IStrategy");
        if (type.GetConstructor(Type.EmptyTypes) is null)
            throw new InvalidDataException($"{manifest.EntryType} needs a public parameterless constructor");
        return new LoadedStrategy(manifest, () => (IStrategy)Activator.CreateInstance(type)!, context);
    }

    /// <summary>Extracts a package zip into <paramref name="root"/>/name/version and returns that directory.</summary>
    public static string Extract(byte[] zip, string root)
    {
        string staging = Path.Combine(root, "_staging", Guid.NewGuid().ToString("N"));
        using (var archive = new ZipArchive(new MemoryStream(zip), ZipArchiveMode.Read))
        {
            foreach (var entry in archive.Entries)
            {
                string target = Path.GetFullPath(Path.Combine(staging, entry.FullName));
                if (!target.StartsWith(Path.GetFullPath(staging), StringComparison.OrdinalIgnoreCase))
                    throw new InvalidDataException("Package entry escapes the package directory");
                if (entry.FullName.EndsWith('/')) continue;
                System.IO.Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                entry.ExtractToFile(target, overwrite: true);
            }
        }
        StrategyManifest manifest = ReadManifest(staging);
        string final = Path.Combine(root, manifest.Name, manifest.Version + "-" + DateTime.UtcNow.ToString("yyyyMMddHHmmssfff"));
        System.IO.Directory.CreateDirectory(Path.GetDirectoryName(final)!);
        System.IO.Directory.Move(staging, final);
        return final;
    }

    /// <summary>Builds a package zip from a manifest and a set of assembly files (tests, tooling).</summary>
    public static byte[] BuildPackage(StrategyManifest manifest, params string[] files)
    {
        var ms = new MemoryStream();
        using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, leaveOpen: true))
        {
            var entry = archive.CreateEntry(ManifestFile);
            using (var w = new StreamWriter(entry.Open())) w.Write(manifest.ToJson());
            foreach (string f in files) archive.CreateEntryFromFile(f, Path.GetFileName(f));
        }
        return ms.ToArray();
    }
}

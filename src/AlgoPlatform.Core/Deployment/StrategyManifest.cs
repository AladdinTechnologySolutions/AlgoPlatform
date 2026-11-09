using System.Text.Json;
using System.Text.Json.Serialization;

namespace AlgoPlatform.Core.Deployment;

public enum RunMode { Live, Paper, Replay }

/// <summary>The <c>strategy.json</c> manifest shipped inside every strategy package (FR-2).</summary>
public sealed class StrategyManifest
{
    public static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        WriteIndented = false,
    };

    public string Name { get; set; } = "";
    public string Version { get; set; } = "";
    public string EntryType { get; set; } = "";
    public string[] Instruments { get; set; } = [];
    public string Mode { get; set; } = "Paper";
    public string ResourceClass { get; set; } = "dedicated-core";
    public RiskLimits Limits { get; set; } = new();
    public Dictionary<string, JsonElement> Parameters { get; set; } = new();

    [JsonIgnore]
    public RunMode RunMode => Enum.TryParse<RunMode>(Mode, ignoreCase: true, out var m) ? m : RunMode.Paper;

    [JsonIgnore]
    public bool IsSandbox => string.Equals(ResourceClass, ResourceClasses.Shared, StringComparison.OrdinalIgnoreCase);

    public static StrategyManifest Parse(string json) =>
        JsonSerializer.Deserialize<StrategyManifest>(json, JsonOptions) ?? throw new InvalidDataException("Empty manifest");

    public string ToJson() => JsonSerializer.Serialize(this, JsonOptions);

    /// <summary>Numeric parameter lookup; strings that parse as numbers are accepted.</summary>
    public double GetParameter(string name, double defaultValue)
    {
        if (!Parameters.TryGetValue(name, out JsonElement v)) return defaultValue;
        return v.ValueKind switch
        {
            JsonValueKind.Number => v.GetDouble(),
            JsonValueKind.String when double.TryParse(v.GetString(), System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out double d) => d,
            JsonValueKind.True => 1,
            JsonValueKind.False => 0,
            _ => defaultValue,
        };
    }

    /// <summary>Returns every problem found; an empty list means the manifest is deployable.</summary>
    public List<string> Validate()
    {
        var errors = new List<string>();
        if (string.IsNullOrWhiteSpace(Name)) errors.Add("name is required");
        else if (Name.Length > 64 || !Name.All(c => char.IsLetterOrDigit(c) || c is '-' or '_' or '.'))
            errors.Add("name must be at most 64 characters of letters, digits, '-', '_' or '.'");
        if (string.IsNullOrWhiteSpace(Version)) errors.Add("version is required");
        if (string.IsNullOrWhiteSpace(EntryType) || !EntryType.Contains(','))
            errors.Add("entryType must be an assembly-qualified type name, e.g. \"MeanRev.EuStrategy, MeanRev\"");
        if (Instruments.Length == 0) errors.Add("at least one instrument is required");
        if (!Enum.TryParse<RunMode>(Mode, ignoreCase: true, out _)) errors.Add("mode must be Live, Paper or Replay");
        if (!ResourceClasses.IsKnown(ResourceClass)) errors.Add("resourceClass must be 'dedicated-core' or 'shared'");
        if (Limits.MaxOrderQty <= 0) errors.Add("limits.maxOrderQty must be > 0");
        if (Limits.MaxPosition <= 0) errors.Add("limits.maxPosition must be > 0");
        if (Limits.MaxNotional <= 0) errors.Add("limits.maxNotional must be > 0");
        if (Limits.MaxOrdersPerSec <= 0) errors.Add("limits.maxOrdersPerSec must be > 0");
        if (Limits.FatFingerBps <= 0) errors.Add("limits.fatFingerBps must be > 0");
        return errors;
    }
}

/// <summary>Per-strategy hard limits. Notional is in currency units; prices are fixed-point in the engine.</summary>
public sealed class RiskLimits
{
    public long MaxOrderQty { get; set; } = 1000;
    public long MaxPosition { get; set; } = 5000;
    public long MaxNotional { get; set; } = 2_000_000;
    public int MaxOrdersPerSec { get; set; } = 50;
    /// <summary>Sum of |position| over all instruments. 0 means MaxPosition * number of instruments.</summary>
    public long MaxGrossPosition { get; set; }
    /// <summary>Max distance of a limit price from the last mid, in basis points.</summary>
    public int FatFingerBps { get; set; } = 500;
}

public static class ResourceClasses
{
    public const string DedicatedCore = "dedicated-core";
    public const string Shared = "shared";

    public static bool IsKnown(string rc) =>
        string.Equals(rc, DedicatedCore, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(rc, Shared, StringComparison.OrdinalIgnoreCase);
}

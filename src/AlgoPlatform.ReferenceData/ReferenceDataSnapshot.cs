using System.Text;
using System.Text.Json;
using AlgoPlatform.Core.Journal;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.ReferenceData;

/// <summary>JSON form of one instrument. Prices are decimals here and fixed-point in the snapshot.</summary>
public sealed class InstrumentRecord
{
    public int Id { get; set; }
    public string Symbol { get; set; } = "";
    public double TickSize { get; set; } = 0.01;
    public long LotSize { get; set; } = 1;
    public string Currency { get; set; } = "USD";
    public double MinPx { get; set; }
    public double MaxPx { get; set; } = 1_000_000;
    public string Status { get; set; } = "Open";        // Open | Halted | Closed
    public string TradingHours { get; set; } = "00:00-24:00";
}

public sealed class ReferenceDataDocument
{
    public int Version { get; set; }
    public List<InstrumentRecord> Instruments { get; set; } = new();
}

/// <summary>
/// Immutable, versioned instrument master (FR-4). Lookups by id are an array index; nothing here
/// allocates or calls out of process, so it is safe on the hot path. A new version is a new object
/// and hosts swap the reference between events.
/// </summary>
public sealed class ReferenceDataSnapshot : IReferenceData, IJournalBlob
{
    public const int MaxInstrumentId = 4095;

    private static readonly JsonSerializerOptions s_json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    private readonly InstrumentInfo[] _byId;
    private readonly bool[] _present;
    private readonly Dictionary<string, InstrumentInfo> _bySymbol;
    private readonly ReferenceDataDocument _source;

    private ReferenceDataSnapshot(ReferenceDataDocument doc)
    {
        _source = doc;
        int maxId = 0;
        foreach (var r in doc.Instruments)
        {
            if (r.Id <= 0 || r.Id > MaxInstrumentId) throw new InvalidDataException($"Instrument id {r.Id} out of range 1..{MaxInstrumentId}");
            if (string.IsNullOrWhiteSpace(r.Symbol)) throw new InvalidDataException($"Instrument {r.Id} has no symbol");
            maxId = Math.Max(maxId, r.Id);
        }
        _byId = new InstrumentInfo[maxId + 1];
        _present = new bool[maxId + 1];
        _bySymbol = new Dictionary<string, InstrumentInfo>(StringComparer.Ordinal);
        foreach (var r in doc.Instruments)
        {
            if (_present[r.Id]) throw new InvalidDataException($"Duplicate instrument id {r.Id}");
            var info = new InstrumentInfo(new InstrumentId(r.Id), r.Symbol, Price.FromDouble(r.TickSize), r.LotSize,
                Price.FromDouble(r.MinPx), Price.FromDouble(r.MaxPx), string.Equals(r.Status, "Open", StringComparison.OrdinalIgnoreCase));
            if (info.TickSize <= 0 || info.LotSize <= 0) throw new InvalidDataException($"Instrument {r.Symbol}: tick and lot size must be positive");
            _byId[r.Id] = info;
            _present[r.Id] = true;
            if (!_bySymbol.TryAdd(r.Symbol, info)) throw new InvalidDataException($"Duplicate symbol {r.Symbol}");
        }
        Version = doc.Version;
    }

    public int Version { get; }
    public int Count => _bySymbol.Count;
    public IEnumerable<InstrumentInfo> All => _bySymbol.Values;
    public ReferenceDataDocument Document => _source;

    public bool TryGet(InstrumentId id, out InstrumentInfo info)
    {
        int i = id.Value;
        if ((uint)i < (uint)_present.Length && _present[i])
        {
            info = _byId[i];
            return true;
        }
        info = default;
        return false;
    }

    public bool TryGetBySymbol(string symbol, out InstrumentInfo info) => _bySymbol.TryGetValue(symbol, out info);

    public string CurrencyOf(InstrumentId id) =>
        _source.Instruments.Find(r => r.Id == id.Value)?.Currency ?? "";

    public static ReferenceDataSnapshot FromDocument(ReferenceDataDocument doc) => new(doc);

    public static ReferenceDataSnapshot FromJson(string json) =>
        new(JsonSerializer.Deserialize<ReferenceDataDocument>(json, s_json) ?? throw new InvalidDataException("Empty reference data"));

    public static ReferenceDataSnapshot Load(string path) => FromJson(File.ReadAllText(path));

    public string ToJson() => JsonSerializer.Serialize(_source, s_json);

    public byte[] ToJournalBytes() => Encoding.UTF8.GetBytes(ToJson());

    public static ReferenceDataSnapshot FromJournalBytes(byte[] bytes) => FromJson(Encoding.UTF8.GetString(bytes));

    /// <summary>A copy of this snapshot's document with a different version number.</summary>
    public ReferenceDataSnapshot WithVersion(int version) => new(new ReferenceDataDocument
    {
        Version = version,
        Instruments = _source.Instruments,
    });

    /// <summary>Small built-in universe used by tests, samples and the default host config.</summary>
    public static ReferenceDataSnapshot Default() => FromDocument(new ReferenceDataDocument
    {
        Version = 1,
        Instruments =
        [
            new() { Id = 1, Symbol = "VOD.L", TickSize = 0.01, LotSize = 1, Currency = "GBP", MinPx = 50, MaxPx = 150 },
            new() { Id = 2, Symbol = "BARC.L", TickSize = 0.01, LotSize = 1, Currency = "GBP", MinPx = 100, MaxPx = 300 },
            new() { Id = 3, Symbol = "AAPL", TickSize = 0.01, LotSize = 1, Currency = "USD", MinPx = 100, MaxPx = 400 },
            new() { Id = 4, Symbol = "MSFT", TickSize = 0.01, LotSize = 1, Currency = "USD", MinPx = 200, MaxPx = 600 },
        ],
    });
}

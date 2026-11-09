using System.Buffers.Binary;
using System.Security.Cryptography;
using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Journal;
using AlgoPlatform.Core.Timing;
using AlgoPlatform.Host.Engine;
using AlgoPlatform.ReferenceData;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Replay;

/// <summary>Collects a replay's outputs as encoded journal records, so they can be compared byte for byte.</summary>
public sealed class CapturingJournalSink : IJournalSink
{
    private readonly byte[] _scratch = new byte[JournalCodec.MaxFixedPayload];
    public List<byte[]> Outputs { get; } = new();

    public void Input(in HostEvent evt) { }

    public void Output(in OrderCommand cmd)
    {
        int len = JournalCodec.EncodeOutput(in cmd, _scratch);
        Outputs.Add(EncodeRecord(cmd.CauseSeq, cmd.TimestampNs, JournalCodec.OutputType(cmd.Type), _scratch.AsSpan(0, len)));
    }

    /// <summary>The exact bytes a <see cref="JournalWriter"/> would write for this record.</summary>
    public static byte[] EncodeRecord(long seq, long timestampNs, int type, ReadOnlySpan<byte> payload)
    {
        var rec = new byte[JournalWriter.RecordHeaderSize + payload.Length];
        BinaryPrimitives.WriteInt64LittleEndian(rec, seq);
        BinaryPrimitives.WriteInt64LittleEndian(rec.AsSpan(8), timestampNs);
        BinaryPrimitives.WriteInt32LittleEndian(rec.AsSpan(16), type);
        BinaryPrimitives.WriteInt32LittleEndian(rec.AsSpan(20), payload.Length);
        payload.CopyTo(rec.AsSpan(JournalWriter.RecordHeaderSize));
        return rec;
    }

    public static byte[] EncodeRecord(in JournalRecord r) => EncodeRecord(r.Seq, r.TimestampNs, r.Type, r.Payload.Span);
}

public sealed class ReplayResult
{
    public required string StrategyName { get; init; }
    public required int InputEvents { get; init; }
    public required List<byte[]> Outputs { get; init; }
    public required List<byte[]> OriginalOutputs { get; init; }
    /// <summary>Index of the first output that differs from the original run, or -1 if identical.</summary>
    public required int FirstMismatch { get; init; }
    public required string Digest { get; init; }
    public required StrategyStatus FinalStatus { get; init; }
    public required TimeSpan Elapsed { get; init; }
    public bool MatchesOriginal => FirstMismatch < 0;
}

/// <summary>
/// Deterministic replay (FR-9): rebuilds the strategy's engine from the journal's SessionStart record,
/// feeds it every journalled input in order with a virtual clock (no sleeping, no real timers), and
/// compares the order requests it produces with the ones the original run journalled.
/// </summary>
public static class JournalReplayer
{
    public static ReplayResult Run(string journalPath, Func<IStrategy> factory) => Run(JournalReader.ReadAll(journalPath), factory);

    public static ReplayResult Run(IReadOnlyList<JournalRecord> records, Func<IStrategy> factory)
    {
        var started = DateTime.UtcNow;
        JournalRecord? sessionRecord = null;
        foreach (var r in records)
        {
            if (r.Type != (int)EventType.SessionStart) continue;
            sessionRecord = r;
            break;
        }
        if (sessionRecord is null) throw new InvalidDataException("Journal has no SessionStart record");
        SessionInfo session = StrategyEngine.ParseSession(sessionRecord.Value.Payload.ToArray());

        var clock = new VirtualClock();
        var sink = new CapturingJournalSink();
        var engine = new StrategyEngine(session.StrategyId, session.Manifest, factory(), NullOrderSink.Instance, sink,
            new EngineOptions { ReplayMode = true, Clock = clock });

        int inputs = 0;
        var original = new List<byte[]>();
        foreach (JournalRecord r in records)
        {
            if (r.IsOutput)
            {
                original.Add(CapturingJournalSink.EncodeRecord(in r));
                continue;
            }
            HostEvent e = JournalCodec.DecodeInput(in r, DecodeBlob);
            clock.Set(e.TimestampNs);
            engine.Process(ref e);
            inputs++;
        }

        int mismatch = -1;
        int n = Math.Max(original.Count, sink.Outputs.Count);
        for (int i = 0; i < n; i++)
        {
            if (i < original.Count && i < sink.Outputs.Count && original[i].AsSpan().SequenceEqual(sink.Outputs[i])) continue;
            mismatch = i;
            break;
        }

        return new ReplayResult
        {
            StrategyName = session.Manifest.Name,
            InputEvents = inputs,
            Outputs = sink.Outputs,
            OriginalOutputs = original,
            FirstMismatch = mismatch,
            Digest = DigestOf(sink.Outputs),
            FinalStatus = engine.Status,
            Elapsed = DateTime.UtcNow - started,
        };
    }

    public static object? DecodeBlob(EventType type, byte[] blob) =>
        type == EventType.ReferenceData ? ReferenceDataSnapshot.FromJournalBytes(blob) : blob;

    public static string DigestOf(List<byte[]> records)
    {
        using var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var r in records) sha.AppendData(r);
        return Convert.ToHexString(sha.GetHashAndReset());
    }
}

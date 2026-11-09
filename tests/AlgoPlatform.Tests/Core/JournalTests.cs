using AlgoPlatform.Core.Events;
using AlgoPlatform.Core.Journal;
using AlgoPlatform.ReferenceData;
using AlgoPlatform.Replay;
using AlgoPlatform.Sdk;

namespace AlgoPlatform.Tests.Core;

public class JournalTests
{
    [Fact]
    public void Writer_and_reader_round_trip_and_grow_past_initial_capacity()
    {
        string path = Path.Combine(TestSupport.TempDir("journal"), "a.journal");
        var payload = new byte[1000];
        using (var w = new JournalWriter(path, initialCapacity: 64 * 1024))
        {
            for (int i = 1; i <= 2_000; i++)
            {
                payload[0] = (byte)i;
                w.Append(i, i * 10, 1 + i % 5, payload.AsSpan(0, i % 1000));
            }
        }
        var records = JournalReader.ReadAll(path);
        Assert.Equal(2_000, records.Count);
        for (int i = 1; i <= 2_000; i++)
        {
            var r = records[i - 1];
            Assert.Equal(i, r.Seq);
            Assert.Equal(i * 10, r.TimestampNs);
            Assert.Equal(1 + i % 5, r.Type);
            Assert.Equal(i % 1000, r.Payload.Length);
        }
    }

    [Fact]
    public void Reader_sees_records_of_a_journal_still_being_written()
    {
        string path = Path.Combine(TestSupport.TempDir("journal"), "live.journal");
        using var w = new JournalWriter(path);
        w.Append(1, 1, 1, [1, 2, 3]);
        w.Append(2, 2, 2, [4]);
        var records = JournalReader.ReadAll(path);   // zeroed mapped tail terminates the read
        Assert.Equal(2, records.Count);
    }

    [Fact]
    public void Codec_round_trips_every_input_kind_and_outputs()
    {
        var tick = new HostEvent
        {
            Type = EventType.Tick, Seq = 5, TimestampNs = 77,
            Tick = new Tick(5, 77, new InstrumentId(3), 1_000_000, 10, 1_000_100, 20, 1_000_050, 7),
        };
        var exec = new HostEvent
        {
            Type = EventType.Execution, Seq = 6, TimestampNs = 78,
            Exec = new ExecutionReport(6, 123, 456, new InstrumentId(2), Side.Sell, OrdStatus.PartiallyFilled, 99, 5, 15, 85, 0),
        };
        var control = new HostEvent { Type = EventType.Control, Seq = 7, TimestampNs = 79, A = (long)ControlCode.Kill, B = 3 };
        foreach (var e in new[] { tick, exec, control })
        {
            var buf = new byte[JournalCodec.MaxFixedPayload];
            int len = JournalCodec.EncodeInput(e, buf);
            var back = JournalCodec.DecodeInput(new JournalRecord(e.Seq, e.TimestampNs, (int)e.Type, buf.AsMemory(0, len)));
            Assert.Equal(e.Type, back.Type);
            if (e.Type == EventType.Tick) Assert.Equal(e.Tick, back.Tick);
            else if (e.Type == EventType.Execution) Assert.Equal(e.Exec, back.Exec);
            else
            {
                Assert.Equal(e.A, back.A);
                Assert.Equal(e.B, back.B);
            }
        }

        var cmd = new OrderCommand
        {
            Type = CommandType.Replace, StrategyId = 4, OrderId = 99, Instrument = new InstrumentId(1), Side = Side.Buy,
            OrdType = OrdType.Limit, Tif = Tif.Ioc, Px = 1234, Qty = 50, RejectCode = 6, CauseSeq = 10, TimestampNs = 11,
        };
        var ob = new byte[JournalCodec.MaxFixedPayload];
        int olen = JournalCodec.EncodeOutput(cmd, ob);
        var decoded = JournalCodec.DecodeOutput(new JournalRecord(10, 11, JournalCodec.OutputType(cmd.Type), ob.AsMemory(0, olen)));
        Assert.Equal(cmd, decoded);
    }

    [Fact]
    public void Journal_service_writes_channels_including_reference_data_blobs()
    {
        string path = Path.Combine(TestSupport.TempDir("journal"), "svc.journal");
        var svc = new JournalService(64);
        svc.Start();
        var ch = svc.Open(path);
        var snap = ReferenceDataSnapshot.Default();
        ch.Input(new HostEvent { Type = EventType.ReferenceData, Seq = 1, TimestampNs = 1, A = snap.Version, Payload = snap });
        for (int i = 2; i < 1_000; i++)
            ch.Input(new HostEvent { Type = EventType.Timer, Seq = i, TimestampNs = i, A = i });   // more than the ring: spins, never drops
        ch.Output(new OrderCommand { Type = CommandType.New, OrderId = 42, Qty = 1, CauseSeq = 999 });
        ch.Close();
        Assert.True(svc.WaitClosed(ch, TimeSpan.FromSeconds(5)));
        svc.Dispose();

        var records = JournalReader.ReadAll(path);
        Assert.Equal(1_000, records.Count);
        var refEvt = JournalCodec.DecodeInput(records[0], JournalReplayer.DecodeBlob);
        var back = Assert.IsType<ReferenceDataSnapshot>(refEvt.Payload);
        Assert.True(back.TryGetBySymbol("VOD.L", out var vod));
        Assert.Equal(1, vod.Id.Value);
        Assert.True(records[^1].IsOutput);
        Assert.Equal(42, JournalCodec.DecodeOutput(records[^1]).OrderId);
    }
}

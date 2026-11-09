using System.Buffers.Binary;

namespace AlgoPlatform.Core.Journal;

public readonly record struct JournalRecord(long Seq, long TimestampNs, int Type, ReadOnlyMemory<byte> Payload)
{
    public bool IsOutput => Type >= JournalCodec.OutputBase;
}

/// <summary>Reads journal files written by <see cref="JournalWriter"/>. Control path / replay only.</summary>
public static class JournalReader
{
    /// <summary>Reads a journal, including one that is still being written.</summary>
    public static List<JournalRecord> ReadAll(string path)
    {
        byte[] data;
        using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
        {
            data = new byte[fs.Length];
            int read = 0;
            while (read < data.Length)
            {
                int n = fs.Read(data, read, data.Length - read);
                if (n == 0) break;
                read += n;
            }
        }
        return Parse(data);
    }

    public static List<JournalRecord> Parse(byte[] data)
    {
        var records = new List<JournalRecord>();
        if (data.Length < JournalWriter.FileHeaderSize) return records;
        if (BinaryPrimitives.ReadInt32LittleEndian(data) != JournalWriter.Magic)
            throw new InvalidDataException("Not an AlgoPlatform journal");
        int pos = JournalWriter.FileHeaderSize;
        while (pos + JournalWriter.RecordHeaderSize <= data.Length)
        {
            var h = data.AsSpan(pos);
            int type = BinaryPrimitives.ReadInt32LittleEndian(h[16..]);
            if (type == 0) break;   // end marker (zeroed mapped tail)
            int len = BinaryPrimitives.ReadInt32LittleEndian(h[20..]);
            if (pos + JournalWriter.RecordHeaderSize + len > data.Length) break;   // torn final record
            records.Add(new JournalRecord(
                BinaryPrimitives.ReadInt64LittleEndian(h),
                BinaryPrimitives.ReadInt64LittleEndian(h[8..]),
                type,
                new ReadOnlyMemory<byte>(data, pos + JournalWriter.RecordHeaderSize, len)));
            pos += JournalWriter.RecordHeaderSize + len;
        }
        return records;
    }
}

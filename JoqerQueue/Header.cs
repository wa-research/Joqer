using System.IO.MemoryMappedFiles;

namespace JoqerQueue
{
    public class Header
    {
        public Header() { }

        public byte Version { get; set; }
        public QueueOptions Flags { get; set; }
        public short MaxDataSegments { get; set; }
        public PageCount DataSegmentSize { get; set; }
        public PageCount IndexSegmentSize { get; set; }
        public SequenceNumber FirstValidDataSequenceNumber { get; set; }
        public SequenceNumber NextAvailableDataSequenceNumber { get; set; }
        public SequenceNumber FirstValidIndexSequenceNumber { get; set; }
        public SequenceNumber NextAvailableIndexSequenceNumber { get; set; }
        public SequenceNumber NextIndexIsnToReadWithDefaultReader { get; set; }
        public short ActiveDataFile { get { return NextAvailableDataSequenceNumber.FileNumber; } }
        public short ActiveIndexFile { get { return NextAvailableIndexSequenceNumber.FileNumber; } }

        public static class Offsets
        {
            // Byte
            public const int Version = 0;
            // Byte
            public const int Flags = Version + 1;
            // Int16
            public const int MaxDataSegments = Flags + 1;
            // Int32
            public const int DataSegmentSize = MaxDataSegments + 2;
            // Int32
            public const int IndexSegmentSize = DataSegmentSize + 4;
            // long
            public const int FirstValidDataSequenceNumber = IndexSegmentSize + 4;
            // long
            public const int FirstValidIndexSequenceNumber = FirstValidDataSequenceNumber + 8;
            // long
            public const int NextAvailableDataSequenceNumber = FirstValidIndexSequenceNumber + 8;
            // long
            public const int NextAvailableIndexSequenceNumber = NextAvailableDataSequenceNumber + 8;
            // long
            public const int NextIndexIsnToReadWithDefaultReader = NextAvailableIndexSequenceNumber + 8;
        }

        public static Header Open(MemoryMappedViewAccessor va)
        {
            Header h = new Header()
            {
                Version = va.ReadByte(Offsets.Version),
                Flags = (QueueOptions)va.ReadByte(Offsets.Flags),
                MaxDataSegments = va.ReadInt16(Offsets.MaxDataSegments),
                DataSegmentSize = new PageCount(va.ReadInt32(Offsets.DataSegmentSize)),
                IndexSegmentSize = new PageCount(va.ReadInt32(Offsets.IndexSegmentSize)),
                FirstValidDataSequenceNumber = new SequenceNumber(va.ReadInt64(Offsets.FirstValidDataSequenceNumber)),
                FirstValidIndexSequenceNumber = new SequenceNumber(va.ReadInt64(Offsets.FirstValidIndexSequenceNumber)),
                NextAvailableDataSequenceNumber = new SequenceNumber(va.ReadInt64(Offsets.NextAvailableDataSequenceNumber)),
                NextAvailableIndexSequenceNumber = new SequenceNumber(va.ReadInt64(Offsets.NextAvailableIndexSequenceNumber)),
                NextIndexIsnToReadWithDefaultReader = new SequenceNumber(va.ReadInt64(Offsets.NextIndexIsnToReadWithDefaultReader))
            };

            return h;
        }
    }
}


using System;
using System.Collections.Generic;
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

        private Dictionary<Guid, int> _readerBookmarkOffsets;

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
            // table of custom reader bookmarks-- (guid, long) pairs starts after the default reader bookmark
            public const int ReaderBookmarkTableStart = NextIndexIsnToReadWithDefaultReader + 8;
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

            h.ReHydrateReaderBookmarkLookup(va);

            return h;
        }

        private void ReHydrateReaderBookmarkLookup(MemoryMappedViewAccessor va)
        {
            _readerBookmarkOffsets = new Dictionary<Guid, int>();
            int pos = Offsets.ReaderBookmarkTableStart;
            Guid g = ReadGuid(va, pos);
            while (g != Guid.Empty && pos < va.Capacity) {
                _readerBookmarkOffsets.Add(g, pos + 16);
                pos += (16 + 8);
                g = ReadGuid(va, pos);
            }
        }

        private Guid ReadGuid(MemoryMappedViewAccessor va, long position)
        {
            byte[] buf = new byte[16];
            va.ReadArray<byte>(position, buf, 0, 16);
            return new Guid(buf);
        }

        internal ReaderBookmark RegisterReaderBookmark(MemoryMappedViewAccessor vm)
        {
            ReaderBookmark bm = new ReaderBookmark(Guid.NewGuid(), FirstValidIndexSequenceNumber);
            // _readerBookmarkOffsets already contains all registered bookmarks, we can use it to calculate the next bookmark's offset
            int offset = _readerBookmarkOffsets.Count * (16 + 8) + Offsets.ReaderBookmarkTableStart;
            vm.WriteArray(offset, bm.Guid.ToByteArray(), 0, 16);
            vm.Write(offset + 16, bm.SequenceNumber.LogicalOffset);
            _readerBookmarkOffsets.Add(bm.Guid, offset + 16);

            return bm;
        }

        internal bool IsBookmarkRegistered(Guid id)
        {
            return _readerBookmarkOffsets.ContainsKey(id);
        }

        internal int GetReaderOffset(Guid id)
        {
            if (id == Guid.Empty) return Offsets.NextIndexIsnToReadWithDefaultReader;
            else if (_readerBookmarkOffsets.ContainsKey(id)) return _readerBookmarkOffsets[id];

            throw new KeyNotFoundException("The bookmark with GUID='" + id.ToString() + "' does not exist.");
        }

        internal IEnumerable<ReaderBookmark> Bookmarks(MemoryMappedViewAccessor va)
        {
            yield return new ReaderBookmark(Guid.Empty, NextIndexIsnToReadWithDefaultReader);
            foreach (var kv in _readerBookmarkOffsets) {
                yield return new ReaderBookmark(kv.Key, new SequenceNumber(va.ReadInt64(kv.Value)));
            }
        }
    }
}


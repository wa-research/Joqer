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
        public SequenceNumber DefaultReaderBookmark { get; set; }
        public short ActiveDataFile { get { return NextAvailableDataSequenceNumber.FileNumber; } }
        public short ActiveIndexFile { get { return NextAvailableIndexSequenceNumber.FileNumber; } }

        private Dictionary<Guid, int> _bookmarkOffsets;

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
            public const int DefaultReaderBookmark = NextAvailableIndexSequenceNumber + 8;
            // table of custom reader bookmarks-- (guid, long) pairs starts after the default reader bookmark
            public const int ReaderBookmarkTableStart = DefaultReaderBookmark + 8;
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
                DefaultReaderBookmark = new SequenceNumber(va.ReadInt64(Offsets.DefaultReaderBookmark))
            };

            h.ReHydrateBookmarkLookup(va);

            return h;
        }

        private void ReHydrateBookmarkLookup(MemoryMappedViewAccessor va)
        {
            _bookmarkOffsets = new Dictionary<Guid, int>();
            foreach (var o in BookmarkOffsets(va)) {
                if (o.ID != ReaderBookmark.Unused)
                    _bookmarkOffsets.Add(o.ID, o.Offset + 16);
            }
        }

        internal int LookUpBookmarkOffset(Guid id)
        {
            if (id == Guid.Empty) return Offsets.DefaultReaderBookmark;
            else if (_bookmarkOffsets.ContainsKey(id)) return _bookmarkOffsets[id];

            throw new KeyNotFoundException("The bookmark with GUID='" + id.ToString() + "' does not exist.");
        }

        private Guid ReadGuid(MemoryMappedViewAccessor va, long position)
        {
            byte[] buf = new byte[16];
            va.ReadArray<byte>(position, buf, 0, 16);
            return new Guid(buf);
        }

        internal ReaderBookmark RegisterBookmark(MemoryMappedViewAccessor va)
        {
            ReaderBookmark bm = new ReaderBookmark(Guid.NewGuid(), FirstValidIndexSequenceNumber);
            int offset = FindNextAvailableOffset(va);

            //TODO: Extend header segment once we run out of space
            //if (offset + _bookmarkWidth > va.Capacity) { }

            bm = WriteBookmark(va, bm, offset);
            _bookmarkOffsets.Add(bm.Guid, offset + 16);

            return bm;
        }

        internal void UnregisterBookmark(MemoryMappedViewAccessor va, Guid bookmarkId)
        {
            int offset = LookUpBookmarkOffset(bookmarkId);
            _bookmarkOffsets.Remove(bookmarkId);
            WriteBookmark(va, new ReaderBookmark(ReaderBookmark.Unused, SequenceNumber.Zero), offset);
        }

        private static ReaderBookmark WriteBookmark(MemoryMappedViewAccessor va, ReaderBookmark bm, int offset)
        {
            va.WriteArray(offset, bm.Guid.ToByteArray(), 0, 16);
            va.Write(offset + 16, bm.SequenceNumber.LogicalOffset);
            return bm;
        }

        private int FindNextAvailableOffset(MemoryMappedViewAccessor va)
        {
            int offset = Offsets.ReaderBookmarkTableStart - _bookmarkWidth;
            foreach (var o in BookmarkOffsets(va)) {
                offset = o.Offset;
                if (o.ID == ReaderBookmark.Unused)
                    return offset;
            }
            // There were no unused slots, next position is after the last bookmark
            return offset + _bookmarkWidth;
        }

        private const int _bookmarkWidth = 16 + 8;

        private struct BookmarkOffset
        {
            public Guid ID;
            public int Offset;
        }

        private IEnumerable<BookmarkOffset> BookmarkOffsets(MemoryMappedViewAccessor va)
        {
            int pos = Offsets.ReaderBookmarkTableStart;
            Guid g = ReadGuid(va, pos);
            while (g != Guid.Empty && pos + _bookmarkWidth < va.Capacity) {
                yield return new BookmarkOffset { ID = g, Offset = pos };
                pos += _bookmarkWidth;
                g = ReadGuid(va, pos);
            }
        }

        internal IEnumerable<ReaderBookmark> Bookmarks(MemoryMappedViewAccessor va)
        {
            yield return new ReaderBookmark(Guid.Empty, DefaultReaderBookmark);
            foreach (var kv in _bookmarkOffsets) {
                yield return new ReaderBookmark(kv.Key, new SequenceNumber(va.ReadInt64(kv.Value)));
            }
        }
    }
}


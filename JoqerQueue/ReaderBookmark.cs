using System;

namespace JoqerQueue
{
    public struct ReaderBookmark
    {
        public static readonly Guid Unused = new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff");

        public ReaderBookmark(Guid id, SequenceNumber sn)
        {
            Guid = id;
            SequenceNumber = sn;
        }

        public Guid Guid;
        public SequenceNumber SequenceNumber;
    }
}

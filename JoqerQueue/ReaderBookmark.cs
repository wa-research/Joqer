using System;

namespace JoqerQueue
{
    public struct ReaderBookmark
    {
        public ReaderBookmark(Guid id, SequenceNumber sn)
        {
            Guid = id;
            SequenceNumber = sn;
        }

        public Guid Guid;
        public SequenceNumber SequenceNumber;
    }
}

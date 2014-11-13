using System;

namespace JoqerQueue
{
    public class PersistentDefaultReaderCursor : IReaderCursor
    {
        Queue _queue;
        private int _indexFieldSize;
        PageCount _indexSegmentSize;
        Guid _readerBookmark = Guid.Empty;

        public PersistentDefaultReaderCursor(Queue q)
        {
            _queue = q;
            _indexFieldSize = q.GetIndexRecordSizeBytes();
            _indexSegmentSize = q.Header.IndexSegmentSize;
        }

        public SequenceNumber CurrentIsn()
        {
            return _queue.ReadBookmark(_readerBookmark);
        }

        public SequenceNumber MaxIsn()
        {
            return _queue.NextAvailableIndexSequenceNumber();
        }

        public SequenceNumber Advance(SequenceNumber isn)
        {
            SequenceNumber nextisn = IncrementIndexSequenceNumber(isn, _indexSegmentSize.Bytes);
            return _queue.UpdateBookmark(_readerBookmark, nextisn); 
        }

        public SequenceNumber IncrementIndexSequenceNumber(SequenceNumber isn, long segmentSize)
        {
            if ((ulong)(isn.FileOffset + _indexFieldSize) >= (ulong)segmentSize) {
                isn = isn.NextFile();
            } else {
                isn.LogicalOffset += _indexFieldSize;
            }

            return isn;
        }
    }
}
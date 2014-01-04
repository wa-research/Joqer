
namespace JoqerQueue
{
    public class PersistentDefaultReaderCursor : IReaderCursor
    {
        Queue _queue;
        private int _indexFieldSize;
        PageCount _indexSegmentSize;

        public PersistentDefaultReaderCursor(Queue q)
        {
            _queue = q;
            _indexFieldSize = q.GetIndexSizeBytes();
            _indexSegmentSize = q.Header.IndexSegmentSize;
        }

        public SequenceNumber CurrentIsn()
        {
            return _queue.ReadNextIsnForDefaultReader();
        }

        public SequenceNumber Advance(SequenceNumber isn)
        {
            SequenceNumber nextisn = IncrementIndexSequenceNumber(isn, _indexSegmentSize.Bytes);
            _queue.UpdateNextIsnForDefaultReader(nextisn);
            return nextisn;
        }

        public SequenceNumber IncrementIndexSequenceNumber(SequenceNumber isn, long segmentSize)
        {
            if ((ulong)(isn.FileOffset + _indexFieldSize) > (ulong)segmentSize) {
                isn = isn.NextFile();
            } else {
                isn.LogicalOffset += _indexFieldSize;
            }

            return isn;
        }
    }
}
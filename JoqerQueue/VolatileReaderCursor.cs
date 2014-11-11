
namespace JoqerQueue
{
    public class VolatileReaderCursor : IReaderCursor
    {
        Queue _queue;
        private int _indexFieldSize;
        PageCount _indexSegmentSize;
        SequenceNumber _currentSn;

        public VolatileReaderCursor(Queue q)
        {
            _queue = q;
            _indexFieldSize = q.GetIndexRecordSizeBytes();
            _indexSegmentSize = q.Header.IndexSegmentSize;
            _currentSn = q.Header.FirstValidIndexSequenceNumber;
        }

        public VolatileReaderCursor(Queue q, SequenceNumber startingSn) : this(q)
        {
            _currentSn = startingSn;
        }

        public SequenceNumber Advance(SequenceNumber isn)
        {
            _currentSn = _currentSn.Increment(_indexFieldSize, _indexSegmentSize);
            return _currentSn;
        }

        public SequenceNumber CurrentIsn()
        {
            return _currentSn;
        }

        public SequenceNumber MaxIsn()
        {
            return _queue.ReadNextAvailableIndexSequenceNumber();
        }
    }
}

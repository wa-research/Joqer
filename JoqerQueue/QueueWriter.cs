using System;
using System.IO.MemoryMappedFiles;

namespace JoqerQueue
{
    public class QueueWriter : IDisposable
    {
        Queue _queue;
        QueueWriterSettings _settings;

        MemoryView _dataView;
        MemoryView _indexView;

        PageCount _dataSegmentSize;
        PageCount _indexSegmentSize;

        public LockMode LockMode { get { return _settings.LockMode; } }

        static readonly object _singleProcessLock = new object();

        int _indexFieldSize;
        Action<MemoryView.ViewInfo, SequenceNumber, int> _indexWriter;
        
        internal static QueueWriter Create(Queue q, QueueWriterSettings settings)
        {
            QueueWriter w = new QueueWriter
            { 
                _queue = q, 
                _settings = settings,
                _indexFieldSize = q.GetIndexRecordSizeBytes(),
                _dataSegmentSize = q.Header.DataSegmentSize,
                _indexSegmentSize = q.Header.IndexSegmentSize,
                _dataView = new MemoryView(q.Header.DataSegmentSize, q.DataSegmentFilePath, defaultViewSize: settings.PageCount),
                _indexView = new MemoryView(q.Header.IndexSegmentSize, q.IndexSegmentFilePath)
            };

            switch (settings.LockMode) {
                case LockMode.SingleThread:
                    w.Lock = w.LockNone;
                    break;
                case LockMode.SingleProcess:
                    w.Lock = w.LockLocal;
                    break;
                case LockMode.MultiProcess:
                    w.Lock = w.LockGlobal;
                    break;
            }

            return w;
        }

        private Action<MemoryView.ViewInfo, SequenceNumber, int> IndexRecordWriter
        {
            get { return _indexWriter = _indexWriter ?? SelectIndexRecordWriter(_queue.Header.Flags); }
        }


        private Func<Func<SequenceNumber>, SequenceNumber> Lock;
        private SequenceNumber LockGlobal(Func<SequenceNumber> a) { using (new GlobalLock(_queue.ParentFolder)) { return a(); } }
        private SequenceNumber LockLocal(Func<SequenceNumber> a) { lock (_singleProcessLock) { return a(); } }
        private SequenceNumber LockNone(Func<SequenceNumber> a) { return a(); }

        public void Enqueue(byte[] body)
        {
            // |slot len: 4 | ... payload ... |
            int bodyLen = body.Length;
            int slotSize = 4 + bodyLen;


            // Reserve write space
            SequenceNumber dsn = Lock(() => ReserveSlot(slotSize));

            // Write data in the segment we reserved - we don't need to lock as nobody else will want to write here, 
            // and readers won't access it until we update the index
            var view = _dataView.GetView(dsn, slotSize);

            view.WriteArrayWithLengthPrefix(body);

            // Lock the lockfile again to append to the index and advance the index pointer.
            // We keep the lock until the index entry is written and only then update the head;
            // this way there will be no holes in the index if we fail while writing to it.
            // (In other words, if we fail in writing the index, our data entry will be ignored).
            Lock(() => UpdateIndex(dsn, bodyLen, IndexRecordWriter));
        }

        private SequenceNumber ReserveSlot(int slotSize)
        {
            if (slotSize < 1)
                throw new ArgumentOutOfRangeException("slotSize", "Slot size must be a positive integer");
            if (slotSize > _dataSegmentSize.Bytes)
                throw new ArgumentOutOfRangeException("Body too large to store in the queue");    
            
            SequenceNumber dsn = _queue.ReadNextAvailableDataSequenceNumber().WithFileRollover(slotSize, _dataSegmentSize);
            _queue.UpdateNextAvailableDataSequenceNumber(dsn.IncrementFileOffset(slotSize));

            return dsn;
        }

        private SequenceNumber UpdateIndex(SequenceNumber dataLsn, int bodyLength,  Action<MemoryView.ViewInfo, SequenceNumber, int> recordWriter)
        {
            SequenceNumber isn = _queue.ReadNextAvailableIndexSequenceNumber().WithFileRollover(_indexFieldSize, _indexSegmentSize);
            recordWriter(_indexView.GetView(isn, _indexFieldSize), dataLsn, bodyLength);
            _queue.UpdateNextAvailableIndexSequenceNumber(isn.IncrementFileOffset(_indexFieldSize));

            return isn;
        }

        #region View Writers
        // We are using veiw writers to avoid the ungainly if branching in UpdateIndex method. This might have gained us a 
        // few microseconds of speed (considering UpdateIndex is called millions of times) but that was not the primary motivation.
        private Action<MemoryView.ViewInfo, SequenceNumber, int> SelectIndexRecordWriter(QueueOptions queueOptions)
        {
            bool sz = queueOptions.HasFlag(QueueOptions.StoreSizeInIndex);
            bool ts = queueOptions.HasFlag(QueueOptions.StoreTimestampInIndex);

            if (!sz && !ts) {
                return IndexWriterDsn;
            } else if (sz && !ts) {
                return IndexWriterDsnAndSize;
            } else if (!sz && ts) {
                return IndexWriterDsnAndTime;
            } else if (sz && ts) {
                return IndexWriterDsnLengthAndTime;
            }
            throw new ArgumentOutOfRangeException("Invalid index width", "indexWidth");
        }
        
        private void IndexWriterDsn(MemoryView.ViewInfo view, SequenceNumber dataLsn, int bodyLength)
        {
            view.Write(dataLsn.LogicalOffset);
        }

        private void IndexWriterDsnAndSize(MemoryView.ViewInfo view, SequenceNumber dataLsn, int bodyLength)
        {
            view.Write(dataLsn.LogicalOffset);
            view.Write(sizeof(long), bodyLength);
        }

        private void IndexWriterDsnAndTime(MemoryView.ViewInfo view, SequenceNumber dataLsn, int bodyLength)
        {
            view.Write(dataLsn.LogicalOffset);
            // This is not as testable, but is more efficient than reading the ticks when 
            // we don't use them and pass them on every invocation like we do with bodyLength
            view.Write(sizeof(long), DateTime.UtcNow.Ticks);
        }

        private void IndexWriterDsnLengthAndTime(MemoryView.ViewInfo view, SequenceNumber dataLsn, int bodyLength)
        {
            view.Write(dataLsn.LogicalOffset);
            view.Write(sizeof(long), bodyLength);
            view.Write(sizeof(long) + sizeof(int), DateTime.UtcNow.Ticks);
        }
        #endregion

        public void Dispose()
        {
            try { _dataView.Dispose(); } catch { }
            try { _indexView.Dispose(); } catch { }
            _queue = null;
            _dataView = null;
            _indexView = null;
        }
    }
}

using System;
using System.Threading;
using System.Threading.Tasks;

namespace JoqerQueue
{
    public class QueueReader : IDisposable
    {
        Queue _queue;
        MemoryView _dataView;
        MemoryView _indexView;
        private IReaderCursor _cursor;
        private int _indexFieldSize;
        QueueReaderSettings _settings;
        CancellationTokenSource _cancellator = new CancellationTokenSource();

        public bool IsRunning { get; private set; }

        public event EventHandler<byte[]> Message;

        internal static QueueReader Create(Queue q, QueueReaderSettings settings)
        {
            if (settings.PollInterval < 0)
                settings.PollInterval = 1;

            return new QueueReader
            {
                _queue = q,
                _settings = settings,
                _cursor = settings.Cursor ?? new PersistentDefaultReaderCursor(q),
                _indexFieldSize = q.GetIndexRecordSizeBytes(),
                _dataView = new MemoryView(q.Header.DataSegmentSize, q.DataSegmentFilePath, readOnly: true, defaultViewSize: settings.PageCount),
                _indexView = new MemoryView(q.Header.IndexSegmentSize, q.IndexSegmentFilePath, readOnly: true),
            };
        }

        private void PollerLoop(EventHandler<byte[]> ev)
        {
            while (!_cancellator.Token.IsCancellationRequested) {
                Thread.Sleep(_settings.PollInterval);

                SequenceNumber isn = _cursor.CurrentIsn();
                SequenceNumber maxisn = _cursor.MaxIsn();

                while (!_cancellator.Token.IsCancellationRequested && isn.LogicalOffset < maxisn.LogicalOffset) {
                    ev.Invoke(this, Dequeue(isn));
                    isn = _cursor.Advance(isn);
                }
            }
        }

        public void Start()
        {
            if (IsRunning)
                return;
            
            var ev = Message;
            if (ev == null) {
                IsRunning = false;
                return;
            }

            IsRunning = true;

            Task t = Task.Factory.StartNew(() => PollerLoop(ev), _cancellator.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            var ts = t.Status;
        }

        public void Stop()
        {
            _cancellator.Cancel();

            IsRunning = false;
        }

        public byte[] DequeueOne()
        {
            SequenceNumber currentisn = _cursor.CurrentIsn();
            SequenceNumber maxisn = _cursor.MaxIsn();

            if (currentisn.LogicalOffset >= maxisn.LogicalOffset)
                return null;

            var data = Dequeue(currentisn);
            _cursor.Advance(currentisn);

            return data;
        }

        Func<SequenceNumber, byte[]> _dequeueFunc;
        private Func<SequenceNumber, byte[]> DequeueFunction
        {
            get
            {
                return _dequeueFunc = _dequeueFunc ??
                    (_queue.Header.Flags.HasFlag(QueueOptions.StoreSizeInIndex) ? 
                        (Func<SequenceNumber, byte[]>)DequeueWithLengthInIndex : 
                        (Func<SequenceNumber, byte[]>)DequeueWithLengthInBody);
            }
        }

        public byte[] Dequeue(SequenceNumber isn)
        {
            return DequeueFunction(isn);
        }

        private byte[] DequeueWithLengthInBody(SequenceNumber isn)
        {
            var vi = _indexView.GetView(isn, _indexFieldSize);
            var dsn = new SequenceNumber { LogicalOffset = vi.ReadInt64() };
            var dv = _dataView.GetView(dsn, sizeof(int));
            int len = dv.ReadInt32();

            return ReadData(dv, dsn, len);
        }

        private byte[] DequeueWithLengthInIndex(SequenceNumber isn)
        {
            var vi = _indexView.GetView(isn, _indexFieldSize);
            var dsn = new SequenceNumber { LogicalOffset = vi.ReadInt64() };
            var len = vi.ReadInt32(sizeof(long));

            return ReadData(_dataView.GetView(dsn, len), dsn, len);
        }

        private byte[] ReadData(MemoryView.ViewInfo vi, SequenceNumber dsn, int len)
        {
            if (!vi.FitsIntoCurrentView(dsn, len)) {
                vi = _dataView.GetView(dsn, len);
            }
            return vi.ReadArray(4, len);
        }

        public void Dispose()
        {
            if (_cancellator != null && IsRunning)
                _cancellator.Cancel();

            IsRunning = false;

            _cursor = null;

            try { _dataView.Dispose(); } catch { }
            try { _indexView.Dispose(); } catch { }
            _queue = null;
            _dataView = null;
            _indexView = null;
        }
    }
}

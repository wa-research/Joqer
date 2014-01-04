﻿using System;
using System.IO.MemoryMappedFiles;
using System.Threading;

namespace JoqerQueue
{
    public class QueueReader : IDisposable
    {
        Queue _queue;
        MemoryView _dataView;
        MemoryView _indexView;
        private IReaderCursor _cursor;
        private int _indexFieldSize;


        public bool IsRunning { get; private set; }

        public event EventHandler<byte[]> Message;

        internal static QueueReader Create(Queue q, QueueReaderSettings settings)
        {
            return new QueueReader
            {
                _queue = q,
                _cursor = settings.Cursor ?? new PersistentDefaultReaderCursor(q),
                _indexFieldSize = q.GetIndexSizeBytes(),
                _dataView = new MemoryView(q.Header.DataSegmentSize, q.DataSegmentFilePath, readOnly: true, defaultViewSize: settings.PageCount),
                _indexView = new MemoryView(q.Header.IndexSegmentSize, q.IndexSegmentFilePath, readOnly: true),
            };
        }

        public void StartLoop()
        {
            var ev = Message;
            if (ev == null) {
                IsRunning = false;
                return;
            }

            IsRunning = true;

            while (IsRunning) {
                SequenceNumber isn = _cursor.CurrentIsn();
                SequenceNumber maxisn = _queue.ReadNextAvailableIndexSequenceNumber();

                while (isn.LogicalOffset < maxisn.LogicalOffset) {
                    ev.Invoke(this, Dequeue(isn));
                    isn = _cursor.Advance(isn);
                }
                Thread.Sleep(1);
            }
        }

        public void StopLoop()
        {
            IsRunning = false;
        }

        public byte[] DequeueOne()
        {
            SequenceNumber currentisn = _cursor.CurrentIsn();
            SequenceNumber maxisn = _queue.ReadNextAvailableIndexSequenceNumber();

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
            var dsn = new SequenceNumber { LogicalOffset = vi.View.ReadInt64(vi.ViewOffset) };
            var dv = _dataView.GetView(dsn, sizeof(int));
            int len = dv.View.ReadInt32(dv.ViewOffset);

            return ReadData(dv, dsn, len);
        }

        private byte[] DequeueWithLengthInIndex(SequenceNumber isn)
        {
            var vi = _indexView.GetView(isn, _indexFieldSize);
            var dsn = new SequenceNumber { LogicalOffset = vi.View.ReadInt64(vi.ViewOffset) };
            var len = vi.View.ReadInt32(vi.ViewOffset + sizeof(long));

            return ReadData(_dataView.GetView(dsn, len), dsn, len);
        }

        private byte[] ReadData(MemoryView.ViewInfo vi, SequenceNumber dsn, int len)
        {
            byte[] data = new byte[len];
            if (!vi.FitsIntoCurrentView(dsn, len)) {
                vi = _dataView.GetView(dsn, len);
            }
            vi.View.ReadArray(vi.ViewOffset + 4, data, 0, (int)len);

            return data;
        }

        public void Dispose()
        {
            _cursor = null;

            try { _queue.Dispose(); } catch { }
            try { _dataView.Dispose(); } catch { }
            try { _indexView.Dispose(); } catch { }
            _queue = null;
            _dataView = null;
            _indexView = null;
        }
    }
}
using System;
using System.IO;
using System.Linq;
using JoqerQueue;

namespace JoqerCtl
{
    class QueueInfo
    {
        public void Print(string name)
        {
            string fullPath = Path.GetFullPath(name);
            if (!Directory.Exists(fullPath))
                throw new ApplicationException(string.Format("Queue directory '{0}' does not exist.", fullPath));

            Queue q = Queue.Open(name);

            string lockFile = q.HeadFilePath();
            if (!File.Exists(lockFile))
                throw new ApplicationException(string.Format("Queue lock file '{0}' does not exist", lockFile));

            var h = q.Header;

            Info("Path:", fullPath);
            Info("Header version:", h.Version);
            Info("Mode:", h.MaxDataSegments == 0 ? "Unbounded" : "Circular");
            Info("Max data segments:", h.MaxDataSegments == 0 ? Int16.MaxValue : h.MaxDataSegments);
            Info("Data segment size KB:", h.DataSegmentSize.Bytes / 1024);
            Info("Index segment size KB:", h.IndexSegmentSize.Bytes / 1024);
            Info("Active data segment:", h.ActiveDataFile);
            Info("Active index segment:", h.ActiveIndexFile);
            Info("Active data segment free space:", h.DataSegmentSize.Bytes - h.NextAvailableDataSequenceNumber.FileOffset);
            Info("Active index segment free entries:", (h.IndexSegmentSize.Bytes - h.NextAvailableIndexSequenceNumber.FileOffset) / q.GetIndexRecordSizeBytes());
            Info("Total items writen:", ((h.ActiveIndexFile * h.IndexSegmentSize.Bytes) + h.NextAvailableIndexSequenceNumber.FileOffset) / q.GetIndexRecordSizeBytes());
            //Info("First valid data file:", h.FirstValidDataSequenceNumber.FileNumber);
            //Info("First valid index file:", h.FirstValidIndexSequenceNumber.FileNumber);
            Console.WriteLine();
            Info("Next item to read:", ISN(h.NextIndexIsnToReadWithDefaultReader, q.GetIndexRecordSizeBytes()));
            Info("Next item to write:", ISN(h.NextAvailableIndexSequenceNumber, q.GetIndexRecordSizeBytes()));
            Info("Queue depth:", SequenceDiff(h.NextAvailableIndexSequenceNumber, h.NextIndexIsnToReadWithDefaultReader, q.GetIndexRecordSizeBytes(), h.IndexSegmentSize.Bytes));

            Info("Flags:", PrintFlags(h.Flags));
        }

        private long SequenceDiff(SequenceNumber sn1, SequenceNumber sn2, int recordSize, long indexSegmentSize)
        {
            long sz1 = sn1.FileNumber * indexSegmentSize + sn1.FileOffset;
            long sz2 = sn2.FileNumber * indexSegmentSize + sn2.FileOffset;

            return (sz1 - sz2) / recordSize;
        }

        private object PrintFlags(QueueOptions queueOptions)
        {
            var o = queueOptions.ToString();
            return (o == "0") ? "None" : o;
        } 

        private string ISN(SequenceNumber isn, int slotWidth)
        {
            return string.Format("{0}:{1}", isn.FileNumber, isn.FileOffset / slotWidth);
        }

        private string LSN(SequenceNumber lsn)
        {
            return string.Format("{0}:{1:x12}", lsn.FileNumber, lsn.FileOffset);
        }

        private void Info(string label, object value)
        {
            Console.WriteLine("\t{0,-35}{1,20}", label, value);
        }


    }
}

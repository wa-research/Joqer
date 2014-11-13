using System;
using System.IO;
using System.Linq;
using JoqerQueue;

namespace JoqerCtl
{
    class QueueInfoPrinter
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
            var info = new QueueInfo(q);

            var indexWidth = info.IndexRecordWidthInBytes;

            Info("Path:", info.FullPath);
            Info("Header version:", info.HeaderVersion);
            Info("Mode:", info.GrowthMode.ToString());
            Info("Max data segments:", info.MaxDataSegments);
            Info("Data segment size KB:", info.DataSegmentSize.Bytes / 1024);
            Info("Index segment size KB:", info.IndexSegmentSize.Bytes / 1024);
            Info("Active data segment:", info.ActiveDataSegment);
            Info("Active index segment:", info.ActiveIndexSegment);
            Info("Active data segment free space:", info.ActiveDataSegmentFreeSpace);
            Info("Active index segment free entries:", info.ActiveIndexSegmentFreeEntries);
            Info("Total items writen:", info.TotalItemsWritten);
            //Info("First valid data file:", h.FirstValidDataSequenceNumber.FileNumber);
            //Info("First valid index file:", h.FirstValidIndexSequenceNumber.FileNumber);
            Console.WriteLine();
            Info("Next index position to read:", ISN(h.DefaultReaderBookmark, info.IndexRecordWidthInBytes));
            Info("Next index position to write:", ISN(info.NextAvailableIndexSequenceNumber, info.IndexRecordWidthInBytes));
            Info("Queue depth:", SequenceDiff(info.NextAvailableIndexSequenceNumber, h.DefaultReaderBookmark, info.IndexRecordWidthInBytes, info.IndexSegmentSize.Bytes));

            Info("Flags:", PrintFlags(h.Flags));

            Console.WriteLine();
            Info("Readers", string.Empty);
            Console.WriteLine();


            foreach (var bm in q.Bookmarks()) {
                Info(bm.Guid.ToString(), ISN(bm.SequenceNumber, indexWidth));
            }
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

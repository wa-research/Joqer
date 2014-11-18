using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JoqerQueue
{
    public class QueueInfo
    {
        public int IndexRecordWidthInBytes { get; set; }
        public string FullPath { get; set; }
        public SequenceNumber NextAvailableIndexSequenceNumber { get; set; }
        public SequenceNumber FirstValidIndexSequenceNumber { get; set; }
        public byte HeaderVersion { get; set; }
        public short ActiveDataSegment { get; set; }
        public short ActiveIndexSegment { get; set; }
        public QueueGrowthMode GrowthMode { get; set; }
        public short MaxDataSegments { get; set; }
        public PageCount DataSegmentSize { get; set; }
        public PageCount IndexSegmentSize { get; set; }
        public long ActiveDataSegmentFreeSpace { get; set; }
        public long ActiveIndexSegmentFreeEntries { get; set; }
        /// <summary>
        /// Total items written in the queue since the queue was created. Does not necessarily reflect the total items available in the queue. Use QueueDepth(bookmarkID) to calculate the actual queue depth.
        /// </summary>
        public long TotalItemsWritten { get; set; }
        public QueueOptions Flags { get; set; }

        public QueueInfo(Queue q)
        {
            var h = q.Header;

            FullPath = Path.GetFullPath(q.ParentFolder);
            HeaderVersion = h.Version;
            GrowthMode = h.MaxDataSegments == 0 ? QueueGrowthMode.Unbounded : QueueGrowthMode.Circular;
            MaxDataSegments = h.MaxDataSegments == 0 ? Int16.MaxValue : h.MaxDataSegments;
            DataSegmentSize = h.DataSegmentSize;
            IndexSegmentSize = h.IndexSegmentSize;
            IndexRecordWidthInBytes = q.GetIndexRecordSizeBytes();
            ActiveDataSegment = h.ActiveDataFile;
            ActiveIndexSegment = h.ActiveIndexFile;
            FirstValidIndexSequenceNumber = q.Header.FirstValidIndexSequenceNumber;
            NextAvailableIndexSequenceNumber = q.NextAvailableIndexSequenceNumber();
            ActiveDataSegmentFreeSpace = h.DataSegmentSize.Bytes - h.NextAvailableDataSequenceNumber.FileOffset;
            ActiveIndexSegmentFreeEntries = (h.IndexSegmentSize.Bytes - h.NextAvailableIndexSequenceNumber.FileOffset) / q.GetIndexRecordSizeBytes();
            TotalItemsWritten = ((h.ActiveIndexFile * h.IndexSegmentSize.Bytes) + h.NextAvailableIndexSequenceNumber.FileOffset) / q.GetIndexRecordSizeBytes();
            Flags = h.Flags;
        }
    }
}

using System;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Reflection;
using System.Runtime.InteropServices;

namespace JoqerQueue
{
    public partial class Queue : IDisposable
    {
        public const byte Version = 1;
        public const string HeadFileName = "head";
        public const string DataFileSuffix = "data";
        public const string IndexFileSuffix = "index";

        // All instances of the class will share the same counter on a particular queue in order to support parallel
        public static readonly Guid GUID = Marshal.GetTypeLibGuidForAssembly(Assembly.GetExecutingAssembly());

        public string ParentFolder { get; private set; }
        public Header Header { get { return _header; } }

        Header _header;

        MemoryMappedFile _mmap_head;
        MemoryMappedViewAccessor _mmap_view_header;

        private Queue(string queuePath)
        {
            ParentFolder = queuePath;
            _header = new Header();
        }

        public static Queue Open(string queuePath)
        {
            var q = new Queue(queuePath);
            q._mmap_head = q.OpenHead();
            q._mmap_view_header = q._mmap_head.CreateViewAccessor();
            q._header = Header.Open(q._mmap_view_header);
            return q;
        }

        public static QueueWriter OpenWriter(string queuePath, LockMode lockMode = LockMode.MultiProcess)
        {
            return OpenWriter(queuePath, new QueueWriterSettings { LockMode = lockMode });
        }

        public static QueueWriter OpenWriter(string queuePath, QueueWriterSettings settings)
        {
            return Queue.Open(queuePath).GetWriter(settings);
        }

        public static QueueReader OpenReader(string queuePath, QueueReaderSettings settings = default(QueueReaderSettings))
        {
            return Queue.Open(queuePath).GetReader(settings);
        }

        public QueueReader GetReader(QueueReaderSettings settings)
        {
            return QueueReader.Create(this, settings);
        }

        public QueueWriter GetWriter(QueueWriterSettings settings)
        {
            return QueueWriter.Create(this, settings);
        }

        public static Queue Create(string queuePath, PageCount dataSegmentSize, PageCount indexSegmentSize, QueueOptions options, short maxSegments = 0, short segmentsToCreateAhead = 0)
        {
            var q = new Queue(queuePath);

            string lockFile = q.HeadFilePath();
            string dataFile = q.DataSegmentFilePath(0);
            string indexFile = q.IndexSegmentFilePath(0);

            using (var hmm = FileHelpers.CreateFileAndMap(lockFile, new PageCount(1), FileHelpers.LocalName(lockFile))) {
                using (var va = hmm.CreateViewAccessor()) {
                    q.Header.Version = Version;
                    q.Header.Flags = options;
                    q.Header.MaxDataSegments = maxSegments;
                    q.Header.DataSegmentSize = dataSegmentSize;
                    q.Header.IndexSegmentSize = AdjustIndexSize(indexSegmentSize, GetIndexSizeBytes(options));
                    q.WriteHeader(va);
                }
            }

            FileHelpers.CreateFileAndMap(dataFile, q.Header.DataSegmentSize, FileHelpers.GlobalName(dataFile));
            FileHelpers.CreateFileAndMap(indexFile, q.Header.IndexSegmentSize, FileHelpers.GlobalName(indexFile));

            for (short s = 1; s < segmentsToCreateAhead; s++) {
                var df = q.DataSegmentFilePath(s);
                FileHelpers.CreateFileAndMap(df, q.Header.DataSegmentSize, FileHelpers.GlobalName(df));
            }

            return q;
        }

        /// <summary>
        /// Adjust the number of pages such that there are no empty bytes at the end of the page
        /// </summary>
        /// <param name="i">Desired index page count</param>
        /// <param name="width">Index width in bytes</param>
        /// <returns>Nearest larger index size that leaves no empty bytes at the end of each file</returns>
        private static PageCount AdjustIndexSize(PageCount i, int width)
        {
            if (((double)i.Bytes / width) == (i.Bytes / width))
                return i;

            double w = (double)width / sizeof(int);
            return new PageCount { Pages = (int)(Math.Ceiling((double)i.Pages / w) * w) };
        } 

        private void WriteHeader(MemoryMappedViewAccessor ViewAccessor)
        {
            ViewAccessor.Write(Header.Offsets.Version, _header.Version);
            ViewAccessor.Write(Header.Offsets.Flags, (byte)_header.Flags);
            ViewAccessor.Write(Header.Offsets.MaxDataSegments, _header.MaxDataSegments);
            ViewAccessor.Write(Header.Offsets.DataSegmentSize, _header.DataSegmentSize.Pages);
            ViewAccessor.Write(Header.Offsets.IndexSegmentSize, _header.IndexSegmentSize.Pages);
            ViewAccessor.Write(Header.Offsets.FirstValidDataSequenceNumber, _header.FirstValidDataSequenceNumber.LogicalOffset);
            ViewAccessor.Write(Header.Offsets.FirstValidIndexSequenceNumber, _header.FirstValidIndexSequenceNumber.LogicalOffset);
            ViewAccessor.Write(Header.Offsets.NextAvailableDataSequenceNumber, _header.NextAvailableDataSequenceNumber.LogicalOffset);
            ViewAccessor.Write(Header.Offsets.NextAvailableIndexSequenceNumber, _header.NextAvailableIndexSequenceNumber.LogicalOffset);
            ViewAccessor.Write(Header.Offsets.NextIndexIsnToReadWithDefaultReader, _header.NextIndexIsnToReadWithDefaultReader.LogicalOffset);
        }

        public MemoryMappedFile OpenHead()
        {
            var head = FileHelpers.OpenMmf(HeadFilePath());

            if (head == null)
                throw new ApplicationException("Could not open the queue head");

            return head;
        }

        public MemoryMappedFile OpenIndexFile(short activeIndexFileNo)
        {
            var f = FileHelpers.OpenMmf(IndexSegmentFilePath(activeIndexFileNo));

            if (f == null)
                throw new ApplicationException(string.Format("Could not open index segment {0}", IndexSegmentFilePath(activeIndexFileNo)));

            return f;
        }

        public MemoryMappedFile OpenDataSegment(short segmentFileNo)
        {
            var f = FileHelpers.OpenMmf(DataSegmentFilePath(segmentFileNo));

            if (f == null)
                throw new ApplicationException(string.Format("Could not open data segment {0}", DataSegmentFilePath(segmentFileNo)));

            return f;
        }

        internal MemoryMappedFile OpenOrCreateIndexSegment(short segmentNumber)
        {
            var idx = FileHelpers.OpenOrCreateSegment(IndexSegmentFilePath(segmentNumber), _header.IndexSegmentSize);

            if (idx == null)
                throw new ApplicationException(string.Format("Could not open index segment {0}", IndexSegmentFilePath(segmentNumber)));

            return idx;
        }

        public string HeadFilePath()
        {
            return Path.Combine(ParentFolder, HeadFileName);
        }

        public string DataSegmentFilePath(short fileNo)
        {
            return Path.Combine(ParentFolder, NumberedFile(fileNo, DataFileSuffix));
        }

        public string IndexSegmentFilePath(short fileNo)
        {
            return Path.Combine(ParentFolder, NumberedFile(fileNo, IndexFileSuffix));
        }

        public string NumberedFile(short no, string ext)
        {
            return Path.ChangeExtension(no.ToString().PadLeft(10, '0'), ext);
        }

        public int GetIndexRecordSizeBytes()
        {
            return GetIndexSizeBytes(_header.Flags);
        }

        private static int GetIndexSizeBytes(QueueOptions f)
        {
            return sizeof(long)
                + (f.HasFlag(QueueOptions.StoreSizeInIndex) ? sizeof(int) : 0)
                + (f.HasFlag(QueueOptions.StoreTimestampInIndex) ? sizeof(long) : 0);
        }

        #region Offsets
        internal SequenceNumber ReadNextIsnForDefaultReader()
        {
            return new SequenceNumber { LogicalOffset = _mmap_view_header.ReadInt64(Header.Offsets.NextIndexIsnToReadWithDefaultReader) };
        }

        public void Rewind(SequenceNumber isn)
        {
            UpdateNextIsnForDefaultReader(isn);
        }

        internal void UpdateNextIsnForDefaultReader(SequenceNumber isn)
        {
            _mmap_view_header.Write(Header.Offsets.NextIndexIsnToReadWithDefaultReader, isn.LogicalOffset);
        }

        internal SequenceNumber ReadNextAvailableIndexSequenceNumber()
        {
            return new SequenceNumber { LogicalOffset = _mmap_view_header.ReadInt64(Header.Offsets.NextAvailableIndexSequenceNumber) };
        }

        internal void UpdateNextAvailableIndexSequenceNumber(SequenceNumber isn)
        {
            _mmap_view_header.Write(Header.Offsets.NextAvailableIndexSequenceNumber, isn.LogicalOffset);
        }

        public SequenceNumber ReadNextAvailableDataSequenceNumber()
        {
            return new SequenceNumber { LogicalOffset = _mmap_view_header.ReadInt64(Header.Offsets.NextAvailableDataSequenceNumber) };
        }

        public void UpdateNextAvailableDataSequenceNumber(SequenceNumber dsn)
        {
            _mmap_view_header.Write(Header.Offsets.NextAvailableDataSequenceNumber, dsn.LogicalOffset);
        }
        #endregion

        public void Dispose()
        {
            _header = null;

            try { if (_mmap_view_header != null) _mmap_view_header.Dispose(); } catch { }
            try { if (_mmap_head != null) _mmap_head.Dispose(); } catch { }

            _mmap_head = null;
        }
    }
}

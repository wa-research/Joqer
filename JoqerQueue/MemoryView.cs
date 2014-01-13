using System;
using System.IO.MemoryMappedFiles;
#if WINDOWS && FAST
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
#endif

namespace JoqerQueue
{
    public class MemoryView : IDisposable
    {
        const int DEFAULT_VIEW_SIZE_PAGES = 25600; /*100 MB*/
        PageCount _segmentSizePages;
        PageCount DefaultViewSize;

        short _currentFileNo;
        MemoryMappedFile _file;
        ViewInfo _viewInfo;
        Func<short, string> _fileNameGenerator;

        bool _isReadOnly;

        public MemoryView(PageCount segmentSizePages, Func<short, string> fileNameGenerator, bool readOnly = false, int defaultViewSize = DEFAULT_VIEW_SIZE_PAGES)
        {
            if (defaultViewSize == 0)
                defaultViewSize = DEFAULT_VIEW_SIZE_PAGES;

            DefaultViewSize = new PageCount(defaultViewSize);

            _segmentSizePages = segmentSizePages;
            _fileNameGenerator = fileNameGenerator;
            if (segmentSizePages.Pages < DefaultViewSize.Pages)
                DefaultViewSize = segmentSizePages;
            _viewInfo = default(ViewInfo);

            _isReadOnly = readOnly;
        }

        /// <summary>
        /// Get a view accessor to read or write the data. The caller is responsible for ensuring the 
        /// data to be written will fit into the memory-mapped file at the specified offset
        /// </summary>
        /// <param name="sn"></param>
        /// <param name="slotSize"></param>
        /// <returns></returns>
        internal ViewInfo GetView(SequenceNumber sn, int slotSize)
        {
            short fno = sn.FileNumber;
            if (fno != _currentFileNo) {
                // We are changing the file, re-set the view accessor
                if (_viewInfo.View != null) {
                    _viewInfo.View.Dispose();
                    _viewInfo.View = null;
                }
                _file = _isReadOnly ? OpenSegment(fno) : OpenOrCreateSegment(fno);
                _currentFileNo = fno;
            }

            if (_file == null)
                _file = _isReadOnly ? OpenSegment(fno) : OpenOrCreateSegment(fno);

            // Caller must ensure the slot fits into the data file
            if (!_viewInfo.FitsIntoCurrentView(sn, slotSize)) {
                if (_viewInfo.View != null) {
                    _viewInfo.View.Dispose();
                    _viewInfo.View = null;
                }
                _viewInfo.FileNo = fno;
                _viewInfo.StartingPage = AlignToNearestPage(sn.FileOffset);
                var requiredSize = AlignToNearestPage(slotSize) + 1;
                if (requiredSize.Pages > DefaultViewSize.Pages)
                    _viewInfo.EndingPage = AlignToNearestPage(sn.FileOffset + slotSize) + 1;
                else 
                    _viewInfo.EndingPage = _viewInfo.StartingPage + DefaultViewSize;
                if (_viewInfo.EndingPage.Pages > _segmentSizePages.Pages)
                    _viewInfo.EndingPage = _segmentSizePages;
            }

            long size = _viewInfo.GetSize();

            if (_viewInfo.StartingPage.Bytes >= _segmentSizePages.Bytes ||
                _viewInfo.StartingPage.Bytes + size > _segmentSizePages.Bytes)
                throw new ApplicationException("Trying to open a view beyond the end of the current segment");

            if (_viewInfo.View == null) {
                _viewInfo.View = _file.CreateViewAccessor(_viewInfo.StartingPage.Bytes, size);
            }

            _viewInfo.ViewOffset = sn.FileOffset - _viewInfo.StartingPage.Bytes;

            return _viewInfo;
        }

        private PageCount AlignToNearestPage(long offset)
        {
            return new PageCount { Bytes = offset };
        } 
    
        internal MemoryMappedFile OpenOrCreateSegment(short segmentFileNo)
        {
            var ds = FileHelpers.OpenOrCreateSegment(_fileNameGenerator(segmentFileNo), _segmentSizePages);

            if (ds ==null)
                throw new ApplicationException(string.Format("Could not open segment {0}", _fileNameGenerator(segmentFileNo)));

            return ds;
        }

        internal MemoryMappedFile OpenSegment(short segmentFileNo)
        {
            var seg = FileHelpers.OpenMmf(_fileNameGenerator(segmentFileNo));
            if (seg == null)
                throw new ApplicationException(string.Format("Could not open segment {0}", _fileNameGenerator(segmentFileNo)));

            return seg;
        }

        internal struct ViewInfo
        {
            public short FileNo;
            public PageCount StartingPage;
            public PageCount EndingPage;
            public long ViewOffset;

            internal MemoryMappedViewAccessor View;

            public long GetSize()
            {
                return EndingPage.Bytes - StartingPage.Bytes;
            }

            internal bool FitsIntoCurrentView(SequenceNumber sn, int slotSize)
            {
                return StartingPage.Bytes <= sn.FileOffset && sn.FileOffset + slotSize <= EndingPage.Bytes;
            }

            /// <summary>
            /// Read an <c>Int32</c> at the current view offset
            /// </summary>
            /// <returns></returns>
            public int ReadInt32()
            {
                return ReadInt32(0);
            }

            /// <summary>
            /// Read an <c>Int32</c> a <paramref name="delta"/> bytes after the current view offset
            /// </summary>
            /// <param name="delta"></param>
            /// <returns></returns>
            public int ReadInt32(long delta)
            {
                return View.ReadInt32(ViewOffset + delta);
            }
            /// <summary>
            /// Read an <c>Int64</c> at the current offset
            /// </summary>
            /// <returns></returns>
            public long ReadInt64()
            {
                return View.ReadInt64(ViewOffset);
            }

#if WINDOWS && FAST
            public byte[] ReadArray(int delta, int len)
            {
                byte[] arr = new byte[len];
                IntPtr ptr = View.Pointer((int)StartingPage.Bytes);
                try {
                    Marshal.Copy(IntPtr.Add(ptr, (int)ViewOffset + delta), arr, 0, len);
                } finally {
                    View.SafeMemoryMappedViewHandle.ReleasePointer();
                }
                return arr;
            }
#else
            public byte[] ReadArray(int delta, int len)
            {
                byte[] data = new byte[len];
                View.ReadArray(ViewOffset + delta, data, 0, len);
                return data;
            }
#endif

            /// <summary>
            /// Write a long value at a current view offset
            /// </summary>
            /// <param name="value"></param>
            public void Write(long value)
            {
                Write(0, value);
            }

            /// <summary>
            /// Write a long value <paramref name="delta"/> bytes after the current view offset
            /// </summary>
            /// <param name="delta"></param>
            /// <param name="value"></param>
            public void Write(long delta, long value)
            {
                View.Write(ViewOffset + delta, value);
            }

#if WINDOWS && FAST
            public void WriteArrayWithLengthPrefix(byte[] data)
            {
                IntPtr ptr = IntPtr.Add(View.Pointer((int)StartingPage.Bytes), (int)ViewOffset);
                try {
                    Marshal.WriteInt32(ptr, data.Length);
                    Marshal.Copy(data, 0, IntPtr.Add(ptr, sizeof(int)), data.Length);
                } finally {
                    View.SafeMemoryMappedViewHandle.ReleasePointer();
                }
            }
#else
            public void WriteArrayWithLengthPrefix(byte[] data)
            {
                View.Write(data.Length);
                View.WriteArray(ViewOffset + sizeof(int), data, 0, data.Length);
            }
#endif
        }

        public void Dispose()
        {
            if (_viewInfo.View != null) {
                try { _viewInfo.View.Dispose(); } catch { }
                _viewInfo.View = null;
            }
            if (_file != null) {
                try { _file.Dispose(); } catch { }
                _file = null;
            }
        }
    }

#if WINDOWS && FAST
    // From: http://connect.microsoft.com/VisualStudio/feedback/details/537635/no-way-to-determine-internal-offset-used-by-memorymappedviewaccessor-makes-safememorymappedviewhandle-property-unusable#tabs
    internal unsafe static class Helper
    {
        static SYSTEM_INFO info;

        static Helper()
        {
            GetSystemInfo(ref info);
        }

        internal static IntPtr Pointer(this MemoryMappedViewAccessor acc, int offset)
        {
            int num = offset % info.dwAllocationGranularity;
            byte* tmp_ptr = null;
            RuntimeHelpers.PrepareConstrainedRegions();
            acc.SafeMemoryMappedViewHandle.AcquirePointer(ref tmp_ptr);
            tmp_ptr += num;

            return new IntPtr(tmp_ptr);
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern void GetSystemInfo(ref SYSTEM_INFO lpSystemInfo);

        internal struct SYSTEM_INFO
        {
            internal int dwOemId;
            internal int dwPageSize;
            internal IntPtr lpMinimumApplicationAddress;
            internal IntPtr lpMaximumApplicationAddress;
            internal IntPtr dwActiveProcessorMask;
            internal int dwNumberOfProcessors;
            internal int dwProcessorType;
            internal int dwAllocationGranularity;
            internal short wProcessorLevel;
            internal short wProcessorRevision;
        }
    }
#endif
}

using System;
using System.IO.MemoryMappedFiles;

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

            public MemoryMappedViewAccessor View;

            public long GetSize()
            {
                return EndingPage.Bytes - StartingPage.Bytes;
            }

            internal bool FitsIntoCurrentView(SequenceNumber sn, int slotSize)
            {
                return StartingPage.Bytes <= sn.FileOffset && sn.FileOffset + slotSize <= EndingPage.Bytes;
            }
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
}

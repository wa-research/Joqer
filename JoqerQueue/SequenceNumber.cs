
namespace JoqerQueue
{
    public struct SequenceNumber
    {
        private long _offset;

        public SequenceNumber(long offset)
        {
            _offset = offset;
        }

        public long FileOffset { get { return (_offset & 0x0000FFFFFFFFFFFF); } set { _offset = ((long)((ulong)_offset & 0xFFFF000000000000UL)) | (value & 0x0000FFFFFFFFFFFF); } }
        public short FileNumber { get { return (short)(_offset >> 48); } set { _offset = (long)value << 48 | FileOffset; } }
        public long LogicalOffset { get { return _offset; } set { _offset = value; } }

        public const long FirstWriteOffset = 0L;

        public SequenceNumber NextFile()
        {
            return new SequenceNumber { _offset = FirstWriteOffset, FileNumber = (short)(FileNumber + 1) };
        }

        public SequenceNumber IncrementFileOffset(long increment)
        {
            return new SequenceNumber { _offset = _offset, FileOffset = FileOffset + increment };
        }
    }

    public static class SequenceNumberExtensions
    {
        public static SequenceNumber WithFileRollover(this SequenceNumber sn, int slotSize, PageCount segmentSize)
        {
            // Do we need to roll over into the next file?
            if ((ulong)(sn.FileOffset + slotSize) > (ulong)segmentSize.Bytes) {
                sn = sn.NextFile();
            }
            return sn;
        }
    }
}

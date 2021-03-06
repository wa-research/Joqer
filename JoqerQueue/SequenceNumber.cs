﻿
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
        public static SequenceNumber Zero = new SequenceNumber(FirstWriteOffset); 

        public SequenceNumber NextFile()
        {
            return new SequenceNumber { _offset = FirstWriteOffset, FileNumber = (short)(FileNumber + 1) };
        }

        internal SequenceNumber IncrementFileOffset(long increment)
        {
            return new SequenceNumber { _offset = _offset, FileOffset = FileOffset + increment };
        }
    }

    public static class SequenceNumberExtensions
    {
        public static SequenceNumber NextFileIfNotEnoughSpaceAtCurrentPosition(this SequenceNumber sn, int slotSize, PageCount segmentSize)
        {
            // Do we need to roll over into the next file?
            if ((ulong)(sn.FileOffset + slotSize) > (ulong)segmentSize.Bytes) {
                sn = sn.NextFile();
            }
            return sn;
        }

        public static SequenceNumber Increment(this SequenceNumber isn, int slotSize, PageCount segmentSize)
        {
            if ((ulong)(isn.FileOffset + slotSize) >= (ulong)segmentSize.Bytes) {
                isn = isn.NextFile();
            } else {
                isn.LogicalOffset += slotSize;
            }
            return isn;
        }

        public static string ToString(this SequenceNumber isn, int slotSize)
        {
            return string.Format("{0}:{1}", isn.FileNumber, isn.FileOffset / slotSize);
        }
    }
}

using System;

namespace JoqerQueue
{
    public struct QueueReaderSettings
    {
        public Guid Guid;
        public int PageCount; 
        public IReaderCursor Cursor;
        public int PollInterval;
    }
}

using System;

namespace JoqerQueue
{
    public struct QueueReaderSettings
    {
        /// <summary>
        /// Default poll interval in milliseconds
        /// </summary>
        public const int DefaultPollInterval = 150;

        /// <summary>
        /// Reader ID for non-default reader
        /// </summary>
        public Guid Guid;
        /// <summary>
        /// Number of memory pages (4096b) to keep in memory at once
        /// </summary>
        public int PageCount; 
        /// <summary>
        /// Cursor to use for traversal of enqueued items
        /// </summary>
        public IReaderCursor Cursor;
        /// <summary>
        /// Polling interval in milliseconds
        /// </summary>
        public int PollInterval;
    }
}

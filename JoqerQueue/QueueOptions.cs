using System;

namespace JoqerQueue
{
    [Flags]
    public enum QueueOptions : byte
    {
        StoreSizeInIndex = 0x01,
        StoreTimestampInIndex = 0x02
    }
}

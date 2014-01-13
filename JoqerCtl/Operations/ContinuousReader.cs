using System;
using System.Text;
using JoqerQueue;

namespace JoqerCtl
{
    class ContinuousReader
    {
        Queue _queue;

        public ContinuousReader(Queue q)
        {
            _queue = q;
        }

        public void Read()
        {
            int i = 0;
            long cnt = 0;
            using (var q = _queue.GetReader(new QueueReaderSettings { PollInterval = 150 })) {
                q.Message += (s, e) => {
                    var str = Encoding.ASCII.GetString(e);
                    if (str == null)
                        Console.WriteLine("{0,6}:ERROR: Payload should not be zero-length", ++i);
                    cnt++;
                    if (cnt % 100000 == 0)
                        Console.WriteLine("Read {0} messages", cnt);
                };
                q.StartLoop();
            }
        }
    }
}

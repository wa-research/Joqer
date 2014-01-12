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
            using (var q = _queue.GetReader(new QueueReaderSettings { PollInterval = 150 })) {
                q.Message += (s, e) => {
                    var str = Encoding.ASCII.GetString(e);
                    if (str == null)
                        Console.WriteLine("{0,6}:ERROR: Payload should not be zero-length", ++i);
                    else if (str.Length < 20)
                        Console.WriteLine("{0,6}:WARNING: str is less than 20: {1}", ++i, str);
                    //else
                    //    Console.WriteLine("{0}", str.Substring(0, 20));
                };
                q.StartLoop();
            }
        }
    }
}

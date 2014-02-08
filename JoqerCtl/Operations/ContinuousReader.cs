using System;
using System.Text;
using JoqerQueue;

namespace JoqerCtl
{
    class ContinuousReader : IDisposable
    {
        QueueReader _reader;

        public ContinuousReader(Queue q)
        {
            _reader = q.GetReader(new QueueReaderSettings { PollInterval = 150 });
        }

        public void Start()
        {
            int i = 0;
            long cnt = 0;
            _reader.Message += (s, e) => {
                var str = Encoding.ASCII.GetString(e);
                if (str == null)
                    Console.WriteLine("{0,6}:ERROR: Payload should not be zero-length", ++i);
                cnt++;
                if (cnt % 100000 == 0)
                    Console.WriteLine("Read {0} messages", cnt);
            };
            _reader.Start();
        }

        public void Dispose()
        {
            if (_reader != null) {
                _reader.Stop();
                _reader.Dispose();
            }
        }
    }
}

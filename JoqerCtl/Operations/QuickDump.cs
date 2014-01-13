using System;
using System.Diagnostics;
using System.Text;
using JoqerQueue;

namespace JoqerCtl
{
    class QuickDump
    {
        public void ReadOne(string queuePath)
        {
            using (var q = Queue.OpenReader(queuePath)) {
                var b = q.DequeueOne();
                if (b != null)
                    Console.WriteLine(Encoding.UTF8.GetString(b));
                else
                    Console.WriteLine("NO DATA");
            }
        }

        const int MB = 1024 * 1024;

        public void ReadAll(string queuePath)
        {
            using (var q = Queue.OpenReader(queuePath)) {
                float i = 0;
                float sz = 0;
                int nulls = 0;
                var s = Stopwatch.StartNew();
                var b = q.DequeueOne();
                while (b != null) {
                    i++;
                    sz += b.Length;
                    b = q.DequeueOne();
                }
                s.Stop();
                float msec = s.ElapsedMilliseconds;
                Console.WriteLine("Read {0} MB in {1} msec at {2} MB/sec", sz / MB, msec, Math.Round(sz / msec * 1000 / MB, 2));
                Console.WriteLine("  or {0} messages at {1} ops per second, {2} usec per op on average", i, Math.Round(i / msec * 1000, 2), msec / i * 1000);
            }
        }
    }
}

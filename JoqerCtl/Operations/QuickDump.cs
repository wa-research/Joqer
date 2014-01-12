using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
        public void ReadAll(string queuePath)
        {
            using (var q = Queue.OpenReader(queuePath)) {
                float i = 0;
                var s = Stopwatch.StartNew();
                var b = q.DequeueOne();
                while (b != null) {
                    //Console.WriteLine(Encoding.ASCII.GetString(b).Substring(0, 20));
                    i++;
                    b = q.DequeueOne();
                }
                s.Stop();
                float msec = s.ElapsedMilliseconds;
                Console.WriteLine("Read {0} messages in {1} msec at {2} ops per second or {3}usec per op", i, s.ElapsedMilliseconds, Math.Round(i / s.ElapsedMilliseconds * 1000, 2), msec / i * 1000);
            }
        }
    }
}

using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JoqerQueue;

namespace JoqerCtl
{
    class TestFill
    {
        const int WORKERS = 1;
        const int JOBS_PER_WORKER = 10000;
        const int PAYLOAD_SIZE = 256;
        const int VIEW_SIZE = 5120;

        string _queueName;

        public TestFill(string queuePath)
        {
            _queueName = queuePath;
        }
        public void Run(string[] args, string[] flags)
        {
            if (args.Contains("help")) {
                Console.WriteLine("USAGE: {0} test -w[workers|1] -j[jobs per worker|{1}] -l{{lockmode:'p|m|s'|'p'}} -s{{payload size|256}} -v{{wiew size pages|5120}}", Environment.GetCommandLineArgs()[0].ToLowerInvariant(), JOBS_PER_WORKER);
                Environment.Exit(1);
            }

            int workers = GetIntFlag(flags, 'w', WORKERS);
            int jobs_per_worker = GetIntFlag(flags, 'j', JOBS_PER_WORKER);
            LockMode mode = GetLockMode(flags);
            int payload_size = GetIntFlag(flags, 's', PAYLOAD_SIZE);
            int view_size = GetIntFlag(flags, 'v', VIEW_SIZE);

            if (mode == LockMode.SingleThread && workers > 1)
                Console.WriteLine("WARNING: Using multiple threads with no locking!");

            Console.WriteLine("Starting new test with {0} workers enqueieing {1} payloads of {2} bytes each in lock mode {3}", workers, jobs_per_worker, payload_size, mode);

            var s = Stopwatch.StartNew();
            Task[] tasks = new Task[workers];
            for (int i = 0; i < workers; i++) {
                tasks[i] = Task.Factory.StartNew(Fill, Tuple.Create(i, jobs_per_worker, payload_size, mode, view_size, _queueName));
            }

            try {
                // Wait for all the tasks to finish.
                Task.WaitAll(tasks);
                s.Stop();
                float totalJobs = workers * jobs_per_worker;
                float msec = s.ElapsedMilliseconds;
                float ticks = s.ElapsedTicks;

                Console.WriteLine("Enqueued {0} jobs in {1} ms at {2} ops per second or {3} usec per op ({4} ticks per op)", totalJobs, s.ElapsedMilliseconds, Math.Round(totalJobs / s.ElapsedMilliseconds * 1000, 2), msec / totalJobs * 1000, ticks / totalJobs);
            } catch (AggregateException e) {
                Console.Error.WriteLine("\nThe following exceptions have been thrown by WaitAll():");
                for (int j = 0; j < e.InnerExceptions.Count; j++) {
                    Console.Error.WriteLine("\n-------------------------------------------------\n{0}", e.InnerExceptions[j].ToString());
                }
            }
        }

        private LockMode GetLockMode(string[] flags)
        {
            LockMode mode = LockMode.MultiProcess;
            if (flags != null)
                mode = flags
                    .Where(f => f[0] == 'l')
                    .Select(f => {
                        switch (f[1]) {
                            case 'm':
                                return LockMode.SingleProcess;
                            case 's':
                                return LockMode.SingleThread;
                            default:
                                return LockMode.MultiProcess;
                        }
                    })
                    .FirstOrDefault();

            return mode;
        }

        private int GetIntFlag(string[] flags, char flag, int defaultValue)
        {
            var val = flags != null ?
                flags.Where(f => f[0] == flag).Select(f => int.Parse(f.Substring(1))).FirstOrDefault() :
                defaultValue;

            if (val == default(int))
                val = defaultValue;

            return val;
        }

        static void Fill(object o)
        {
            var tt = o as Tuple<int, int, int, LockMode, int, string>;
            int task = tt.Item1;
            int jobs = tt.Item2;
            int size = tt.Item3;
            LockMode mode = tt.Item4;
            int view = tt.Item5;
            string qname = tt.Item6;

            using (var q = Queue.OpenWriter(qname, new QueueWriterSettings { LockMode = mode, PageCount = view })) {
                string dots = new string(' ', size);
                StringBuilder sb = new StringBuilder(size);
                int tid = Thread.CurrentThread.ManagedThreadId;
                sb.Append('W').Append(task).Append(":T").Append(tid).Append(dots);
                sb.Length = size;
                var payload = Encoding.ASCII.GetBytes(sb.ToString());
                for (int i = 0; i < jobs; i++) {
                    q.Enqueue(payload);
                    //Console.WriteLine(sb.ToString().Substring(0, 20));
                }
            }
            //Console.WriteLine("Worker {0} finished.", task);
        }
    }
}

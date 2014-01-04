using System;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace JoqerCtl
{
    class HammerFill
    {
        public void Run(string[] args)
        {
            if (args.Length != 2) {
                Console.WriteLine("USAGE: {0} hammer [minutes (Defaults to {1})]", Environment.GetCommandLineArgs()[0].ToLowerInvariant(), 5);
                Environment.Exit(1);
            }

            int minutes = 5;
            int.TryParse(args[1], out minutes);


            Console.WriteLine("Spawning new processes for the next {0} minutes", minutes);

            Random rnd = new Random(42);
            long end = DateTime.UtcNow.AddMinutes(minutes).Ticks;
            long totalPayloads = 0;
            int proc = 0;

            while (DateTime.UtcNow.Ticks < end) {
                var w = rnd.Next(1, 10);
                var j = rnd.Next(100, 25000);
                totalPayloads += w * j;
                var parms = string.Format("/c mmfwriter.exe test {0} {1}", w, j);
                var pi = new ProcessStartInfo
                {
                    FileName = @"c:\windows\system32\cmd.exe",
                    Arguments = parms,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                };

                Process p = new Process() { StartInfo = pi };
                p.OutputDataReceived += (s, e) => Console.WriteLine(e.Data);
                p.ErrorDataReceived += (s, e) => Console.Error.WriteLine(e.Data);
                p.Start();
                p.BeginOutputReadLine();
                proc++;
                Thread.Sleep(rnd.Next(150, 500));
            }

            Console.WriteLine("Total items enqueued {0} in {1} processes", totalPayloads, proc);
            WaitForInputAndExit();
        }

        static void Fill(object o)
        {
            var tt = o as Tuple<int, int>;
            int task = tt.Item1;
            int jobs = tt.Item2;
            using (var q = JoqerQueue.Queue.OpenWriter(Controller.TestQueue, lockMode: LockMode.MultiProcess)) {
                string dots = new string(' ', 124);
                for (int i = 0; i < jobs; i++) {
                    StringBuilder sb = new StringBuilder(128);
                    int tid = Thread.CurrentThread.ManagedThreadId;
                    sb.Append('W').Append(task).Append(":J").Append(i).Append(":T").Append(tid).Append(dots);
                    sb.Length = 128;
                    q.Enqueue(Encoding.ASCII.GetBytes(sb.ToString()));
                    Console.WriteLine(sb.ToString().Substring(0, 20));
                }
            }
            //Console.WriteLine("Worker {0} finished.", task);
        }
    
        #region Helpers
        private static void WaitForInputAndExit()
        {
            Console.WriteLine("Press ENTER to quit the program");
            Console.ReadLine();
            Environment.Exit(0);
        }

        private static void WaitForInput()
        {
            Console.WriteLine("Press ENTER to continue");
            Console.ReadLine();
        }
        #endregion
    }
}

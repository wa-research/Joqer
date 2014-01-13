using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using JoqerQueue;

namespace JoqerCtl
{
    class Controller
    {
        public const string DefaultBaseFolder = @"\var\spool";
        public const string TestQueue = @"\var\spool\TestQueue";

        public const int DefaultQueueSegmentCapacityPages = 2560;
        static void Main(string[] args)
        {
            string USAGE = "USAGE: " + Environment.GetCommandLineArgs()[0].ToLower() + " " + ValidOperations + " {flags} {queueName}";

            Console.WriteLine();
            if (args.Length == 0) {
                Console.WriteLine(USAGE);
                Environment.Exit(1);
            }

            try {
                
                string operation = args[0];

                if (!IsValidOperation(operation))
                    throw new Exception("Invalid operation: " + operation);

                int i = 0;
                List<string> flags = new List<string>();

                while ((++i < args.Length) && (args[i][0] == '-')) {
                    flags.Add(args[i].Substring(1));
                }

                string name = Guid.NewGuid().ToString("n");
                if (i < args.Length) {
                    name = args[i];
                }
                new Controller(operation, name, flags.ToArray(), args);

            } catch (Exception ex) {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
                Console.WriteLine();
                Console.WriteLine(USAGE);
            }
        }

        public Controller(string operation, string fullPath, string[] flags, string[] args)
        {
            fullPath = FullPath(fullPath);

            //Capacity is expressed in pages 
            PageCount capacity = new PageCount(flags.Where(f => f[0] == 'c').Select(f => int.Parse(f.Substring(1))).FirstOrDefault());
            if (capacity.Pages == 0)
                capacity.Pages = DefaultQueueSegmentCapacityPages;

            // Maximum segments to use in circular mode
            short segments = flags.Where(f => f[0] == 'g').Select(f => short.Parse(f.Substring(1))).FirstOrDefault();
            // Number of data segments to write ahead
            short writeAhead = flags.Where(f => f[0] == 'a').Select(f => short.Parse(f.Substring(1))).FirstOrDefault();

            QueueOptions opt = flags.Where(f => f[0] == 'o').Select(f => ParseOptions(f.Substring(1))).FirstOrDefault();

            if (operation == "create") {
                var q = CreateQueue(fullPath, capacity, opt, segments, writeAhead);
                Console.WriteLine("Created queue {0}", q);
            } else if (operation == "info") {
                new QueueInfo().Print(fullPath);
            } else if (operation == "reset") {
                DeleteQueue(fullPath);
                var q = CreateQueue(fullPath, capacity, opt, segments, writeAhead);
                Console.WriteLine("Re-set queue {0}", q);
            } else if (operation == "read") {
                var readloop = new ContinuousReader(Queue.Open(TestQueue));
                readloop.Start();
                WaitForInputAndExit();
            } else if (operation == "readone") {
                new QuickDump().ReadOne(fullPath);
            } else if (operation == "readall") {
                new QuickDump().ReadAll(fullPath);
            } else if (operation == "test") {
                new TestFill(TestQueue).Run(args, flags);
            } else if (operation == "hammer") {
                new HammerFill().Run(args);
            }
        }

        private QueueOptions ParseOptions(string p)
        {
            QueueOptions o = default(QueueOptions);
            if (string.IsNullOrEmpty(p))
                return o;

            if (p.IndexOf("s", StringComparison.OrdinalIgnoreCase) > -1)
                o |= QueueOptions.StoreSizeInIndex;

            if (p.IndexOf("t", StringComparison.OrdinalIgnoreCase) > -1)
                o |= QueueOptions.StoreTimestampInIndex;

            return o;
        }

        private void DeleteQueue(string path)
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive:true);
        }

        private string FullPath(string name)
        {
            if (Path.GetFileName(name) == name)
                name = Path.Combine(DefaultBaseFolder, name);

            if (!Path.IsPathRooted(name)) {
                return Path.GetFullPath(name);
            }
            return name;
        }

        private string CreateQueue(string path, PageCount capacity, QueueOptions flags, short maxSegments = 0, short writeAhead = 0)
        {
            // Index should be sized proportionately to data file
            PageCount idxCapacity = new PageCount(capacity.Pages / 4);
            if (idxCapacity.Pages == 0)
                idxCapacity.Pages = 1;

            string fullPath = path;

            if (Directory.Exists(fullPath))
                throw new ApplicationException(string.Format("Queue directory '{0}' already exists.", fullPath));

            Directory.CreateDirectory(fullPath);

#if !MONO
            var sec = new DirectorySecurity();
            sec.AddAccessRule(new FileSystemAccessRule("Everyone", FileSystemRights.FullControl, AccessControlType.Allow));
            Directory.SetAccessControl(fullPath, sec);
#endif
            Queue.Create(path, capacity, idxCapacity, flags, maxSegments, writeAhead);

            return fullPath;
        }

        private const string ValidOperations = "create|info|reset|read|readone|readall|test|hammer";
        private static bool IsValidOperation(string operation)
        {
            return ("|" + ValidOperations).Contains("|" + operation.ToLowerInvariant());
        }
    
        private static void WaitForInputAndExit()
        {
            Console.WriteLine("Press ENTER to quit the program");
            Console.ReadLine();
            Environment.Exit(0);
        }
    }
}

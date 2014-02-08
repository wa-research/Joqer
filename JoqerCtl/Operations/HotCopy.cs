using System;
using System.IO;
using JoqerQueue;

namespace JoqerCtl
{
    class HotCopy
    {
        public void Copy(string fullPath, string[] args)
        {
            if (args == null || args.Length == 0)
                throw new ArgumentException("Please specify target path");

            if (!Directory.Exists(fullPath))
                throw new ApplicationException(string.Format("Queue directory '{0}' does not exist.", fullPath));

            Queue q = Queue.Open(fullPath);

            string lockFile = q.HeadFilePath();
            if (!File.Exists(lockFile))
                throw new ApplicationException(string.Format("Queue lock file '{0}' does not exist", lockFile));

            var h = q.Header;

            string targetFullPath = Path.GetFullPath(args[0]);
            if (!Directory.Exists(targetFullPath))
                throw new ApplicationException(string.Format("Target directory '{0}' must exist", targetFullPath));

            Console.WriteLine("{0} -> {1}", q.HeadFilePath(), Path.Combine(targetFullPath, "head"));
            File.Copy(q.HeadFilePath(), Path.Combine(targetFullPath, "head"));

            // Once we copied the head, we don't really care how much gets written in the active segment, since 
            // it is the head that points to the latest valid index entry
            // Copy all index files in order
            for (short i = q.Header.FirstValidIndexSequenceNumber.FileNumber; i <= q.Header.ActiveIndexFile; i++) {
                string p = q.IndexSegmentFilePath(i);
                File.Copy(p, Path.Combine(targetFullPath, Path.GetFileName(p)));
            }

            for (short i = q.Header.FirstValidDataSequenceNumber.FileNumber; i <= q.Header.ActiveDataFile; i++) {
                string p = q.DataSegmentFilePath(i);
                File.Copy(p, Path.Combine(targetFullPath, Path.GetFileName(p)));
            }
        }
    }
}

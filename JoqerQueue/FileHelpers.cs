using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Security.AccessControl;
using System.Threading;

namespace JoqerQueue
{
    public static class FileHelpers
    {
        private static TimeSpan _delay = TimeSpan.FromMilliseconds(5);

        public static MemoryMappedFile OpenOrCreateSegment(string path, PageCount pages)
        {
            MemoryMappedFile mmf = null;

            // Local map seems to be a better choice
            mmf = CreateFileAndMap(path, pages, LocalName(path));
            if (mmf != null)
                return mmf;

            for (int retries = 0; retries < 40; retries++) {
                if (!File.Exists(path)) {
                    try {
                        mmf = CreateFileAndMap(path, pages, GlobalName(path))
                            ?? CreateFileAndMap(path, pages, LocalName(path));
                        break;
                    } catch (IOException) { }
                } else {
                    mmf = OpenMmf(path);
                    if (mmf != null)
                        break;
                }
                RelaxForAWhile(retries);
            }

            return mmf;
        } 

        private static void RelaxForAWhile(int attempt)
        {
            var d = (double)(Math.Abs(attempt) + 1);

            var duration = Math.Round(250 * Math.Log10(d));
            Thread.Sleep(TimeSpan.FromMilliseconds(duration));
        }

        /// <summary>
        /// Opens a memory-mapped queue file for writing
        /// </summary>
        /// <param name="path">File system path to an individual queue file to access</param>
        /// <returns></returns>
        public static MemoryMappedFile OpenMmf(string path)
        {
            MemoryMappedFile mmf = 
                OpenExisting(GlobalName(path))
                ?? OpenExisting(LocalName(path))
                ?? CreateFromFile(path, GlobalName(path))
                ?? CreateFromFile(path, LocalName(path), retryOnAccessDenied: true);

            return mmf;
        }

        private static MemoryMappedFile OpenExisting(string name)
        {
            try {
                return MemoryMappedFile.OpenExisting(name);
            } catch (FileNotFoundException) { }
            return null;
        }

        //Tries to open from file and retries to ensure we are not getting 'The file is open by another process' 
        //when another process is about to release the file
        public static MemoryMappedFile CreateFromFile(string path, string name, bool retryOnAccessDenied = false)
        {
            MemoryMappedFileSecurity security = new MemoryMappedFileSecurity();
            security.AddAccessRule(new AccessRule<MemoryMappedFileRights>("Everyone", MemoryMappedFileRights.ReadWrite, AccessControlType.Allow));

            for (var retries = 0; retries < 40; retries++) {
                try {
                    // Opening with FileShare.ReadWrite will allow processes to map the file into the local session if they can't access Globally scoped map.
                    // The OS is happy to let us have multiple maps of the same file. As we only write to the queue through memory map, never directly to the file, this should be safe, 
                    // and it seems it maps the same memory, only with session-private page pointers.
                    FileStream fs = new FileStream(path, FileMode.Open, FileSystemRights.ReadData | FileSystemRights.WriteData, FileShare.ReadWrite, 0x1000, FileOptions.None);
                    return MemoryMappedFile.CreateFromFile(fs, name, 0, MemoryMappedFileAccess.ReadWrite, security, HandleInheritability.None, leaveOpen: false);
                } catch (FileNotFoundException) {
                    break;
                } catch (ArgumentException) {
                    // Sometimes, we miss a file just create in another thread and the file system reports file size 0, which causes the 'CreateFromFile with Capacity=0 to fail
                    // This is a race condition and should not happen
                    // TODO: Investigate locking
                    var mmf = OpenExisting(name);
                    if (mmf != null)
                        return mmf;
                } catch (IOException) {
                } catch (UnauthorizedAccessException) {
                    var mmf = OpenExisting(name);
                    if (mmf != null)
                        return mmf;
                    if (!retryOnAccessDenied)
                        break;
                }
                RelaxForAWhile(retries);
            }
            return null;
        }

        public static MemoryMappedFile CreateFileAndMap(string path, PageCount pages, string name)
        {
            string fullPath = Path.GetFullPath(path);

            MemoryMappedFileSecurity security = new MemoryMappedFileSecurity();
            security.AddAccessRule(new AccessRule<MemoryMappedFileRights>("Everyone", MemoryMappedFileRights.ReadWrite, AccessControlType.Allow));

            var sec = new FileSecurity();
            sec.AddAccessRule(new FileSystemAccessRule("Everyone", FileSystemRights.FullControl, AccessControlType.Allow));

            if (!File.Exists(fullPath)) {
                using (var gl = new GlobalLock(path)) {
                    if (!File.Exists(fullPath)) {
                        try {
                            FileStream fs = new FileStream(fullPath, FileMode.CreateNew, FileSystemRights.ReadData | FileSystemRights.WriteData, FileShare.ReadWrite, 0x1000, FileOptions.None, sec);
                            return MemoryMappedFile.CreateFromFile(fs, name, (long)pages.Bytes, MemoryMappedFileAccess.ReadWrite, security, HandleInheritability.None, leaveOpen: false);
                        } catch (UnauthorizedAccessException) {
                        }
                    }
                }
            }
            // Maybe the file got created in the meantime by another thread
            return OpenExisting(name);
        }

        public static string GlobalName(string name)
        {
            return @"Global\" + name.Replace("\\", "_");
        }

        public static string LocalName(string name)
        {
            return @"Local\" + name.Replace("\\", "_");
        }

    }
}

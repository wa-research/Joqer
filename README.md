Joqer
=====

A simple and fast serverless queue. Production ready.

Notable features:

* Persistent, close to bandwidth speed.
* Accepts concurrent writes from multiple threads and processes, without a dedicated server process.
* CLS compliant, should work in Medium trust.
* Enqueues very large messages (max size on disk 2^32 per message, however, a single message must fit into process memory space).
* Queue sizes only limited by availalbe disk space.
* Multiple lock-free concurrent readers.
* Fast reader catch-up: a reader can read all messages until the queue head in a loop, doesn't need to pop each message separately.

Production ready.

Try 
===

1. Create a queue folder with `JoqerCtl`:

        JoqerCtl create c:\var\spool\TestQueue

2. Test it with 10 parallel threads, multiple process lock, 5000 messages of 100k each:

        c:\Joqer> JoqerCtl test -w10 -j5000 -s102400 -mp
        Starting new test with 10 workers enqueieing 5000 payloads of 102400 bytes each in lock mode MultiProcess

        Enqueued 50000 jobs in 85204 ms at 586.83 ops per second or 1704.08 usec per op (17040.96 ticks per op)
    
That's 58.6 MB/sec on a virtual machine!

Single process mode is still thread safe and writes at 63 MB/sec on a virtual machine (Windows 7/32-bit on Parallels 9, 2 virtual cores on MacBook Air i7):

        c:\Joqer> JoqerCtl test -w10 -j5000 -s1024 -lp
        Starting new test with 10 workers enqueieing 5000 payloads of 1024 bytes each in lock mode SingleProcess
        
        Enqueued 50000 jobs in 793 ms at 63051.7 ops per second or 15.86 usec per op (158.6284 ticks per op)

Use
===

1. Create a queue folder with `JoqerCtl`:

        JoqerCtl create c:\var\spool\JobQueue
    
2. Write to queue:

        byte[] payload = JobToEnqueue();
        using (var q = Queue.OpenWriter(@"c:\var\spool\JobQueue", lockMode: LockMode.MultiProcess)) {
            q.Enqueue(payload);
        }
    
3. Read one message with persistent progress (reader records its progress and restarts from last record read):

        using (var q = Queue.OpenReader(queuePath)) {
            byte[] b = q.DequeueOne();
            if (b != null)
                ProcessMessage(b);
            else
                Console.WriteLine("NO DATA");
        }
    
4. Read continuously waiting for new messages:

        public void Read(string queuePath)
        {
            int i = 0;
            using (var q = Queue.OpenReader(queuePath).GetReader()) {
                q.Message += (s, e) => {
                    ProcessMessage(e);
                };
                q.StartLoop();
            }
        }
    
The read loop will rapidly iterate through all messages until it reaches the queue head, and will then poll every 1 ms for new messages, again catching up in burst. Progress is written in the queue header, allowing for reader restarts.

JoqerCtl
========

A queue must be initialized before use. JoqerCtl is a controller app to create, reset, delete, and test queues. 

Once a queue is created, it's options cannot be changed.

      JoqerCtl create [options] {queuePath}
      JoqerCtl reset [options] {queuePath}
      
      -c    segment capacity in memory pages (1 page = 4096 bytes; default 2560 pages or 10MB per segment)
            a message cannot span multiple segments; if large messages need to be stored, pass -c125000 or larger
            
      -o    options
              s - store payload size in index
              t - store timestamp in index (UTC ticks)
              
      -g    maximum number of queue segments to use in circular mode (defaults to unbounded)
      

      

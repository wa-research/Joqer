Joqer
=====

A simple and fast persistent serverless queue.

Notable features:

* Accepts concurrent writes from multiple threads and processes, without a dedicated server process.
* Persistent messages, written close to disk bandwidth speed.
* High-throughput at memory speeds.
* Enqueues very large messages (limited by process memory space).
* Queue sizes only limited by available disk space.
* Multiple lock-free concurrent readers.
* Fast reader catch-up: a reader reads all messages until the queue head in a loop, doesn't need to pop each message separately.
* CLS compliant, using either pure .NET (theoretically should work in Medium trust) or native memory access for blazing throughput.
* Production ready.

Great for...
=========

* Background job queues
* Binary logging and log forwarding
* Application event queue

Try 
===

1. Create a queue folder with `JoqerCtl`:

        JoqerCtl create c:\var\spool\TestQueue

2. Test it with 10 parallel workers, multiple process lock, 5000 messages of 100k each, on a virtual machine (Windows 7/32-bit on Parallels 9, 2 virtual cores on MacBook Air i7):

        c:\Joqer> JoqerCtl test -w10 -j5000 -s102400
        
        Starting new test with 10 workers writing 5000 messages of 102400 bytes each in lock mode MultiProcess

        Enqueued 4882.813 MB in 25467 msec at 191.73 MB/sec
              or 50000 messages at 1963.33 ops per second or 509.34 usec per op (5093.507 ticks per op) on average
    
That's 191.7 MB/sec throughput on a virtual machine, muliple writer processes and no server needed!

Single process mode writes at 265 MB/sec:

        c:\Joqer> JoqerCtl test -w10 -j5000 -s102400 -lm

        Starting new test with 10 workers writing 5000 messages of 102400 bytes each in lock mode SingleProcess

        Enqueued 4882.813 MB in 18370 msec at 265.8 MB/sec
              or 50000 messages at 2721.83 ops per second or 367.4 usec per op (3674.039 ticks per op) on average

Read it back at ~180 MB/sec:

        C:\Joqer> JoqerCtl readall TestQueue

        Read 4882.813 MB in 26172 msec at 186.57 MB/sec
          or 50000 messages at 1910.44 ops per second, 523.44 usec per op on average

Use
===

1. Create a queue folder with `JoqerCtl`:

        JoqerCtl create c:\var\spool\JobQueue
    
2. Write to queue:

        byte[] payload = JobToEnqueue();
        using (var q = Queue.OpenWriter(@"c:\var\spool\JobQueue", lockMode: LockMode.MultiProcess)) {
            q.Enqueue(payload);
        }
    
3. Read one message with persistent progress (reader records its progress and continues from last record read after process restart):

        using (var q = Queue.OpenReader(queuePath)) {
            byte[] b = q.DequeueOne();
            if (b != null)
                ProcessMessage(b);
            else
                Console.WriteLine("NO DATA");
        }
    
4. Read continuously:

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
    
The read loop will rapidly iterate through all messages until it reaches the queue head, and will then poll every 150 ms for new messages, again catching up in burst. Progress is written in the queue header, allowing for reader restarts.

JoqerCtl
========

A queue must be initialized before use. `JoqerCtl` is a controller app to create, reset, delete, and test queues. 

Once a queue is created, it's options cannot be changed.

      JoqerCtl create [options] {queuePath}
      JoqerCtl reset [options] {queuePath}
      
      -c    segment capacity in memory pages (1 page = 4096 bytes; default 2560 pages or 10MB per segment)
            a message cannot span multiple segments; if large messages need to be stored, pass -c125000 or larger
            
      -o    options
              s - store payload size in index
              t - store timestamp in index (UTC ticks)
              
      -g    maximum number of queue segments to use in circular mode (defaults to unbounded)
      

      

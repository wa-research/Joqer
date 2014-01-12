using System;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Threading;

namespace JoqerQueue
{
    // From: http://stackoverflow.com/a/7810107
    class GlobalLock : IDisposable
    {
        public bool _hasHandle = false;
        Mutex _mutex;
        static MutexSecurity _security = new MutexSecurity();

        static GlobalLock()
        {
            MutexAccessRule _rule = new MutexAccessRule(new SecurityIdentifier(WellKnownSidType.WorldSid, null), MutexRights.FullControl, AccessControlType.Allow);
            _security.AddAccessRule(_rule);
        } 

        public GlobalLock(string name, int timeOut = 5000)
        {
            InitMutex(name);
            try {
                var t = (timeOut <= 0) ? Timeout.Infinite : timeOut;

				_hasHandle = _mutex.WaitOne(t, false);
                if (_hasHandle == false) {
                    throw new TimeoutException("Could not acquire exclusive access on mutex " + name);
                }
            } catch (AbandonedMutexException) {
                _hasHandle = true;
            }
        }

        private static TimeSpan _delay = new TimeSpan(500);

        private void InitMutex(string name)
        {

            int retries = 20;
            bool isnew = false;
            while (retries > 0) {
                try {
                    _mutex = new Mutex(false, @"Global\Mutex_" + name.Replace("\\", "_").Replace(':','_').Replace('/','_'), out isnew, _security);
                    break;
                } catch (UnauthorizedAccessException) {
                    Thread.Sleep(_delay);
                    retries--;
                }
            }
        }

        public void Dispose()
        {
            if (_mutex != null) {
                if (_hasHandle)
                    _mutex.ReleaseMutex();
                _mutex.Dispose();
            }
        }
    }
}

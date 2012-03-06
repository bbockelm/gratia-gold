

"""
This library for POSIX-style locking was adopted from earlier work in Gratia.
"""

import os
import sys
import time
import errno
import fcntl
import atexit
import signal
import struct
import logging

fd = None

# Record the PID that initially took the lock, to prevent unlinking
# when a fork'ed child exits.
pid_with_lock = None
def close_and_unlink_lock():
    global fd
    global pid_with_lock
    if fd:
        if pid_with_lock == os.getpid():
            try:
                os.unlink(fd.name)
            except OSError, oe:
                # If we dropped privileges, we can't unlink!
                if oe.errno != errno.EACCES:
                    raise
        os.ftruncate(fd.fileno(), 0)
        fd.close()
atexit.register(close_and_unlink_lock)

log = logging.getLogger("gratia_gold.locking")

def exclusive_lock(lock_location, timeout=3600):
    """
    Grabs an exclusive lock on lock_location

    If the lock is owned by another process, and that process is older than the
    timeout, then the other process will be signaled.  If the timeout is
    negative, then the other process is never signaled.

    If we are unable to hold the lock, this call will not block on the lock;
    rather, it will throw an exception.
    """

    lock_location = os.path.abspath(lock_location)
    lockdir = os.path.dirname(lock_location)
    if not os.path.isdir(lockdir):
        raise Exception("Lock is to be created in a directory %s which does "
            "not exist." % lockdir)
    log.debug("Trying to acquire lock %s." % lock_location)

    global fd
    global pid_with_lock
    fd = open(lock_location, "w")

    # POSIX file locking is cruelly crude.  There's nothing to do besides
    # try / sleep to grab the lock, no equivalent of polling.
    # Why hello, thundering herd.

    # An alternate would be to block on the lock, and use signals to interupt.
    # This would mess up Gratia's flawed use of signals already, and not be
    # able to report on who has the lock.  I don't like indefinite waits!
    max_tries = 5
    for tries in range(1, max_tries+1):
        try:
            fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fd.write("%d" % os.getpid())
            pid_with_lock = os.getpid()
            fd.flush()
            log.debug("Successfully acquired lock %s." % lock_location)
            return
        except IOError, ie:
            if not ((ie.errno == errno.EACCES) or (ie.errno == errno.EAGAIN)):
                raise
            if check_lock(fd, timeout):
                time.sleep(.2) # Fast case; however, we have *no clue* how
                               # long it takes to clean/release the old lock.
                               # Nor do we know if we'd get it if we did
                               # fcntl.lockf w/ blocking immediately.  Blech.
                # Check again immediately, especially if this was the last
                # iteration in the for loop.
                try:
                    fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fd.write("%d" % os.getpid())
                    pid_with_lock = os.getpid()
                    fd.flush()
                    return
                except IOError, ie:
                    if not ((ie.errno == errno.EACCES) or (ie.errno == errno.EAGAIN)):
                        raise
        fd.close()
        fd = open(lock_location, "w")
        log.warning("Unable to acquire lock, try %i; will sleep for %i "
            "seconds and try %i more times." % (tries, tries, max_tries-tries))
        time.sleep(tries)

    raise Exception("Unable to acquire lock")

def check_lock(fd, timeout):
    """
    For internal use only.

    Given a fd that is locked, determine which process has the lock.
    Kill said process if it is older than "timeout" seconds.
    This will log the PID of the "other process".
    """

    pid = get_lock_pid(fd)
    if pid == os.getpid():
        return True

    if timeout < 0:
        log.warning("Another process, %d, holds the probe lockfile." % pid)
        return False

    try:
        age = get_pid_age(pid)
    except:
        log.warning("Another process, %d, holds the probe lockfile." % pid)
        log.error("Unable to get the other process's age; will not time "
            "it out.")
        return False

    log.warning("Another process, %d (age %d seconds), holds the probe "
        "lockfile." % (pid, age))

    if age > timeout:
        log.warning("Killing old process with lockfile: %d" % pid)
        os.kill(pid, signal.SIGKILL)
    else:
        return False

    return True

linux_struct_flock = "hhxxxxqqixxxx"
try:
    os.O_LARGEFILE
except AttributeError:
    start_len = "hhlli"

def get_lock_pid(fd):
    # For reference, here's the definition of struct flock on Linux
    # (/usr/include/bits/fcntl.h).
    #
    # struct flock
    # {
    #   short int l_type;   /* Type of lock: F_RDLCK, F_WRLCK, or F_UNLCK.  */
    #   short int l_whence; /* Where `l_start' is relative to (like `lseek').  */
    #   __off_t l_start;    /* Offset where the lock begins.  */
    #   __off_t l_len;      /* Size of the locked area; zero means until EOF.  */
    #   __pid_t l_pid;      /* Process holding the lock.  */
    # };
    #
    # Note that things are different on Darwin
    # Assuming off_t is unsigned long long, pid_t is int
    try:
        if sys.platform == "darwin":
            arg = struct.pack("QQihh", 0, 0, 0, fcntl.F_WRLCK, 0)
        else:
            arg = struct.pack(linux_struct_flock, fcntl.F_WRLCK, 0, 0, 0, 0)
        result = fcntl.fcntl(fd, fcntl.F_GETLK, arg)
    except IOError, ie:
        if ie.errno != errno.EINVAL:
            raise
        DebugPrint(0, "Unable to determine which PID has the lock due to a "
            "python portability failure.  Contact the developers with your"
            " platform information for support.")
        return False
    if sys.platform == "darwin":
        _, _, pid, _, _ = struct.unpack("QQihh", result)
    else:
        _, _, _, _, pid = struct.unpack(linux_struct_flock, result)
    return pid

def get_pid_age(pid):
    now = time.time()
    st = os.stat("/proc/%d" % pid)
    return now - st.st_ctime

if __name__ == "__main__":
    # Quick test of the locking capabilities.

    ExclusiveLock("lock_test")
    if os.fork() == 0:
        ExclusiveLock("lock_test")
        print "Child got the lock.  Sleep 5, then exit"
        time.sleep(5)
        os._exit(0)
    print "Parent got the lock.  Sleep 5, then exit"
    time.sleep(5)



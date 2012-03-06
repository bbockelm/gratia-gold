
"""
Module for interacting with Gold

Takes a summarized Gratia job and either charges or refunds it.
"""

import os
import pwd
import errno
import logging

log = logging.getLogger("gratia_gold.gold")

def setup_env(cp):
    gold_home = cp.get("gold", "home")
    if not os.path.exists(gold_home):
        raise Exception("GOLD_HOME %s does not exist!" % gold_home)
    os.environ['GOLD_HOME'] = gold_home
    paths = os.environ['PATH'].split(";")
    paths.append(os.path.join(gold_home, "bin"))
    paths.append(os.path.join(gold_home, "sbin"))
    os.environ['PATH'] = ";".join(paths)
    
def drop_privs(cp):
    gold_user = cp.get("gold", "username")
    pw_info = pwd.getpwnam(gold_user)
    try:
        os.setgid(pw_info.pw_gid)
        os.setuid(pw_info.pw_uid)
    except OSError, oe:
        if oe.errno != errno.EPERM:
            raise
        log.warn("Unable to drop privileges to %s - continuing" % gold_user)

def call_gcharge(job):
    args = []
    args += ["-u", job['user']
    if job['project_name']:
        args += ["-p", job['project_name']]
    args += ["-m", job["machine_name"]]
    if job['processors']:
        args += ["-P", job['processors']]
    args += ["-N", job["node_count"]]
    args += ["-e", job["endtime"]]
    raise NotImplementedError()
    pid = os.fork()
    fd = open(logfile, "w")
    if pid == 0:
        os.close(fd.fileno, 1)
        os.close(fd.fileno, 2)
        os.execv("gcharge", args)
    pid2 = 0
    while pid != pid2:
        pid2, status = os.wait()
    return status

def refund(cp, job):
    raise NotImplementedError()
    pid = os.fork()
    fd = open(logfile, "w")
    if pid == 0:
        os.close(fd.fileno, 1)
        os.close(fd.fileno, 2)
        os.execv("grefund", args)
    pid2 = 0
    while pid != pid2:
        pid2, status = os.wait()
    return status



"""
Module for interacting with Gold

Takes a summarized Gratia job and either charges or refunds it.
"""

import os

def call_gcharge(job):
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


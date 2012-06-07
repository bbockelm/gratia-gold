
"""
Module for interacting with Gold

Takes a summarized Gratia job and either charges or refunds it.
"""

import os
import pwd
import errno
import logging
from datetime import datetime, timedelta

#log = logging.getLogger("gratia_gold.gold")

def setup_env(cp):
    gold_home = cp.get("gold", "home")
    if not os.path.exists(gold_home):
        raise Exception("GOLD_HOME %s does not exist!" % gold_home)
    os.environ['GOLD_HOME'] = gold_home
    paths = os.environ['PATH'].split(";")
    paths.append(os.path.join(gold_home, "bin"))
    paths.append(os.path.join(gold_home, "sbin"))
    # join the elements in paths by ;
    os.environ['PATH'] = ";".join(paths)
    
def drop_privs(cp):
    gold_user = cp.get("gold", "username")
    pw_info = pwd.getpwnam(gold_user)
    try:
        os.setgid(pw_info.pw_gid)
        os.setuid(pw_info.pw_uid)
    except OSError, oe:
        # errno.EPERM (Operation not permitted)
        if oe.errno != errno.EPERM:
            raise
        log.warn("Unable to drop privileges to %s - continuing" % gold_user)


def get_digits_from_a_string(string1):
    '''
    The number of processors or node_count sometimes shows 1L or None.
    This function only read digits from a given string,
    and return the corresponding number in a string format.
    For example, 1L will return "1".
    None will return "1". 
    123L will return "123".
    '''
    if string1 is None:
        return "1"
    if (type(string1) is int) or (type(string1) is long):
        return str(int(string1))
    digitsofstring1 = ""
    for i in range(len(string1)):
        if string1[i]>='0' and string1[i]<='9':
            digitsofstring1 += string1[i]
    if digitsofstring1 == "":
        numberofstring1 = "1"
    else:
        numberofstring1 = digitsofstring1
    return numberofstring1


def call_gcharge(job, log):
    '''
    Modified by Yaling Zheng
    job has the following information 
    dbid, resource_type, vo_name, user, charge, wall_duration, cpu, node_count, njobs, 
    processors, endtime, machine_name, project_name

    2012-05-09 20:19:46 UTC [yzheng@osg-xsede:~/mytest]$ gcharge -h
    Usage:
    gcharge [-u user_name] [-p project_name] [-m machine_name] [-C
    queue_name] [-Q quality_of_service] [-P processors] [-N nodes] [-M
    memory] [-D disk] [-S job_state] [-n job_name] [--application
    application] [--executable executable] [-t charge_duration] [-s
    charge_start_time] [-e charge_end_time] [-T job_type] [-d
    charge_description] [--incremental] [-X | --extension property=value]*
    [--debug] [-?, --help] [--man] [--quiet] [-v, --verbose] [-V, --version]
    [[-j] gold_job_id] [-q quote_id] [-r reservation_id] {-J job_id}
    '''
    args = ["gcharge"]
    # force the user name to be yzheng
    if job['user']:
        args += ["-u", job['user']]
    if job['project_name']:
        args += ["-p", job['project_name']]
    # force the project name to be OSG-Staff
    # job['project_name'] = "OSG-Staff"
    # force the machine name to be grid1.osg.xsede
    job['machine_name'] = "grid1.osg.xsede"
    args += ["-m", job['machine_name']]
    
    originalnumprocessors = job['processors']
    job['processors'] = get_digits_from_a_string(originalnumprocessors)
    args += ["-P", job['processors']]
    
    originalnodecount = job['node_count']
    job['node_count'] = get_digits_from_a_string(originalnodecount)
    args += ["-N", job['node_count']]
    # if there is no endtime, force the end time to be the day after tomorrow
    if job['endtime'] is None:
        today = datetime.today()
        dt = datetime(today.year, today.month, today.day, today.hour, today.minute, today.second)
        job['endtime'] = str(dt+timedelta(1,0)) # now + 24 hours
    args += ["-e", job['endtime']]
    if job['dbid']:
        args += ["-J", str(job['dbid'])]
    
    # [-t charge_duration]
    # 'charge' is a must option
    if job['charge'] is None:
        job['charge'] = "3600" # default 3600 seconds, which is 1 hour
    args += ["-t", job['charge']]
    log.debug("gcharge " + str(args))
    pid = os.fork()
    gchargestatus = 0
    if pid==0:
        try:
            os.execvp("gcharge", args)
        except:
            log.debug("job charge failed ... \n")
            log.error("job charge failed ... \n")
            gchargestatus = -1
    pid2 = 0
    while pid != pid2:
        pid2, status = os.wait()
    if gchargestatus == 0:
        log.debug("job charge succeed ... \n")
    return gchargestatus


def refund(cp, job, log):
    args = ["grefund"]
    args += ["-J", job["dbid"]]
    log.debug("grefund "+ str(args))
    pid = os.fork()
    grefundstatus = 0
    if pid == 0:
        try:
            os.execvp("grefund", args)
        except:
            log.debug("job refund failed ... \n")
            log.error("job refund failed ...\n")
            grefundstatus = -1
    pid2 = 0
    while pid != pid2:
        pid2, status = os.wait()
    if grefundstatus == 0:
        log.debug("job refund succeed ... \n")
    return grefundstatus


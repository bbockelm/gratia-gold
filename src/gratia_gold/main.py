
"""
Library for synchronizing Gratia job accounting with GOLD.

This library will connect to the Gratia database, summarize the accounting
records it finds, and submit the summaries to GOLD via a command-line script.

"""

import os
import logging
import optparse
import ConfigParser

import gold
import gratia
import transaction

log = None
logfile = None

def parse_opts():

    parser = optparse.OptionParser()
    parser.add_option("-c", "--config", dest="config",
        help="Location of the configuration file.",
        default="/etc/gratia-gold.cfg")

    opts, args = parser.parse_args()

    if not os.path.exists(opts.config):
        raise Exception("Configuration file, %s, does not exist." % \
            opts.config)

    return opts, args

def config_logging(cp):
    global log
    global logfile
    log = logging.getLogger("gratia")
    logfile = "/var/log/gratia-gold/gratia-gold.log"

def call_gcharge(job):

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

def stop_txn(cp, txn):
    txn_file = cp.get("transaction", "last_successful_id")
    txn_fp = open(txn_file, "w")
    simplejson.dump(txn, txn_fp)
    txn_fp.fsync()
    txn_fp.close()

def _add_if_exists(cp, attribute, info):
    """
    If section.attribute exists in the config file, add its value into a
    dictionary.

    @param cp: ConfigParser object representing our config
    @param attribute: Attribute in section.
    @param info: Dictionary that we add data into.
    """
    try:
        info[attribute] = cp.get("gratia", attribute)
    except:
        pass

def query_gratia(cp, txn):
    info = {}
    _add_if_exists(cp, section, "user", info)
    _add_if_exists(cp, section, "passwd", info)
    _add_if_exists(cp, section, "db", info)
    _add_if_exists(cp, section, "host", info)
    _add_if_exists(cp, section, "port", info)
    if 'port' in info:
        info['port'] = int(info['port'])

    conn = MySQLdb.connect(**info)
    curs = conn.cursor()

    jobs = curs.execute(GRATIA_QUERY, txn)

def summarize_gratia(cp, txn):
    raise NotImplementedError()

def refund(cp, job):
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
 

def check_rollback(cp):
    """
    Read the rollback log, and rollback any pending charges.
    """
    rollback_file = cp.get("transaction", "rollback")
    if not os.path.exists(rollback_file) or not os.access(rollback_file,
            os.R_OK):
        return open(rollback_file, "w")
        
    refund_file = "%s.refund" % rollback_file
    try:
        refund_fd = open(rollback_file, "r")
    except IOError, ie:
        if ie.errno != 2:
            raise
        refund_fd = None
    count = 0
    if refund_fd:
        for line in refund_fd.readlines():
            count += 1
    rollback_fd = open(rollback_file, "r")
    for line in rollback_fd.readlines():
        refund_fd.write(line)
        refund_fd.fsync()
        line = line.strip()
        md5sum, job = line.split(":",1)
        md5sum2 = md5.md5(job).hash_digest()
        if md5sum != md5sum2:
            raise Exception("Rollback log doesn't match md5sum (%s!=%s): %s" \
                % (md5sum, md5sum2, line))
        job_dict = simplejson.loads(job)
        refund(cp, job_dict)
    rollback_fd.close()
    os.unlink(rollback_file)
    return open(rollback_file, "w")

def add_rollback(fd, job):
    job_str = str(job)
    if job_str.split("\n") > 1:
        raise Exception("Job description contains newline")
    digest = md5.md5(job_str).hash_digest
    fd.write("%s:%s\n" % (digest, job_str))
    fd.fsync()

def main():

    opts, args = parse_opts()

    cp = ConfigParser.ConfigParser()
    cp.read(opts.config)

    config_logging(cp)

    job_count = 0
    while job_count == 0:

        txn = transaction.start_txn(cp)
        print txn

        roll_fd = transaction.check_rollback(cp)

        jobs = gratia.query_gratia(cp, txn)

        for job in jobs:
            print job

        processed_jobs = {}
        max_id = 0
        for job in jobs:
        # Record the job into rollback log.  We write it in before we call
        # gcharge - this way, if the script is killed unexpectedly, we'll
        # refund the job.  So, this errs on the conservative side.
            transaction.add_rollback(roll_fd, job)
            status = gold.call_gcharge(job)
            if status != 0:
                roll_fd.close()
                transaction.check_rollback(cp)
                log.error("Job charging failed.")
                return 1
            job_count += 1

            # Keep track of the max ID
            if job['dbid'] > max_id:
                max_id = job['dbid']+1

        if job_count == 0:
            max_id = txn['last_dbid'] + gratia.MAX_ID

        txn['last_dbid'] = max_id
        transaction.commit_txn(cp, txn)

    return 0


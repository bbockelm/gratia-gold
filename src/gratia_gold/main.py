
"""
Library for synchronizing Gratia job accounting with GOLD.

This library will connect to the Gratia database, summarize the accounting
records it finds, and submit the summaries to GOLD via a command-line script.

"""

import os
import time
import random
import logging
import optparse
import ConfigParser

import gold
import gratia
import locking
import transaction

log = None
logfile = None
logfile_handler = None

def parse_opts():

    parser = optparse.OptionParser(conflict_handler="resolve")
    parser.add_option("-c", "--config", dest="config",
                      help="Location of the configuration file.",
                      default="/etc/gratia-gold.cfg")
    parser.add_option("-v", "--verbose", dest="verbose",
                      default=False, action="store_true",
                      help="Increase verbosity.")
    parser.add_option("-s", "--cron", dest="cron",
                      type="int", default=0,
                      help = "Called from cron; cron interval (adds a random sleep)")
    
    opts, args = parser.parse_args()

    if not os.path.exists(opts.config):
        raise Exception("Configuration file, %s, does not exist." % \
            opts.config)

    return opts, args


def config_logging(cp, opts):
    global log
    global logfile
    # return a logger with the specified name gratia_gold
    log = logging.getLogger("gratia_gold")

    # log to the console
    # no stream is specified, so sys.stderr will be used for logging output
    console_handler = logging.StreamHandler()

    # Log to file, default is /var/log/gratia-gold/gratia-gold.cfg
    logfile = cp.get("logging", "file")

    logfile_handler = logging.FileHandler(logfile)

    # default log level - make logger/console match
    # Logging messages which are less severe than logging.WARNING will be ignored
    log.setLevel(logging.WARNING)
    console_handler.setLevel(logging.WARNING)
    logfile_handler.setLevel(logging.WARNING)

    if opts.verbose: 
        log.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.DEBUG)
        logfile_handler.setLevel(logging.DEBUG)

    # formatter
    formatter = logging.Formatter("[%(process)d] %(asctime)s %(levelname)7s:  %(message)s")
    console_handler.setFormatter(formatter)
    logfile_handler.setFormatter(formatter)
    if opts.cron == 0:
        log.addHandler(console_handler)
    log.addHandler(logfile_handler)
    log.debug("Logger has been configured")


def main():
    opts, args = parse_opts()
    cp = ConfigParser.ConfigParser()
    cp.read(opts.config)
    config_logging(cp, opts)

    if opts.cron > 0:
        random_sleep = random.randint(1, opts.cron)
        log.info("gratia-gold called from cron; sleeping for %d seconds." % \
            random_sleep)
        time.sleep(random_sleep)

    lockfile = cp.get("transaction", "lockfile")
    locking.exclusive_lock(lockfile)

    gold.drop_privs(cp)
    gold.setup_env(cp)
    
    # read min_dbid and max_dbid from the gratia database and
    # also save max(min_dbid, last_successful_id) into the file last_successful_id 
    (min_dbid, max_dbid) = gratia.initialize_txn(cp)
    log.debug("min_dbid is "+ str(min_dbid) + " max_dbid is "+str(max_dbid))
    curr_txn = transaction.start_txn(cp)
    curr_txt_id = curr_txn['last_successful_id'] 
    
    curr_dbid = min_dbid

    if (curr_txt_id < min_dbid):
        curr_txt['last_successful_id'] = min_dbid
    else:
        curr_dbid = curr_txt_id

    txn = curr_txn
    txn['last_successful_id'] = curr_dbid
    while curr_dbid <=  max_dbid:

        log.debug("Current transaction: probe=%(probename)s, DBID=%(last_successful_id)s" % txn)

        roll_fd = transaction.check_rollback(cp)

        jobs = gratia.query_gratia(cp, txn)
        
        for job in jobs:
            log.debug("Processing job: %s" % str(job))

        processed_jobs = {}
        max_id = 0
        job_count = 0
        for job in jobs:
            # Record the job into rollback log.  We write it in before we call
            # gcharge - this way, if the script is killed unexpectedly, we'll
            # refund the job.  So, this errs on the conservative side.
            transaction.add_rollback(roll_fd, job)
            status = gold.call_gcharge(job)
            if status != 0:
                #roll_fd.close()
                transaction.check_rollback(cp)
                continue
            job_count += 1

            # Keep track of the max ID
            if job['dbid'] > max_id:
                max_id = job['dbid']+1

        if job_count == 0:
            max_id = txn['last_successful_id'] + gratia.MAX_ID

        txn['last_successful_id'] = max_id
        transaction.commit_txn(cp, txn)
        curr_dbid = max_id
    return 0


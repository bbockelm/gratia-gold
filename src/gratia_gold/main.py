
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

def parse_opts():

    parser = optparse.OptionParser()
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
    log = logging.getLogger("gratia_gold")

    # log to the console
    console_handler = logging.StreamHandler()

    # Log to file
    logfile = cp.get("logging", "file")
    logfile_handler = logging.FileHandler(logfile)

    # default log level - make logger/console match
    log.setLevel(logging.WARNING)
    console_handler.setLevel(logging.WARNING)
    logfile_handler.setLevel(logging.INFO)

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

    job_count = 0
    while job_count == 0:

        txn = transaction.start_txn(cp)
        log.debug("Current transaction: probe=%(probename)s, DBID=%(last_dbid)s" % txn)

        roll_fd = transaction.check_rollback(cp)

        jobs = gratia.query_gratia(cp, txn)

        for job in jobs:
            log.debug("Processing job: %s" % str(job))

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


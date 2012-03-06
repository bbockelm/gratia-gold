
"""
Module for managing in-progress transactions.

Allows us to rollback Gratia->Gold uploads on failure.
"""

import os
import md5
import logging

import simplejson

import gold

log = logging.getLogger("gratia_gold.transaction")

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
        refund_fd = open(refund_file, "rw")
    except IOError, ie:
        if ie.errno != 2:
            raise
        refund_fd = None
    refund_count = 0
    if refund_fd:
        for line in refund_fd.readlines():
            refund_count += 1
        log.info("There are %i refunds" % refund_count)
    else:
        refund_fd = open(refund_file, "w")
    # Check for a rollback file - if there is none, there's no transaction to
    # undo; just return a new file handle.
    try:
        rollback_fd = open(rollback_file, "r")
    except IOError, ie:
        if ie.errno == 2:
            return open(rollback_file, "w")
        raise
    skip_count = 0
    # We have a rollback file.  If there's refunds already issued, skip those.
    for line in rollback_fd.readlines():
        skip_count += 1
        if skip_count <= refund_count:
            continue
        # Parse the rollback to prepare the refund
        md5sum, job = line.strip().split(":",1)
        md5sum2 = md5.md5(job).hexdigest()
        if md5sum != md5sum2:
            raise Exception("Rollback log doesn't match md5sum (%s!=%s): %s" \
                % (md5sum, md5sum2, line.strip()))
        job_dict = simplejson.loads(job)
        # Perform refund, then write it out.  We err on the side of issuing
        # too many refunds.
        gold.refund(cp, job_dict)
        refund_fd.write(line)
        os.fsync(refund_fd.fileno())
    rollback_fd.close()
    refund_fd.close()
    # We were able to rollback everything that failed - remove the records
    os.unlink(rollback_file)
    os.unlink(refund_file)
    return open(rollback_file, "w")

def add_rollback(fd, job):
    job_str = simplejson.dumps(job)
    if len(job_str.split("\n")) > 1:
        raise Exception("Job description contains newline")
    digest = md5.md5(job_str).hexdigest()
    fd.write("%s:%s\n" % (digest, job_str))
    os.fsync(fd.fileno())

def start_txn(cp):
    txn_file = cp.get("transaction", "last_successful_id")
    try:
        txn_fp = open(txn_file, "r")
    except IOError, ie:
        if ie.errno != 2:
            raise
        return {'last_dbid': 0}
    txn_obj = simplejson.load(txn_fp)
    return txn_obj

def commit_txn(cp, txn):
    txn_file = cp.get("transaction", "last_successful_id")
    txn_fp = open(txn_file, "w")
    simplejson.dump(txn, txn_fp)
    os.fsync(txn_fp.fileno())
    txn_fp.close()




"""
Module for managing in-progress transactions.

Allows us to rollback Gratia->Gold uploads on failure.
"""

import simplejson

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

def start_txn(cp):
    txn_file = cp.get("transaction", "last_successful_id")
    try:
        txn_fp = open(txn_file, "r")
    except IOError, ie:
        if ie.errno != 2:
            raise
        return {}
    txn_obj = simplejson.load(txn_fp)
    return txn_obj

def stop_txn(cp, txn):
    txn_file = cp.get("transaction", "last_successful_id")
    txn_fp = open(txn_file, "w")
    simplejson.dump(txn, txn_fp)
    txn_fp.fsync()
    txn_fp.close()



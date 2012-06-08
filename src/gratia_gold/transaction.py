
"""
Module for managing in-progress transactions.

Allows us to rollback Gratia->Gold uploads on failure.
"""

import os
import md5
import logging

import simplejson

import gold
import MySQLdb

from gratia import _add_if_exists

log = logging.getLogger("gratia_gold.transaction")

def check_rollback(cp, log):
    """
    Read the rollback log, and rollback any pending charges.
    """
    rollback_file = cp.get("transaction", "rollback")
    if not os.path.exists(rollback_file) or not os.access(rollback_file,
            os.R_OK):
        return open(rollback_file, "w")
        
    refund_file = "%s.refund" % rollback_file
    try:
        refund_fd = open(refund_file, "r+")
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
        print line
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
        gold.refund(cp, job_dict, log)
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

def initialize_txn(cp, opts, log):
    '''
    initialize the last_successful_id to be the maximum of
    the minimum dbid of the database
    and last_successful_id
    '''
    info = {}
    # _add_if_exists(cp, "user", info)
    info['user'] = opts.user
    # _add_if_exists(cp, "passwd", info)
    info['passwd'] = opts.passwd
    _add_if_exists(cp, "db", info)
    #_add_if_exists(cp, "host", info)
    info['host'] = opts.host
    # _add_if_exists(cp, "port", info)
    info['port'] = opts.port
    if 'port' in info:
        info['port'] = int(info['port'])
    try:
        db = MySQLdb.connect(**info)
    except Exception:
        log.debug("Connection to database failed ... \n")
        # print "exception"
        return 0, 0
    cursor = db.cursor()
    cursor.execute("select MIN(dbid) from JobUsageRecord");
    row = cursor.fetchone()
    minimum_dbid = int(row[0])
    log.debug("minimum_dbid: " + str(minimum_dbid))
    cursor.execute("select MAX(dbid) from JobUsageRecord");
    row = cursor.fetchone()
    maximum_dbid = int(row[0])
    log.debug("maximum_dbid: " + str(maximum_dbid))
    # now, we want to put it into the file
    # we check the file, if the file is empty, then it is the
    # the minimum dbid, otherwise, we choose 
    # to be the maximum of the "minimum dbid" and the last_successful_id in the file
    txn={}
    txn_previous = start_txn(cp, opts)
    txn['last_successful_id']=max(minimum_dbid, txn_previous['last_successful_id'])
    # txn['probename'] = cp.get("gratia", "probe")
    txn['probename'] = opts.probename
    commit_txn(cp, txn, log)
    return minimum_dbid, maximum_dbid

def start_txn(cp, opts):
    '''
    read the content of the txn file
    '''
    txn_file = cp.get("transaction", "last_successful_id")
    try:
        txn_fp = open(txn_file, "r")
        txn_obj = simplejson.load(txn_fp)
        txn_fp.close()
        return txn_obj
    except IOError, ie:
        if ie.errno != 2:
            raise
        # probename = cp.get("gratia", "probe")
        probename = opts.probename
        return {'probename':probename, 'last_successful_id': 0}

def commit_txn(cp, txn, log):
    '''
    update the txn file
    '''
    txn_file = cp.get("transaction", "last_successful_id")
    txn_fp = open(txn_file, "w")
    simplejson.dump(txn, txn_fp)
    log.debug("updating ... " + str(txn))
    os.fsync(txn_fp.fileno())
    txn_fp.close()



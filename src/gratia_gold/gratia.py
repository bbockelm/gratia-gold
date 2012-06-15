
"""
Module for interacting with Gratia.

Connects to the database,
Queries the database,
Summarizes resulting queries.
"""

import logging

import gold
import transaction

import MySQLdb

log = logging.getLogger("gratia_gold.gratia")

MAX_ID = 100000

GRATIA_QUERY = \
"""
SELECT
  max(JUR.dbid) as dbid,
  ResourceType,
  ReportableVOName,
  LocalUserId,
  sum(Charge) as Charge,
  sum(WallDuration) as WallDuration,
  sum(CpuUserDuration) as CpuUserDuration,
  sum(CpuSystemDuration) as CpuSystemDuration,
  NodeCount,
  count(Njobs) as Njobs,
  Processors,
  DATE(EndTime),
  MachineName,
  ProjectName
FROM
  JobUsageRecord JUR
JOIN
  JobUsageRecord_Meta JURM ON JUR.dbid = JURM.dbid
WHERE
  JUR.dbid >= %%(last_successful_id)s AND
  JUR.dbid < %%(last_successful_id)s + %d AND   
  ProbeName REGEXP %%(probename)s
GROUP BY
  ResourceType,
  ReportableVOName,
  LocalUserId,
  NodeCount,
  Processors,
  DATE(EndTime),
  MachineName,
  ProjectName
ORDER BY JUR.dbid ASC
LIMIT 10000
""" % MAX_ID

# %d day of month (00-31)

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
    _add_if_exists(cp, "user", info)
    _add_if_exists(cp, "passwd", info)
    _add_if_exists(cp, "db", info)
    _add_if_exists(cp, "host", info)
    _add_if_exists(cp, "port", info)
    if 'port' in info:
        info['port'] = int(info['port'])
    try:
        conn = MySQLdb.connect(**info)
        log.debug("Successfully connected to database ...")
    except:
        log.error("Failed to connect to database; and the reason is:"+ str(conn))
        raise Exception("Failed to connect to database")
    curs = conn.cursor()

    txn['probename'] = cp.get("gratia", "probe")

    results = []
    curs.execute(GRATIA_QUERY, txn)
    for row in curs.fetchall():
        info = {}
        info['dbid'] = row[0] #dbid in gratia
        info['resource_type'] = row[1] # ResourceType in gratia
        info['vo_name'] = row[2] # ReportableVOName in gratia
        info['user'] = row[3] # LocalUserId in gratia
        info['charge'] = row[4] # Charge in gratia
        info['wall_duration'] = row[5] # WallDuration in gratia
        info['cpu'] = row[6] + row[7] # CpuUserDuration + CpuSystemDuration in gratia
        info['node_count'] = row[8] # NodeCount in gratia
        info['njobs'] = row[9] # Njobs in gratia
        info['processors'] = row[10] # Processors in gratia
        info['endtime'] = row[11].strftime("%Y-%m-%d %H:%M:%S") # EndTime in gratia
        # info['machine_name'] = row[12] # MachineName in gratia
        # force the machine_name to be opts.machinename
        info['machine_name'] = cp.get("gratia", "machinename")
        info['project_name'] = row[13] # ProjectName in gratia
        results.append(info)
    return results

def initialize_txn(cp):
    '''
    initialize the last_successful_id to be the maximum of
    the minimum dbid of the database
    and last_successful_id
    '''
    info = {}
    _add_if_exists(cp, "user", info)
    _add_if_exists(cp, "passwd", info)
    _add_if_exists(cp, "db", info)
    _add_if_exists(cp, "host", info)
    _add_if_exists(cp, "port", info)
    if 'port' in info:
        info['port'] = int(info['port'])
    try:
        db = MySQLdb.connect(**info)
    except:
        log.error("Connection to database failed; and the reason is: "+str(db))
        raise Exception("Failed to connect to database.");
    cursor = db.cursor()
    cursor.execute("select MIN(dbid), MAX(dbid) from JobUsageRecord");
    row = cursor.fetchone()
    minimum_dbid = int(row[0])
    maximum_dbid = int(row[1])
    log.debug("minimum_dbid: " + str(minimum_dbid) + " maximum_dbid: " + str(maximum_dbid))
    # now, we want to put it into the file.
    # we check the file, if the file is empty, then it is the
    # the minimum dbid, otherwise, we choose 
    # to be the maximum of the "minimum dbid" and the last_successful_id in the file
    txn={}
    txn_previous = transaction.start_txn(cp)
    txn['last_successful_id']=max(minimum_dbid, txn_previous['last_successful_id'])
    txn['probename'] = cp.get("gratia", "probe")
    transaction.commit_txn(cp, txn)
    return minimum_dbid, maximum_dbid

def summarize_gratia(cp):
    raise NotImplementedError()


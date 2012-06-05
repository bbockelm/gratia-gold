
"""
Module for interacting with Gratia.

Connects to the database,
Queries the database,
Summarizes resulting queries.
"""

import logging

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

def query_gratia(cp, opts, txn):
    print "calling query_gratia ..."
    info = {}
    #_add_if_exists(cp, "user", info)
    print "opts.user="+opts.user + " opts.passwd="+opts.passwd + " opts.host="+opts.host + " opts.port=" + str(opts.port) + " opts.probename=" + opts.probename
    info['user'] = opts.user
    #_add_if_exists(cp, "passwd", info)
    info['passwd'] = opts.passwd
    _add_if_exists(cp, "db", info)
    #_add_if_exists(cp, "host", info)
    info['host'] = opts.host
    #_add_if_exists(cp, "port", info)
    info['port'] = opts.port
    if 'port' in info:
        info['port'] = int(info['port'])
    try:
        conn = MySQLdb.connect(**info)
        print "successfully connected ..."
    except Exception:
        print "exception"
        return 0
    curs = conn.cursor()

    # probe = cp.get("gratia", "probe")
    probe = opts.probename
    txn['probename'] = probe

    results = []
    # print GRATIA_QUERY
    curs.execute(GRATIA_QUERY, txn)
    for row in curs.fetchall():
        # print "fetching one line"
        info = {}
        info['dbid'] = row[0]
        info['resource_type'] = row[1]
        info['vo_name'] = row[2]
        info['user'] = row[3]
        info['charge'] = row[4]
        info['wall_duration'] = row[5]
        info['cpu'] = row[6] + row[7]
        info['node_count'] = row[8]
        info['njobs'] = row[9]
        info['processors'] = row[10]
        info['endtime'] = row[11].strftime("%Y-%m-%d %H:%M:%S")
        info['machine_name'] = row[12]
        info['project_name'] = row[13]
        results.append(info)
    return results

def summarize_gratia(cp):
    raise NotImplementedError()



"""
Module for interacting with Gratia.

Connects to the database,
Queries the database,
Summarizes resulting queries.
"""

GRATIA_QUERY = \
"""
SELECT
FROM
  JobUsageRecord JUR
JOIN
  JobUsageRecord_Meta JURM ON JUR.dbid = JURM.dbid
WHERE
  dbid > %(last_dbid)d AND
  ProbeName REGEXP %(probename)s
GROUP BY
ORDER BY dbid ASC
LIMIT 10000
"""

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


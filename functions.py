import MySQLdb
import logging
import sys
import json
from datetime import datetime


def db_connect(host="butterfree-bu.nexus.csiro.au", user="root", passwd="branches", db="aws"):
    """
    Connects to the AWS database, default parameters supplied for normal connection
    """
    try:
        conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in get_station_aws_id()\n" + str(e))
        sys.exit(1)

    return conn


def db_make_timeseries_query(daily_minutes, aws_id, params, start_time, end_time):
    """
    Makes SQL statement for timeseries requests

    daily_minutes: choose 'daily' or 'minutes'
    params: choose '*' or array of column names
    """
    sql = 'SELECT '
    if sql == '*':
        sql += '*'
    else:
        sql += ', '.join(params)
    sql += '\n'
    sql += 'FROM '
    if daily_minutes == 'daily':
        sql += 'tbl_daily'
    else:
        sql += 'tbl_15min'
    sql += '\n'
    sql += 'WHERE aws_id = "' + aws_id + '"\n'
    sql += 'AND stamp BETWEEN "' + start_time + '" AND "' + end_time + '"\n'
    sql += 'ORDER BY stamp;'

    return sql


def db_get_results(conn, query):
    """
    Executes given query agains given DB connection
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in get_station_aws_id()\n" + str(e))
        sys.exit(1)
    finally:
        cursor.close()
        conn.commit()
        conn.close()

    #stringify (from JSON)
    rs = []
    for row in rows:
        r = []
        for col in row:
            if type(col) is datetime:
                r.append(col.strftime('%Y-%m-%dT%H:%M:%S'))
            else:
                r.append(col)
        rs.append(r)

    return rs


def make_aws_timeseries_json(daily_minutes, params, timeseries_data, jsonp=False, jsonp_function_name=''):
    """
    Makes AWS timeseries formated JSON or JSON-P of given timeseries data
    """

    # TODO: update * parameter list
    if params == '*':
        p = [
                'id',
                'aws_id',
                'stamp',
                'arrival',
                'airT','appT',
                'dp',
                'rh',
                'deltaT',
                'soilT',
                'gsr',
                'Wmin',
                'Wavg',
                'Wmax',
                'Wdir',
                'rain',
                'leaf',
                'canT',
                'canRH',
                'batt',
                'pressure'
        ]
    else:
        p = ','.join(params)

    header = {
        'timestep': daily_minutes,
        'parameters': p,
        'no_readings': len(timeseries_data)
    }

    msg = json.dumps({
        'header': header,
        'data': timeseries_data
    })

    if jsonp:
        return jsonp_function_name + '(' + msg + ');'
    else:
        #normal JSON
        return msg


'''
#every unique key on the table must use every column in the tables partitioning expression
CREATE TABLE tbl_15min_partition (
  aws_id VARCHAR(21) DEFAULT NULL,
  stamp DATETIME DEFAULT NULL,
  arrival TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  airT DOUBLE DEFAULT NULL,
  appT DOUBLE DEFAULT NULL,
  dp DOUBLE DEFAULT NULL,
  rh DOUBLE DEFAULT NULL,
  deltaT DOUBLE DEFAULT NULL,
  soilT DOUBLE DEFAULT NULL,
  gsr DOUBLE DEFAULT NULL,
  Wmin DOUBLE DEFAULT NULL,
  Wavg DOUBLE DEFAULT NULL,
  Wmax DOUBLE DEFAULT NULL,
  Wdir DOUBLE DEFAULT NULL,
  rain DOUBLE DEFAULT NULL,
  leaf DOUBLE DEFAULT NULL,
  canT DOUBLE DEFAULT NULL,
  canRH DOUBLE DEFAULT NULL,
  batt DOUBLE DEFAULT NULL,
  pressure DOUBLE DEFAULT NULL,
  KEY idx_awsid_stamp (aws_id,stamp),
  KEY idx_stamp (stamp),
  KEY idx_stamp_awsid (stamp,aws_id)
) ENGINE=MYISAM DEFAULT CHARSET=latin1
PARTITION BY RANGE (YEAR(stamp)) (
    PARTITION p0 VALUES LESS THAN (2010),
    PARTITION p1 VALUES LESS THAN (2011),
    PARTITION p2 VALUES LESS THAN (2012),
    PARTITION p3 VALUES LESS THAN (2013),
    PARTITION p4 VALUES LESS THAN (2014),
    PARTITION p5 VALUES LESS THAN (2015),
    PARTITION p6 VALUES LESS THAN MAXVALUE
);
'''
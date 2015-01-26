import MySQLdb
import logging
import sys
from datetime import datetime, date
import settings


def db_connect(host=settings.DB_SERVER, user=settings.DB_USR, passwd=settings.DB_PWB, db=settings.DB_DB):
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


def db_disconnect(conn):
    conn.close()


# TODO: remove SQL injection possibility, see http://stackoverflow.com/questions/4574609/executing-select-where-in-using-mysqldb
def db_make_timeseries_query(timestep, station_ids, owners, params, start_date, end_date):
    """
    Makes SQL statement for timeseries requests

    daily_minutes: choose 'daily' or 'minutes'
    params: choose '*' or array of column names
    """
    query = 'SELECT '
    if not params or params == '*':
        query += '*'
    else:
        query += ', '.join(params.split(','))
    query += '\n'
    query += 'FROM '
    if timestep == 'minutes':
        query += 'tbl_15min'
    else:
        query += 'tbl_daily'
    query += '\n'

    query += 'WHERE 1 '

    if station_ids:
        # strip of the ' 1'
        query = query[:-2]
        query += ' aws_id IN ("' + '","'.join(station_ids.split(',')) + '")\n'
    else:
        # if we have aws_ids, ignore any owners values
        if owners:
            # strip the ' 1'
            query = query[:-2]
            query += ' owner IN ("' + '","'.join(owners.split(',')) + '")\n'

    #must have a start_time & end_time
    query += 'AND DATE(stamp) BETWEEN "' + start_date + '" AND "' + end_date + '"\n'
    query += 'ORDER BY stamp;'

    return query


# TODO: pass errors up
def db_get_timeseries_data(conn, query):
    """
    Executes given timeseries query against given DB connection
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in get_station_aws_id()\n" + str(e))
    finally:
        cursor.close()
        conn.commit()

    #stringify (from JSON)
    rs = []
    for row in rows:
        r = []
        for col in row:
            if type(col) is date:
                r.append(col.strftime('%Y-%m-%d'))
            elif type(col) is datetime:
                r.append(col.strftime('%Y-%m-%dT%H:%M:%S'))
            else:
                r.append(col)
        rs.append(r)

    return rs


# TODO: pass errors up
def get_station_details_obj(conn, aws_ids=None, owners=None):
    """
    Executes a JSON array of station details for a given station ID against a given DB connection
    """
    print aws_ids
    # make query
    query = '''
    SELECT
    aws_id, name, district, owner, lon, lat, elevation, status
    FROM tbl_stations
    WHERE 1'''

    if aws_ids:
        # strip of the ' 1'
        query = query[:-2]
        query += ' aws_id IN (%s)\n'
        in_p = ', '.join(map(lambda x: '%s', aws_ids.split(',')))
        query = query % in_p

    else:
        # if we have aws_ids, ignore any owners values
        if owners:
            # strip the ' 1'
            query = query[:-2]
            query += ' owner IN (%s)\n'
            in_p = ', '.join(map(lambda x: '%s', owners.split(',')))
            query = query % in_p

    query += ' ORDER BY name;'

    # execute query
    try:
        cursor = conn.cursor()

        if aws_ids:
            cursor.execute(query, aws_ids.split(','))
        elif owners:
            cursor.execute(query, owners.split(','))
        else:
            cursor.execute(query)

        rows = cursor.fetchall()

        obj = []
        for row in rows:
            aws_id, name, district, owner, lon, lat, elevation, status = row
            obj.append({
                'aws_id': aws_id,
                'name': name,
                'district': district,
                'owner': owner,
                'lon': lon,
                'lat': lat,
                'elevation': elevation,
                'status': status
            })
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in get_station_aws_id()\n" + str(e))

        obj = "ERROR: " + e.message
    finally:
        cursor.close()
        conn.commit()

    return obj


def make_aws_timeseries_obj(daily_minutes, params, timeseries_data):
    """
    Makes AWS timeseries object for given timeseries data
    """

    # TODO: update * parameter list
    # TODO: handle both minutes and daily *
    if not params or params == '*':
        p = [
                'id',
                'aws_id',
                'stamp',
                'arrival',
                'airT',
                'appT',
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
        p = params
    header = {
        'timestep': daily_minutes,
        'parameters': p,
        'no_readings': len(timeseries_data)
    }

    msg = {
        'header': header,
        'data': timeseries_data
    }

    return msg


# TODO: pass errors up
def get_networks_obj(conn):
    query = '''SELECT owner_id, owner_name FROM tbl_owners;'''

    # execute query
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        obj = []
        for row in rows:
            id, name = row
            obj.append({
                'id': id,
                'name': name
            })

    except MySQLdb.Error, e:
        print "Error: %s" % (e.message)

        obj = "ERROR: " + e.message
    finally:
        cursor.close()
        conn.commit()

    return obj



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
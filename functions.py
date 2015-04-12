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


# TODO: from Tim (to routes)
# DONE Limit the number of results (either number of stations or number of readings)
# DONE Return data sorted by a given property value (ascending or descending)
# Sort the JSON name/value pairs alphabetically? (This would make debugging a little easier for me.)
# TODO: remove SQL injection possibility, see http://stackoverflow.com/questions/4574609/executing-select-where-in-using-mysqldb
def db_make_timeseries_query(timestep, station_ids, owners, params, start_date, end_date, sortby, sortdir, limit):
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
        query += 'tbl_data_minutes'
    else:
        query += 'tbl_data_day'
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
    if sortby is not None:
        query += 'ORDER BY ' + sortby + ' '
    else:
        query += 'ORDER BY stamp '
    if sortdir is not None:
        query += sortdir
    else:
        query += 'ASC'
    if limit is not None:
        query += '\nLIMIT ' + str(limit)
    query += ';'

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


def get_station_details_obj(conn, aws_ids, owners, sortby, sortdir, longlat=(140, -38)):
    """
    Executes a JSON array of station details for a given station ID against a given DB connection
    """
    # make query
    query = 'SELECT '
    query += 'aws_id, name, district, owner, lon, lat, elevation, status'
    if sortby == 'distancefrom':
        if longlat is None:
            return {'ERROR': 'You have specified sortby == distancefrom but you have not given a longlat coordinate or your coordinate is not in the format (lon,lat)'}
        else:
            l = str(longlat)\
                .replace('(', '')\
                .replace(')', '')\
                .split(',')
            lon = l[0]
            lat = l[1]
            # sanity check on distance coordinates
            if float(lon) < 112 or float(lon) > 154 or float(lat) > -10 or float(lat) < -45:
                return {'ERROR': 'You have specified long or lat values that are too large or too small (outside Australia'}
            query += ', SQRT(ABS(' + lon + ' - lon) + ABS(' + lat + ' - lat)) AS distancefrom'
    query += '\nFROM tbl_stations\n'
    query += 'WHERE 1\n'

    if aws_ids is not None:
        # strip of the ' 1'
        query = query[:-2]
        query += ' aws_id IN (%s)\n'
        in_p = ', '.join(map(lambda x: '%s', aws_ids.split(',')))
        query = query % in_p

    else:
        # if we have aws_ids, ignore any owners values
        if owners is not None:
            # strip the ' 1'
            query = query[:-2]
            query += ' owner IN (%s)\n'
            in_p = ', '.join(map(lambda x: '%s', owners.split(',')))
            query = query % in_p

    if sortby is not None:
        query += 'ORDER BY ' + sortby + ' '
    else:
        query += 'ORDER BY name '
    if sortdir is not None:
        query += sortdir
    else:
        query += 'ASC'
    query += ';'

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
            if sortby == 'distancefrom':
                aws_id, name, district, owner, lon, lat, elevation, status, distancefrom = row
            else:
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

        obj = {"ERROR": str(e[1])}
    finally:
        cursor.close()
        conn.commit()

    return obj


def make_aws_timeseries_obj(timestep, params, timeseries_data):
    """
    Makes AWS timeseries object for given timeseries data
    """
    if not params or params == '*':
        if timestep == 'minutes':
            p = [
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
                    'pressure',
                    'wetT',
                    'vp'
            ]
        else:
            p = [
                    'aws_id',
                    'stamp',
                    'arrival',
                    'airT_min',
                    'airT_avg',
                    'airT_max',
                    'appT_min',
                    'appT_avg',
                    'appT_max',
                    'dp_min',
                    'dp_avg',
                    'dp_max',
                    'rh_min',
                    'rh_avg',
                    'rh_max',
                    'deltaT_min',
                    'deltaT_avg',
                    'deltaT_max',
                    'soilT_min',
                    'soilT_avg',
                    'soilT_max',
                    'gsr_total',
                    'Wmin',
                    'Wavg',
                    'Wmax',
                    'rain_total',
                    'leaf_min',
                    'leaf_avg',
                    'leaf_max',
                    'canT_min',
                    'canT_avg',
                    'canT_max',
                    'canRH_min',
                    'canRH_avg',
                    'canRH_max',
                    'pressure_min',
                    'pressure_avg',
                    'pressure_max',
                    'ggd_start',
                    'ggd_total',
                    'wetT_min',
                    'wetT_avg',
                    'wetT_max',
                    'vp_min',
                    'vp_avg',
                    'vp_max',
                    'batt_min',
                    'batt_avg',
                    'batt_max',
                    'frost_hrs',
                    'deg_days',
                    'et_asce_s',
                    'et_asce_t',
                    'et_meyer',
                    'readings'
            ]
    else:
        p = params.split(',')
    header = {
        'timestep': timestep,
        'parameters': p,
        'no_readings': len(timeseries_data)
    }

    msg = {
        'header': header,
        'data': timeseries_data
    }

    return msg


# TODO: complete
def get_stations_parameters(aws_id):
    pass


# TODO: pass errors up
def get_network_obj(conn, owner_id):
    query = '''SELECT owner_id, owner_name FROM tbl_owners WHERE owner_id = "''' + owner_id + '''";'''

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


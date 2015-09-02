import MySQLdb
import logging
import sys
from datetime import datetime, date
import re
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
    # validate timestep, only if we have one, else assume daily
    if timestep:
        if timestep not in ['daily', 'minutes']:
            return [False, 'Timestep must be either \'daily\' or \'minutes\' or left blank, in which case it defaults to daily']

    # validate parameters
    if params != '*' and params:
        if timestep == 'minutes':
            allowed_params = ['aws_id', 'stamp', 'arrival', 'airT', 'appT', 'dp', 'rh', 'deltaT', 'soilT', 'gsr', 'Wmin', 'Wavg', 'Wmax', 'Wdir', 'rain', 'leaf', 'canT', 'canRH', 'batt', 'pressure', 'wetT', 'vp']
        else:
            allowed_params = ['aws_id', 'stamp', 'arrival', 'airT_min', 'airT_avg', 'airT_max', 'appT_min', 'appT_avg', 'appT_max', 'dp_min', 'dp_avg', 'dp_max', 'rh_min', 'rh_avg', 'rh_max', 'deltaT_min', 'deltaT_avg', 'deltaT_max', 'soilT_min', 'soilT_avg', 'soilT_max', 'gsr_total', 'Wmin', 'Wavg', 'Wmax', 'rain_total', 'leaf_min', 'leaf_avg', 'leaf_max', 'canT_min', 'canT_avg', 'canT_max', 'canRH_min', 'canRH_avg', 'canRH_max', 'pressure_min', 'pressure_avg', 'pressure_max', 'gdd_start', 'gdd_total', 'wetT_min', 'wetT_avg', 'wetT_max', 'vp_min', 'vp_avg', 'vp_max', 'batt_min', 'batt_avg', 'batt_max', 'frost_hrs', 'deg_days', 'et_asce_s', 'et_asce_t', 'et_meyer', 'readings']

        for param in params.split(','):
            if not param in allowed_params:
                return [False, 'Parameter \'' + param + '\' is not found in the parameters list']

    # validate station ID
    if station_ids:
        if ',' in station_ids:
            for station_id in station_ids.split(','):
                if not station_id in ['ADLD01', 'ADLD02', 'AWNRM01', 'AWNRM02', 'AWNRM03', 'AWNRM04', 'AWNRM05', 'BIN001', 'BOW001', 'CAN001', 'CAR001', 'CON001', 'COO001', 'GRY001', 'JES001', 'JOY001', 'LEW001', 'LMW01', 'LMW02', 'LMW03', 'LMW04', 'LMW05', 'LMW06', 'LMW07', 'LMW08', 'LMW09', 'LMW10', 'MAC001', 'MIN001', 'MPWA01', 'MPWA02', 'MPWA03', 'MPWA04', 'MPWA06', 'MTM001', 'MVGWT01', 'MVGWT02', 'MVGWT03', 'MVGWT04', 'MVGWT05', 'MVGWT06', 'MVGWT07', 'MVGWT08', 'MVGWT09', 'RIV001', 'RMPW01', 'RMPW02', 'RMPW03', 'RMPW04', 'RMPW05', 'RMPW06', 'RMPW07', 'RMPW08', 'RMPW09', 'RMPW10', 'RMPW11', 'RMPW12', 'RMPW13', 'RMPW14', 'RMPW15', 'RMPW16', 'RMPW17', 'RMPW18', 'RMPW19', 'RMPW20', 'RMPW21', 'RMPW22', 'RMPW23', 'RMPW24', 'RMPW25', 'RMPW26', 'RMPW27', 'RMPW28', 'RMPW29', 'RMPW30', 'RMPW31', 'RMPW32', 'ROB001', 'RPWA05', 'STG001', 'SYM001', 'TAT001', 'TBRG01', 'TBRG02', 'TBRG03', 'TBRG04', 'TBRG06', 'TBRG08', 'TBRG09', 'TIN001', 'WIR001']:
                    return [False, 'station_id ' + station_id + ' is not found in the stations list']

    # validate start & end times
    if not start_date or not end_date:
        return [False, 'start and end datest must be supplied (in YYYY-MM-DD formats)']

    if not re.match('\d\d\d\d-\d\d-\d\d', start_date):
        return [False, 'start date is not in the correct format (YYYY-MM-DD)']

    if not re.match('\d\d\d\d-\d\d-\d\d', end_date):
        return [False, 'end date is not in the correct format (YYYY-MM-DD)']

    # validate sortby
    if sortby:
        if timestep == 'minutes':
            allowed_params = ['aws_id', 'stamp', 'arrival', 'airT', 'appT', 'dp', 'rh', 'deltaT', 'soilT', 'gsr', 'Wmin', 'Wavg', 'Wmax', 'Wdir', 'rain', 'leaf', 'canT', 'canRH', 'batt', 'pressure', 'wetT', 'vp']
        else:
            allowed_params = ['aws_id', 'stamp', 'arrival', 'airT_min', 'airT_avg', 'airT_max', 'appT_min', 'appT_avg', 'appT_max', 'dp_min', 'dp_avg', 'dp_max', 'rh_min', 'rh_avg', 'rh_max', 'deltaT_min', 'deltaT_avg', 'deltaT_max', 'soilT_min', 'soilT_avg', 'soilT_max', 'gsr_total', 'Wmin', 'Wavg', 'Wmax', 'rain_total', 'leaf_min', 'leaf_avg', 'leaf_max', 'canT_min', 'canT_avg', 'canT_max', 'canRH_min', 'canRH_avg', 'canRH_max', 'pressure_min', 'pressure_avg', 'pressure_max', 'gdd_start', 'gdd_total', 'wetT_min', 'wetT_avg', 'wetT_max', 'vp_min', 'vp_avg', 'vp_max', 'batt_min', 'batt_avg', 'batt_max', 'frost_hrs', 'deg_days', 'et_asce_s', 'et_asce_t', 'et_meyer', 'readings']
        if not sortby in allowed_params:
            return [False, 'sortby value \'' + sortby + '\' is not a valid parameter. It can be left unset.']

    # validate sortdir
    if sortdir:
        if not sortdir in ['ASC', 'DESC']:
            return [False, 'If set, sortdir must be \'ASC\', \'DESC\'. If not set, \'ASC\' is used']

    # validate limit
    if limit:
        try:
            int(limit)
            if int(limit) < 0:
                raise ValueError
        except ValueError:
            return [False, 'limit, if set, must be a positive integer']

    # all user inputs validated, make query
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

    return [True, query]


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
    if rows:
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
    else:
        rs = None

    return rs


def get_station_details_obj(conn, station_ids, owners, sortby, sortdir, longlat=(140, -38), properties=False):
    """
    Executes a JSON array of station details for a given station ID against a given DB connection
    """
    # validate station ID
    if station_ids:
        if ',' in station_ids:
            for station_id in station_ids.split(','):
                if not station_id in ['ADLD01', 'ADLD02', 'AWNRM01', 'AWNRM02', 'AWNRM03', 'AWNRM04', 'AWNRM05', 'BIN001', 'BOW001', 'CAN001', 'CAR001', 'CON001', 'COO001', 'GRY001', 'JES001', 'JOY001', 'LEW001', 'LMW01', 'LMW02', 'LMW03', 'LMW04', 'LMW05', 'LMW06', 'LMW07', 'LMW08', 'LMW09', 'LMW10', 'MAC001', 'MIN001', 'MPWA01', 'MPWA02', 'MPWA03', 'MPWA04', 'MPWA06', 'MTM001', 'MVGWT01', 'MVGWT02', 'MVGWT03', 'MVGWT04', 'MVGWT05', 'MVGWT06', 'MVGWT07', 'MVGWT08', 'MVGWT09', 'RIV001', 'RMPW01', 'RMPW02', 'RMPW03', 'RMPW04', 'RMPW05', 'RMPW06', 'RMPW07', 'RMPW08', 'RMPW09', 'RMPW10', 'RMPW11', 'RMPW12', 'RMPW13', 'RMPW14', 'RMPW15', 'RMPW16', 'RMPW17', 'RMPW18', 'RMPW19', 'RMPW20', 'RMPW21', 'RMPW22', 'RMPW23', 'RMPW24', 'RMPW25', 'RMPW26', 'RMPW27', 'RMPW28', 'RMPW29', 'RMPW30', 'RMPW31', 'RMPW32', 'ROB001', 'RPWA05', 'STG001', 'SYM001', 'TAT001', 'TBRG01', 'TBRG02', 'TBRG03', 'TBRG04', 'TBRG06', 'TBRG08', 'TBRG09', 'TIN001', 'WIR001']:
                    return [False, 'station_id ' + station_id + ' is not found in the stations list']

    # validate sortby
    if sortby:
        allowed_params = ['name', 'station_id', 'owner', 'lat', 'lon', 'elevation', 'status', 'distancefrom']
        if not sortby in allowed_params:
            return [False, 'sortby value \'' + sortby + '\' is not a valid station parameter. It can be left unset but, if set, must be chose from ' + ','.join(allowed_params)]
        if sortby == 'station_id':
            sortby = 'aws_id'

    # validate sortdir
    if sortdir:
        if not sortdir in ['ASC', 'DESC']:
            return [False, 'If set, sortdir must be \'ASC\', \'DESC\'. If not set, \'ASC\' is used']

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
                return [False, 'You have specified long or lat values that are too large or too small (outside Australia']
            query += ', SQRT(ABS(' + lon + ' - lon) + ABS(' + lat + ' - lat)) AS distancefrom'
    query += '\nFROM tbl_stations\n'
    query += 'WHERE 1\n'

    if station_ids is not None:
        # strip of the ' 1'
        query = query[:-2]
        query += ' aws_id IN (%s)\n'
        in_p = ', '.join(map(lambda x: '%s', station_ids.split(',')))
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

        if station_ids:
            cursor.execute(query, station_ids.split(','))
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

        return [False, str(e)]
    finally:
        cursor.close()
        conn.commit()

    return [True, obj]


def make_aws_timeseries_obj(timestep, params, timeseries_data):
    """
    Makes AWS timeseries object for given timeseries data
    """
    if timestep != 'minutes':
        timestep = 'daily'

    if not params or params == '*':
        # ensure default timestep is daily
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
def get_stations_parameters(conn, station_id):
    if not station_id in ['ADLD01', 'ADLD02', 'AWNRM01', 'AWNRM02', 'AWNRM03', 'AWNRM04', 'AWNRM05', 'BIN001', 'BOW001', 'CAN001', 'CAR001', 'CON001', 'COO001', 'GRY001', 'JES001', 'JOY001', 'LEW001', 'LMW01', 'LMW02', 'LMW03', 'LMW04', 'LMW05', 'LMW06', 'LMW07', 'LMW08', 'LMW09', 'LMW10', 'MAC001', 'MIN001', 'MPWA01', 'MPWA02', 'MPWA03', 'MPWA04', 'MPWA06', 'MTM001', 'MVGWT01', 'MVGWT02', 'MVGWT03', 'MVGWT04', 'MVGWT05', 'MVGWT06', 'MVGWT07', 'MVGWT08', 'MVGWT09', 'RIV001', 'RMPW01', 'RMPW02', 'RMPW03', 'RMPW04', 'RMPW05', 'RMPW06', 'RMPW07', 'RMPW08', 'RMPW09', 'RMPW10', 'RMPW11', 'RMPW12', 'RMPW13', 'RMPW14', 'RMPW15', 'RMPW16', 'RMPW17', 'RMPW18', 'RMPW19', 'RMPW20', 'RMPW21', 'RMPW22', 'RMPW23', 'RMPW24', 'RMPW25', 'RMPW26', 'RMPW27', 'RMPW28', 'RMPW29', 'RMPW30', 'RMPW31', 'RMPW32', 'ROB001', 'RPWA05', 'STG001', 'SYM001', 'TAT001', 'TBRG01', 'TBRG02', 'TBRG03', 'TBRG04', 'TBRG06', 'TBRG08', 'TBRG09', 'TIN001', 'WIR001']:
        return [False, 'station_id ' + station_id + ' is not found in the stations list']

    try:
        cursor = conn.cursor()
        cursor.execute('SELECT airT, appT, dp, rh, deltaT, soilT, gsr, Wmin, Wavg, Wmax, Wdir, rain, leaf, canT, canRH, pressure, wetT, vp FROM tbl_data_minutes WHERE aws_id = "' + station_id + '" LIMIT 1;')
        rows = cursor.fetchall()

        properties = []
        for row in rows:
            if row[0] is not None:
                properties.append('airT')
            if row[1] is not None:
                properties.append('appT')
            if row[2] is not None:
                properties.append('db')
            if row[3] is not None:
                properties.append('rh')
            if row[4] is not None:
                properties.append('deltaT')
            if row[5] is not None:
                properties.append('soilT')
            if row[6] is not None:
                properties.append('gsr')
            if row[7] is not None:
                properties.append('Wmin')
            if row[8] is not None:
                properties.append('Wavg')
            if row[9] is not None:
                properties.append('Wmax')
            if row[10] is not None:
                properties.append('Wdir')
            if row[11] is not None:
                properties.append('rain')
            if row[12] is not None:
                properties.append('leaf')
            if row[13] is not None:
                properties.append('canT')
            if row[14] is not None:
                properties.append('canRH')
            if row[15] is not None:
                properties.append('pressure')
            if row[16] is not None:
                properties.append('WetT')
            if row[17] is not None:
                properties.append('vp')

            return [True, properties]

        cursor.close()

        if len(properties) < 1:
            return [False, 'no values for station ' + station_id]

    except Exception, e:
        return [False, 'DB error: ' + e.message]


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


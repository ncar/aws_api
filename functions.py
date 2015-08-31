import MySQLdb
import logging
import sys
from datetime import datetime, date
import re
from lxml import etree
from StringIO import StringIO
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


def get_station_details_obj(conn, station_ids, owners, sortby, sortdir, longlat=(140, -38), parameters=False):
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


def get_station_scm(conn, station_id):
    """
    Gets the Station scm file from the database

    :param conn: MySQL connection object
    :param station_id
    :return: an SCM (XML) file as a string
    """
    sql = "SELECT scm FROM tbl_stations WHERE aws_id = '" + station_id + "';"
    try:
        if conn is None:
            conn = db_connect()

        cursor = conn.cursor()
        cursor.execute(sql)

        result = ''
        while 1:
            row = cursor.fetchone()
            if row is None:
                break
            #aws_id, scm
            result = str(row[0])
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in get_station_details()\n" + str(e))
        sys.exit(1)
    finally:
        cursor.close()
        conn.commit()
        #conn.close()

    return result


# from processor
def reading_vars_from_scm(scm_doc):
    """
    Gets an array of byte conversion information for the 15min buffer of a DMP file from an SCM file

    :param scm_doc: the station's SCM file
    :return: an array of dicts
    """
    logging.debug("def var_order_from_string()")

    t = etree.parse(StringIO(scm_doc))

    #Get instrument order from Buffer0
    instance_entries = []
    for instrument_buffer in t.xpath('//Buffer0/Entries/Entry'):
        instance_entry = dict()
        for child in instrument_buffer.getchildren():
            if child.tag == 'Inst':
                instance_entry['inst'] = child.text
            if child.tag == 'Action':
                instance_entry['action'] = child.text
        instance_entries.append(instance_entry)

    #Get instrument details from Instruments
    instruments = []
    for instrument in t.xpath('/Scheme/Instruments/*'):
        if instrument.tag.startswith('INST') or instrument.tag.startswith('Inst'):
            i = dict()
            #print etree.tostring(instrument, pretty_print=True)
            i['inst'] = instrument.tag.replace('INST', '').replace('Inst', '')
            i['model'] = instrument.get('Model')
            for inst in instrument.getchildren():
                if inst.tag == 'Channel':
                    i['name'] = inst.get('Name')
                if inst.tag.startswith('Scal'):
                    # some Scaling seems to miss type and this seems to eb for A gain B types only
                    if inst.get('Type') is None or inst.get('Type').startswith("'A'"):
                        i['type'] = 'conversion'
                        i['conv'] = inst.get('Type')
                        i['a'] = inst.get('a')
                        i['b'] = inst.get('b')
                    elif inst.get('Type').startswith("Formula"):
                        i['type'] = 'formula'
                        i['conv'] = inst.get('Formula')
                    #print etree.tostring(inst, pretty_print=True)
            instruments.append(i)

    #Merge Buffer0 listing  with instrument details
    instance_entries_with_details = []
    for instance_entry in instance_entries:
        for instrument in instruments:
            if instrument['inst'] == instance_entry.get('inst'):
                instance_entries_with_details.append(dict(instance_entry.items() + instrument.items()))

    return instance_entries_with_details


# from processor
def readings_vars_additions_from_db(conn, reading_vars):
    try:
        if conn is None:
            conn = db_connect()

        cursor = conn.cursor()
        reading_var_additions = []
        for reading_var in reading_vars:
            # TODO: move column name selection from DB to code
            sql = '''SELECT db_col_name, bytes
                     FROM tbl_var_lookup
                     WHERE mea_name = "''' + reading_var.get('name') + '''"
                     AND mea_action = "''' + reading_var.get('action').replace('AVE10', 'AVE') + '''";'''
            cursor.execute(sql)
            row = cursor.fetchone()
            if row is None:
                break
            reading_var_addition = reading_var
            reading_var_addition['db_col'] = str(row[0])
            reading_var_addition['bytes'] = int(row[1])
            reading_var_additions.append(reading_var_addition)
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in readings_vars_additions_from_db()\n" + str(e))
        sys.exit(1)
    finally:
        cursor.close()
        conn.commit()
        #conn.close()

    return reading_var_additions


def get_parameter_col_names_station_from_scm(conn, station_id):
    if conn is None:
            conn = db_connect()

    # get the SCM file
    scm_doc = get_station_scm(conn, station_id)

    props = []
    deets = readings_vars_additions_from_db(conn, reading_vars_from_scm(scm_doc))
    for deet in deets:
        if deet['db_col'] != 'nothing' and deet['db_col'] is not None:
            props.append(deet['db_col'])

    return props


def get_parameter_details(conn, parameter_id, station_id, timestep):
    if parameter_id and station_id:
        return [False, 'Please select either a parameter_id or a station_id, not both']
    if parameter_id:
        sql =   '''
                SELECT db_column AS parameter_id, NAME, aggregation, datatype, units
                FROM tbl_parameters WHERE db_column = "''' + parameter_id + '''";
                '''
    elif station_id:
        if timestep == 'daily':
            # station's daily parameters
            sql =   '''
                    SELECT db_column AS parameter_id, NAME, aggregation, datatype, units
                    FROM tbl_parameters WHERE db_column IN (
                    SELECT parameter_id_daily
                    FROM tbl_stations_parameters JOIN tbl_parameters_minutes_daily
                    ON tbl_stations_parameters.parameter_id = tbl_parameters_minutes_daily.parameter_id_minutes
                    WHERE aws_id = "''' + station_id + '''")
                    AND timestep = 'daily';
                    '''
        else:
            # station's minutes parameters
            sql =   '''
                    SELECT parameter_id, NAME, aggregation, datatype, units
                    FROM tbl_stations_parameters JOIN tbl_parameters
                    ON tbl_stations_parameters.parameter_id = tbl_parameters.db_column
                    WHERE aws_id = "''' + station_id + '''"
                    AND timestep = 'minutes';
                    '''
    else:
        if timestep == 'daily':
            # all daily parameters
            sql =   '''
                    SELECT db_column AS parameter_id, NAME, aggregation, datatype, units
                    FROM tbl_parameters
                    WHERE timestep = 'daily';
                    '''
        else:
            # all minutes parameters
            sql =   '''
                    SELECT db_column AS parameter_id, NAME, aggregation, datatype, units
                    FROM tbl_parameters
                    WHERE timestep = 'minutes';
                    '''

    try:
        if conn is None:
            conn = db_connect()

        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()

        header = [
            'parameter_id',
            'name',
            'aggregation',
            'datatype',
            'units'
        ]

        return [
            True,
            {
                'header': header,
                'data': [list(i) for i in rows]
            }
        ]

    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in readings_vars_additions_from_db()\n" + str(e))
        sys.exit(1)
    finally:
        cursor.close()
        conn.commit()
        #conn.close()


def get_stations_with_parameter(conn, parameter_id):
    if conn is None:
            conn = db_connect()

    sql = 'SELECT aws_id FROM tbl_stations_parameters WHERE parameter_id = "' + parameter_id + '" ORDER BY aws_id;'
    try:
        if conn is None:
            conn = db_connect()

        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        r = []
        for row in rows:
            r.append(row[0])
        return r

    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in readings_vars_additions_from_db()\n" + str(e))
        sys.exit(1)
    finally:
        cursor.close()
        conn.commit()
        #conn.close()


# TODO: don't forget to add in cron for this
def make_station_parameter_lookup_minutes():
    try:
        # make a DB connection
        conn = db_connect()

        # clear cache
        sql = 'DELETE FROM tbl_stations_parameters;'
        cursor = conn.cursor()
        cursor.execute(sql)


        # get all station ids
        sql = '''SELECT aws_id FROM tbl_stations;'''
        cursor.execute(sql)

        # get each station's parameters
        stations_parameters = []
        for aws in cursor.fetchall():
            # store each station's parameters for insertion
            for param in get_parameter_col_names_station_from_scm(conn, aws[0]):
                stations_parameters.append([aws[0], param])

        # make the insert SQL
        sql = 'INSERT INTO tbl_stations_parameters (aws_id, parameter_id) VALUES \n'
        for sp in stations_parameters:
            sql += '("' + sp[0] + '","' + sp[1] + '"),\n'
        sql = sql.rstrip('\n').rstrip(',')
        sql += ';'

        # insert all stations' parameters into the cache table
        cursor.execute(sql)
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in make_station_parameter_lookup()\n" + str(e))
        sys.exit(1)
    finally:
        cursor.close()
        conn.commit()
        # disconnect from DB
        db_disconnect(conn)


def get_stations_parameters(conn, station_id):
    if conn is None:
        conn = db_connect()

    parameters = []
    try:
        # make a DB connection
        conn = db_connect()

        # clear cache
        sql = 'SELECT parameter_id FROM tbl_stations_parameters WHERE aws_id = "' + station_id + '";'
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()

        for row in rows:
            parameters.append(row[0])

    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in make_station_parameter_lookup()\n" + str(e))
        return [False]
    finally:
        cursor.close()
        conn.commit()
        # disconnect from DB
        db_disconnect(conn)

    return [True, parameters]
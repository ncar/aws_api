from flask import Blueprint, Response, request, render_template
import json
import settings
import functions
routes = Blueprint('routes', __name__)


@routes.route('/favicon.ico')
def favicon():
    return ''


@routes.route('/')
def home():
    resp = {
        'Title': 'AWS data API',
        'Documentation': settings.BASE_URI + 'documentation',
        'Data Query Entry Point': settings.BASE_URI + 'data',
        'Network Register': settings.BASE_URI + 'network/',
        'Station Register': settings.BASE_URI + 'station/',
        'Property Register': settings.BASE_URI + 'property/',
    }

    return Response(json.dumps(resp), status=200, mimetype='application/json')


@routes.route('/documentation')
def documentation():
    return render_template('documentation.html')


# TODO: from Tim
# Sort the JSON name/value pairs alphabetically? (This would make debugging a little easier for me.)
# JQuery 1.4+s $.param() method format --> timestep=minutes&station_ids[]=RMPW12&start_date=2015-01-01&end_date=2015-01-02&properties[]=stamp&properties[]=airT&properties[]=rain
# header params as array --> {"timestep": "minutes", "no_readings": 192, "parameters": ["stamp", "airT", "rain"]}
@routes.route('/data')
@routes.route('/data/')
def data():
    """
    Entry point to the data delivery part of the API

    - list subregisters: networks, stations, properties
    """
    # attempts to make the query
    query = functions.db_make_timeseries_query(
        request.args.get('timestep'),
        request.args.get('station_ids'),
        request.args.get('owners'),
        request.args.get('properties'),
        request.args.get('start_date'),
        request.args.get('end_date'),
        request.args.get('sortby'),
        request.args.get('sortdir'),
        request.args.get('limit'))

    # return a 400 if the query cannot be made
    if not query[0]:
        return Response(json.dumps({'ERRORS': query[1]}), status=400, mimetype='application/json')

    # query ok so proceed to connect
    try:
        conn = functions.db_connect()
        rows = functions.db_get_timeseries_data(conn, query[1])
        functions.db_disconnect(conn)

        data_obj = functions.make_aws_timeseries_obj(request.args.get('timestep'), request.args.get('properties'), rows)

        #convert data object to JSON
        resp = json.dumps(data_obj)

        #wrap JSON-P
        if request.args.get('callback'):
            resp = request.args.get('callback') + '(' + resp + ');'

        return Response(resp, status=200, mimetype='application/json')
    except Exception, e:
        return Response(json.dumps({'ERRORS': 'DB error: ' + e.message}), status=500, mimetype='application/json')


@routes.route('/network/')
def networks():
    """
    Network register: no metadata

    - list of networks
    """
    conn = functions.db_connect()
    data_obj = functions.get_networks_obj(conn)
    functions.db_disconnect(conn)

    #convert data object to JSON
    resp = json.dumps(data_obj)

    #wrap JSON-P
    if request.args.get('callback'):
        resp = request.args.get('callback') + '(' + resp + ');'

    return Response(resp, status=200, mimetype='application/json')


@routes.route('/network/<string:network_id>')
def network(network_id):
    """
    A network
    """
    conn = functions.db_connect()
    data_obj = functions.get_network_obj(conn, network_id)
    functions.db_disconnect(conn)

    #convert data object to JSON
    resp = json.dumps(data_obj)

    #wrap JSON-P
    if request.args.get('callback'):
        resp = request.args.get('callback') + '(' + resp + ');'

    return Response(resp, status=200, mimetype='application/json')


@routes.route('/station/')
def stations():
    """
    Stations register

    Optional QSA parameters 'networks' or 'aws_ids' (not both)
    """
    conn = functions.db_connect()
    data_obj = functions.get_station_details_obj(conn,
                                                 station_ids=request.args.get('aws_ids'),
                                                 owners=request.args.get('networks'),
                                                 sortby=request.args.get('sortby'),
                                                 sortdir=request.args.get('sortdir'),
                                                 longlat=request.args.get('longlat'))
    functions.db_disconnect(conn)

    if data_obj[0]:
        #convert data object to JSON
        resp = json.dumps(data_obj[1])

        #wrap JSON-P
        if request.args.get('callback'):
            resp = request.args.get('callback') + '(' + resp + ');'

        return Response(resp, status=200, mimetype='application/json')
    else:
        return Response(data_obj[1], status=400, mimetype='text/plain')


# TODO: from Tim
# Filter by stations that measure a particular property/properties, OR
# Include a list of properties measured by each station with the station data
@routes.route('/station/<string:station_id>')
def station(station_id):
    """
    A station's details

    Same as /station/?aws_id=<station_id>
    """
    conn = functions.db_connect()

    data_obj = functions.get_station_details_obj(conn,
                                                 station_ids=station_id,
                                                 owners=None,
                                                 sortby=None,
                                                 sortdir=None,
                                                 longlat=None,
                                                 properties=True)

    properties = functions.get_stations_parameters(conn, station_id)

    functions.db_disconnect(conn)

    if data_obj[0]:
        if properties[0]:
            data_obj[1][0]['properties'] = properties[1]

            #convert data object to JSON
            resp = json.dumps(data_obj[1][0])

            #wrap JSON-P
            if request.args.get('callback'):
                resp = request.args.get('callback') + '(' + resp + ');'

            return Response(resp, status=200, mimetype='application/json')
        else:
            return Response(properties[1], status=400, mimetype='text/plain')
    else:
        return Response(data_obj[1], status=400, mimetype='text/plain')


@routes.route('/properties/')
def properties():
    if not request.args.get('station_id'):
        return Response('You must set a query string arg of station_id for this call', status=400, mimetype='text/plain')

    conn = functions.db_connect()
    properties = functions.get_stations_parameters(conn, request.args.get('station_id'))
    functions.db_disconnect(conn)
    if properties[0]:
        return Response(json.dumps(properties[1]), status=400, mimetype='application/json')
    else:
        return Response(properties[1], status=400, mimetype='text/plain')

from flask import Blueprint, Response, request, render_template
import json
import settings
import functions
routes = Blueprint('routes', __name__)


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
    errors = []

    # check for bad parameters
    if not request.args.get('start_date') or not request.args.get('end_date'):
        errors.append('You must, at least, specify a start_date and end_date query string argument')

    if len(errors) > 0:
        return Response(json.dumps({'ERRORS': errors}), status=400, mimetype='application/json')
    else:
        # parameters ok, make the query
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
        # ensure we have a valid query
        if query[0]:
            conn = functions.db_connect()
            rows = functions.db_get_timeseries_data(conn, query)
            functions.db_disconnect(conn)
            data_obj = functions.make_aws_timeseries_obj(request.args.get('timestep'), request.args.get('properties'), rows)

            #convert data object to JSON
            resp = json.dumps(data_obj)

            #wrap JSON-P
            if request.args.get('callback'):
                resp = request.args.get('callback') + '(' + resp + ');'

            return Response(resp, status=200, mimetype='application/json')
        # we have an invalid query, return error to user
        else:
            return Response(query[1], status=400, mimetype='text/plain')


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
                                                 aws_ids=request.args.get('aws_ids'),
                                                 owners=request.args.get('networks'),
                                                 sortby=request.args.get('sortby'),
                                                 sortdir=request.args.get('sortdir'),
                                                 longlat=request.args.get('longlat'))
    functions.db_disconnect(conn)

    #convert data object to JSON
    resp = json.dumps(data_obj)

    #wrap JSON-P
    if request.args.get('callback'):
        resp = request.args.get('callback') + '(' + resp + ');'

    return Response(resp, status=200, mimetype='application/json')


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
                                                 aws_ids=station_id,
                                                 owners=None,
                                                 sortby=None,
                                                 sortdir=None,
                                                 longlat=None)
    functions.db_disconnect(conn)

    #convert data object to JSON
    resp = json.dumps(data_obj)

    #wrap JSON-P
    if request.args.get('callback'):
        resp = request.args.get('callback') + '(' + resp + ');'

    return Response(resp, status=200, mimetype='application/json')


# TODO: complete or remove
@routes.route('/property/')
def property():
    return 'Property'

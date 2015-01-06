from flask import Blueprint, Response
import json
import settings
routes = Blueprint('routes', __name__)


@routes.route('/')
def home():
    resp = {
        'Documentation': settings.BASE_URI + 'documentation',
        'Data Entry': settings.BASE_URI + 'data',
        'Network Register': settings.BASE_URI + 'data/network/',
        'Station Register': settings.BASE_URI + 'data/station/',
        'All-station Property Register': settings.BASE_URI + 'data/property/',
    }

    return Response(json.dumps(resp), status=200, mimetype='application/json')


@routes.route('/documentation')
def documentation():
    return 'Documentation'


@routes.route('/data')
@routes.route('/data/')
def data():
    """
    Entry point to the data delivery part of the API

    - list subregisters: networks, stations, properties
    """
    return 'Data'


@routes.route('/data/network')
@routes.route('/data/network/')
def networks():
    """
    Network register: no metadata

    - list of networks
    """
    return 'Network'


@routes.route('/data/network/<string:network_id>')
def network(network_id):
    """
    A network

    - list of networks
    """
    return 'A network: ' + network_id


@routes.route('/data/station')
@routes.route('/data/station/')
def stations():
    """
    Station register: no metadata

    - list of stations
    """
    return 'Stations'


@routes.route('/data/station/<string:station_id>')
def station(station_id):
    """
    A station

    - list of stations
    """
    return 'A network: ' + station_id


@routes.route('/data/property')
@routes.route('/data/property/')
def properties():
    """
    Property register: no metadata

    - list of properties
    """
    return 'properties'


@routes.route('/data/property/<string:property_id>')
def property(property_id):
    """
    A station

    - list of stations
    """
    return 'A network: ' + property_id
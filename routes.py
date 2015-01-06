from flask import Blueprint, Response, request
routes = Blueprint('routes', __name__)


@routes.route('/')
def hello_world():
    return 'Hello World from routes!'
from flask_restful import Resource, Api, reqparse, abort
from flask import Response

class Swagger(Resource):
    def __init__(self):
        super(Swagger, self).__init__()

    def get(self):
        resp = Response("""
            <head>
            <meta http-equiv="refresh" content="0; url=http://petstore.swagger.io/?url=http://localhost:5000/api/swagger.json" />
            </head>""", mimetype='text/html')
        return resp
from flask_restful import Resource, Api, reqparse, abort
from jsonreducer.ObservationReducer import ObservationReducer
import requests


class ExampleList(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('data', type = str, required = True, help = 'No data provided', location = 'json')
        super(ExampleList, self).__init__()

    def get(self):
        nextPage = 'http://fhirtest.uhn.ca/baseDstu3/Observation/test-289?_format=json'
        r = requests.get(nextPage)
        json = r.json()

        test = ObservationReducer(json)
        return test.getReduced()

    def post(self):
        return None, 404


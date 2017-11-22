from flask_restful import Resource, Api, reqparse, abort
from jsonreducer.ObservationReducer import ObservationReducer
from lib import mongodbConnection
import requests
import configuration
from lib import aggregator
from flask import Response


class Aggregation(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('aggregation_type', type = str, help = 'No aggregation type provided', location = 'args')
        self.parser.add_argument('output_type', type = str, help = 'No output type provided', location = 'args')
        self.parser.add_argument('feature_id', type = str, location = 'args')

        super(Aggregation, self).__init__()

    def get(self, crawler_id):
        args = self.parser.parse_args()
        aggregation_type = args["aggregation_type"]
        output_type = args["output_type"]
        feature_id = args["feature_id"]

        features = mongodbConnection.get_db().features.find_one({"_id": feature_id})["attributes"] if feature_id is not None else []

        if output_type == "csv":
            result = aggregator.aggregateCSV(crawler_id, aggregation_type, features)
            return Response(result, mimetype='text/csv')
        else:
            return aggregator.aggregate(crawler_id, aggregation_type)

    def post(self):
        return None


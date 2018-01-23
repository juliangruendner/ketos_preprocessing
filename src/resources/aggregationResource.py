from flask_restful import Resource, Api, reqparse, abort
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

        super(Aggregation, self).__init__()

    def get(self, crawler_id):
        args = self.parser.parse_args()
        aggregation_type = args["aggregation_type"]
        output_type = args["output_type"]

        crawlerJob = mongodbConnection.get_db().crawlerJobs.find_one({"_id": crawler_id})
        resource = crawlerJob["resource"] or "Observation"
        feature_set = crawlerJob["feature_set"] or []
        searchParams = crawlerJob["search_params"] or []
        resourceMapping = crawlerJob["resource_mapping"] or []

        ret = None
        if resource == "Observation":
            ret = aggregator.aggregateFeatures(crawler_id, aggregation_type)

            if output_type == "csv":
                result = aggregator.writeFeaturesCSV(ret, feature_set)
                ret = Response(result, mimetype='text/csv')
        else:
            ret = list(mongodbConnection.get_db()[crawler_id])

            if output_type == "csv":
                result = aggregator.writeCSV(ret, resourceMapping, searchParams)
                Response(result, mimetype='text/csv')
        
        return ret


    def post(self):
        return None


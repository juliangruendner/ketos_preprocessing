from flask_restful import Resource, Api, reqparse, abort
from lib import mongodbConnection
import requests
import configuration
import csv
import io
from functools import reduce
from lib.aggregator import Aggregator
from flask import Response
import logging
logger = logging.getLogger(__name__)


class Aggregation(Resource):
    allowedAggregationTypes = ["all", "latest", "oldest"]

    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('aggregation_type', type = str, required = True, help = 'No aggregation type provided', location = 'args')
        self.parser.add_argument('output_type', type = str, help = 'No output type provided', location = 'args')

        super(Aggregation, self).__init__()

    def get(self, crawler_id):
        args = self.parser.parse_args()
        aggregation_type = args["aggregation_type"]
        output_type = args["output_type"]

        crawlerJob = mongodbConnection.get_db().crawlerJobs.find_one({"_id": crawler_id})
        if crawlerJob["status"] != "finished":
            return "Crawler Job did not finish yet", 404

        if aggregation_type.lower() not in self.allowedAggregationTypes:
            return "Wrong aggregation type provided: " + aggregation_type, 400
        
        if aggregation_type.lower() == "all" and output_type == "csv":
            return "Time series not supported", 400

        aggregator = Aggregator(crawler_id, aggregation_type.lower(), crawlerJob["feature_set"], crawlerJob["resource_configs"])
        aggregator.aggregate()
       
        if output_type == "csv":
            return Response(aggregator.getCSVOfAggregated(), mimetype='text/csv')
        elif output_type == "json":
            return aggregator.getRestructured()
        else:
            return aggregator.getAggregated()


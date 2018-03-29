from flask_restful import reqparse, abort
from flask_restful_swagger_2 import Api, swagger, Resource
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


allowedAggregationTypes = ["all", "latest", "oldest"]
supportedOutputTypes = ["csv", "json"]

AGGREGATION_TYPE_STR = 'Allowed types: ' + str(allowedAggregationTypes)
OUTPUT_TYPE_STR = 'Supported types: leave empty for raw or use ' + str(supportedOutputTypes)

parser = reqparse.RequestParser()
parser.add_argument('aggregation_type', type = str, required = True, help = AGGREGATION_TYPE_STR, location = 'args')
parser.add_argument('output_type', type = str, location = 'args')


class Aggregation(Resource):

    def __init__(self):
        super(Aggregation, self).__init__()

    @swagger.doc({
        "description": "Aggregate data and output as specified type.",
        "tags": ["aggregation"],
        "parameters": [
            {
                "name": "crawler_id",
                "in": "path",
                "type": "string",
                "description": "The ID of the crawler whose crawled data should be aggregated.",
                "required": True
            },
            {
                "name": "aggregation_type",
                "in": "query",
                "type": "string",
                "description": AGGREGATION_TYPE_STR,
                "required": True
            },
            {
                "name": "output_type",
                "in": "query",
                "type": "string",
                "description": OUTPUT_TYPE_STR,
            }
        ],
        "responses": {
            "200": {
                "description": "Retrieved a json with the inserted/updated Resource."
            }
        }
    })
    def get(self, crawler_id):
        args = parser.parse_args()
        aggregation_type = args["aggregation_type"]
        output_type = args["output_type"]

        crawlerJob = mongodbConnection.get_db().crawlerJobs.find_one({"_id": crawler_id})
        if crawlerJob["status"] != "finished":
            return "Crawler Job did not finish yet", 404

        if aggregation_type.lower() not in allowedAggregationTypes:
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


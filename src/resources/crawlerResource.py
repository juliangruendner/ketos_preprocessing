from flask_restful import reqparse, abort
from flask_restful_swagger_2 import Api, swagger, Resource
from resources import aggregationResource
import configuration
from flask import request
from lib import mongodbConnection
from bson.objectid import ObjectId
from bson import json_util
from datetime import datetime
from lib import crawler
from cerberus import Validator
import json
import urllib.parse
import logging
logger = logging.getLogger(__name__)

NO_PATIENTS_STR = "No patients provided"
XOR_RESOURCE_FEATURE_SET = "No resource XOR feature_set provided"

def feature_set_validator(value):
    FEATURE_SET_SCHEMA = {
        'resource': {'required': True, 'type': 'string'},
        'key': {'required': True, 'type': 'string'},
        'value': {'required': True, 'type': 'string'}
    }
    v = Validator(FEATURE_SET_SCHEMA)
    if v.validate(value):
        return value
    else:
        raise ValueError(json.dumps(v.errors))

def resource_mapping_validator(value):
    FEATURE_SET_SCHEMA = {
        'resource_path': {'required': True, 'type': 'string'},
        'result_path': {'required': True, 'type': 'string'}
    }
    v = Validator(FEATURE_SET_SCHEMA)
    if v.validate(value):
        return value
    else:
        raise ValueError(json.dumps(v.errors))

parser = reqparse.RequestParser(bundle_errors=True)
parser.add_argument('resource', type = str, location = 'json')
parser.add_argument('search_params', type = dict, location = 'json')
parser.add_argument('resource_mapping', type = resource_mapping_validator, action = 'append', location = 'json')
parser.add_argument('feature_set', type = feature_set_validator, action = 'append', location = 'json')
parser.add_argument('aggregation_type', type = str, location = 'json')


class Crawler(Resource):
    crawler_parser = parser.copy()
    crawler_parser.add_argument('patient', type = str, required = True, help = NO_PATIENTS_STR, location = 'json')

    def __init__(self):
        super(Crawler, self).__init__()
    
    # Synchronous version of crawler
    def post(self):
        args = self.crawler_parser.parse_args()

        if (args["feature_set"] is None and args["resource"] is None) or (args["feature_set"] is not None and args["resource"] is not None):
            return XOR_RESOURCE_FEATURE_SET, 400

        crawlerJob = crawler.createCrawlerJob(str(ObjectId()), "running", args["patient"], args["feature_set"], 
            args["resource"], args["search_params"], args["resource_mapping"], args["aggregation_type"])
        crawlerStatus = crawler.executeCrawlerJob(crawlerJob)

        return {"csv_url": crawlerJob["url"], "crawler_status": crawlerStatus}

class CrawlerJobs(Resource):
    crawler_parser = parser.copy()
    crawler_parser.add_argument('patient_ids', type = str, action = 'append', required = True, help = NO_PATIENTS_STR, location = 'json')

    def __init__(self):
        super(CrawlerJobs, self).__init__()
    
    
    @swagger.doc({
        "description":'Get all Crawler Jobs.',
        "responses": {
            "200": {
                "description": "Retrieved Crawler Job(s) as json."
            }
        }
    })
    def get(self):
        return list(mongodbConnection.get_db().crawlerJobs.find())

    @swagger.doc({
        "description":'Start a Crawler Job.',
        "responses": {
            "200": {
                "description": "Retrieved a json with the created Crawler ID."
            },
            "400": {
                "description": NO_PATIENTS_STR + " or " + XOR_RESOURCE_FEATURE_SET
            }
        }
    })
    def post(self):
        args = self.crawler_parser.parse_args()
        if (args["feature_set"] is None and args["resource"] is None) or (args["feature_set"] is not None and args["resource"] is not None):
            return XOR_RESOURCE_FEATURE_SET, 400

        crawlerJob = crawler.createCrawlerJob(str(ObjectId()), "queued", args["patient_ids"], args["feature_set"], 
            args["resource"], args["search_params"], args["resource_mapping"], args["aggregation_type"])

        return {"id": crawlerJob["_id"]}

    def delete(self):
        ret = mongodbConnection.get_db().crawlerJobs.delete_many({})

        return ret.deleted_count

class CrawlerJob(Resource):
    def __init__(self):
        super(CrawlerJob, self).__init__()
    
    @swagger.doc({
        "description":'Get a single Crawler Job.',
        "parameters":[
            {
                "name": "crawler_id",
                "in": "path",
                "type": "string",
                "description": "The ID of the crawler to be retrieved.",
                "required": True
            }
        ],
        "responses": {
            "200": {
                "description": "Retrieved Crawler Job as json."
            }
        } 
    })
    def get(self, crawler_id):
        return mongodbConnection.get_db().crawlerJobs.find_one({"_id": crawler_id})
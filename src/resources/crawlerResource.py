from flask_restful import reqparse, abort
from flask_restful_swagger_2 import swagger, Resource
from resources import aggregationResource
import configuration
from flask import request
from lib import mongodbConnection, crawler
from bson.objectid import ObjectId
from bson import json_util
from datetime import datetime
from cerberus import Validator
import json
import copy
import urllib.parse
import logging
logger = logging.getLogger(__name__)
import sys
from lib.brainApiAccess import BrainApiAccess


NO_PATIENTS_STR = "No patients provided"

FEATURE_SET_SCHEMA = {
    'resource': {'required': True, 'type': 'string'},   # Name of resource
    'key': {'required': True, 'type': 'string'},        # Key of resource to apply search query, e.g. "code"
    'value': {'required': True, 'type': 'string'},      # Value that key of resource must have to be retrieved, e.g. find code "21522001"
    'name': {'type': 'string'},                        # Human readable name of the value and to be column name of table, e.g. "Abdominal Pain"
    'resource_val_path': {'type': 'string'}
}

RESOURCE_CONFIG_SCHEMA = {
    'resource_name': {'required': True, 'type': 'string'},                  # Name of resource, e.g. "Condition"
    'resource_value_relative_path': {'required': True, 'type': 'string'},   # Path to look for actual value of a resource, e.g. "category/coding/code"
    'sort_order': {'type': 'list', 'schema': {'type': 'string'}}            # Order to sort retrieved values after, necessary for sorting for newest value
}

from swagger_resources import crawlerResourceSwagger

def feature_set_validator(value):
    v = Validator(FEATURE_SET_SCHEMA)
    if v.validate(value):
        return value
    else:
        raise ValueError(json.dumps(v.errors))

def resource_config_validator(value):
    v = Validator(RESOURCE_CONFIG_SCHEMA)
    if v.validate(value):
        return value
    else:
        raise ValueError(json.dumps(v.errors))

parser = reqparse.RequestParser(bundle_errors=True)
parser.add_argument('feature_set', type = feature_set_validator, action = 'append', required = True, location = 'json')
parser.add_argument('aggregation_type', type = str, default="latest", location = 'json')
parser.add_argument('resource_configs', type = resource_config_validator, action = 'append', location = 'json')


class Crawler(Resource):
    crawler_parser = parser.copy()
    crawler_parser.add_argument('patient', type = str, required = True, action='append', help = NO_PATIENTS_STR, location = 'json')

    def __init__(self):
        super(Crawler, self).__init__()
    
    @swagger.doc({
        "description": "Start a Crawler Job for a single patient and wait until it's finished.",
        "tags": ["crawler"],
        "parameters": crawlerResourceSwagger.swagger_params_patient,
        "responses": {
            "200": {
                "description": "Retrieved a json with a URL to the generated CSV and the exit status of the Crawler.",
                "schema": {
                    "type": "object",
                    "properties": {
                        "csv_url": {
                            "type": "string"
                        },
                        "crawler_status": {
                            "type": "string"
                        }
                    },
                    "example": {
                        "csv_url": "URL",
                        "crawler_status": "One of [success, error]"
                    }
                }
            }
        }
    })

    @BrainApiAccess()
    def post(self):
        args = self.crawler_parser.parse_args()
        crawler_id = str(ObjectId())

        logger.debug(args["resource_configs"])
        crawlerJob = crawler.createCrawlerJob(crawler_id, "running", args["patient"], args["feature_set"], args["aggregation_type"], args["resource_configs"])
        crawlerStatus = crawler.executeCrawlerJob(crawlerJob)
        return {"crawler_id": crawler_id, "csv_url": crawlerJob["url"], "crawler_status": crawlerStatus}


class CrawlerJobs(Resource):
    crawler_jobs_parser = parser.copy()
    crawler_jobs_parser.add_argument('patient_ids', type = str, action = 'append', required = True, help = NO_PATIENTS_STR, location = 'json')

    def __init__(self):
        super(CrawlerJobs, self).__init__()
    
    @swagger.doc({
        "description": "Get all submitted Crawler Jobs.",
        "tags": ["crawler"],
        "responses": {
            "200": {
                "description": "Retrieved Crawler Job(s) as json.",
                "schema": {
                    "type": "array",
                    "items": crawlerResourceSwagger.swagger_crawler_schema,
                }
            }
        }
    })

    @BrainApiAccess()
    def get(self):

        print(request.environ['REMOTE_ADDR'], file=sys.stderr)

        return list(mongodbConnection.get_db().crawlerJobs.find())

    @swagger.doc({
        "description": "Submit a Crawler Job.",
        "tags": ["crawler"],
        "parameters": crawlerResourceSwagger.swagger_params_patients,
        "responses": {
            "200": {
                "description": "Retrieved a json with the created Crawler ID.",
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "string"
                        }
                    }
                }
            },
            "400": {
                "description": NO_PATIENTS_STR
            }
        }
    })

    @BrainApiAccess()
    def post(self):
        args = self.crawler_jobs_parser.parse_args()

        crawlerJob = crawler.createCrawlerJob(str(ObjectId()), "queued", args["patient_ids"], args["feature_set"], args["aggregation_type"], args["resource_configs"])

        return {"id": crawlerJob["_id"]}

    @swagger.doc({
        "description": "Remove all submitted Crawler Jobs.",
        "tags": ["crawler"],
        "responses": {
            "200": {
                "description": "Number of deleted Crawler Jobs."
            }
        }
    })
    def delete(self):
        ret = mongodbConnection.get_db().crawlerJobs.delete_many({})

        return ret.deleted_count

class CrawlerJob(Resource):

    @BrainApiAccess()
    def __init__(self):
        super(CrawlerJob, self).__init__()
    
    @swagger.doc({
        "description": "Retrieve a single Crawler Job.",
        "tags": ["crawler"],
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
                "description": "Retrieved Crawler Job as json.",
                "schema": crawlerResourceSwagger.swagger_crawler_schema
            }
        } 
    })

    def get(self, crawler_id):

        return mongodbConnection.get_db().crawlerJobs.find_one({"_id": crawler_id})
    
    def delete(self, crawler_id):
        ret = mongodbConnection.get_db().crawlerJobs.delete_many({"_id": crawler_id})

        return ret.deleted_count
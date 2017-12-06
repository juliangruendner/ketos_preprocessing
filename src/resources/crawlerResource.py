from flask_restful import Api, reqparse, abort
from flask_restful_swagger_2 import swagger, Resource
from jsonreducer.ObservationReducer import ObservationReducer
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

NO_RESOURCE_STR = "No resource provided"
NO_PATIENTS_STR = "No patients provided"
NO_FEATURES_STR = "No features provided"

def feature_set_validator(value):
    FEATURE_SET_SCHEMA = {
        'resource': {'required': True, 'type': 'string'},
        'params': {'required': True, 'type': 'list', 'allow_unknown': True,
            'schema': {
                'type': 'dict',
                'allow_unknown': True,
                'schema': {
                    'name': {'type': 'string'},
                    'value': {'type': 'string'}
                }
            }
        }
    }
    v = Validator(FEATURE_SET_SCHEMA)
    if v.validate(value):
        return value
    else:
        raise ValueError(json.dumps(v.errors))

class Crawler(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('resource', type = str, required = True, help = NO_RESOURCE_STR, location = 'json')
    parser.add_argument('patient', type = str, required = True, help = NO_PATIENTS_STR, location = 'json')
    parser.add_argument('aggregation_type', type = str,location = 'json')
    parser.add_argument('feature_id', type = str, location = 'json')

    def __init__(self):
        super(Crawler, self).__init__()
    
    def post(self):
        from api import api

        args = self.parser.parse_args()
        resource = args["resource"]
        patient = args["patient"]
        aggregation_type = args["aggregation_type"]
        feature_id = args["feature_id"]
        crawler_id = str(ObjectId())

        crawler.crawlResourceForSubject(resource, patient, crawler_id)
        
        url_agg = api.url_for(aggregationResource.Aggregation, crawler_id=crawler_id)
        url_params = {"output_type": "csv"}
        url_params["aggregation_type"] = aggregation_type if aggregation_type is not None else "latest"
        if feature_id is not None:
            url_params["feature_id"] = feature_id

        return {"csv_url": url_agg + "?" + urllib.parse.urlencode(url_params)}

class CrawlerJobs(Resource):
    crawler_parser = reqparse.RequestParser(bundle_errors=True)
    crawler_parser.add_argument('patient_ids', type = str, action = 'append', required = True, help = NO_PATIENTS_STR, location = 'json')
    crawler_parser.add_argument('feature_set', action= 'append', type = feature_set_validator, required = True, help = NO_FEATURES_STR, location = 'json')

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
        # "reqparser": {
        #     "name": "crawler_parser"
        #     "parser": crawler_parser
        # },
        "responses": {
            "200": {
                "description": "Retrieved a json with the created Crawler ID."
            },
            "400": {
                "description": NO_RESOURCE_STR + " or " + NO_PATIENTS_STR
            }
        }
    })
    def post(self):
        args = self.crawler_parser.parse_args()

        ret = mongodbConnection.get_db().crawlerJobs.insert_one({
            "_id": str(ObjectId()),
            "patient_ids": args["patient_ids"],
            "feature_set": args["feature_set"],
            "status": "queued",
            "finished": [],
            "queued_time": str(datetime.now()),
            "start_time": None
        })

        return {"id": str(ret.inserted_id)}

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
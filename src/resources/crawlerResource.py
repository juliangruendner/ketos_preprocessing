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

class Crawler(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('aggregation_type', type = str,location = 'json')
    parser.add_argument('patient', type = str, required = True, help = NO_PATIENTS_STR, location = 'json')
    parser.add_argument('resource', type = str, required = False, location = 'json')
    parser.add_argument('search_params', type = dict, required = False, location = 'json')
    parser.add_argument('resource_mapping', type = dict, required = False, location = 'json')
    parser.add_argument('feature_set', type = feature_set_validator, action = 'append', location = 'json')

    def __init__(self):
        super(Crawler, self).__init__()
    
    # Synchronous version of crawler
    def post(self):
        from api import api

        args = self.parser.parse_args()
        patient = args["patient"]
        aggregation_type = args["aggregation_type"]
        resource = args["resource"]
        searchParams = args["search_params"]
        resourceMapping = args["resource_mapping"]
        feature_set = args["feature_set"]

        if (args["feature_set"] is None and args["resource"] is None) or (args["feature_set"] is not None and args["resource"] is not None):
            return XOR_RESOURCE_FEATURE_SET, 400

        crawler_id = str(ObjectId())
        url_agg = api.url_for(aggregationResource.Aggregation, crawler_id=crawler_id)
        url_params = {"output_type": "csv"}
        url_params["aggregation_type"] = aggregation_type if aggregation_type is not None else "latest"
        url = "http://"+configuration.HOSTEXTERN+":"+str(configuration.WSPORT)+url_agg + "?" + urllib.parse.urlencode(url_params)
        
        ret = mongodbConnection.get_db().crawlerJobs.insert_one({
            "_id": crawler_id,
            "patient_ids": [patient],
            "feature_set": args["feature_set"],
            "resource": resource,
            "search_params": searchParams,
            "resource_mapping": resourceMapping,
            "status": "running",
            "finished": [patient],
            "queued_time": str(datetime.now()),
            "start_time": str(datetime.now()),
            "url": url
        })

        if resource is not None and resource is not "Observation":
            crawler.crawlResourceForSubject(resource, patient, crawler_id, searchParams)
        else:
            for feature in feature_set:
                crawler.crawlResourceForSubject(feature["resource"], patient, crawler_id, {feature["key"]: feature["value"]})
        
        mongodbConnection.get_db().crawlerJobs.update({"_id": crawler_id}, {"$set": {"status": "finished", "end_time": str(datetime.now())}})

        return {"csv_url": url}

class CrawlerJobs(Resource):
    crawler_parser = reqparse.RequestParser(bundle_errors=True)
    crawler_parser.add_argument('patient_ids', type = str, action = 'append', required = True, help = NO_PATIENTS_STR, location = 'json')
    crawler_parser.add_argument('resource', type = str, required = False, location = 'json')
    crawler_parser.add_argument('search_params', type = dict, required = False, location = 'json')
    crawler_parser.add_argument('resource_mapping', type = dict, required = False, location = 'json')
    crawler_parser.add_argument('feature_set', action= 'append', type = feature_set_validator, required = False, location = 'json')

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
        from api import api

        args = self.crawler_parser.parse_args()
        if (args["feature_set"] is None and args["resource"] is None) or (args["feature_set"] is not None and args["resource"] is not None):
            return XOR_RESOURCE_FEATURE_SET, 400

        tmpid = str(ObjectId())
        url_params = {"output_type": "csv", "aggregation_type": "latest"}

        ret = mongodbConnection.get_db().crawlerJobs.insert_one({
            "_id": tmpid,
            "patient_ids": args["patient_ids"],
            "feature_set": args["feature_set"],
            "resource": args["resource"],
            "search_params": args["search_params"],
            "resource_mapping": args["resource_mapping"],
            "status": "queued",
            "finished": [],
            "queued_time": str(datetime.now()),
            "start_time": None,
            "url": "http://"+configuration.HOSTEXTERN+":"+str(configuration.WSPORT)+api.url_for(aggregationResource.Aggregation, crawler_id=tmpid)+ "?" + urllib.parse.urlencode(url_params)
        })

        return {"id": str(ret.inserted_id)}

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
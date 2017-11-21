from flask_restful import Api, reqparse, abort
from flask_restful_swagger_2 import swagger, Resource
from jsonreducer.ObservationReducer import ObservationReducer
import configuration
from flask import request
from lib import mongodbConnection
from bson.objectid import ObjectId
from bson import json_util
from datetime import datetime
import json

NO_RESOURCE_STR = "No resource provided"
NO_PATIENTS_STR = "No patients provided"

class CrawlerJob(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('resource', type = str, required = True, help = NO_RESOURCE_STR, location = 'json')
    parser.add_argument('patients', type = str, action = 'append', required = True, help = NO_PATIENTS_STR, location = 'json')

    def __init__(self):
        super(CrawlerJob, self).__init__()
    
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
        "reqparser": {
            "name": "crawler_parser",
            "parser": parser
        },
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
        args = self.parser.parse_args()
        resource = args["resource"]
        patients = args["patients"]

        ret = mongodbConnection.get_db().crawlerJobs.insert_one({
            "_id": str(ObjectId()),
            "resource": args["resource"],
            "patients": args["patients"],
            "status": "queued",
            "finished": [],
            "queued_time": str(datetime.now()),
            "start_time": None
        })

        return {"id": str(ret.inserted_id)}

class CrawlerJobs(Resource):
    def __init__(self):
        super(CrawlerJobs, self).__init__()

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
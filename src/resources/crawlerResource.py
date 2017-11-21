from flask_restful import Resource, Api, reqparse, abort
from jsonreducer.ObservationReducer import ObservationReducer
import configuration
from flask import request
from lib import mongodbConnection
from bson.objectid import ObjectId
from bson import json_util
from datetime import datetime
import json


class CrawlerJobs(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('resource', type = str, required = True, help = 'No resource provided', location = 'json')
        self.parser.add_argument('patients', type = str, action = 'append', required = True, help = 'No patients provided', location = 'json')
        super(CrawlerJobs, self).__init__()

    def get(self, crawler_id=None):
        return list(mongodbConnection.get_db().crawlerJobs.find()) if crawler_id is None else mongodbConnection.get_db().crawlerJobs.find_one({"_id": crawler_id})

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

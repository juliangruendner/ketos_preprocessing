from flask_restful import Resource, Api, reqparse, abort
from jsonreducer.ObservationReducer import ObservationReducer
import requests
import configuration


class Aggregation(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('data', type = str, required = True, help = 'No data provided', location = 'json')
        super(Aggregation, self).__init__()

    def get(self):
        result = configuration.MONGODB.patients.aggregate([
            {"$unwind": "$observations"},
            {"$group" : {"_id" : {"attribute": "$observations.attribute", "patient_id": "$_id"}, "entry": {"$push": "$$CURRENT.observations"}}},
            {"$unwind": "$entry"},
            {"$sort"  : {"entry.timestamp": -1}},
            {"$group" : {"_id": "$_id", "observations": {"$push": "$entry"}}},
            { "$group" : {"_id": "$_id.patient_id", "observations": { "$push": "$$CURRENT.observations"}}}
        ])
        return list(result)

    def post(self):
        return None


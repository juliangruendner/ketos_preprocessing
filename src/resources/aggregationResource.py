from flask_restful import Resource, Api, reqparse, abort
from jsonreducer.ObservationReducer import ObservationReducer
from lib import mongodbConnection
import requests
import configuration


class Aggregation(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('AggType', type = str, help = 'No aggregation type provided', location = 'args')
        super(Aggregation, self).__init__()

    def get(self, crawler_id):
        args = self.parser.parse_args()
        aggtype = args["AggType"]

        mongorequest = [
            {"$unwind": "$observations"},
            {"$group" : {"_id" : {"attribute": "$observations.attribute", "patient_id": "$_id"}, "entry": {"$push": "$$CURRENT.observations"}}},
            {"$unwind": "$entry"},
            {"$sort"  : {"entry.timestamp": -1}}
        ]

        if aggtype == None or aggtype.lower() == "all":
            mongorequest += [
                {"$group" : {"_id": "$_id", "observations": {"$push": "$entry"}}},
                {"$group" : {"_id": "$_id.patient_id", "observations": { "$push": "$$CURRENT.observations"}}}
            ]
        elif aggtype.lower() == "first" or aggtype.lower() == "last":
            mongorequest += [
                {"$group" : {"_id": "$_id", "observations": {"$"+aggtype.lower(): "$entry"}}},
                {"$group" : {"_id": "$_id.patient_id", "observations": { "$push": "$$CURRENT.observations"}}}
            ]
        elif aggtype.lower() == "avg":
            mongorequest += [
                {"$group" : {"_id": "$_id" , "attribute": { "$first": "$_id.attribute" }, "observations": { "$avg": "$entry.value"}}},
                {"$group" : {"_id": "$_id.patient_id", "observations": { "$push": {"avg": "$$CURRENT.observations", "attribute": "$_id.attribute"}}}}
            ]
        else:
            return None

        result = mongodbConnection.get_db()[crawler_id].aggregate(mongorequest)
        return list(result)

    def post(self):
        return None


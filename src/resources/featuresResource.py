from flask_restful import Resource, Api, reqparse, abort
from lib import mongodbConnection
from bson.objectid import ObjectId
from flask import request


class FeaturesBrowser(Resource):
    def __init__(self):
        super(FeaturesBrowser, self).__init__()

    def get(self, crawler_id):

        mongorequest1 = [
            {"$unwind": "$observations"},
            {"$group": {"_id": {"attribute": "$observations.attribute", "patient_id": "$_id"}, 
                "attribute": {"$first": "$$CURRENT.observations.attribute"}, 
                "value": {"$first": "$$CURRENT.observations.value"}, 
                "meta": {"$first": "$$CURRENT.observations.meta"}}
            },
            {"$group": {"_id": "$_id.patient_id", "attributes": {"$push": {"attribute": "$$CURRENT.attribute", "meta": "$$CURRENT.meta", "value": "$$CURRENT.value"}}}}
        ]

        mongorequest2 = [
            {"$unwind": "$observations"},
            {"$group": {"_id": {"attribute": "$observations.attribute"}, 
                "attribute": {"$first": "$$CURRENT.observations"}}},
            {"$project": {"_id": 0, "attribute": 1}}
        ]

        features = {}

        for attribute in list(mongodbConnection.get_db()[crawler_id].aggregate(mongorequest2)):
            features[attribute["attribute"]["attribute"]] = {"counter": 0, "meta": attribute["attribute"]["meta"]}

        for patient in list(mongodbConnection.get_db()[crawler_id].aggregate(mongorequest1)):
            for attribute in patient["attributes"]:
                if attribute["value"] is not None:
                    features[attribute["attribute"]]["counter"] += 1
        return features

class FeaturesSet(Resource):
    def __init__(self):
        super(FeaturesSet, self).__init__()

    def get(self, set_id):
        return mongodbConnection.get_db().features.find_one({"_id": set_id})

class FeaturesSets(Resource):
    def __init__(self):
        super(FeaturesSets, self).__init__()

    def get(self):
        return list(mongodbConnection.get_db().features.find())

    def post(self):
        data = request.get_json(force=True)

        ret = mongodbConnection.get_db().features.insert_one({
            "_id": str(ObjectId()),
            "attributes": data["attributes"]
        })

        return {"id": str(ret.inserted_id)}


from flask_restful import Api, reqparse, abort
from flask_restful_swagger_2 import swagger, Resource
from lib import mongodbConnection
from models import featureModel
from bson.objectid import ObjectId
from flask import request
from cerberus import Validator
import json

def attributes_validator(value):
    ATTRIBUTES_SCHEMA = {
        'type': {
            'required': True, 'type': 'dict', 'allow_unknown': True,
            'schema': {
                'code': {'required': True, 'type': 'string'}
            }
        },
    }
    v = Validator(ATTRIBUTES_SCHEMA)
    if v.validate(value):
        return value
    else:
        raise ValueError(json.dumps(v.errors))

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
    feature_parser = reqparse.RequestParser(bundle_errors=True)
    feature_parser.add_argument('attributes', action='append', type=attributes_validator, required=True, help = 'Wrong format', location = 'json')

    def __init__(self):
        super(FeaturesSets, self).__init__()

    @swagger.doc({
        "description":'Get all feature definitions.',
        "responses": {
            "200": {
                "description": "Retrieved feature definitions as json."
            }
        }
    })
    def get(self):
        return list(mongodbConnection.get_db().features.find())

    @swagger.doc({
        "description":'Save a feature definition.',
        "parameters": [
            {
                'name': 'body',
                'description': 'Request body',
                'in': 'body',
                'schema': featureModel.FeatureModel,
                'required': True,
            }
        ],
        "responses": {
            "200": {
                "description": "Retrieved a json with the created feature ID."
            },
            "400": {
                "description": "Wrong format of features."
            }
        }
    })
    def post(self):
        args = self.feature_parser.parse_args()
        ret = mongodbConnection.get_db().features.insert_one({
            "_id": str(ObjectId()),
            "attributes": args["attributes"]
        })

        return {"id": str(ret.inserted_id)}


from flask_restful import reqparse, abort
from flask_restful_swagger_2 import Api, swagger, Resource
from lib import mongodbConnection
import requests
import configuration
from pymongo import ReturnDocument
from lib import aggregator
from flask import Response
from bson.objectid import ObjectId

NO_RESOURCE_NAME_STR = "No resource name provided!"
NO_RESOURCE_MAPPING_STR = "No resource mapping provided!"

def insert_resource_config(resource_name, resource_mapping):
    ret = mongodbConnection.get_db().resourceConfig.find_one_and_update(
        {"resource_name" : resource_name},
        {"$set": {"resource_name" : resource_name, "resource_mapping" : resource_mapping}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )

    return ret

class ResourceConfigList(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('resource_name', type = str, required = True, help = NO_RESOURCE_NAME_STR, location = 'json')
        self.parser.add_argument('resource_mapping', type = dict, required = True, help = NO_RESOURCE_MAPPING_STR, location = 'json')

        super(ResourceConfigList, self).__init__()
    
    def get(self):
        return list(mongodbConnection.get_db().resourceConfig.find())

    def post(self):
        args = self.parser.parse_args()
        resource_name = args["resource_name"]
        resource_mapping = args["resource_mapping"]

        return insert_resource_config(resource_name, resource_mapping)

class ResourceConfig(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('resource_mapping', type = dict, required = True, help = NO_RESOURCE_MAPPING_STR, location = 'json')

        super(ResourceConfig, self).__init__()

    def get(self, resource_name):
        return mongodbConnection.get_db().resourceConfig.find_one({"resource_name": resource_name})

    def post(self, resource_name):
        args = self.parser.parse_args()
        resource_mapping = args["resource_mapping"]

        return insert_resource_config(resource_name, resource_mapping)


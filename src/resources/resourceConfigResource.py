from flask_restful import reqparse, abort
from flask_restful_swagger_2 import Api, swagger, Resource
from lib import mongodbConnection
import json
import requests
import configuration
from pymongo import ReturnDocument
from lib import aggregator, resourceLoader
from flask import Response
from cerberus import Validator
from bson.objectid import ObjectId

NO_RESOURCE_NAME_STR = "No resource name provided!"
NO_RESOURCE_MAPPING_STR = "No resource mapping provided!"

def resource_mapping_validator(value):
    FEATURE_SET_SCHEMA = {
        'resource_path': {'required': True, 'type': 'string'},
        'result_path': {'required': True, 'type': 'string'}
    }
    v = Validator(FEATURE_SET_SCHEMA)
    if v.validate(value):
        return value
    else:
        raise ValueError(json.dumps(v.errors))

def insert_resource_config(resource_name, resource_mapping):
    mongodbConnection.get_db().resourceConfig.find_one_and_delete({"resource_name" : resource_name})
    mongodbConnection.get_db().resourceConfig.insert_one(
        {"_id": resource_name, "resource_name" : resource_name, "resource_mapping" : resource_mapping}
    )

    ret = mongodbConnection.get_db().resourceConfig.find_one({"resource_name" : resource_name})
    resourceLoader.writeResource(ret)
    return ret

def remove_resource_config(resource_name):
    resourceLoader.deleteResource(resource_name)
    return mongodbConnection.get_db().resourceConfig.find_one_and_delete({"resource_name" : resource_name})

class ResourceConfigList(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('resource_name', type = str, required = True, help = NO_RESOURCE_NAME_STR, location = 'json')
        self.parser.add_argument('resource_mapping', type = resource_mapping_validator, action = 'append', required = True, help = NO_RESOURCE_MAPPING_STR, location = 'json')

        super(ResourceConfigList, self).__init__()
    
    def get(self):
        return list(mongodbConnection.get_db().resourceConfig.find())

    def post(self):
        args = self.parser.parse_args()
        resource_name = args["resource_name"]
        resource_mapping = args["resource_mapping"]

        return insert_resource_config(resource_name, resource_mapping)

    def delete(self):
        args = self.parser.parse_args()
        resource_name = args["resource_name"]

        remove_resource_config(resource_name)

        return {"resource_name": resource_name} , 200

class ResourceConfig(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('resource_mapping', type = resource_mapping_validator, action = 'append', required = True, help = NO_RESOURCE_MAPPING_STR, location = 'json')

        super(ResourceConfig, self).__init__()

    def get(self, resource_name):
        return mongodbConnection.get_db().resourceConfig.find_one({"resource_name": resource_name})

    def post(self, resource_name):
        args = self.parser.parse_args()
        resource_mapping = args["resource_mapping"]

        return insert_resource_config(resource_name, resource_mapping)

    def delete(self, resource_name):
        remove_resource_config(resource_name)

        return {"resource_name": resource_name}, 200


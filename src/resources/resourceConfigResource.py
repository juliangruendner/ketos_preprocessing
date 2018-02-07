from flask_restful import reqparse, abort
from flask_restful_swagger_2 import Api, swagger, Resource
from lib import mongodbConnection
import json
import requests
import configuration
from pymongo import ReturnDocument
from lib import aggregator, resourceLoader
from flask import Response
from bson.objectid import ObjectId
import logging
logger = logging.getLogger(__name__)


NO_RESOURCE_NAME_STR = "No resource name provided!"
NO_RESOURCE_VALUE_PATH_STR = "No value path for resource provided!"

def insert_resource_config(resource_name, resource_value_relative_path, sort_order):
    mongodbConnection.get_db().resourceConfig.find_one_and_delete({"_id" : resource_name})
    mongodbConnection.get_db().resourceConfig.insert_one(
        {"_id": resource_name, "resource_value_relative_path" : resource_value_relative_path, "sort_order": sort_order}
    )

    ret = mongodbConnection.get_db().resourceConfig.find_one({"_id" : resource_name})
    resourceLoader.writeResource(ret)
    return ret

def remove_resource_config(resource_name):
    resourceLoader.deleteResource(resource_name)
    return mongodbConnection.get_db().resourceConfig.find_one_and_delete({"_id" : resource_name})

parser = reqparse.RequestParser()
parser.add_argument('resource_value_relative_path', type = str, required = True, help = NO_RESOURCE_VALUE_PATH_STR, location = 'json')
parser.add_argument('sort_order', type = str, action = 'append', location = 'json')


class ResourceConfigList(Resource):
    def __init__(self):
        self.resource_parser = parser.copy()
        self.resource_parser.add_argument('resource_name', type = str, required = True, help = NO_RESOURCE_NAME_STR, location = 'json')

        super(ResourceConfigList, self).__init__()
    
    def get(self):
        return list(mongodbConnection.get_db().resourceConfig.find())

    def post(self):
        args = self.resource_parser.parse_args()
        resource_name = args["resource_name"]
        resource_value_relative_path = args["resource_value_relative_path"]
        sort_order = args["sort_order"]

        return insert_resource_config(resource_name, resource_value_relative_path, sort_order)

    def delete(self):
        args = self.resource_parser.parse_args()
        resource_name = args["resource_name"]

        remove_resource_config(resource_name)

        return {"resource_name": resource_name} , 200

class ResourceConfig(Resource):
    def __init__(self):
        self.resource_parser = parser.copy()
        super(ResourceConfig, self).__init__()

    def get(self, resource_name):
        return mongodbConnection.get_db().resourceConfig.find_one({"_id": resource_name})

    def post(self, resource_name):
        args = self.resource_parser.parse_args()
        resource_value_relative_path = args["resource_value_relative_path"]
        sort_order = args["sort_order"]

        return insert_resource_config(resource_name, resource_value_relative_path, sort_order)

    def delete(self, resource_name):
        remove_resource_config(resource_name)

        return {"resource_name": resource_name}, 200


from flask_restful import reqparse, abort
from flask_restful_swagger_2 import Api, swagger, Resource
from lib import mongodbConnection
from swagger_resources import resourceConfigResourceSwagger
import json
import requests
import configuration
from pymongo import ReturnDocument
from lib import aggregator, resourceLoader
from flask import Response
from bson.objectid import ObjectId
import logging
from lib.brainApiAccess import BrainApiAccess
logger = logging.getLogger(__name__)


NO_RESOURCE_NAME_STR = "No resource name provided!"
NO_RESOURCE_VALUE_PATH_STR = "No value path for resource provided!"

def insert_resource_config(resource_name, resource_value_relative_path, sort_order):
    mongodbConnection.get_db().resourceConfig.find_one_and_delete({"_id" : resource_name})
    mongodbConnection.get_db().resourceConfig.insert_one(
        {"_id": resource_name, "resource_value_relative_path" : resource_value_relative_path, "sort_order": sort_order, "resource_name": resource_name}
    )

    ret = mongodbConnection.get_db().resourceConfig.find_one({"_id" : resource_name}, {"_id": False})
    resourceLoader.writeResource(ret)

    return ret

def remove_resource_config(resource_name):
    resourceLoader.deleteResource(resource_name)
    return mongodbConnection.get_db().resourceConfig.find_one_and_delete({"_id" : resource_name}, {"_id": False})

parser = reqparse.RequestParser()
parser.add_argument('resource_value_relative_path', type = str, required = True, help = NO_RESOURCE_VALUE_PATH_STR, location = 'json')
parser.add_argument('sort_order', type = str, action = 'append', location = 'json')
parser.add_argument('resource_name', type = str, required = True, help = NO_RESOURCE_NAME_STR, location = 'json')


class ResourceConfig(Resource):

    @BrainApiAccess()
    def __init__(self):
        super(ResourceConfig, self).__init__()
    
    @swagger.doc({
        "description": "Get all configured Resources.",
        "tags": ["resources"],
        "responses": {
            "200": {
                "description": "Retrieved a json with a all configured Resources.",
                "schema": {
                    "type": "array",
                    "items": resourceConfigResourceSwagger.swagger_params[0]["schema"]
                }
            }
        }
    })
    def get(self):
        return list(mongodbConnection.get_db().resourceConfig.find({}, {"_id": False}))

    @swagger.doc({
        "description": "Insert/Update a Resource Config.",
        "tags": ["resources"],
        "parameters": resourceConfigResourceSwagger.swagger_params,
        "responses": {
            "200": {
                "description": "Retrieved a json with the inserted/updated Resource.",
                "schema": resourceConfigResourceSwagger.swagger_params[0]["schema"]
            }
        }
    })
    def post(self):
        args = parser.parse_args()
        resource_name = args["resource_name"]
        resource_value_relative_path = args["resource_value_relative_path"]
        sort_order = args["sort_order"]

        return insert_resource_config(resource_name, resource_value_relative_path, sort_order)

    @swagger.doc({
        "description": "Remove a Resource Config.",
        "tags": ["resources"],
        "parameters": resourceConfigResourceSwagger.swagger_params,
        "responses": {
            "200": {
                "description": "Retrieved a json with the name of the removed Resource.",
                "schema": resourceConfigResourceSwagger.swagger_params[0]["schema"]
            }
        }
    })
    def delete(self):
        args = parser.parse_args()
        resource_name = args["resource_name"]

        remove_resource_config(resource_name)

        return {"resource_name": resource_name}, 200
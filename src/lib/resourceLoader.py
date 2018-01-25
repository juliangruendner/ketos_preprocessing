import os
import json
import copy
import configuration
from lib import mongodbConnection
from bson.objectid import ObjectId
import logging
logger = logging.getLogger(__name__)


def loadResources():
    searchDir = os.path.join(os.path.dirname(__file__), '../fhir_resource_configs')

    for f in os.listdir(searchDir):
        path = os.path.join(searchDir, f)
        if os.path.isfile(path):
            config_file = open(path,'r')
            file_content = json.loads(config_file.read())

            mongodbConnection.get_db().resourceConfig.find_one_and_delete({"resource_name" : file_content["resource_name"]})
            mongodbConnection.get_db().resourceConfig.insert_one(
                {"_id": file_content["resource_name"], "resource_name" : file_content["resource_name"], "resource_mapping" : file_content["resource_mapping"]}
            )

            config_file.close()

def writeResource(resource_config):
    resource_copy = copy.copy(resource_config)
    config_dir = os.path.join(os.path.dirname(__file__), '../fhir_resource_configs')
    path = os.path.join(config_dir, resource_copy["resource_name"] + ".json")

    read_config_file = open(path, 'r')
    read_config_file_content = read_config_file.read()
    read_config_file.close()

    try:
        config_file = open(path,'w')
        config_file.write(str(json.dumps(resource_copy, indent=4)))
    except Exception:
        logger.error("Writing to resource file " + path + " failed", exc_info=1)
        config_file = open(path,'w')
        config_file.write(read_config_file_content)

    config_file.close()

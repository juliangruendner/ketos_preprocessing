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
            try:
                config_file = open(path,'r')
                file_content = json.loads(config_file.read())

                if(file_content["resource_name"] is None or file_content["resource_val_path"] is None):
                    raise ValueError('Wrong format of file. Must contain fields "resource_name" and "resource_val_path".')

                mongodbConnection.get_db().resourceConfig.find_one_and_delete({"_id" : file_content["resource_name"]})
                mongodbConnection.get_db().resourceConfig.insert_one({
                    "_id": file_content["resource_name"],
                    "resource_val_path": file_content["resource_val_path"],
                    "sort_order": file_content["sort_order"],
                    "resource_name": file_content["resource_name"],
                    "key_path": file_content.get("key_path"),
                    "key": file_content.get("key"),
                })

                logger.info("Added resource " + file_content["resource_name"] + " of file " + path + " to db.")
            except Exception:
                logger.error("Reading resource file " + path + " failed. Skipping.", exc_info=1)
                continue

            config_file.close()

def writeResource(resource_config):
    resource_copy = copy.copy(resource_config)
    config_dir = os.path.join(os.path.dirname(__file__), '../fhir_resource_configs')
    path = os.path.join(config_dir, resource_copy["resource_name"] + ".json")

    restore = False
    if os.path.isfile(path):
        restore = True
        read_config_file = open(path, 'r')
        read_config_file_content = read_config_file.read()
        read_config_file.close()

    try:
        config_file = open(path,'w')
        config_file.write(str(json.dumps(resource_copy, indent=4)))

        logger.info("Updated resource " + resource_copy["resource_name"] + " of file " + path + " to db.")
    except Exception:
        logger.error("Writing to resource file " + path + " failed", exc_info=1)
        if restore:
            config_file = open(path,'w')
            config_file.write(read_config_file_content)

    config_file.close()

def deleteResource(resource_name):
    config_dir = os.path.join(os.path.dirname(__file__), '../fhir_resource_configs')
    path = os.path.join(config_dir, resource_name + ".json")

    if os.path.isfile(path):
        logger.info("Removing resource file " + resource_name)
        os.remove(path)

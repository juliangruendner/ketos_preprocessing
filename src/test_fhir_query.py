import importlib
import configuration
import requests
import urllib
from fhirclient import client
from lib import mongodbConnection
from jsonreducer.ObservationReducer import ObservationReducer
from resources import aggregationResource
from bson.objectid import ObjectId
from datetime import datetime
import traceback
import logging
logger = logging.getLogger(__name__)
import sys
import time

settings = {
    'app_id': 'ketos_data',
    'api_base': configuration.HAPIFHIR_URL,
}

server = client.FHIRClient(settings=settings)

def crawlResourceForSubject(resourceName, subject, key, value):
    # Dynamically load module for resource
    try:
        resource = getattr(importlib.import_module("fhirclient.models." + resourceName.lower()), resourceName)
    except Exception:
        logger.error("Resource " + resourceName + " does not exist", exc_info=1)
        raise

    # Perform search
    try:

        serverSearchParams = {"patient": subject, key: value}
        '''if resourceName == 'Patient':
            serverSearchParams = {"_id": subject}
        else:
            serverSearchParams = {"patient": subject, key: value}
        '''
        search = resource.where(serverSearchParams)
        ret = search.perform_resources(server.server)
    except Exception:
        print("Search failed")
        raise
    
    for element in ret:
        element = resource.as_json(element)
        print(element)
    
    return ret


if __name__ == '__main__':
    # set false in production mode
    ret = crawlResourceForSubject("Condition", "1,34", "code", "68566005,56689002")



#GET http://hapi.fhir.org/baseDstu3/Patient?gender=male,female&_pretty=true
#GET http://hapi.fhir.org/baseDstu3/Patient?_filter=gendner=male
#{&filter=[mime-type]}}
#http://hapi.fhir.org/baseDstu3/Patient?{&filter=gender=male}
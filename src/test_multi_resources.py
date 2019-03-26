import importlib
import configuration
import requests
import urllib
from fhirclient import client
from fhirclient.models import bundle
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
import json

settings = {
    'app_id': 'ketos_data',
    'api_base': configuration.HAPIFHIR_URL,
}

f_input = '{"patient_ids": [6,1,34],"feature_set": [{"resource": "Observation","key": "code","value": "1060601000000109"},{    "resource": "Observation",    "key": "code",    "value": "279495008"},{    "resource": "Observation",    "key": "code",    "value": "422695002"},{    "resource": "Observation",    "key": "code",    "value": "422503008"},{    "resource": "Observation",    "key": "code",    "value": "425115008"},{    "resource": "Patient",    "key": "",    "value": "",    "name": "gender",    "resource_val_path": "gender"}    ]}'

server = client.FHIRClient(settings=settings)

def createResourceMap(feature_set):

    resource_map = {}

    for feature in feature_set:

        if feature['resource'] not in resource_map:
            resource_map[feature['resource']] = {}
            resource_map[feature['resource']]['feature_maps'] = {}
            resource_map[feature['resource']]['feature_list'] = []

        resource_map[feature['resource']]['feature_maps'][feature['value']] = {
            'name': feature.get('name'),
            'resource_val_path': feature.get('resource_val_path')
        }
        
        resource_map[feature['resource']]['feature_list'].append(feature['value'])
    
    for key, resource in resource_map.items():
        resource['feature_list'] = ','.join(map(str, resource['feature_list']))

    return resource_map


def crawlResourceForSubject(resourceName, subject, key, value):
    # Dynamically load module for resource
    return

if __name__ == '__main__':
    # set false in production mode
    #ret = crawlResourceForSubject("Condition", "12,3,4,5,6,7,8,9,10,11,12,13,14,15", "code", "")
    feature_input = json.loads(f_input)

    res_map = createResourceMap(feature_input['feature_set'])

    print(res_map['Observation']['feature_list'])
    print(res_map['Patient']['feature_list'])



#GET http://hapi.fhir.org/baseDstu3/Patient?gender=male,female&_pretty=true
#GET http://hapi.fhir.org/baseDstu3/Patient?_filter=gendner=male
#{&filter=[mime-type]}}
#http://hapi.fhir.org/baseDstu3/Patient?{&filter=gender=male}



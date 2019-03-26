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

settings = {
    'app_id': 'ketos_data',
    'api_base': configuration.HAPIFHIR_URL,
}

server = client.FHIRClient(settings=settings)

def getElemByPath(path, element):
    for cur_path in path.split("."):
        if isinstance(element, dict):
            element = element[cur_path]
        elif isinstance(element, list):
            element = element[0][cur_path]
    
    return element

def crawlResourceForSubject(resourceName, subject, key, value):
    # Dynamically load module for resource
    try:
        resource = getattr(importlib.import_module("fhirclient.models." + resourceName.lower()), resourceName)
    except Exception:
        logger.error("Resource " + resourceName + " does not exist", exc_info=1)
        raise

    # Perform search
    try:

        serverSearchParams = {"patient": subject, "_count": "100"}
        '''if resourceName == 'Patient':
            serverSearchParams = {"_id": subject}
        else:
            serverSearchParams = {"patient": subject, key: value}
        '''
        search = resource.where(serverSearchParams)
        ret = search.perform(server.server)
    except Exception:
        print("Search failed")
        raise

    insert_list = []
    next_page = True
    while next_page: 
        for entry_elem in ret.entry:
            ret_element = entry_elem.resource.as_json()
            cur_elem_key = getElemByPath("code.coding.code", ret_element)
            print(cur_elem_key, file=sys.stderr)
            name = code_inf_map[cur_elem_key]['name']
            resource_val_path = code_inf_map[cur_elem_key]['resource_val_path']

            element = resource.as_json(ret_element)
            element["_id"] = str(ObjectId())
            element["feature"] = cur_elem_key
            element["name"] = name if name is not None else cur_elem_key
             
            if resource_name == "Patient":
                element["patient_id"] = pat_ids
            elif resource_name == "Condition":
                element["patient_id"] = ret_element.patient.reference.replace("Patient/", "")
            else:
                element["patient_id"] = ret_element.subject.reference.replace("Patient/", "")

            element["resource_val_path"] = resource_val_path
            
            insert_list.append(element)

        if len(ret.link) < 2 or ret.link[1].relation != "next":
            next_page = False
            break
            
        res = server.server.request_json(ret.link[1].url)
        ret = bundle.Bundle(res)
    
    '''while ret.link[1].relation == "next":
        from fhirclient.models import bundle
        res = server.server.request_json(ret.link[1].url)
        ret = bundle.Bundle(res)
        print(ret.link[1].url)

    print(ret.link)
    print(len(ret.entry))
    print(ret.link[1].relation)
    print(ret.link[1].url)
    ret2 = server.server.request_json(ret.link[1].url)'''


    return 
    for element in ret:
        #print(element.patient.reference.replace("Patient/", ""))
        #print('{"resource": "Condition","key": "code.coding.code","value":"', element.code.coding[0].code, '","name": "', element.code.coding[0].display, '"},', sep="")
        element = resource.as_json(element)
        
    
    return ret


if __name__ == '__main__':
    # set false in production mode
    ret = crawlResourceForSubject("Condition", "12,13,14", "code", "")



#GET http://hapi.fhir.org/baseDstu3/Patient?gender=male,female&_pretty=true
#GET http://hapi.fhir.org/baseDstu3/Patient?_filter=gendner=male
#{&filter=[mime-type]}}
#http://hapi.fhir.org/baseDstu3/Patient?{&filter=gender=male}
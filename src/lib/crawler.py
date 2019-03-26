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
from fhirclient.models import bundle
import traceback


settings = {
    'app_id': 'ketos_data',
    'api_base': configuration.HAPIFHIR_URL,
}
server = client.FHIRClient(settings=settings)

def createCrawlerJob(crawler_id, crawler_status, patient_ids, feature_set, aggregation_type, resource_configs):
    from api import api

    if isinstance(patient_ids, str):
        patient_ids = [patient_ids]

    url_params = {"output_type": "csv", "aggregation_type": aggregation_type}
    url = "http://"+configuration.HOSTEXTERN+":"+str(configuration.WSPORT)+api.url_for(aggregationResource.Aggregation, crawler_id=crawler_id)+ "?" + urllib.parse.urlencode(url_params)

    crawlerJob =  {
        "_id": crawler_id,
        "patient_ids": patient_ids,
        "feature_set": feature_set,
        "resource_configs": resource_configs,
        "status": crawler_status,
        "finished": [],
        "queued_time": str(datetime.now()),
        "start_time": None,
        "url": url
    }

    mongodbConnection.get_db().crawlerJobs.insert_one(crawlerJob)
    return crawlerJob

def getResourceConfig(resource_name, resource_configs):
        # If resource config was not provided in crawler job -> read config from mongo db
        resource_configs = [] if resource_configs is None else resource_configs
        return next((c for c in resource_configs if c["resource_name"] == resource_name),
            mongodbConnection.get_db().resourceConfig.find_one({"_id": resource_name}))

def executeCrawlerJob(crawlerJob):
    mongodbConnection.get_db().crawlerJobs.update({"_id": crawlerJob["_id"]}, {"$set": {"status": "running", "start_time": str(datetime.now())}})

    try:

        resource_map = create_resource_map(crawlerJob["feature_set"])
        pat_ids = ','.join(crawlerJob["patient_ids"])
        
        for resource_name, resource in resource_map.items():
            crawlResourceGroupsForSubjects(resource_name, pat_ids, crawlerJob["_id"], resource['feature_list'], resource['feature_maps'], crawlerJob['resource_configs'])
            #crawlResourceGroupsForSubjects(resource_name, pat_ids, crawlerJob["_id"], feature["value"], feature.get('name'), feature.get('resource_val_path'))

        mongodbConnection.get_db().crawlerJobs.update({"_id": crawlerJob["_id"]}, {"$set": {"status": "finished", "end_time": str(datetime.now())}})
    except Exception as e:
        print("error executing crawler", e, file=sys.stderr)
        traceback.print_exc()
        logger.error("Execution of Crawler " + crawlerJob["_id"] + " failed", exc_info=1)
        mongodbConnection.get_db().crawlerJobs.update({"_id": crawlerJob["_id"]}, {"$set": {"status": "error", "end_time": str(datetime.now())}})
        return "error"

def crawlObservationForSubject(subject, collection, key, name):
    url_params = {"_pretty": "true", "subject": subject, "_format": "json", "_count": 100, key: name}

    next_page = configuration.HAPIFHIR_URL+"Observation"+'?'+urllib.parse.urlencode(url_params)

    all_entries = []
    while next_page != None:

        try:
            request = requests.get(next_page)

        except Exception as e:
            # this avoids connection refused when to many varialbes arre requested
            time.sleep(10)
            continue

        json = request.json()

        if "entry" not in json:
            return

        entries = json["entry"]

        if len(json["link"]) > 1 and json["link"][1]["relation"] == "next":
            next_page = json["link"][1]["url"]
        else:
            next_page = None

        all_entries += entries

    observations = []
    for entry in all_entries:
        reducer = ObservationReducer(entry["resource"])
        reduced = reducer.getReduced()
        #patient = reducer.getEntity()
        observations.append(reduced)

    mongodbConnection.get_db()[collection].find_one_and_update(
        { "_id": subject },
        {"$push": { "observations" : {"$each": observations}}},
        {"resource": "Observation"},
        upsert=True
    )

def create_resource_map(feature_set):

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

def crawlResourceGroupsForSubjects(resource_name, pat_ids, collection, values, code_inf_map, resource_configs):

    # Dynamically load module for resource
    key_path = getResourceConfig(resource_name, resource_configs).get('key_path')
    key = getResourceConfig(resource_name, resource_configs).get('key')

    try:
        resource = getattr(importlib.import_module("fhirclient.models." + resource_name.lower()), resource_name)
    except Exception:
        logger.error("Resource " + resource_name + " does not exist", exc_info=1)
        raise

    try:
        if resource_name == 'Patient':
            search_patient_resources(resource, pat_ids, values, resource_name, code_inf_map, resource_configs, key_path, collection)
            return
        else:
            serverSearchParams = {"patient": pat_ids, key: values}

        search = resource.where(serverSearchParams)
        ret = search.perform(server.server)
    except Exception:
        logger.error("Search failed", exc_info=1)
        raise

    if(len(ret.entry) == 0):
        logger.info("No values found for search for patients " + pat_ids + " on resource " + resource_name)
        return

    process_search_results(ret, resource_name, values, code_inf_map, resource_configs, key_path, collection)

def search_patient_resources(resource, pat_ids, values, resource_name, code_inf_map, resource_configs, key_path, collection):

    for pat_id in pat_ids.split(","):
        serverSearchParams = {"_id": pat_id}
        search = resource.where(serverSearchParams)
        ret = search.perform(server.server)

        if(len(ret.entry) == 0):
            logger.info("No values found for search for patients " + pat_id + " on resource " + resource_name)
            continue
        
        process_search_results(ret, resource_name, values, code_inf_map, resource_configs, key_path, collection)
    

def process_search_results(ret, resource_name, values, code_inf_map, resource_configs, key_path, collection):

    insert_list = []
    next_page = True

    while next_page:
        for entry_elem in ret.entry:
            if resource_name == 'Patient':
                process_patient_resource(insert_list, entry_elem, values, code_inf_map, resource_configs)
            else:
                process_resource(insert_list, entry_elem, resource_name, key_path, values, code_inf_map, resource_configs)

        if len(ret.link) < 2 or ret.link[1].relation != "next":
            next_page = False
            break
            
        res = server.server.request_json(ret.link[1].url)
        ret = bundle.Bundle(res)
    
    mongodbConnection.get_db()[collection].insert(list(insert_list))

def process_resource(insert_list, entry_elem, resource_name, key_path, values, code_inf_map, resource_configs):
    ret_element = entry_elem.resource
    element = ret_element.as_json()
    cur_elem_key = getElemByPath(key_path, element)

    name = code_inf_map[cur_elem_key]['name']
    resource_val_path = code_inf_map[cur_elem_key]['resource_val_path']
    element["_id"] = str(ObjectId())
    element["feature"] = cur_elem_key
    element["name"] = name if name is not None else cur_elem_key
    
    if resource_name == "Condition":
        element["patient_id"] = ret_element.patient.reference.replace("Patient/", "")
    else:
        element["patient_id"] = ret_element.subject.reference.replace("Patient/", "")

    element["resource_val_path"] = resource_val_path
    
    insert_list.append(element)


def process_patient_resource(insert_list, entry_elem, values, code_inf_map, resource_configs):

    ret_element = entry_elem.resource
    
    for value in values.split(","):
        element = ret_element.as_json()
        cur_elem_key = value
        name = code_inf_map[cur_elem_key]['name']
        resource_val_path = code_inf_map[cur_elem_key]['resource_val_path']
        element["_id"] = str(ObjectId())
        element["feature"] = cur_elem_key
        element["name"] = name if name is not None else cur_elem_key
        element["patient_id"] = ret_element.id
        element["resource_val_path"] = resource_val_path
        
        insert_list.append(element)



def crawlResourceForSubject(resourceName, pat_ids, collection, key, value, name, resource_val_path):
    # Dynamically load module for resource
    try:
        resource = getattr(importlib.import_module("fhirclient.models." + resourceName.lower()), resourceName)
    except Exception:
        logger.error("Resource " + resourceName + " does not exist", exc_info=1)
        raise

    # Perform search
    try:
        if resourceName == 'Patient':
            serverSearchParams = {"_id": pat_ids}
        else:
            serverSearchParams = {"patient": pat_ids, key: value}

        search = resource.where(serverSearchParams)
        ret = search.perform(server.server)
    except Exception:
        logger.error("Search failed", exc_info=1)
        raise

    if(len(ret.entry) == 0):
        logger.info("No values found for search for patients " + pat_ids + " on resource " + resourceName)
        return

    insert_list = []
    next_page = True
    while next_page:

        for entry_elem in ret.entry:
            ret_element = entry_elem.resource
            element = resource.as_json(ret_element)
            element["_id"] = str(ObjectId())
            element["feature"] = value 
            element["name"] = name if name is not None else value

            if resourceName == "Patient":
                element["patient_id"] = pat_ids
            elif resourceName == "Condition":
                element["patient_id"] = ret_element.patient.reference.replace("Patient/", "")
            else:
                element["patient_id"] = ret_element.subject.reference.replace("Patient/", "")

            if resource_val_path is not None:
                element["resource_val_path"] = resource_val_path
            
            insert_list.append(element)

        if len(ret.link) < 2 or ret.link[1].relation != "next":
            next_page = False
            break
            
        res = server.server.request_json(ret.link[1].url)
        ret = bundle.Bundle(res)

    mongodbConnection.get_db()[collection].insert(list(insert_list))

def getElemByPath(path, element):
    for cur_path in path.split("."):
        if isinstance(element, dict):
            element = element[cur_path]
        elif isinstance(element, list):
            element = element[0][cur_path]
    
    return element
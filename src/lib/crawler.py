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

    # TODO: add resource configurations as fields that overwrite default, e.g. "Condition": {...}
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

def executeCrawlerJob(crawlerJob):
    mongodbConnection.get_db().crawlerJobs.update({"_id": crawlerJob["_id"]}, {"$set": {"status": "running", "start_time": str(datetime.now())}})
    
    try:
        for subject in crawlerJob["patient_ids"]:
            for feature in crawlerJob["feature_set"]:
                if feature["resource"] == "Observation":
                    crawlObservationForSubject(subject, crawlerJob["_id"], feature["key"], feature["value"])
                else:
                    crawlResourceForSubject(feature["resource"], subject, crawlerJob["_id"], feature["key"], feature["value"], feature["name"])
             
            mongodbConnection.get_db().crawlerJobs.update({"_id": crawlerJob["_id"]}, {"$push": {"finished": subject}})

        mongodbConnection.get_db().crawlerJobs.update({"_id": crawlerJob["_id"]}, {"$set": {"status": "finished", "end_time": str(datetime.now())}})
        return "success"

    except Exception as e:
        print("-----------------")
        traceback.print_exc()
        print("-----------------")
        logger.error("Execution of Crawler " + crawlerJob["_id"] + " failed")
        mongodbConnection.get_db().crawlerJobs.update({"_id": crawlerJob["_id"]}, {"$set": {"status": "error", "end_time": str(datetime.now())}})
        return "error"

def crawlObservationForSubject(subject, collection, key, name):
    url_params = {"_pretty": "true", "subject": subject, "_format": "json", "_count": 100, key: name}

    next_page = configuration.HAPIFHIR_URL+"Observation"+'?'+urllib.parse.urlencode(url_params)
    print(next_page)

    all_entries = []
    
    while next_page != None:
        request = requests.get(next_page)
        json = request.json()

        if "entry" not in json:
            return

        entries = json["entry"]

        if len(json["link"]) > 1 and json["link"][1]["relation"] == "next" :
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

def crawlResourceForSubject(resourceName, subject, collection, key, value, name):
    # Dynamically load module for resource
    try:
        resource = getattr(importlib.import_module("fhirclient.models." + resourceName.lower()), resourceName)
    except Exception as e:
        logger.error("Resource " + resourceName + " does not exist", exc_info=1)
        raise

    # Perform search
    try:
        serverSearchParams = {"patient": subject, key: value}
        search = resource.where(serverSearchParams)
        ret = search.perform_resources(server.server)
    except Exception as e:
        logger.error("Search failed", exc_info=1)
        raise

    if(len(ret) == 0):
        logger.info("No values found for search for patient " + subject + " on resource " + resourceName)
        return

    insert_list = []
    for element in ret:
        element = resource.as_json(element)
        element["_id"] = str(ObjectId())

        # Add this for later selection in aggregation
        element["feature"] = value 
        element["name"] = name if name is not None else value
        
        insert_list.append(element)

    mongodbConnection.get_db()[collection].insert(list(insert_list))

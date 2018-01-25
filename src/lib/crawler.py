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
import logging
logger = logging.getLogger(__name__)

settings = {
    'app_id': 'ketos_data',
    'api_base': configuration.HAPIFHIR_URL,
}
server = client.FHIRClient(settings=settings)

def createCrawlerJob(crawler_id, crawler_status, patient_ids, feature_set, resource, search_params, resource_mapping, aggregation_type):
    from api import api

    if isinstance(patient_ids, str):
        patient_ids = [patient_ids]
    
    # Get mapping from db if no resource_mapping is given
    if resource_mapping is None:
        resource_mapping = mongodbConnection.get_db().resourceConfig.find_one({"resource_name": resource})
        if resource_mapping is not None:
            resource_mapping = resource_mapping["resource_mapping"]

    if aggregation_type is None:
        aggregation_type = "latest"

    url_params = {"output_type": "csv", "aggregation_type": aggregation_type}
    url = "http://"+configuration.HOSTEXTERN+":"+str(configuration.WSPORT)+api.url_for(aggregationResource.Aggregation, crawler_id=crawler_id)+ "?" + urllib.parse.urlencode(url_params)

    crawlerJob =  {
        "_id": crawler_id,
        "patient_ids": patient_ids,
        "feature_set": feature_set,
        "resource": resource,
        "search_params": search_params,
        "resource_mapping": resource_mapping,
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
            if crawlerJob["resource"] is not None and crawlerJob["resource"] is not "Observation":
                crawlResourceForSubject(crawlerJob["resource"], subject, crawlerJob["_id"], crawlerJob["search_params"])

            else:
                for feature in crawlerJob["feature_set"]:
                    crawlObservationForSubject(subject, crawlerJob["_id"], feature["key"], feature["value"])

            mongodbConnection.get_db().crawlerJobs.update({"_id": crawlerJob["_id"]}, {"$push": {"finished": subject}})

        mongodbConnection.get_db().crawlerJobs.update({"_id": crawlerJob["_id"]}, {"$set": {"status": "finished", "end_time": str(datetime.now())}})
        return "success"

    except Exception as e:
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
        upsert=True
    )

def crawlResourceForSubject(resourceName, subject, collection, searchParams):
    # Dynamically load module for resource
    try:
        resource = getattr(importlib.import_module("fhirclient.models." + resourceName.lower()), resourceName)
    except Exception as e:
        logger.error("Resource " + resourceName + " does not exist", exc_info=1)
        raise

    # Perform search
    try:
        serverSearchParams = {"patient": subject}
        serverSearchParams = {**serverSearchParams, **searchParams} if searchParams is not None else serverSearchParams
        search = resource.where(serverSearchParams)
        ret = search.perform_resources(server.server)
    except Exception as e:
        logger.error("Search failed", exc_info=1)
        raise

    if(len(ret) == 0):
        logger.info("No values found for search " + serverSearchParams + " on resource " + resourceName)

    insert_list = []
    for element in ret:
        element = resource.as_json(element)
        element["_id"] = str(ObjectId())
        insert_list.append(element)

    mongodbConnection.get_db()[collection].insert(list(insert_list))

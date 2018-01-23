import importlib
import configuration
from fhirclient import client
from lib import mongodbConnection
import logging
import requests
logger = logging.getLogger(__name__)

settings = {
    'app_id': 'ketos_data',
    'api_base': configuration.HAPIFHIR_URL,
}
server = client.FHIRClient(settings=settings)

def crawlObservationForSubject(subject, collection, key, value):
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
    except AttributeError as e:
        logger.error("Resource " + resourceName + " does not exist", exc_info=1)
        return

    # Perform search
    try:
        serverSearchParams = {"patient": subject, **searchParams}
        search = resource.where(serverSearchParams)
        ret = search.perform_resources(server.server)
    except Exception as e:
        logger.error("Search failed", exc_info=1)
        return

    if(len(ret) == 0):
        logger.info("No values found for search " + serverSearchParams + " on resource " + resourceName)

    mongodbConnection.get_db()[collection].find_one_and_update(
        { "_id": subject },
        {"$push": { "resourceValue" : {"$each": list(map(lambda x: resource.as_json(x), ret))}}},
        upsert=True
    )

import importlib
import configuration
from fhirclient import client
from lib import mongodbConnection
import logging
logger = logging.getLogger(__name__)

settings = {
    'app_id': 'ketos_data',
    'api_base': configuration.HAPIFHIR_URL,
}
server = client.FHIRClient(settings=settings)

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

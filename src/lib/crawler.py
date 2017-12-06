from lib import mongodbConnection
from jsonreducer.ObservationReducer import ObservationReducer
import configuration
import requests
import urllib.parse

# TODO: remove subject and read from features
def crawlResourceForSubject(resource, subject, collection, key, name):
    url_params = {"_pretty": "true", "subject": subject, "_format": "json", "_count": 100, key: name}

    next_page = configuration.HAPIFHIR_URL+resource+'?'+urllib.parse.urlencode(url_params)
    print(next_page)

    all_entries = []
    
    while next_page != None:
        request = requests.get(next_page)
        json = request.json()
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

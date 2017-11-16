import mongodbConnection
from jsonreducer.ObservationReducer import ObservationReducer
import configuration
import requests

def crawlResourceForSubject(resource, subject):
    next_page = configuration.HAPIFHIR_URL+resource+'?_pretty=true&subject='+subject+'&_format=json&_count=100'
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


    mongodbConnection.get_db().patients.find_one_and_replace(
        { "_id": subject },
        { "observations" : observations},
        new=True,
        upsert=True
    )

def crawlResource(resource, subjects):
    for subject in subjects:
        crawlResourceForSubject(resource, subject)
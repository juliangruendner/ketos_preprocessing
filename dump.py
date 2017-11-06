import requests
from urllib.parse import urlparse
from pymongo import MongoClient
import pprint
from bson.objectid import ObjectId
from ObservationReducer import ObservationReducer

# Setup
MONGOHOST = "localhost"
MONGOPORT = 27017
MONGODB = "database"
MONGOCOLLECTION = "collection"

client = MongoClient(MONGOHOST, MONGOPORT)
db = client[MONGODB]
collection = db[MONGOCOLLECTION]

allEntries = []

#pid = "test-1796238"
pid = "Patient/140570"
nextPage = 'http://fhirtest.uhn.ca/baseDstu3/Observation?_pretty=true&subject='+pid+'&_format=json&_count=100'


while nextPage != None:
    r = requests.get(nextPage)
    json = r.json()
    entries = json["entry"]

    if len(json["link"]) > 1 and json["link"][1]["relation"] == "next" :
        nextPage = json["link"][1]["url"]
    else:
        nextPage = None

    print(nextPage)

    allEntries += entries
    break

for entry in allEntries:
    reducer = ObservationReducer(entry["resource"])
    reduced = reducer.getReduced()
    patient = reducer.getEntity()

    db.patients.find_one_and_update(
      { "_id": patient },
      {"$push" : { "observations" : reduced}},
      new=True,
      upsert=True
    )

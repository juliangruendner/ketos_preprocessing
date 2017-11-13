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
#pid = "Patient/140570"
pid = "185"
nextPage = 'http://gruendner.de:8080/gtfhir/base/Observation?_pretty=true&subject='+pid+'&_format=json&_count=100'


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

    result = db.patients.aggregate([
        {"$unwind": "$observations"},
        {"$group" : {"_id" : {"attribute": "$observations.attribute", "patient_id": patient}, "entry": {"$push": "$$CURRENT.observations"}}},
        {"$unwind": "$entry"},
        {"$sort"  : {"entry.value": -1}},
        {"$group" : {"_id": "$_id", "observations": {"$push": "$entry"}}},
    ])


# ALL
# [
#     { $unwind: "$observations"},
#     { $group : {_id : {attribute: "$observations.attribute", patient_id: "$_id"}, entry: { $push: "$$CURRENT.observations"}}},
#     { $unwind: "$entry"},
#     { $sort  : {"entry.timestamp": -1}},
#     { $group : {_id: "$_id", observations: {$push: "$entry"}}}, # use first, last instead of push for other semantic
#     { $group : {_id: "$_id.patient_id", observations: { $push: "$$CURRENT.observations"}}}
# ]
# AVG
# [
#     { $unwind: "$observations"},
#     { $group : {_id : {attribute: "$observations.attribute", patient_id: "$_id"}, entry: { $push: "$$CURRENT.observations"}}},
#     { $unwind: "$entry"},
#     { $sort  : {"entry.timestamp": -1}},
#     { $group : {_id: "$_id" , attribute: { $first: "$_id.attribute" }, observations: { $avg: "$entry.value"}}},
#     { $group : {_id: "$_id.patient_id", observations: { $push: {avg: "$$CURRENT.observations", attribute: "$_id.attribute"}}}}
# ]

    # db.patients.update_one(
    #     {"_id": patient},
    #     {"$unset": {"observations": ""}},
    # )

    # for entry in result:
    #     print(entry)
    #     print("---------------------------------")
    #     db.patients.update_one(
    #         {"_id": patient},
    #         {"$push": {"observations": entry}}
    #     )

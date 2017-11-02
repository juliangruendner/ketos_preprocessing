# http://api.mongodb.com/python/current/tutorial.html
from pymongo import MongoClient
import pprint
from bson.objectid import ObjectId

# Setup
MONGOHOST = "localhost"
MONGOPORT = 27017
MONGODB = "database"
MONGOCOLLECTION = "collection"

client = MongoClient(MONGOHOST, MONGOPORT)
db = client[MONGODB]
collection = db[MONGOCOLLECTION]

# Insert patient
patient = {
  "resourceType": "Patient",
  "id": "1c6299f7-2d06-4b5d-9efb-a8216a405a92",
  "meta": {
    "versionId": "8",
    "lastUpdated": "2017-10-31T12:52:38.564-04:00"
  },
  "gender": "male",
  "birthDate": "1967-06-20",
  "address": [
    {
      "line": [
        "123 Yonge St."
      ],
      "city": "Toronto",
      "state": "ON",
      "postalCode": "M1T3P0",
      "country": "CA"
    }
  ],
  "maritalStatus": {
    "coding": [
      {
        "system": "http://hl7.org/fhir/v3/MaritalStatus",
        "code": "M"
      }
    ],
    "text": "M"
  }
}

patient_id = db.patients.insert_one(patient).inserted_id

# Access patient field
print("------------------------ By ID ------------------------")
pprint.pprint(db.patients.find_one({"_id": patient_id}))
print("------------------------ By gender: male ------------------------")
pprint.pprint(db.patients.find_one({"gender": "male"}))
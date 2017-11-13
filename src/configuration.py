import os
from pymongo import MongoClient

MONGOHOST = os.environ.get("MONGOHOST", "localhost")
MONGOPORT = os.environ.get("MONGOPORT", 27017)
MONGOCLIENT = MongoClient(MONGOHOST, MONGOPORT)
MONGODB = MONGOCLIENT[os.environ.get("MONGOBD", "dataws")]
HAPIFHIR_URL = os.environ.get("HAPIFHIR_URL", "http://fhirtest.uhn.ca/baseDstu3/")
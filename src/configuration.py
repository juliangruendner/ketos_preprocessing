import os

MONGOHOST = os.environ.get("MONGOHOST", "localhost")
MONGOPORT = int(os.environ.get("MONGOPORT", 27017))
MONGOTABLE = os.environ.get("MONGOBD", "dataws")
HAPIFHIR_URL = os.environ.get("HAPIFHIR_URL", "http://fhirtest.uhn.ca/baseDstu3/")
DEBUG = os.environ.get("DEBUG", True)
WSHOST = os.environ.get("WSHOST", "0.0.0.0")
WSPORT = int(os.environ.get("WSPORT", 5000))
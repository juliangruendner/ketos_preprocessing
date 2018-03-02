import os

HOSTEXTERN = os.environ.get("HOSTEXTERN", "localhost")
MONGOHOST = os.environ.get("MONGOHOST", "localhost")
MONGOPORT = int(os.environ.get("MONGOPORT", 27017))
MONGOTABLE = os.environ.get("MONGOBD", "dataws")
HAPIFHIR_URL = os.environ.get("HAPIFHIR_URL", "http://ketos.ai:8080/gtfhir/base/")
DEBUG = os.environ.get("DEBUG", True)
WSHOST = os.environ.get("WSHOST", "0.0.0.0")
WSPORT = int(os.environ.get("WSPORT", 5000))
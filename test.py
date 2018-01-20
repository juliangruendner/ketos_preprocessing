import io
import sys
import csv
import importlib
from fhirclient import client
from functools import reduce

settings = {
    'app_id': 'my_web_app',
    'api_base': 'http://fhirtest.uhn.ca/baseDstu3',
}
server = client.FHIRClient(settings=settings)

resource = "Condition"
concept = {"subject": "10203"}
mappings = {"Condition/code/text": "Condition"}
conditions = {"clinical-status": "inactive"} # maybe automatically convert from camelcase

lines = []

try:
    resource = getattr(importlib.import_module("fhirclient.models." + resource.lower()), resource)
except AttributeError as e:
    print("Resource", resource, "does not exist")
    sys.exit()

try:
    searchParams = {**concept, **conditions}
    search = resource.where(searchParams)
    ret = search.perform_resources(server.server)
except Exception as e:
    print("Search failed")
    sys.exit()

if(len(ret) == 0):
    print("No values found for search", searchParams, "on resource", resource)
    sys.exit()

for element in ret:
    line = concept.copy()
    for resourcePath, targetName in mappings.items():
        line[targetName] = reduce(getattr, resourcePath.split("/")[1:], element) # "deep" getattr
        
        # if not isinstance(line[targetName], [bool, str, int, float]):
        #     print("Value of path", resourcePath, "is a non primitive type! Only use paths that lead to primitive types.")
        #     sys.exit()

    lines.append(line)

fieldnames = set(concept.keys())
for val in mappings.values():
    fieldnames.add(val)

output = io.StringIO()
writer = csv.DictWriter(output, fieldnames=fieldnames)
writer.writeheader()

for line in lines:
    writer.writerow(line)

print(output.getvalue())
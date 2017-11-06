import requests
from urllib.parse import urlparse


limit = 400
ids = []

nextPage = 'http://fhirtest.uhn.ca/baseDstu3/Patient?_pretty=true&_format=json&_count=100'

while len(ids) < limit:
    r = requests.get(nextPage)
    entries = r.json()["entry"]

    nextPage = r.json()["link"][1]["url"]

    print(nextPage)

    for entry in entries:
        ids.append(entry["resource"]["id"])

print(ids)

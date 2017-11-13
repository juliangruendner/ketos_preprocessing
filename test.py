from jsonreducer.ObservationReducer import ObservationReducer
import requests

nextPage = 'http://fhirtest.uhn.ca/baseDstu3/Observation/test-289?_format=json'
r = requests.get(nextPage)
json = r.json()

test = ObservationReducer(json)
print(test.getReduced())

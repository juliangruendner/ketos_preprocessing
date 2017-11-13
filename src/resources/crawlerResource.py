from flask_restful import Resource, Api, reqparse, abort
from jsonreducer.ObservationReducer import ObservationReducer
import requests
import configuration


class Crawler(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('data', type = str, required = True, help = 'No data provided', location = 'json')
        super(Crawler, self).__init__()

    def get(self, patient_id):
        return self.post(patient_id)

    def post(self, patient_id):
        nextPage = configuration.HAPIFHIR_URL+'Observation?_pretty=true&subject='+patient_id+'&_format=json&_count=100'
        allEntries = []
        print("XXX"+nextPage)
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

            configuration.MONGODB.patients.find_one_and_update(
            { "_id": patient },
            {"$push" : { "observations" : reduced}},
            new=True,
            upsert=True
            )


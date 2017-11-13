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
        next_page = configuration.HAPIFHIR_URL+'Observation?_pretty=true&subject='+patient_id+'&_format=json&_count=100'
        all_entries = []
        
        while next_page != None:
            request = requests.get(next_page)
            json = request.json()
            entries = json["entry"]

            if len(json["link"]) > 1 and json["link"][1]["relation"] == "next" :
                next_page = json["link"][1]["url"]
            else:
                next_page = None

            all_entries += entries

        observations = []
        for entry in all_entries:
            reducer = ObservationReducer(entry["resource"])
            reduced = reducer.getReduced()
            #patient = reducer.getEntity()
            observations.append(reduced)

        configuration.MONGODB.patients.find_one_and_replace(
            { "_id": patient_id },
            { "observations" : observations},
            new=True,
            upsert=True
        )


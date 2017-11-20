from flask_restful import Resource, Api, reqparse, abort
from lib import mongodbConnection


class Features(Resource):
    def __init__(self):
        super(Features, self).__init__()

    def get(self, crawler_id):
        mongorequest = [
            {"$unwind": "$observations"},
            {"$group": {"_id": {"attribute": "$observations.attribute", "patient_id": "$_id"}, 
                "attribute": {"$first": "$$CURRENT.observations.attribute"}, 
                "meta": {"$first": "$$CURRENT.observations.meta"}}
            },
            {"$group": {"_id": "$_id.patient_id", "attributes": {"$push": {"attribute": "$$CURRENT.attribute", "meta": "$$CURRENT.meta"}}}}
        ]

        return list(mongodbConnection.get_db()[crawler_id].aggregate(mongorequest))
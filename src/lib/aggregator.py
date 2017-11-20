from jsonreducer.ObservationReducer import ObservationReducer
from lib import mongodbConnection
import configuration
import csv
import io


def aggregate(crawler_id, aggtype):
    mongorequest = [
        {"$unwind": "$observations"},
        {"$group" : {"_id" : {"attribute": "$observations.attribute", "patient_id": "$_id"}, "entry": {"$push": "$$CURRENT.observations"}}},
        {"$unwind": "$entry"},
        {"$sort"  : {"entry.timestamp": 1}}
    ]

    if aggtype == None or aggtype.lower() == "all":
        mongorequest += [
            {"$group" : {"_id": "$_id", "observations": {"$push": "$entry"}}},
            {"$group" : {"_id": "$_id.patient_id", "observations": { "$push": "$$CURRENT.observations"}}}
        ]
    elif aggtype.lower() == "latest" or aggtype.lower() == "oldest":
        tmp = ""
        
        if aggtype.lower() == "oldest":
            tmp = "first"
        else :
            tmp = "last"

        mongorequest += [
            {"$group" : {"_id": "$_id", "observations": {"$"+tmp: "$entry"}}},
            {"$group" : {"_id": "$_id.patient_id", "observations": { "$push": "$$CURRENT.observations"}}}
        ]
    elif aggtype.lower() == "avg":
        mongorequest += [
            {"$group" : {"_id": "$_id" , "attribute": { "$first": "$_id.attribute" }, "observations": { "$avg": "$entry.value"}}},
            {"$group" : {"_id": "$_id.patient_id", "observations": { "$push": {"avg": "$$CURRENT.observations", "attribute": "$_id.attribute"}}}}
        ]
    else:
        return None

    result = mongodbConnection.get_db()[crawler_id].aggregate(mongorequest)
    return list(result)


def aggregateCSV(crawler_id, aggtype, features):

    result = aggregate(crawler_id, aggtype)
    all_features = {}
    for patient in result:
        for observation in patient["observations"]:
                all_features[observation["attribute"]] = observation["meta"]["attribute"].lower()

    output = io.StringIO()

    fieldnames = ["subject"]

    if not features:
        for feature in all_features:
            fieldnames.append(all_features[feature])
    else:
        for feature in features:
            fieldnames.append(all_features[feature])

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for patient in result:
        row = {}
        row["subject"] = patient["_id"]
        for observation in patient["observations"]:
            if observation["attribute"] in features or not features:
                row[all_features[observation["attribute"]]] = observation["value"]
        writer.writerow(row)

    return output.getvalue()
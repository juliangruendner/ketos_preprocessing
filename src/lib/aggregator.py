import csv
import io
from lib import mongodbConnection
from functools import reduce
import logging
logger = logging.getLogger(__name__)

def aggregateFeatures(crawler_id, aggtype):
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
        tmp = "first" if aggtype.lower() == "oldest" else "last"
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


def writeFeaturesCSV(aggregated, features):
    features = None
    all_features = {}
    for patient in aggregated:
        for observation in patient["observations"]:
                all_features[observation["attribute"]] = observation["meta"]["attribute"].lower()

    output = io.StringIO()

    fieldnames = ["subject"]

    if not features:
        for feature in all_features:
            fieldnames.append(all_features[feature])
    else:
        for feature in features:
            fieldnames.append(all_features[feature["type"]["code"]])

    lines = []
    for patient in aggregated:
        row = {}
        row["subject"] = patient["_id"]
        for observation in patient["observations"]:
            if observation["meta"]["attribute"].lower() in fieldnames or not features:
                col_name = all_features[observation["attribute"]]
                if isinstance(observation["value"], list):
                    for idx, val in enumerate(observation["value"]):
                        tmp_col = col_name+"."+str(idx)
                        row[tmp_col] = val     
                        fieldnames.append(tmp_col) 
                else:
                    row[col_name] = observation["value"]
        lines.append(row)

    writer = csv.DictWriter(output, fieldnames=set(fieldnames))
    writer.writeheader()

    for line in lines:
        writer.writerow(line)

    return output.getvalue()

def writeCSV(db_content, resourceMapping):
    lines = []

    # Map values of returned objects to new row names
    for element in db_content:
        line = {"patient": element["patient"]["reference"]}
        for resourcePath, targetName in resourceMapping.items():
            try:
                line[targetName] = reduce(dict.get, resourcePath.split("/")[1:], element) # "deep" getattr
                
                if not isinstance(line[targetName], (bool, str, int, float)):
                    logger.error("Value of path " +resourcePath + " is a non primitive type! Only use paths that lead to primitive types.")

            except Exception as e:
                logger.warn("Path " +  resourcePath + " does not exist in element with id " + element["id"] + ". None is inserted.")
                line[targetName] = None

        lines.append(line)

    # Write .csv
    fieldnames = ["patient"]
    for val in resourceMapping.values():
        fieldnames.append(val)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for line in lines:
        writer.writerow(line)

    return output.getvalue()
import csv
import io
from lib import mongodbConnection
from functools import reduce
import logging
logger = logging.getLogger(__name__)


class Aggregator():

    def __init__(self, crawler_id, aggregation_type, feature_set, resource_configs):
        self.crawler_id = crawler_id
        self.aggregation_type = aggregation_type
        self.feature_set = feature_set
        self.resource_configs = [] if resource_configs is None else resource_configs

        self.aggregatedElements = []

    def getResourceConfig(self, resource_name):
        # If resource config was not provided in crawler job -> read config from mongo db
        return next((c for c in self.resource_configs if c["resource_name"] == resource_name),
            mongodbConnection.get_db().resourceConfig.find_one({"_id": resource_name}))
                
    def aggregateObservations(self):
        mongorequest = [
            {"$unwind": "$observations"},
            {"$group" : {"_id" : {"attribute": "$observations.attribute", "patient_id": "$_id"}, "entry": {"$push": "$$CURRENT.observations"}}},
            {"$unwind": "$entry"},
            {"$sort"  : {"entry.timestamp": 1}}
        ]

        if self.aggregation_type == "" or self.aggregation_type == "all":
            mongorequest += [
                {"$group" : {"_id": "$_id", "observations": {"$push": "$entry"}}},
                {"$group" : {"_id": "$_id.patient_id", "observations": { "$push": "$$CURRENT.observations"}}}
            ]
        elif self.aggregation_type == "latest" or self.aggregation_type == "oldest":        
            tmp = "first" if self.aggregation_type == "oldest" else "last"
            mongorequest += [
                {"$group" : {"_id": "$_id", "observations": {"$"+tmp: "$entry"}}},
                {"$group" : {"_id": "$_id.patient_id", "observations": { "$push": "$$CURRENT.observations"}}}
            ]
        elif self.aggregation_type == "avg":
            mongorequest += [
                {"$group" : {"_id": "$_id" , "attribute": { "$first": "$_id.attribute" }, "observations": { "$avg": "$entry.value"}}},
                {"$group" : {"_id": "$_id.patient_id", "observations": { "$push": {"avg": "$$CURRENT.observations", "attribute": "$_id.attribute"}}}}
            ]
        else:
            return None

        result = list(mongodbConnection.get_db()[self.crawler_id].aggregate(mongorequest))
        for res in result:
            res["resourceType"] = "Observation"
        
        self.aggregatedElements.extend(result)

    def aggregateFeature(self, resource, selection):
        resource_config = self.getResourceConfig(resource)
               
        mongorequest = [
            {"$match": {"feature": selection, "resourceType": resource}}
        ]

        foundSearchPath = False
        allElementsForFeature = list(mongodbConnection.get_db()[self.crawler_id].aggregate(mongorequest + [
            {"$group": {"_id": None, "count": {"$sum": 1}}}
        ]))

        if len(allElementsForFeature) == 0:
            raise ValueError("Feature " + selection + " of resource " + resource + " has no elements.")

        numAllElementsForFeature = allElementsForFeature[0]["count"]

        if resource_config["sort_order"] is not None:       
            for sortPath in resource_config["sort_order"]:
                mongoSortPath = ".".join(sortPath.split("/")) # Change "/" to "."

                elementsWithPath = list(mongodbConnection.get_db()[self.crawler_id].aggregate(mongorequest + [
                    {"$match": {mongoSortPath: {"$exists": True }}},
                    {"$group": {"_id": None, "count": {"$sum": 1}}}
                ]))

                if len(elementsWithPath) == 0:
                    continue

                numElementsWithPath = elementsWithPath[0]["count"]

                # Check if every element has attribute search path
                if numAllElementsForFeature == numElementsWithPath:
                    mongorequest += [
                        {"$sort": {mongoSortPath: 1}}
                    ]
                    foundSearchPath = True
                    break
        else:
            logger.warning("No sort order provided.")

        if foundSearchPath:
            sortedFeature = list(mongodbConnection.get_db()[self.crawler_id].aggregate(mongorequest))
                    
            if self.aggregation_type == "" or self.aggregation_type == "all":
                self.aggregatedElements.extend(sortedFeature)
            elif self.aggregation_type == "latest":
                self.aggregatedElements.append(sortedFeature[-1])
            elif self.aggregation_type == "oldest":
                self.aggregatedElements.append(sortedFeature[0])
        else:
            raise ValueError("Elements have different fields to sort! Sorting not possible.")

    def getAggregated(self):
        return self.aggregatedElements

    def getCSVOfAggregated(self):
        lines = [] # for csv: contains a row for every patient with respective columns
        fieldnames = ["patient"]
        
        # Write .csv
        for element in self.aggregatedElements:
            addDict = {}
            currentPatient = ""

            if element["resourceType"] == "Observation":
                currentPatient = element["_id"]

                for observation in element["observations"]:
                    col_name = observation["meta"]["attribute"]
                    fieldnames.append(col_name)
                    if isinstance(observation["value"], list):
                        for idx, val in enumerate(observation["value"]):
                            tmp_col = col_name+"."+str(idx)
                            addDict[tmp_col] = val
                            fieldnames.append(tmp_col) 
                    else:
                        addDict[col_name] = observation["value"]
                
            else:
                currentPatient = element["patient"]["reference"]
                fieldnames.append(element["name"])
                
                resource_config = self.getResourceConfig(element["resourceType"])
                
                cur = element
                for path in resource_config["resource_value_relative_path"].split("/"):
                    if isinstance(cur, dict):
                        cur = cur[path]
                    elif isinstance(cur, list):
                        cur = cur[0][path]

                addDict[element["name"]] = cur

            if "Patient/" not in currentPatient:
                currentPatient = "Patient/" + currentPatient

            # Append value if line already exists in lines
            didAddLine = False
            for i, line in enumerate(lines):
                if line["patient"] == currentPatient:
                    lines[i] = {**line, **addDict}
                    didAddLine = True
            
            if not didAddLine:
                lines.append({"patient": currentPatient, **addDict})
        
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for line in lines:
            writer.writerow(line)
        
        return output.getvalue()

    def aggregate(self):
        isObservationAdded = False

        for feature in self.feature_set:   
            if feature["resource"] == "Observation":
                if not isObservationAdded:
                    self.aggregateObservations()
                    isObservationAdded = True
            else:
                try:
                    self.aggregateFeature(feature["resource"], feature["value"])
                except ValueError:
                    logger.warning("Elements of Feature " + feature["value"] + " from resource " + feature["resource"] + " could not be retrieved.")
                    continue
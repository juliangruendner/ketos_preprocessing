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
        self.restructuredElements = []

    def getAggregated(self):
        return self.aggregatedElements

    def getResourceConfig(self, resource_name):
        # If resource config was not provided in crawler job -> read config from mongo db
        return next((c for c in self.resource_configs if c["resource_name"] == resource_name),
            mongodbConnection.get_db().resourceConfig.find_one({"_id": resource_name}))

### Aggregation of crawled data ###
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
        # Before actually sorting check if every single element has the attribute that should be sorted after -> throw error if they do not
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
            foundSearchPath = True

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

    def restructureElements(self):
        for element in self.aggregatedElements:
            addDict = {}
            currentPatient = ""

            if element["resourceType"] == "Observation":
                currentPatient = element["_id"]

                for observation in element["observations"]:
                    col_name = observation["meta"]["attribute"]
                    if isinstance(observation["value"], list):
                        for idx, val in enumerate(observation["value"]):
                            tmp_col = col_name+"."+str(idx)
                            addDict[tmp_col] = val
                    else:
                        addDict[col_name] = observation["value"]
                
            else:
                currentPatient = element["patient"]["reference"]
                
                resource_config = self.getResourceConfig(element["resourceType"])
                
                cur = element
                for path in resource_config["resource_value_relative_path"].split("/"):
                    if isinstance(cur, dict):
                        cur = cur[path]
                    elif isinstance(cur, list):
                        cur = cur[0][path]

                addDict[element["name"]] = cur

            if "Patient/" in currentPatient:
                currentPatient = currentPatient.replace("Patient/", "")

            # Append value if line already exists in lines
            didAddLine = False
            for i, line in enumerate(self.restructuredElements):
                if line["patient"] == currentPatient:
                    self.restructuredElements[i] = {**line, **addDict}
                    didAddLine = True
            
            if not didAddLine:
                self.restructuredElements.append({"patient": currentPatient, **addDict})

    def getRestructured(self):
        ret = []
        for element in self.restructuredElements:
            insert = {"patient_id": element["patient"], "entries": []}
            for key, value in element.items():
                if key != "patient":
                    insert["entries"].append({key: value})
            ret.append(insert)

        return ret

    def getCSVOfAggregated(self):
        fieldnames = ["patient"]

        for element in self.restructuredElements:
            fieldnames.extend(element.keys())
        
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=sorted(list(set(fieldnames))))
        writer.writeheader()

        for line in self.restructuredElements:
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

        if self.aggregation_type == "latest" or self.aggregation_type == "oldest":
            self.restructureElements()
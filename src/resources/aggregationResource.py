from flask_restful import Resource, Api, reqparse, abort
from lib import mongodbConnection
import requests
import configuration
import csv
import io
from functools import reduce
from lib import aggregator
from flask import Response
import logging
logger = logging.getLogger(__name__)


def getSortedFeature(crawler_id, resource, selection, aggregation_type, sortPaths):
    mongorequest = [
        {"$match": {"feature": selection, "resourceType": resource}}
    ]

    foundSearchPath = False
    allElementsForFeature = list(mongodbConnection.get_db()[crawler_id].aggregate(mongorequest + [
        {"$group": {"_id": None, "count": {"$sum": 1}}}
    ]))

    if len(allElementsForFeature) == 0:
        raise ValueError("Feature " + selection + " of resource " + resource + " has no elements.")

    numAllElementsForFeature = allElementsForFeature[0]["count"]

    if sortPaths is not None:        
        for sortPath in sortPaths:
            mongoSortPath = ".".join(sortPath.split("/")) # Change "/" to "."

            elementsWithPath = list(mongodbConnection.get_db()[crawler_id].aggregate(mongorequest + [
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

    if foundSearchPath:
        return list(mongodbConnection.get_db()[crawler_id].aggregate(mongorequest))
    else:
        raise ValueError("Elements have different fields to sort! Sorting not possible.")


class Aggregation(Resource):
    allowedAggregationTypes = ["all", "latest", "oldest"]

    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('aggregation_type', type = str, required = True, help = 'No aggregation type provided', location = 'args')
        self.parser.add_argument('output_type', type = str, help = 'No output type provided', location = 'args')

        super(Aggregation, self).__init__()

    def get(self, crawler_id):
        args = self.parser.parse_args()
        aggregation_type = args["aggregation_type"]
        output_type = args["output_type"]

        crawlerJob = mongodbConnection.get_db().crawlerJobs.find_one({"_id": crawler_id})
        if crawlerJob["status"] != "finished":
            return "Crawler Job did not finish yet", 404

        if aggregation_type.lower() not in self.allowedAggregationTypes:
            return "Wrong aggregation type provided: " + aggregation_type, 400
        
        if aggregation_type.lower() == "all" and output_type == "csv":
            return "Time series not supported", 400

        allElements = []

        for feature in crawlerJob["feature_set"]:
            if feature["resource"] == "Observation":
                pass # TODO
            else:
                # TODO: read resource config from crawler_job, if provided
                resourceConfig = mongodbConnection.get_db().resourceConfig.find_one({"_id": feature["resource"]})

                try:
                    sortedFeature = getSortedFeature(crawler_id, feature["resource"], feature["value"], aggregation_type, resourceConfig["sort_order"])
                except ValueError:
                    logger.warning("Elements of Feature " + feature["value"] + " from resource " + feature["resource"] + " could not be retrieved.")
                    continue

                if aggregation_type.lower() == "all":
                    allElements.append(sortedFeature)
                elif aggregation_type.lower() == "latest":
                    allElements.append(sortedFeature[-1])
                elif aggregation_type.lower() == "oldest":
                    allElements.append(sortedFeature[0])

        if output_type == "csv":
            lines = [] # for csv: contains a row for every patient with respective columns
            fieldnames = ["patient"]
            
            # Write .csv
            for element in allElements:
                resourceConfig = mongodbConnection.get_db().resourceConfig.find_one({"_id": element["resourceType"]})
                fieldnames.append(element["name"])

                value = reduce(dict.get, resourceConfig["resource_value_relative_path"].split("/"), element) # "deep" getattr
                line = {"patient": element["patient"]["reference"], element["name"]: value}
                lines.append(line)
           
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()

            for line in lines:
                writer.writerow(line)

            return Response(output.getvalue(), mimetype='text/csv')
        
        return allElements 

        # ret = None
        # if resource == "Observation":
        #     ret = aggregator.aggregateFeatures(crawler_id, aggregation_type)

        #     if output_type == "csv":
        #         result = aggregator.writeFeaturesCSV(ret)
        #         ret = Response(result, mimetype='text/csv')
        # else:
        #     ret = list(mongodbConnection.get_db()[crawler_id].find())

        #     if output_type == "csv":
        #         result = aggregator.writeCSV(ret, resourceMapping)
        #         ret = Response(result, mimetype='text/csv')
        
        # return ret


    def post(self):
        return None


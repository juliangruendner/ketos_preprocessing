import unittest
import json
import os

import api
from lib import mongodbConnection
from lib.aggregator import Aggregator

class AggregatorTest(unittest.TestCase):
    aggregatorTestTable = "aggregatorTest"
    crawler_job_path = os.path.join(os.path.dirname(__file__), "crawler_job.json")
    crawler_data_path = os.path.join(os.path.dirname(__file__), "crawler_data.json")

    generatedDir = os.path.join(os.path.dirname(__file__), "generated")
    referenceDir = os.path.join(os.path.dirname(__file__), "reference")

    # Insert test data into MongoDB
    def setUp(self):
        with api.app.app_context(): # necessary because mongodbConnection uses flask functionality
            # TODO: would be better to start MongoDB from here instead of relying that it"s already started
            mongodbConnection.get_db()[self.aggregatorTestTable].delete_many({})

            crawler_data = open(self.crawler_data_path,"r")
            crawler_data_json = json.loads(crawler_data.read())
            mongodbConnection.get_db()[self.aggregatorTestTable].insert(crawler_data_json)

            job_info = open(self.crawler_job_path,"r")
            self.job_info_json = json.loads(job_info.read())

    def test_latest_json(self):
        with api.app.app_context():
            reference = os.path.join(self.referenceDir, "test_latest_json.json")
            generated = os.path.join(self.generatedDir, "test_latest_json.json")

            aggregator = Aggregator(self.aggregatorTestTable, "latest", self.job_info_json["feature_set"], self.job_info_json["resource_configs"])
            aggregator.aggregate()

            ret = aggregator.getRestructured()
            
            # Write results to file for better comparison on error
            generated_file = open(generated,"w")
            generated_file.write(str(json.dumps(ret, indent=2)))
            generated_file.close()

            self.assertEqual(json.loads(open(generated, "r").read()), json.loads(open(reference, "r").read()))
    
    def test_oldest_json(self):
        with api.app.app_context():
            reference = os.path.join(self.referenceDir, "test_oldest_json.json")
            generated = os.path.join(self.generatedDir, "test_oldest_json.json")

            aggregator = Aggregator(self.aggregatorTestTable, "oldest", self.job_info_json["feature_set"], self.job_info_json["resource_configs"])
            aggregator.aggregate()

            ret = aggregator.getRestructured()

            # Write results to file for better comparison on error
            generated_file = open(generated,"w")
            generated_file.write(str(json.dumps(ret, indent=2)))
            generated_file.close()

            self.assertEqual(json.loads(open(generated, "r").read()), json.loads(open(reference, "r").read()))

    def test_latest_csv(self):
        with api.app.app_context():
            reference = os.path.join(self.referenceDir, "test_latest_csv.txt")
            generated = os.path.join(self.generatedDir, "test_latest_csv.txt")

            aggregator = Aggregator(self.aggregatorTestTable, "latest", self.job_info_json["feature_set"], self.job_info_json["resource_configs"])
            aggregator.aggregate()

            ret = aggregator.getCSVOfAggregated()

            # Write results to file for better comparison on error
            generated_file = open(generated,"w")
            generated_file.write(ret)
            generated_file.close()

            self.assertEqual(open(generated, "r").read(), open(reference, "r").read())

    def test_oldest_csv(self):
        with api.app.app_context():
            reference = os.path.join(self.referenceDir, "test_oldest_csv.txt")
            generated = os.path.join(self.generatedDir, "test_oldest_csv.txt")

            aggregator = Aggregator(self.aggregatorTestTable, "oldest", self.job_info_json["feature_set"], self.job_info_json["resource_configs"])
            aggregator.aggregate()

            ret = aggregator.getCSVOfAggregated()

            # Write results to file for better comparison on error
            generated_file = open(generated,"w")
            generated_file.write(ret)
            generated_file.close()

            self.assertEqual(open(generated, "r").read(), open(reference, "r").read())
    
    def test_all(self):
        with api.app.app_context():
            reference = os.path.join(self.referenceDir, "test_all.json")
            generated = os.path.join(self.generatedDir, "test_all.json")

            aggregator = Aggregator(self.aggregatorTestTable, "all", self.job_info_json["feature_set"], self.job_info_json["resource_configs"])
            aggregator.aggregate()

            ret = aggregator.getAggregated()
            
            # Write results to file for better comparison on error
            generated_file = open(generated,"w")
            generated_file.write(str(json.dumps(ret, indent=2)))
            generated_file.close()

            self.assertEqual(json.loads(open(generated, "r").read()), json.loads(open(reference, "r").read()))


if __name__ == "__main__":
    unittest.main()
import unittest
import json
import os

import api
from lib import mongodbConnection
from lib.aggregator import Aggregator

class AggregatorTest(unittest.TestCase):
    aggregatorTestTable = "aggregatorTest"
    crawler_job_path = os.path.join(os.path.dirname(__file__), 'crawler_job.json')
    crawler_data_path = os.path.join(os.path.dirname(__file__), 'crawler_data.json')

    # Insert test data into MongoDB
    def setUp(self):
        with api.app.app_context(): # necessary because mongodbConnection uses flask functionality
            # TODO: would be better to start MongoDB from here instead of relying that it's already started
            mongodbConnection.get_db()[self.aggregatorTestTable].delete_many({})

            crawler_data = open(self.crawler_data_path,'r')
            crawler_data_json = json.loads(crawler_data.read())
            mongodbConnection.get_db()[self.aggregatorTestTable].insert(crawler_data_json)

            job_info = open(self.crawler_job_path,'r')
            self.job_info_json = json.loads(job_info.read())

    def test_latest_json(self):
        with api.app.app_context():
            aggregator = Aggregator(self.aggregatorTestTable, "latest", self.job_info_json["feature_set"], self.job_info_json["resource_configs"])
            aggregator.aggregate()

            ret = aggregator.getRestructured()
            self.assertEqual(ret, json.loads(open(os.path.join(os.path.dirname(__file__), 'reference/test_latest_json.json'), 'r').read()))
    
    def test_oldest_json(self):
        with api.app.app_context():
            aggregator = Aggregator(self.aggregatorTestTable, "oldest", self.job_info_json["feature_set"], self.job_info_json["resource_configs"])
            aggregator.aggregate()

            ret = aggregator.getRestructured()
            self.assertEqual(ret, json.loads(open(os.path.join(os.path.dirname(__file__), 'reference/test_oldest_json.json'), 'r').read()))

    def test_latest_csv(self):
        with api.app.app_context():
            aggregator = Aggregator(self.aggregatorTestTable, "latest", self.job_info_json["feature_set"], self.job_info_json["resource_configs"])
            aggregator.aggregate()

            ret = aggregator.getCSVOfAggregated().replace("\r\n", "\n")
            self.assertEqual(ret, open(os.path.join(os.path.dirname(__file__), 'reference/test_latest_csv.txt'), 'r').read())

    def test_oldest_csv(self):
        with api.app.app_context():
            aggregator = Aggregator(self.aggregatorTestTable, "oldest", self.job_info_json["feature_set"], self.job_info_json["resource_configs"])
            aggregator.aggregate()

            ret = aggregator.getCSVOfAggregated().replace("\r\n", "\n")
            self.assertEqual(ret, open(os.path.join(os.path.dirname(__file__), 'reference/test_oldest_csv.txt'), 'r').read())


if __name__ == "__main__":
    unittest.main()
# Use monkey patch for reloader to not break with background thread (see https://github.com/miguelgrinberg/Flask-SocketIO/issues/567#issuecomment-337120425) 
import eventlet
eventlet.monkey_patch()

import json
import logging
import logging.config
logging.config.dictConfig(json.load(open("logging_config.json", "r")))

from flask import Flask
from flask_restful_swagger_2 import Api
from flask_cors import CORS
from lib import crawlerTask, resourceLoader
from resources.crawlerResource import Crawler, CrawlerJob, CrawlerJobs 
from resources.aggregationResource import Aggregation
from resources.resourceConfigResource import ResourceConfig, ResourceConfigList
import configuration
import os


app = Flask(__name__)
CORS(app) # this will allow cross-origin requests
api = Api(app, add_api_spec_resource=True, api_version='0.0', api_spec_url='/api/swagger') # Wrap the Api and add /api/swagger endpoint

api.add_resource(Crawler, '/crawler', endpoint='crawler')
api.add_resource(CrawlerJobs, '/crawler/jobs', endpoint='jobs')
api.add_resource(CrawlerJob, '/crawler/jobs/<crawler_id>', endpoint='job')
api.add_resource(Aggregation, '/aggregation/<crawler_id>', endpoint='aggregation')
api.add_resource(ResourceConfigList, '/resources_config', endpoint='resources_list')
api.add_resource(ResourceConfig, '/resources_config/<resource_name>', endpoint='resources')

@app.before_first_request
def startCrawlerThread():
    crawlerTask.CrawlerTask(app)

@app.before_first_request
def loadResources():
    with app.app_context():
        resourceLoader.loadResources()

if __name__ == '__main__':
    # set false in production mode
    app.run(debug=True, host=configuration.WSHOST, port=configuration.WSPORT)
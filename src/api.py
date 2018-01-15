# Use monkey patch for reloader to not break with background thread (see https://github.com/miguelgrinberg/Flask-SocketIO/issues/567#issuecomment-337120425) 
import eventlet
eventlet.monkey_patch()

import logging.config
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

import logging
logger = logging.getLogger(__name__)

from flask import Flask
from flask_restful_swagger_2 import Api
from flask_cors import CORS
from lib import crawlerTask
from resources.crawlerResource import Crawler, CrawlerJob, CrawlerJobs 
from resources.aggregationResource import Aggregation
from resources.featuresResource import FeaturesBrowser, FeaturesSet, FeaturesSets
from resources.swaggerResource import Swagger
import configuration
import os


app = Flask(__name__)
CORS(app) # this will allow cross-origin requests; needed for http://petstore.swagger.io in swaggerResource to access whole api output
api = Api(app, add_api_spec_resource=True, api_version='0.0', api_spec_url='/api/swagger') # Wrap the Api and add /api/swagger endpoint

api.add_resource(Swagger, '/swagger', endpoint='swaggerhtml')
api.add_resource(Crawler, '/crawler', endpoint='crawler')
api.add_resource(CrawlerJobs, '/crawler/jobs', endpoint='jobs')
api.add_resource(CrawlerJob, '/crawler/jobs/<crawler_id>', endpoint='job')
api.add_resource(Aggregation, '/aggregation/<crawler_id>', endpoint='aggregation')
api.add_resource(FeaturesBrowser, '/features/browser/<crawler_id>', endpoint='browser')
api.add_resource(FeaturesSet, '/features/sets/<set_id>', endpoint='set')
api.add_resource(FeaturesSets, '/features/sets', endpoint='sets')

@app.before_first_request
def startCrawlerThread():
    crawlerTask.CrawlerTask(app)

if __name__ == '__main__':
    # set false in production mode
    app.run(debug=True, host=configuration.WSHOST, port=configuration.WSPORT)
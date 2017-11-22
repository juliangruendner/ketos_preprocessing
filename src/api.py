from flask import Flask
from flask_restful_swagger_2 import Api
from flask_cors import CORS
from resources import crawlerResource
from resources import aggregationResource
from resources import featuresResource
from resources import swaggerResource
from lib import crawlerTask
import configuration
import os

app = Flask(__name__)

#TODO: is this ok?
CORS(app) # this will allow cross-origin requests; needed for http://petstore.swagger.io in swaggerResource to access whole api output

api = Api(app, add_api_spec_resource=True, api_version='0.0', api_spec_url='/api/swagger') # Wrap the Api and add /api/swagger endpoint

api.add_resource(swaggerResource.Swagger, '/swagger', endpoint='swaggerhtml')
api.add_resource(crawlerResource.CrawlerJob, '/crawler/jobs', endpoint='jobs')
api.add_resource(crawlerResource.CrawlerJobs, '/crawler/jobs/<crawler_id>', endpoint='job')
api.add_resource(aggregationResource.Aggregation, '/aggregation/<crawler_id>', endpoint='aggregation')
api.add_resource(featuresResource.FeaturesBrowser, '/features/browser/<crawler_id>', endpoint='browser')
api.add_resource(featuresResource.FeaturesSet, '/features/sets/<set_id>', endpoint='set')
api.add_resource(featuresResource.FeaturesSets, '/features/sets', endpoint='sets')



if(os.environ.get("WERKZEUG_RUN_MAIN") == "true"):
    crawlerTask.CrawlerTask(app)

if __name__ == '__main__':
    # set false in production mode
    app.run(debug=configuration.DEBUG, host=configuration.WSHOST, port=configuration.WSPORT)
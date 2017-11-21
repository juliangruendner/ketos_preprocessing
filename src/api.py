from flask import Flask
from flask_restful import Api
from resources import crawlerResource
from resources import aggregationResource
from resources import featuresResource
from lib import crawlerTask
import configuration
import os

app = Flask(__name__)
api = Api(app)

api.add_resource(crawlerResource.CrawlerJobs, '/crawler/jobs', '/crawler/jobs/<crawler_id>', endpoint='job')
api.add_resource(aggregationResource.Aggregation, '/aggregation/<crawler_id>', endpoint='aggregation')
api.add_resource(featuresResource.Features, '/features/<crawler_id>', endpoint='features')


if(os.environ.get("WERKZEUG_RUN_MAIN") == "true"):
    crawlerTask.CrawlerTask(app)

if __name__ == '__main__':
    # set false in production mode
    app.run(debug=configuration.DEBUG, host=configuration.WSHOST, port=configuration.WSPORT)
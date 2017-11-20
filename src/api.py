from flask import Flask
from flask_restful import Api
from resources import crawlerResource
from resources import aggregationResource
import configuration
from lib import crawlerTask
import os

app = Flask(__name__)
api = Api(app)

api.add_resource(crawlerResource.CrawlerJob, '/crawler/job/<crawler_id>', endpoint='job')
api.add_resource(crawlerResource.CrawlerJobs, '/crawler/jobs', endpoint='jobs')
api.add_resource(aggregationResource.Aggregation, '/aggregation/<crawler_id>', endpoint='aggregation')      


if(os.environ.get("WERKZEUG_RUN_MAIN") == "true"):
    crawlerTask.CrawlerTask(app)

if __name__ == '__main__':
    # set false in production mode
    app.run(debug=configuration.DEBUG, host=configuration.WSHOST, port=configuration.WSPORT)


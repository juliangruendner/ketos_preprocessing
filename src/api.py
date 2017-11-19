from flask import Flask
from flask_restful import Api
from resources import crawlerResource
from resources import aggregationResource
import configuration
from lib import crawlerTask

app = Flask(__name__)
api = Api(app)

api.add_resource(crawlerResource.CrawlerJob, '/crawler/job/<crawler_id>', endpoint='job')
api.add_resource(crawlerResource.CrawlerJobs, '/crawler/jobs', endpoint='jobs')
api.add_resource(aggregationResource.Aggregation, '/aggregation', endpoint='aggregation')      

if __name__ == '__main__':
    crawlerTask.CrawlerTask(app)
    # set false in production mode
    app.run(debug=configuration.DEBUG, host=configuration.WSHOST, port=configuration.WSPORT)
    


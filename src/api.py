from flask import Flask
from flask_restful import Api
from resources import crawlerResource
from resources import aggregationResource
import configuration

app = Flask(__name__)
api = Api(app)

api.add_resource(crawlerResource.Crawler, '/crawler/<string:patient_id>', endpoint='crawler')
api.add_resource(aggregationResource.Aggregation, '/aggregation', endpoint='aggregation')

if __name__ == '__main__':
    # set false in production mode
    app.run(debug=True, host='0.0.0.0', port=5000)
    
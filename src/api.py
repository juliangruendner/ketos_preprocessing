from flask import Flask
from flask_restful import Api
from resources import exampleResource

app = Flask(__name__)
api = Api(app)

api.add_resource(exampleResource.ExampleList, '/example', endpoint='examples')
#api.add_resource(exampleResource.Example, '/example/<int:example_id>', endpoint='example')

if __name__ == '__main__':
    # set false in production mode
    app.run(debug=True, host='0.0.0.0', port=5000)
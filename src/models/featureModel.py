from flask_restful_swagger_2 import Schema

class TypesModel(Schema):
    type = 'object'
    properties = {
        'code': {
            'type': 'string'
        }
    }

class AttributesModel(Schema):
    type = 'object'
    properties = {
        'type': TypesModel
    }

class FeatureModel(Schema):
    type = 'object'
    properties = {
        'attributes': AttributesModel.array()
    }
    required = ['attributes']
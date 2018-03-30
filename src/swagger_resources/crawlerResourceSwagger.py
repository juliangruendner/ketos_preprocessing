import copy
from datetime import datetime
from lib import util
from resources import crawlerResource
from swagger_resources import resourceConfigResourceSwagger


swagger_feature_set_example = [
    {
        "resource": "Observation",
        "key": "code",
        "value": "279495008"
    },
    {
        "resource": "Observation",
        "key": "code",
        "value": "422503008"
    },
    {
        "resource": "Observation",
        "key": "code",
        "value": "425115008"
    },
    {
        "resource": "Condition",
        "key": "code",
        "value": "14669001",
        "name": "Acute renal failure syndrome"
    },
    {
        "resource": "Condition",
        "key": "code",
        "value": "21522001",
        "name": "Abdominal Pain"
    }
]

swagger_example = {
	"feature_set": swagger_feature_set_example,
	"resource_configs": resourceConfigResourceSwagger.swagger_example
}

swagger_example_patient = copy.deepcopy(swagger_example)
swagger_example_patient["patient"] = "Patient/145"

swagger_example_patients = copy.deepcopy(swagger_example)
swagger_example_patients["patient_ids"] = ["Patient/145", "Patient/146"]

swagger_params = [{
    "name": "body",
    "in": "body",
    "required": True,
    "schema": {
        "type": "object",
        "properties": {
            "feature_set": util.buildSwaggerFrom(crawlerResource.FEATURE_SET_SCHEMA),
            "aggregation_type": {
                "type": "string",
                "required": True,
                "default": "latest"
            },
            "resource_configs": util.buildSwaggerFrom(crawlerResource.RESOURCE_CONFIG_SCHEMA),

        }
    }
}]

swagger_params_patient = copy.deepcopy(swagger_params)
swagger_params_patient[0]["schema"]["example"] = swagger_example_patient
swagger_params_patient[0]["schema"]["properties"]["patient"] = {
    "patient": {
        "type": "string",
        "required": True
    }
}

swagger_params_patients = copy.deepcopy(swagger_params)
swagger_params_patients[0]["schema"]["example"] = swagger_example_patients
swagger_params_patients[0]["schema"]["properties"]["patient_ids"] = {
    "patient_ids": {
        "type": "array",
        "required": True,
        "items": {
            "type": "string"
        }
    }
}

swagger_crawler_example = {
    "_id": "ID",
    "patient_ids": ["ID1", "ID2"],
    "feature_set": swagger_feature_set_example,
    "resource_configs": resourceConfigResourceSwagger.swagger_example,
    "status": "One of [queued, running, finished, error]",
    "finished": ["ID1"],
    "queued_time": str(datetime.now()),
    "start_time": str(datetime.now()),
    "url": "URL to csv"
}

swagger_crawler_schema = {
    "type": "object",
    "properties": {
        "_id": {
            "type": "string"
        },
        "patient_ids": {
            "type": "array",
            "items": {
                "type": "string"
            }
        },
        "status": {
            "type": "string"
        },
        "finished": {
            "type": "array",
            "items": {
                "type": "string"
            }
        },
        "queued_time": {
            "type": "string"
        },
        "start_time": {
            "type": "string"
        },
        "url": {
            "type": "string"
        }
    },
    "example": swagger_crawler_example
}
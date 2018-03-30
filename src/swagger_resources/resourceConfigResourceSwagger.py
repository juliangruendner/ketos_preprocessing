swagger_example = {
    "resource_value_relative_path": "verificationStatus",
    "sort_order": [
        "onsetDateTime",
        "onsetPeriod/end"
    ],
    "resource_name": "Condition"
}

swagger_params = [{
    "name": "body",
    "in": "body",
    "required": True,
    "schema": {
        "type": "object",
        "properties": {
            "resource_value_relative_path": {
                "type": "string",
                "required": True
            },
            "sort_order": {
                "type": "array",
                "items": {
                    "type": "string"
                }
            },
            "resource_name": {
                "type": "string",
                "required": True
            }
        },
        "example": swagger_example
    },
}]
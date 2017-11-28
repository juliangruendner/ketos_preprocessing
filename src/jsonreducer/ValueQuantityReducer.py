from .ValueReducer import ValueReducer

class ValueQuantityReducer(ValueReducer):

    def reduce(self, json):
        if "value" in json:
            self.setValue(json["value"])
        if "unit" in json:
            self.setUnit(json["unit"])

from .ValueReducer import ValueReducer

class ValueQuantityReducer(ValueReducer):

    def reduce(self, json):
        self.setValue(json["value"])
        self.setUnit(json["unit"])

from .ValueReducer import ValueReducer

class ValueStringReducer(ValueReducer):

    def reduce(self, json):
        if "valueString" in json:
            self.setValue(json["valueString"])
        

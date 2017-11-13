import json
from .Reducer import Reducer

class ValueReducer(Reducer):

    def setValue(self, value):
        self.set("value",  value)

    def getValue(self):
        return self.get("value")

    def setUnit(self, unit):
        self.set("unit",  unit)

    def getUnit(self):
        return self.get("unit")

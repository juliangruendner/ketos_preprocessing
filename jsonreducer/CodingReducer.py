from .Reducer import Reducer

class CodingReducer(Reducer):

    def reduce(self, json):
        if "code" in json: self.set("code", json["code"])
        if "system" in json: self.set("system", json["system"])
        if "display" in json: self.set("display", json["display"])

    def getCode(self):
        return self.get("code")

    def getSystem(self):
        return self.get("system")

    def getDisplay(self):
        return self.get("display")

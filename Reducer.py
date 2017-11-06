import json

class Reducer:
    def __init__(self, json):
        self.__json = json
        self.__map = {}
        self.reduce(self.__json)

    def set(self, key, value):
        self.__map[key] = value

    def get(self, key):
        if key in self.__map:
            return self.__map[key]
        else:
            return None

    def reduce(self, json):
        return None

    def getReduced(self):
        return self.__map;

    def asJsonString(self):
        return json.dumps(self.__map)

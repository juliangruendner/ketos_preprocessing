import json
from .Reducer import Reducer

class EAVReducer(Reducer):

    def setTimestamp(self, timestamp):
        self.set("timestamp",  timestamp)

    def setEntity(self, entity):
        self.set("entity",  entity)

    def getEntity(self):
        return self.get("entity")

    def setAttribute(self, attribute):
        self.set("attribute",  attribute)

    def setValue(self, value, typ):
        self.set("value",  value)
        self.updateMeta("type", typ)

    def setMeta(self, meta):
        self.set("meta",  meta)

    def getMeta(self):
        ret = self.get("meta")
        if ret is None:
            return {}
        else:
            return self.get("meta")

    def updateMeta(self, key, value):
        meta = self.getMeta()
        meta[key] = value
        self.setMeta(meta)

    def mergeMeta(self, meta):
        oldmeta = self.getMeta()
        newmeta = {**oldmeta, **meta}
        self.setMeta(newmeta)

from .EAVReducer import EAVReducer
from .ValueQuantityReducer import ValueQuantityReducer
from .CodingReducer import CodingReducer

class ObservationReducer(EAVReducer):

    def reduce(self, json):
        #json = json["resource"]
        entity = json["subject"]["reference"]
        entity = entity.replace("Patient/", "")

        self.setEntity(entity)

        codeReducer = CodingReducer(json["code"]["coding"][0])
        self.setAttribute(codeReducer.getCode()) #["resource"]
        
        if codeReducer.getDisplay() is not None:
            self.updateMeta("attribute", codeReducer.getDisplay())
        elif "text" in json["code"]:
            self.updateMeta("attribute", json["code"]["text"])    

        self.setTimestamp(json["effectiveDateTime"])

        #check if entity has multiple components
        value = None
        typ = None
        if "component" in json :
             value = []
             typ = []
             for component in json["component"]:
                 valueReducer = ValueQuantityReducer(component["valueQuantity"])
                 codeReducerTmp = CodingReducer(component["code"]["coding"][0])
                 typTmp = codeReducerTmp.getReduced()
                 typTmp["unit"] = valueReducer.getUnit()

                 value.append(valueReducer.getValue())
                 typ.append(typTmp)
        elif "valueQuantity" in json :
            valueReducer = ValueQuantityReducer(json["valueQuantity"])

            typ = codeReducer.getReduced()
            typ["unit"] = valueReducer.getUnit()
            value = valueReducer.getValue()


        self.setValue(value, typ)

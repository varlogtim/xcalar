import json
import ast
from datetime import datetime
def loadMemory(fullPath, inStream, schema):
    inObj = json.loads(inStream.read())
    startRow = inObj["startRow"]
    numLocalRows = inObj["numRows"]
    schemaDecoded = ast.literal_eval(schema)
    cols = schemaDecoded['Cols']
    paramVal = schemaDecoded['ParamVal']
    for ii in range(startRow, startRow + numLocalRows):
        rowDict = {}
        rowDict['RowNum'] = ii
        rowDict['ParamVal'] = paramVal
        for k, v in cols.items():
           if v == "int":
               rowDict[k] = int(ii)
           elif v == "float":
               rowDict[k] = float(ii)
           elif v == "string":
               rowDict[k] = str(ii)
           elif v == "datetime":
               rowDict[k] = datetime.fromtimestamp(int(ii))
        yield rowDict

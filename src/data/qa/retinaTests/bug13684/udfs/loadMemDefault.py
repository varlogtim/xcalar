import json
def loadMemory(fullPath, inStream):
    inObj = json.loads(inStream.read())
    startRow = inObj["startRow"]
    numLocalRows = inObj["numRows"]
    for ii in range(startRow, startRow + numLocalRows):
        rowDict = {"row.num": {
                   "int.col": ii,
                   "string\.col": str(ii),
                   "float..col": float(ii)
               },
               "cols": {
                   "array": [0, 1, 2],
                   "object": {"val.0": 0, "val.1": 1, "val.2": 2}
               },
               "a": {"b": ii},
               "a.b": ii-1
              }
        if not (ii % 10):
            rowDict['flaky'] = 1
        yield rowDict

import json
def loadMemory(fullPath, inStream):
    inObj = json.loads(inStream.read())
    startRow = inObj["startRow"]
    numLocalRows = inObj["numRows"]
    for ii in range(startRow, startRow + numLocalRows):
        rowDict = {"rownum": {
                   "intcol": ii,
                   "stringcol": str(ii),
                   "floatcol": float(ii)
               },
               "cols": {
                   "array": [0, 1, 2],
                   "object": {"val0": 0, "val1": 1, "val2": 2}
               },
               "a": {"b": ii},
               "ab": ii-1
              }
        if not (ii % 10):
            rowDict['flaky'] = 1
        yield rowDict

import json
def a(b):
    return str(b)

def loadRandom(fullPath, inStream):
    inObj = json.loads(inStream.read())
    startRow = inObj["startRow"]
    numLocalRows = inObj["numRows"]
    for ii in range(startRow, startRow + numLocalRows):
        yield {"intCol": ii,
               "stringCol": str(ii),
               "floatCol": float(ii)}

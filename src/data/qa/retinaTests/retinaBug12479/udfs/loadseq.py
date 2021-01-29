import json
import random

def loadSeq(fullPath, inStream):
    inObj = json.loads(inStream.read())
    numLocalRows = inObj["numRows"]
    startNum = inObj["startRow"]
    for ii in range(startNum, startNum+numLocalRows):
        yield {"row": ii,
               "int": random.randint(0, 200),
               "float": random.randrange(0, 200)}

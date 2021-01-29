import pandas
import calendar
from decimal import Decimal

def row_gen(colDict):
    nRows = len(colDict[next(iter(colDict.keys()))])
    colStreams = []
    for col in colDict:
        firstVal = colDict[col][0]
        if isinstance(firstVal, Decimal):
            colStreams.append([str(v) for v in colDict[col]])
        elif isinstance(firstVal, pandas.Timestamp):
            colStreams.append([calendar.timegm(value.timetuple()) for value in colDict[col]])
        elif isinstance(firstVal, bytes):
            colStreams.append([v.decode("UTF-8") for v in colDict[col]])
        else:
            colStreams.append(colDict[col])

    colIterators = [iter(c) for c in colStreams]
    for _ in range(nRows):
        yield list(map(next, colIterators))


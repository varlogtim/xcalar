import pandas
import calendar
import sys
from decimal import Decimal
import pyarrow.parquet as pyArrParq

def rowGen(colDict):
    nRows = len(colDict[next(iter(colDict.keys()))])
    colStreams = []
    for col in colDict:
        firstVal = colDict[col][0]
        if isinstance(firstVal, Decimal):
            colStreams.append(str(v) for v in colDict[col])
        elif isinstance(firstVal, pandas.Timestamp):
            colStreams.append(calendar.timegm(value.timetuple()) for value in colDict[col])
        elif isinstance(firstVal, bytes):
            colStreams.append(v.decode("UTF-8") for v in colDict[col])
        else:
            colStreams.append(colDict[col])

    colIterators = [iter(c) for c in colStreams]
    # fieldNames = colDict.keys()
    for _ in range(nRows):
        # recordValues = map(next, colIterators)
        # yield dict(zip(fieldNames, recordValues))
        yield list(map(next, colIterators))

# parquet_file = pyArrParq.read_table(sys.argv[1])
parquet_file = pyArrParq.ParquetFile(sys.argv[1])
parqContents = parquet_file.read(columns=None)
colDict = parqContents.to_pydict()
rows = rowGen(colDict)
for row in rows:
    pass

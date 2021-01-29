import pandas
import calendar
import sys
from decimal import Decimal
import pyarrow.parquet as pyArrParq
import pyarrow as pa 

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
    for _ in range(nRows):
        # yield [next(colIter) for colIter in colIterators]
        # Using the list comprehension is slow here,
        yield list(map(next, colIterators))

parquet_file = pyArrParq.read_table(sys.argv[1])
batches = parquet_file.to_batches(1000)

for batch in batches:
    colDict = batch.to_pydict()
    for row in rowGen(colDict):
        pass

'''
reader = pa.RecordBatchStreamReader(sys.argv[1])
for parqContents in reader.get_next_batch():
    colDict = parqContents.to_pydict()
    rows = rowGen(colDict)
    for row in rows:
        pass
'''

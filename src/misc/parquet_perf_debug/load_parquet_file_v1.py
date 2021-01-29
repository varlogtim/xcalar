import pandas
import calendar
import sys
from decimal import Decimal
import pyarrow.parquet as pyArrParq

def rowGen(colDict):
    nRows = len(colDict[next(iter(colDict.keys()))])
    for rowIndex in range(0, nRows):
        rowDict = {}
        for col in colDict:
            value = colDict[col][rowIndex]
            if isinstance(value, pandas.Timestamp):
                rowDict[str(col)] = calendar.timegm(value.timetuple())
            elif isinstance(value, Decimal):
                rowDict[str(col)] = str(value)
            elif isinstance(value, bytes):
                rowDict[str(col)] = value.decode("UTF-8")
            else:
                rowDict[str(col)] = value
        yield rowDict

# parquet_file = pyArrParq.read_table(sys.argv[1])
parquet_file = pyArrParq.ParquetFile(sys.argv[1])
parqContents = parquet_file.read(columns=None)
colDict = parqContents.to_pydict()
rows = rowGen(colDict)
for row in rows:
    pass

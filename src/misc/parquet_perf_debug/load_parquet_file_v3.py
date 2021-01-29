import sys
import pyarrow.parquet as pyArrParq
from parquet_row_gen import row_gen

# parquet_file = pyArrParq.read_table(sys.argv[1])
parquet_file = pyArrParq.ParquetFile(sys.argv[1])
parqContents = parquet_file.read(columns=None)
colDict = parqContents.to_pydict()
rows = row_gen(colDict)
for row in rows:
    pass

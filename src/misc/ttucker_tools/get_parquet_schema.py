import sys

from xcalar.container.target.s3environ import S3EnvironTarget
from pyarrow.parquet import ParquetFile

target = S3EnvironTarget('fake_target_name', 'fake_target_path')
my_file = target.open(sys.argv[1], 'rb')

parquet_file = ParquetFile(my_file)
metadata = parquet_file.metadata

num_columns = metadata.num_columns
num_rows = metadata.num_rows

# https://github.com/apache/arrow
# arrow/python/pyarrow/_parquet.pyx: ColumnSchema()
# arrow/cpp/src/parquet/types.h: Type struct, ConvertedType struct
for col in metadata.schema:
    print(f"COLUMN:\n{col}")
    name = col.name
    path = col.path  # I think this relates to position in object data.
    physical_type = col.physical_type  # str, struct Type, types.h
    logical_type = col.logical_type  # str, struct ConvertedType, types.h
    # print(f"Column: name='{name}', physical_type='{physical_type}', logical_type='{logical_type}', path='{path}'")

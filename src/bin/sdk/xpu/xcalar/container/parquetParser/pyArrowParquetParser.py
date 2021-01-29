# Copyright 2017 - 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import pandas
import calendar
import contextlib
from decimal import Decimal

from .base import BaseParquetParser, ParseException
from xcalar.container.util import cleanse_column_name

try:
    import pyarrow as pa
    from pyarrow import ArrowException
    import pyarrow.parquet as pyArrParq
    pyArrowAvailable = True
except ImportError:
    pyArrowAvailable = False

# Behavior of pyArrow wrt columns of interest
# If you pass in columns that doesn't exist in the schema, you get the empty list
# If you pass in columns that exists in the schema, but doesn't exist in the row, a row with the columns specified is returned, with the value initialized to None
# If a.b exists in the schema, but a is not in the row, we get a row with the column a initialized to None (as opposed to a.b = None)


class PyArrowParquetParser(BaseParquetParser):
    def __init__(self, parserName, logger):
        super(PyArrowParquetParser, self).__init__(parserName, logger)

    def isAvailable(self):
        return pyArrowAvailable

    def rowGen(self, colDict):
        nRows = len(colDict[next(iter(colDict.keys()))])
        colStreams = []
        for col in colDict:
            firstVal = None
            for val in colDict[col]:
                if val is not None:
                    firstVal = val
                    break
            if isinstance(firstVal, Decimal):
                colStreams.append(
                    str(v) if v is not None else None for v in colDict[col])
            elif isinstance(firstVal, pandas.Timestamp):
                colStreams.append(
                    calendar.timegm(value.
                                    timetuple()) if value is not None else None
                    for value in colDict[col])
            elif isinstance(firstVal, bytes):
                colStreams.append(
                    v.decode("UTF-8") if v is not None else None
                    for v in colDict[col])
            else:
                colStreams.append(colDict[col])

        colIterators = [iter(c) for c in colStreams]
        for _ in range(nRows):
            yield list(map(next, colIterators))

    def genEmptyRows(self, numRows):
        for _ in range(numRows):
            yield []

    def parseParquetBody(self,
                         inStream,
                         columns=None,
                         capitalizeColumns=False,
                         partitionKeys=None):
        parqFile = pyArrParq.ParquetFile(inStream)

        # Need this to protect against segfault. Calling read() on an empty valid parquet file segfaults
        try:
            if parqFile.scan_contents(batch_size=1) == 0:
                return
        except ArrowException:
            pass

        if partitionKeys is None:
            partitionKeys = {}

        inStream.seek(0)
        parquet_file = pyArrParq.read_table(inStream, columns=columns)
        # Currently limiting the chunk_size to 1000 to limit the memory usage,
        # there could be cases where even this could lead to memory explosion
        # there shouldn't be any performance difference for any number chosen
        batches = parquet_file.to_batches(1000)

        parquet_col_names = [x.name for x in parquet_file.schema]
        # validate columns
        if columns:
            for col in columns:
                if "." in col:
                    col = col.split(".")[0]
                    # XXX FIXME This needs to handle . better
                    # for now we are only checking the first key
                    if col in parquet_col_names:
                        continue
                if col not in parquet_col_names and col not in partitionKeys:
                    raise ParseException(
                        "Column name {} does not exist in parquet nor as part of partition keys"
                        .format(col))

        if len(batches) == 0 and len(partitionKeys) > 0:
            batches = ["generated"]

        field_names = None
        for batch in batches:
            if batch == "generated":
                rows = self.genEmptyRows(parqFile.metadata.num_rows)
                field_names = []
            else:
                colDict = batch.to_pydict()
                if not field_names:
                    field_names = [
                        cleanse_column_name(field_name, capitalizeColumns)
                        for field_name in colDict
                    ]
                rows = self.rowGen(colDict)
            for row in rows:
                rowDict = dict(zip(field_names, row))
                for key in partitionKeys:
                    cleansed_key = cleanse_column_name(key, capitalizeColumns)
                    if partitionKeys[key] == "__HIVE_DEFAULT_PARTITION__":
                        rowDict[cleansed_key] = None
                    else:
                        rowDict[cleansed_key] = partitionKeys[key]

                yield rowDict

    def convertToXcalarType(self, parquetType, typeQualifier):
        if parquetType == "binary" or parquetType == "string" or \
           (isinstance(typeQualifier, list) and typeQualifier[0] == "DECIMAL"):
            return "DfString"
        elif parquetType == "int64":
            return "DfInt64"
        elif parquetType == "int32" or parquetType == "int8" or parquetType == "int16":
            return "DfInt32"
        elif parquetType == "double" or parquetType == "int96" or parquetType == "float":
            # int96 is treated as TIMESTAMP. We return uts which is a double
            return "DfFloat64"
        elif parquetType == "group" and typeQualifier == "LIST":
            return "DfArray"
        elif parquetType == "group":
            return "DfObject"
        elif parquetType == "boolean" or parquetType == "bool":
            return "DfBoolean"
        else:
            return "DfUnknown"

    def parseSchemaInt(self, schema, fields):
        for field in fields:
            if isinstance(field.type, pa.lib.StructType):
                fieldSchema = {
                    "xcalarType": "DfObject",
                    "parquetType": "group",
                    "children": {}
                }
                schema[field.name] = fieldSchema
                self.parseSchemaInt(fieldSchema["children"], field.type)
            else:
                typeQualifier = None
                if isinstance(field.type, pa.lib.Decimal128Type):
                    parquetType = "fixed_len_byte_array"
                    typeQualifier = [
                        "DECIMAL", field.type.precision, field.type.scale
                    ]
                elif isinstance(field.type, pa.lib.TimestampType):
                    parquetType = "int96"
                elif isinstance(field.type, pa.lib.ListType):
                    parquetType = "group"
                    typeQualifier = "LIST"
                else:
                    parquetType = str(field.type)

                fieldSchema = {
                    "xcalarType":
                        self.convertToXcalarType(parquetType, typeQualifier),
                    "parquetType":
                        parquetType,
                    "typeQualifier":
                        typeQualifier
                }
                schema[field.name] = fieldSchema

    def parseParquetSchema(self, inStream):
        parqFile = pyArrParq.ParquetFile(inStream)
        schema = {}
        self.parseSchemaInt(schema, parqFile.schema.to_arrow_schema())
        return schema

    def convertToXcalarSchema(self, schema, unknownFields):
        return schema

    def parseParquet(self,
                     inputPath,
                     inputStream,
                     mode,
                     columns=None,
                     schema=None,
                     capitalizeColumns=False,
                     partitionKeys=None):
        with contextlib.ExitStack() as stack:
            if inputStream is not None:
                fp = pa.BufferReader(inputStream.read())
            else:
                fp = stack.enter_context(pa.memory_map(inputPath, "rb"))

            try:
                if mode == "cat" or mode == "catJson":
                    for record in self.parseParquetBody(
                            fp, columns, capitalizeColumns, partitionKeys):
                        yield record
                else:
                    yield self.parseParquetSchema(fp)
            except ArrowException as e:
                raise ParseException(str(e))

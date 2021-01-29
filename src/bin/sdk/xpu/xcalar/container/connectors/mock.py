import json
import datetime
import logging
import random
import os
import string

from .util import mtime_from_datetime
from .object_store import ObjectStoreImpl, KeyEntry

logger = logging.getLogger("xcalar")

# XXX put this in a config file somewhere


class MockConnector(ObjectStoreImpl):
    @staticmethod
    def is_available():
        return True

    def __init__(self):
        if not MockConnector.is_available():
            raise ImportError("Mock connector not available")
        self.numFiles = int(os.environ.get("MOCK_NUM_FILES", 2**10))
        self.numCols = min(int(os.environ.get("MOCK_NUM_COLS", 3)), 26)
        self.numRows = int(os.environ.get("MOCK_NUM_ROWS", 3))
        self.alpha = string.ascii_uppercase

    def list_keys(self, bucket_name, prefix, delimiter):
        for ii in range(self.numFiles):
            yield KeyEntry(
                name=f"s{ii}.json",
                is_prefix=False,
                mtime=mtime_from_datetime(datetime.datetime(2020, 10, 11)),
                size=40)

    def schema_discover(self, bucket, key, input_serialization,
                        retry_on_error):
        status = "success"
        columns = []
        for colNum in range(self.numCols):
            columns.append({
                "name": self.alpha[colNum],
                "mapping": f"$.{self.alpha[colNum]}",
                "type": "DfInt64"
            })
        schema = {"rowpath": "$", "columns": columns}
        return (status, schema)

    def open(self, bucket_name, key, opts):
        if opts == 'rrb':
            return MockSelectDataStream(bucket_name, key, self.numRows,
                                        self.numCols)
        elif opts == 'rb' or opts == 'wb':
            raise NotImplementedError(
                "File mode {} not implemented".format(opts))
        else:
            raise ValueError("Unknown file mode {}".format(opts))


class MockSelectDataStream():
    def __init__(self, bucket, key, numRows, numCols):
        self._bucket = bucket
        self._key = key
        self.alpha = string.ascii_uppercase
        self.numRows = numRows
        self.numCols = numCols

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    def read(self, input_serialization, expression):
        for rowNum in range(self.numRows):
            aRow = {}
            for colId, colNum in enumerate(range(self.numCols)):
                aRow[self.alpha[colId]] = random.randint(100000, 999000)
            yield (json.dumps(aRow) + "\n").encode("utf-8")

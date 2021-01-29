# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import logging
import sys

from xcalar.compute.util.utils import get_proto_field_value
from xcalar.compute.localtypes.Workbook_pb2 import WorkbookScope
from xcalar.compute.localtypes.ResultSet_pb2 import (
    ResultSetMakeRequest, ResultSetReleaseRequest, ResultSetNextRequest,
    ResultSetSeekRequest)

logger = logging.getLogger("xcalar")

# XXX We want this to be bigger, but if it's greater than 2MB we run into
# SYS-18
DEFAULT_BATCH_SIZE = 1000
"""
Default number of records to fetch on each individual request when
cursoring through the result set.
"""


class ResultSetContext:
    def __init__(self,
                 client,
                 table_name=None,
                 dataset_name=None,
                 error_ds=False,
                 session_name=None):
        self._session_name = session_name
        self._client = client
        self._table_name = table_name
        self._dataset_name = dataset_name
        self._error_ds = error_ds
        self._result_set_id = None
        self._rs_open = False
        self._count = None
        self._service = client._result_set_service

        # XXX system workbook per user: SDK-826
        self._client._create_system_workbook()

    @property
    def session_name(self):
        if (self._dataset_name):
            return self._client._sys_wkbk_name
        else:
            return self._session_name

    @property
    def scope(self):
        return self._scope

    @property
    def result_set_id(self):
        return self._result_set_id

    def __enter__(self):
        if self._dataset_name:
            self.name = self._dataset_name
        elif self._table_name:
            self.name = self._table_name
        scope = WorkbookScope()
        scope.workbook.name.username = self._client.username
        scope.workbook.name.workbookName = self.session_name
        self._scope = scope
        req = ResultSetMakeRequest()
        req.name = self.name
        req.scope.CopyFrom(self._scope)
        req.error_dataset = self._error_ds
        resp = self._service.make(req)
        self._rs_open = True
        self._result_set_id = resp.result_set_id
        self._count = resp.num_rows
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        if self._rs_open:
            try:
                req = ResultSetReleaseRequest()
                req.result_set_id = self._result_set_id
                req.scope.CopyFrom(self._scope)
                self._service.release(req)
            finally:
                self._result_set_id = None
                self._rs_open = False

    def record_count(self):
        if self._count is None:
            raise ValueError("Result set is not open")
        return self._count


class ResultSet():
    """
    |br|
    A ResultSet object is a tool to view a specified table/dataset outside of
    Xcalar.

    A ResultSet object can be browsed in a couple ways. The :meth:`record_iterator` method
    returns a generator object that can be used to iterate through the result set.
    The :meth:`print_rs` method can be used to print out a specified number of rows.
    The :meth:`get_row` method can be used to get a specific row in the result set.

    To get a ResultSet object from a specific dataset:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataset = client.get_dataset("sample_dataset")
            >>> result_set = dataset._result_set()

    **Attributes**

    The following attributes are properties associated with the ResultSet object. They cannot be directly set.

    * **name** (:class:`str`) - Name of the Dataset/table this class represents.

    **Methods**

    These are the list of available methods:

    * :meth:`record_iterator`
    * :meth:`get_row`
    * :meth:`print_rs`
    """

    def __init__(self,
                 client,
                 table_name=None,
                 dataset_name=None,
                 session_name=None,
                 error_ds=False):
        self._client = client
        self._table_name = table_name
        self._dataset_name = dataset_name
        self._session_name = session_name
        self._error_ds = error_ds
        self._service = client._result_set_service

    @property
    def _name(self):
        if (self._table_name):
            return self._table_name
        else:
            return self._dataset_name

    @classmethod
    def _format_result(cls, meta, row):
        res = dict()
        for col in meta.column_names:
            res[col] = get_proto_field_value(row.fields[col])
        return res

    def record_iterator(self,
                        start_row=0,
                        num_rows=sys.maxsize,
                        batch_size=DEFAULT_BATCH_SIZE):
        """
        This method returns an iterator over rows of a result set. This will
        continuously fetch batches in multiple parts if needed.  The first
        record will be the record with index `start_row` and the last record
        will have index (`start_row`+`num_rows`-1).

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataset = client.get_dataset("sample_dataset")
            >>> result_set = dataset.result_set()
            >>> iterator = result_set.record_iterator()
            >>> row_0 = next(iterator) # Returns row 0
            >>> for row in iterator:
            ...         print(row)  # Prints rows 1,..,n

        :param start_row: *Optional.* The record to start the record stream on.
        :type start_row: int
        :param num_rows: *Optional.* The number of records to retrieve.
                    By default, this retrieves the full result set.
        :type num_rows: int
        :param batch_size: *Optional.* The number of records to retrieve in
                    each request. A higher value uses more transient memory but
                    can be useful to mitigate high latency.
        :type num_rows: int
        """
        if not isinstance(start_row, int):
            raise TypeError("start_row must be int, not '{}'".format(
                type(start_row)))

        if not isinstance(num_rows, int):
            raise TypeError("num_rows must be int, not '{}'".format(
                type(num_rows)))

        if not isinstance(batch_size, int):
            raise TypeError("batch_size must be int, not '{}'".format(
                type(batch_size)))

        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0, not '{}'".
                             format(batch_size))

        with ResultSetContext(
                self._client,
                table_name=self._table_name,
                dataset_name=self._dataset_name,
                error_ds=self._error_ds,
                session_name=self._session_name) as rs:
            # Seek to the start, if we need to
            if start_row != 0:
                req = ResultSetSeekRequest()
                req.result_set_id = rs.result_set_id
                req.row_index = start_row
                req.scope.CopyFrom(rs.scope)
                self._service.seek(req)

            # XXX Maybe we should warn the user if they request too many rows
            row_count = min(rs.record_count() - start_row, num_rows)
            pos = start_row
            end_row = start_row + row_count    # EXCLUSIVE!
            while pos < end_row:
                req_recs = min(end_row - pos, batch_size)
                req = ResultSetNextRequest()
                req.result_set_id = rs.result_set_id
                req.num_rows = req_recs
                req.scope.CopyFrom(rs.scope)
                resp = self._service.next(req)
                out_rows = resp.rows
                out_meta = resp.metas
                assert len(out_rows)
                for idx, row in enumerate(out_rows):
                    yield self._format_result(out_meta[idx], row)
                    pos += 1

    def get_row(self, row):
        """
        This method returns a specifed row in the result set.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataset = client.get_dataset("sample_dataset")
            >>> result_set = dataset.result_set()
            >>> row_2 = result_set.get_row(2):

        :param row: *Required.* The row from which data is returned.
        :type row: int
        """
        if (not isinstance(row, int)):
            raise TypeError("row must be int, not '{}'".format(type(row)))

        with ResultSetContext(
                self._client,
                table_name=self._table_name,
                dataset_name=self._dataset_name,
                session_name=self._session_name,
                error_ds=self._error_ds) as rs:
            req = ResultSetSeekRequest()
            req.result_set_id = rs.result_set_id
            req.scope.CopyFrom(rs.scope)
            req.row_index = row
            self._service.seek(req)

            req = ResultSetNextRequest()
            req.result_set_id = rs.result_set_id
            req.num_rows = 1
            req.scope.CopyFrom(rs.scope)
            resp = self._service.next(req)
            out_rows = resp.rows
            out_metas = resp.metas
            return self._format_result(out_metas[0], out_rows[0])

    def record_count(self):
        """
        This method returns the number of entries in the result set

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataset = client.get_dataset("sample_dataset")
            >>> result_set = dataset.result_set()
            >>> num_rows = result_set.record_count()

        """
        # XXX This is relatively expensive, since we end up doing an API call
        # to create and free a temporary result set here. We can also cache the
        # value.
        with ResultSetContext(
                self._client,
                table_name=self._table_name,
                dataset_name=self._dataset_name,
                session_name=self._session_name,
                error_ds=self._error_ds) as rs:
            return rs.record_count()

    def print_rs(self, offset=0, num_rows=10):
        """
        This method prints the specified rows of the result set.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataset = client.get_dataset("sample_dataset")
            >>> result_set = dataset.result_set()
            >>> result_set.print_rs(num_rows = 10):

        :param offset: *Optional.* The row from which to begin printing. Default value is 0,
                    which indicates no offset.
        :type offset: int
        :param num_rows: *Optional.* The number of rows to print. Default value is 10.
        :type num_rows: int
        """
        if (not isinstance(offset, int)):
            raise TypeError("offset must be int, not '{}'".format(
                type(offset)))

        if (not isinstance(num_rows, int)):
            raise TypeError("num_rows must be int, not '{}'".format(
                type(num_rows)))

        rec_iterator = self.record_iterator(offset)

        print("\n======================= START {} =======================".
              format(self._name))
        ct = 0
        for row in rec_iterator:
            print("{:5}: {}".format(ct, row))
            ct += 1
            if (ct >= num_rows):
                break
        print(
            "======================= END   %s =======================".format(
                self._name))

# Copyright 2018-2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import json
import sys
import uuid
from google.protobuf.json_format import MessageToDict

from xcalar.compute.coretypes.SourceTypeEnum.ttypes import SourceTypeT
from xcalar.compute.coretypes.DataFormatEnums.ttypes import DfFieldTypeT
from xcalar.compute.coretypes.DataFormatEnums.constants import DfFieldTypeTStr
from xcalar.compute.coretypes.LibApisCommon.ttypes import (
    XcalarApiExportColumnT, XcalarApiRetinaDstT)
from xcalar.compute.coretypes.DagTypes.ttypes import XcalarApiNamedInputT
from xcalar.compute.coretypes.DataTargetTypes.ttypes import ExColumnNameT
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.Status.constants import StatusTStr

from xcalar.compute.util.utils import get_skew

from xcalar.compute.localtypes.DagNode_pb2 import DagNodeInputMsg
from xcalar.compute.localtypes.Operator_pb2 import MergeRequest
from xcalar.compute.localtypes.Table_pb2 import (
    PublishRequest, UnpublishRequest, TableMetaRequest)

from .LegacyApi.WorkItem import (WorkItemDeleteDagNode, WorkItemExport,
                                 WorkItemMakeRetina, WorkItemGetRetinaJson,
                                 WorkItemDeleteRetina)
from .result_set import ResultSet, DEFAULT_BATCH_SIZE

import xcalar.external.dataflow


class Table:
    """
    A Xcalar table which contains data as a result of running a dataflow.

    **Attributes**

    The following attributes are properties associated with the Dataset object. They cannot be directly set.

    * **name** (:class:`str`) - Name of the Dataset this class represents
    """

    def __init__(self, session, name):
        self._client = session.client
        self._session = session
        self._name = name

        # all meta
        self._meta = None

    @property
    def name(self):
        return self._name.split("/")[-1]

    @property
    def schema(self):
        self._get_meta()
        return self._meta.schema

    @property
    def columns(self):
        return list(self.schema.keys())

    @property
    def fqn_name(self):
        if self._session.name == self._client._sys_wkbk_name:
            return self._name
        return "/tableName/{}/{}".format(self._session._session_id.upper(),
                                         self.name)

    @property
    def pinned(self):
        self._fetch_table_meta()
        return self._meta.pinned

    @property
    def shared(self):
        self._fetch_table_meta()
        return self._meta.shared

    @property
    def state(self):
        self._fetch_table_meta()
        return self._meta.shared

    @property
    def session(self):
        return self._session

    @classmethod
    def _table_from_proto_response(cls, session, table_name, table_meta_proto):
        tab = cls(session, table_name)
        if table_meta_proto.status != StatusTStr[StatusT.StatusOk]:
            tab._meta = TableMeta(table_meta_proto)
        return tab

    def publish(self):
        """
        This method will publish the table so that table is available globally
        to be accessed from other sessions.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("my_workbook")
            >>> session = workbook.activate()
            >>> table = session.get_table("my_table")
            >>> fqn_name = table.publish()

        :Returns: Fully qualified name of the table.
        :return_type: :obj:`str`
        """
        publish_req = PublishRequest()
        publish_req.scope.CopyFrom(self._session.scope)
        publish_req.table_name = self.name
        self._client._table_service.publishTable(publish_req)
        return self.fqn_name

    def unpublish(self):
        """
        This method will unpubslish the table so that table is not available globally
        to be accessed from other sessions.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("my_workbook")
            >>> session = workbook.activate()
            >>> table = session.get_table("my_table")
            >>> table.unpublish()
        """
        unpublish_req = UnpublishRequest()
        unpublish_req.scope.CopyFrom(self._session.scope)
        unpublish_req.table_name = self.name
        self._client._table_service.unpublishTable(unpublish_req)

    def get_meta(self, include_per_node_stats=False):
        """
        This method will show the meta info of the table object.
        with include_per_node_stats as True, also includes per node
        stats like records per slot which will be useful to calculate
        skew within node.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("my_workbook")
            >>> session = workbook.activate()
            >>> table = session.get_table("my_table")
            >>> meta = table.get_meta()
        """
        self._fetch_table_meta(include_per_node_stats)
        return self._meta

    def show(self):
        """
        This method will show the first 10 rows of the table

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("my_workbook")
            >>> session = workbook.activate()
            >>> table = session.get_table("my_table")
            >>> table.show() # doctest: +SKIP
        """
        self._result_set().print_rs()

    def merge(self, delta_table):
        """
        This method will merge the delta_table into this table.
        :param delta_table: Table to merge.
        :return: new table version number
        """
        req = MergeRequest()
        req.target_scope.CopyFrom(self._session.scope)
        req.target_table = self.name
        req.delta_scope.CopyFrom(delta_table._session.scope)
        req.delta_table = delta_table.name
        self._session._operator_service.opMerge(req)

    def export(self,
               columns,
               driver_name,
               driver_params={},
               clean_up_node=True):
        """
        (Experimental) This method exports the given table using the specified
        driver and driver parameters.

        :param columns: *Required*. Columns to include in the exported table.
            (column, alias) where `column` is the name in the table and `alias`
            is what the column should be renamed to.
        :type columns: list[(str, str)]
        :param driver_name: *Required*. Name of the driver to use for export.
        :type driver_name: str
        :param driver_params: *Optional*. Driver parameters to use
        :type driver_params: dict
        :param clean_up_node: *Optional*. Cleans up the dag node created by this operation
        :type clean_up_node: bool
        :return_type: None
        :raises TypeError: if any of the parameters are of incorrect type.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("my_workbook")
            >>> session = workbook.activate()
            >>> table = session.get_table("my_table")
            >>> columns = [("user_name", "User"), ("num_friends", "Friend Count")]
            >>> table.export(columns, "single_csv")

        """
        thrift_columns = [XcalarApiExportColumnT(c[0], c[1]) for c in columns]
        dag_node_name = ""
        if clean_up_node:
            dag_node_name = str(uuid.uuid4()).replace('-', '_')
        work_item = WorkItemExport(
            self.name,
            driver_name,
            json.dumps(driver_params),
            thrift_columns,
            destDagNodeName=dag_node_name,
            userName=self._client.username,
            userIdUnique=self._client.user_id)
        self._execute(work_item)
        if clean_up_node:
            work_item = WorkItemDeleteDagNode(
                dag_node_name, SourceTypeT.SrcExport, deleteCompletely=True)
            self._execute(work_item)

    def drop(self, delete_completely=False):
        """
        Delete this table. Its data will no longer be available and nothing
        further can be done with it.

        :param delete_completely: *Optional.* If set to True, remove table metadata backing the dropped table.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("my_workbook")
            >>> session = workbook.activate()
            >>> table = session.get_table("my_table")
            >>> table.drop()

        """
        workItem = WorkItemDeleteDagNode(
            self.name,
            SourceTypeT.SrcTable,
            userName=self._client.username,
            userIdUnique=self._client.user_id,
            deleteCompletely=delete_completely)
        self._execute(workItem)

    def record_count(self):
        """
        Return the number of rows in the table.

        :return_type: :obj:`int`

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("my_workbook")
            >>> session = workbook.activate()
            >>> table = session.get_table("my_table")
            >>> num_rows = table.record_count()
        """
        self._fetch_table_meta()
        return self._meta.total_records_count

    def records(self,
                start_row=0,
                num_rows=sys.maxsize,
                batch_size=DEFAULT_BATCH_SIZE):
        """
        This method returns an iterator over rows of the table. This will
        continuously fetch batches in multiple parts if needed.  The first
        record will be the record with index `start_row` and the last record
        will have index (`start_row`+`num_rows`-1).

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("my_workbook")
            >>> session = workbook.activate()
            >>> table = session.get_table("my_table")
            >>> iterator = table.records()
            >>> row_0 = next(iterator) # Returns row 0
            >>> for record in table.records():
            ...     print(record)  # Prints rows 0,1,..,n

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

        result_set = self._result_set()

        yield from result_set.record_iterator(
            start_row=start_row, num_rows=num_rows, batch_size=batch_size)

    def get_row(self, row):
        """
        Fetch the specified row in the table. If you need many rows, prefer
        to use :meth:`records`.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("my_workbook")
            >>> session = workbook.activate()
            >>> table = session.get_table("my_table")
            >>> row_2 = table.get_row(2)

        :param row: *Required.* The row from which data is returned.
        :type row: int
        """
        if (not isinstance(row, int)):
            raise TypeError("row must be int, not '{}'".format(type(row)))

        result_set = self._result_set()
        return result_set.get_row(row)

    def pin(self):
        """
        This method pins the table.
        Once pinned cannot be dropped using drop api or from other client's like XD.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("my_workbook")
            >>> session = workbook.activate()
            >>> table = session.get_table("my_table")
            >>> table.pin()

        :Returns: None
        """
        req = DagNodeInputMsg()
        req.dag_node_name = self.name
        req.scope.CopyFrom(self._session.scope)
        self._session._dag_node_service.pin(req)
        self._pinned = True

    def unpin(self):
        """
        This method unpins the table.
        The table once unpinned can be dropped.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("my_workbook")
            >>> session = workbook.activate()
            >>> table = session.get_table("my_table")
            >>> table.unpin()

        :Returns: None
        :raises RuntimeError: if operation fails.
        """
        req = DagNodeInputMsg()
        req.dag_node_name = self.name
        req.scope.CopyFrom(self._session.scope)
        self._session._dag_node_service.unpin(req)
        self._pinned = False

    # deprecated
    def is_pinned(self):
        """
        This method returns True if the table is pinned, else False.
        Deprecated, use pinned property instead.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("my_workbook")
            >>> session = workbook.activate()
            >>> table = session.get_table("my_table")
            >>> print(table.is_pinned())
            >>> table.pin()
            >>> print(table.is_pinned())

        :Returns: Boolean value indicating whether or not the table is pinned
        :return_type: :class:`bool`
        """
        self._fetch_table_meta()
        return self._meta.pinned

    def get_dataflow(self):
        """
        Fetch the optimized dataflow that can generate this table


        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> workbook = client.get_workbook("my_workbook")
            >>> session = workbook.activate()
            >>> table = session.get_table("my_table")
            >>> df = table.get_dataflow()

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance.
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        if self.contain_prefixed_fields():
            raise TypeError(
                "Table with prefixed fields doesn't support get_dataflow")
        name = str(uuid.uuid1())
        retinaDst = XcalarApiRetinaDstT()
        retinaDst.target = XcalarApiNamedInputT()
        retinaDst.target.name = self._name
        retinaDst.numColumns = len(self.columns)

        retinaDst.columns = []
        for columnName in self.columns:
            column = ExColumnNameT()
            column.name = columnName
            column.headerAlias = columnName
            retinaDst.columns.append(column)

        # Make the retina
        workItem = WorkItemMakeRetina(name, [retinaDst], [])
        self._execute(workItem)

        # Get the retina
        workItem = WorkItemGetRetinaJson(name)
        out = self._execute(workItem)

        # Delete the retina made in the backend
        workItem = WorkItemDeleteRetina(name)
        self._execute(workItem)
        retinaDict = json.loads(out.retinaJson)
        query_string = json.dumps(retinaDict['query'][:-1])
        columns_to_export = retinaDict['tables'][0]['columns']

        df = xcalar.external.dataflow.Dataflow.create_dataflow_from_query_string(
            self._client, query_string, columns_to_export=columns_to_export)
        return df

    def get_node_skew(self):
        """
        returns table data skew across nodes in the cluster.

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> workbook = client.get_workbook("my_workbook")
            >>> session = workbook.activate()
            >>> table = session.get_table("my_table")
            >>> node_skew = table.get_node_level_skew()

        :return_type: :obj:`int`
        """
        self._fetch_table_meta()
        return self._meta.node_skew

    def contain_prefixed_fields(self):
        for _, col_type in self.schema.items():
            if col_type == DfFieldTypeTStr[DfFieldTypeT.DfFatptr]:
                return True
        return False

    # returns the cached meta(TableMeta) object
    def _get_meta(self):
        if self._meta is None:
            return self.get_meta()
        return self._meta

    def _fetch_table_meta(self, include_per_node_stats=False):
        table_meta_req = TableMetaRequest()
        table_meta_req.scope.CopyFrom(self._session.scope)
        table_meta_req.table_name = self._name
        table_meta_req.include_per_node_stats = include_per_node_stats
        table_meta_res = self._client._table_service.tableMeta(table_meta_req)
        self._meta = TableMeta(table_meta_res)
        return self._meta

    def _result_set(self):
        """
        This method returns a :class:`ResultSet <xcalar.external.result_set.ResultSet>` instance.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("my_workbook")
            >>> session = workbook.activate()
            >>> table = session.get_table("my_table")
            >>> result_set = table._result_set()

        :Returns: A :class:`ResultSet <xcalar.external.result_set.ResultSet>` instance.
        :return_type: :class:`ResultSet <xcalar.external.result_set.ResultSet>`
        """
        return ResultSet(
            self._client,
            table_name=self._name,
            session_name=self._session.name)

    def _execute(self, work_item):
        assert (self._client is not None)
        self._client._legacy_xcalar_api.setSession(self._session)
        ret = self._client._execute(work_item)
        self._client._legacy_xcalar_api.setSession(None)
        return ret


class TableMeta:
    """
    A Xcalar table metadata object which contains table attributes, aggregated stats.
    """

    def __init__(self, table_meta_proto):
        self._table_meta_proto = table_meta_proto

    @property
    def table_id(self):
        return self._table_meta_proto.attributes.table_id

    @property
    def table_name(self):
        return self._table_meta_proto.attributes.table_name

    @property
    def pinned(self):
        return self._table_meta_proto.attributes.pinned

    @property
    def shared(self):
        return self._table_meta_proto.attributes.shared

    @property
    def state(self):
        return self._table_meta_proto.attributes.state

    @property
    def schema(self):
        return {
            col_info.name: col_info.type
            for col_info in self._table_meta_proto.schema.column_attributes
        }

    @property
    def columns(self):
        return list(self.schema.keys())

    @property
    def keys(self):
        return {
            col_info.name: {
                'type': col_info.type,
                'ordering': col_info.ordering
            }
            for col_info in self._table_meta_proto.schema.key_attributes
        }

    @property
    def total_records_count(self):
        return int(self._table_meta_proto.aggregated_stats.total_records_count)

    @property
    def total_size_in_bytes(self):
        return int(self._table_meta_proto.aggregated_stats.total_size_in_bytes)

    @property
    def records_count_per_node(self):
        return [
            int(row_count) for row_count in self._table_meta_proto.
            aggregated_stats.rows_per_node
        ]

    @property
    def size_per_node(self):
        return [
            int(size_node) for size_node in self._table_meta_proto.
            aggregated_stats.size_in_bytes_per_node
        ]

    # this is needed for now to access table from xpus
    # XXX table access from xpu's should be fixed to use table_id instead
    @property
    def xdb_id(self):
        return self._table_meta_proto.attributes.xdb_id

    def node_skew(self):
        return get_skew(self.records_count_per_node)

    def as_json_dict(self):
        return MessageToDict(
            self._table_meta_proto,
            including_default_value_fields=True,
            preserving_proto_field_name=True)

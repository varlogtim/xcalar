# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import getpass
import json
import re
import uuid
import time

from xcalar.external.LegacyApi.WorkItem import (
    WorkItemSessionActivate, WorkItemSessionInact, WorkItemSessionDelete,
    WorkItemSessionDownload, WorkItemSessionUpload, WorkItemDeleteDagNode)
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException as LegacyXcalarApiStatusException

from xcalar.compute.coretypes.QueryStateEnums.ttypes import QueryStateT
from xcalar.compute.coretypes.SourceTypeEnum.ttypes import SourceTypeT
from xcalar.compute.coretypes.Status.ttypes import StatusT

from xcalar.compute.services.DagNode_xcrpc import DagNode as DagNodeService
from xcalar.compute.services.Operator_xcrpc import Operator as OperatorService

from xcalar.compute.localtypes.Sql_pb2 import SQLQueryRequest
from xcalar.compute.localtypes.Dataflow_pb2 import ExecuteRequest as DataflowExecuteRequest
from xcalar.compute.localtypes.Workbook_pb2 import WorkbookScope
from xcalar.compute.localtypes.Table_pb2 import ListTablesRequest
from xcalar.compute.localtypes.DataflowEnums_pb2 import DFS, BFS

import xcalar.compute.util.imd.imd_constants as ImdConstant

import xcalar.external.table
import xcalar.external.dataflow
import xcalar.external.client
import xcalar.external.imd
from xcalar.external.runtime import Runtime

# Sessions created by the SDK are conceptually just "context" holders for
# running dataflows or SQL.  Keep track of them by prefixing with a SDK
# specific name.
SDK_SESSION_NAME_PREFIX = ""


class Session():
    """
    |br|
    A session provides the context in which to run a dataflow.

        **Note**: *In order to create a new session, the*
        :meth:`create_session <xcalar.external.client.Client.create_session>`
        *method should be used.*

        *In order to get a Session instance to represent a specific existing
        session, the*
        :meth:'get_session <xcalar.external.client.Client.get_session>`
        :*method should be used.*

    To :meth:`create <xcalar.external.client.Client.create_session>` a \
        new session:
        >>> from xcalar.external.client import Client
        >>> client = Client()
        >>> session = client.create_session("session_name")

    To :meth:`get <xcalar.external.client.Client.get_session>` a specific \
        session:
        >>> from xcalar.external.client import Client
        >>> client = Client()
        >>> session = client.get_session("session_name")

    **Attributes**

    The following attributes are properties associated with the Session
    object. They cannot be directly set.

    * **name** (:class:`str`) - Unique name that identifies the session.
    * **client** (:class:`xcalar.external.client.Client`) - Cluster client

    **Methods**

    These are the list of available methods:

    * :meth:`execute_dataflow`
    * :meth:`execute_sql`
    * :meth:`destroy`
    * :meth:`list_tables`
    * :meth:`get_table`
    * :meth:`create_imd_table_group`
    * :meth:`list_imd_table_groups`
    * :meth:`get_imd_table_group`

    .. testsetup::

        from xcalar.external.client import Client
        client = Client()
        client.create_session("session_demo")

    .. testcleanup::

        client.get_session("session_demo").delete()
    """

    def __init__(self,
                 client,
                 session_name,
                 session_id,
                 username=getpass.getuser(),
                 created_via_workbook_activate=False):
        self._session_name = session_name
        self._client = client
        self._username = username
        self._user_id = self._client.user_id
        self.userIdUnique = self._user_id
        self._session_id = session_id
        # This flag is used to differentiate a Session created via
        # workbook.activate() and a Session created via client.create_session().
        # This is needed to determine when the backend session can be deleted
        # (which is the case for the later).
        self._created_via_workbook_activate = created_via_workbook_activate
        self._scope = None
        self._kvstore = None

        # services offered in session scope
        self._dag_node_service = DagNodeService(self.client)
        self._operator_service = OperatorService(self.client)

    def _execute(self, workItem):
        assert (self._client is not None)
        self._client._legacy_xcalar_api.setSession(self)
        ret = self._client._execute(workItem)
        self._client._legacy_xcalar_api.setSession(None)
        return ret

    @property
    def client(self):
        return self._client

    @property
    def name(self):
        return self._session_name

    @property
    def username(self):
        return self._username

    @property
    def session_id(self):
        return self._session_id

    @property
    def scope(self):
        if self._scope is None:
            scope = WorkbookScope()
            scope.workbook.name.username = self._username
            scope.workbook.name.workbookName = self._session_name
            self._scope = scope
        return self._scope

    def _apply_df_parameters(self, query_string, params):
        if not params:
            return query_string

        for key, value in params.items():
            replacementKey = "<{}>".format(key)
            query_string = re.sub(replacementKey, str(value), query_string)
        return query_string

    def execute_dataflow(self,
                         dataflow,
                         table_name=None,
                         optimized=False,
                         query_name=None,
                         params=None,
                         is_async=True,
                         pin_results=False,
                         sched_name=None,
                         poll_interval_secs=1,
                         clean_job_state=True,
                         parallel_operations=False):
        """
        This method executes the dataflow.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> session = client.create_workbook("test_wb").activate()
            >>> df = Dataflow(client, datasetName="airlines", prefix="airlines")
            >>> df = df.filter("eq(\"airlines::datetime\", 2)")
            >>> session.execute_dataflow(df, 'filteredAirlines')
            >>> session.destroy()

        :param dataflow: Dataflow to execute
        :type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>` object
        :param table_name: *Optional.* The table name of dataflow result.
        :type table_name: str
        :param optimized: bool: Execute optimized dataflow.  Default value is True.
        :param query_name: *Optional.* Query name used to track execution status
        :type query_name: str
        :param params: *Optional.* Parameters for the dataflow.
        :type params: dict
        :param is_async: *Optional.* To execute dataflow asynchronously. Default value is True.
        :type is_async: bool

        :Returns: Query name which can be used to track the status.
        :return_type: str
        :raises: TypeError: if dataflow is not a :class:`Dataflow <xcalar.external.dataflow.Dataflow>` object
        """
        if (not isinstance(dataflow, xcalar.external.dataflow.Dataflow)):
            raise TypeError(
                "dataflow must be Dataflow object, not '{}'".format(
                    type(dataflow)))
        if (table_name and not isinstance(table_name, str)):
            raise TypeError("table_name must be string, not '{}'".format(
                type(table_name)))
        if (not isinstance(optimized, bool)):
            raise TypeError("optimized must be bool, not '{}'".format(
                type(optimized)))
        if (query_name and not isinstance(query_name, str)):
            raise TypeError("query_name must be string, not '{}'".foramt(
                type(query_name)))
        if (params and not isinstance(params, dict)):
            raise TypeError("params must be dict, not '{}'".format(
                type(params)))
        if (sched_name and not isinstance(sched_name, Runtime.Scheduler)):
            raise TypeError(
                "sched_name must be Runtime.Scheduler, not '{}'".format(
                    type(sched_name)))

        if table_name:
            dataflow.final_table_name = table_name

        # XXX: In the future, we'd like to remove query_name from the interface
        # We should use the dataflow.dataflow_name to tie the name of the
        # executing instance with the user-visible dataflow name
        #
        # Until then, if a query_name IS supplied, don't rely on the caller to
        # provide unique-ness across different invocations - but generate a
        # unique name using the user-supplied name, and calling context
        # (username and session id) with a time-stamp
        #
        user_supplied_qname = None
        if query_name:
            user_supplied_qname = query_name

        # Add a prefix indicating the dataflow instance is SDK invoked (as
        # opposed to XD)

        if optimized is True:
            query_prefix = "XcalarSDKOpt"    # should not have hyphens- see below
        else:
            query_prefix = "XcalarSDK"    # should not have hyphens- see below

        # session ID should be second in the name to extract sessionID from
        # the name. The use of hyphens to separate the name forces us to put
        # sessionID first, since other names may have hyphens

        if user_supplied_qname:
            query_name = (
                f"{query_prefix}-{self.session_id}-{user_supplied_qname}"
                f"-{dataflow.dataflow_name}-{self.username}-{time.time()}")
        else:
            query_name = (
                f"{query_prefix}-{self.session_id}-{dataflow.dataflow_name}"
                f"-{self.username}-{time.time()}")

        # backend only accepts alphanumeric, hyphen or underscore
        # replacing rest of the chars to _
        query_name = re.sub(r'[^a-zA-Z0-9-_]+', '_', query_name)

        sched_name_ = ""
        if sched_name:
            sched_name_ = sched_name.value

        resp_query_name = None
        req = DataflowExecuteRequest()
        query_string = ""
        if optimized is True:
            optimized_query_str = dataflow.optimized_query_string
            if not optimized_query_str:
                raise ValueError(
                    "Dataflow provided cannot be run in optimized way!")
            optimized_query_str = self._apply_df_parameters(
                optimized_query_str, params)
            query_string = json.loads(optimized_query_str)['retina']
        else:
            query_string = dataflow.query_string
            query_string = self._apply_df_parameters(query_string, params)
        req.dataflow_str = query_string
        req.job_name = query_name
        req.scope.CopyFrom(self.scope)
        req.udf_user_name = self.username
        req.udf_session_name = self.name
        req.is_async = True
        req.pin_results = pin_results
        if table_name is not None:
            req.export_to_active_session = True
            req.dest_table = table_name
        req.sched_name = sched_name_
        req.optimized = optimized
        # this parameter was introduced for XD case, so setting to True in sdk
        # cluster config parameters can be used if we want to disable stats
        req.collect_stats = True
        # clean the job state if job successfully completed
        req.clean_job_state = clean_job_state
        req.execution_mode = BFS if parallel_operations is True else DFS

        resp = self._client._dataflow_service.execute(req)
        resp_query_name = resp.job_name

        if is_async:
            return resp_query_name

        # rely on legacy api until query state api is ported
        # to new api
        try:
            query_output = self._client._legacy_xcalar_api.waitForQuery(
                resp_query_name, pollIntervalInSecs=poll_interval_secs)
            if query_output.queryState == QueryStateT.qrError or query_output.queryState == QueryStateT.qrCancelled:
                raise LegacyXcalarApiStatusException(query_output.queryStatus,
                                                     query_output)
        except LegacyXcalarApiStatusException as ex:
            if clean_job_state is False or ex.status not in [
                    StatusT.StatusQrQueryNotExist,
                    StatusT.StatusQrQueryAlreadyDeleted
            ]:
                raise ex
        return resp_query_name

    def delete_job_state(self, query_name, deferred_delete=True):
        self._client._legacy_xcalar_api.deleteQuery(
            query_name, deferredDelete=deferred_delete)

    def execute_sql(self,
                    sql_query,
                    result_table_name=None,
                    table_prefix=None,
                    query_name=None,
                    drop_as_you_go=True,
                    drop_src_tables=False,
                    random_cross_join=False,
                    push_to_select=True,
                    get_ordered_columns=False):
        """
        This method executes the specified SQL query and returns table name.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> session = client.create_workbook("test_wb").activate()
            >>> # XXX:airlines should be a published table or xcalar table already created
            >>> session.execute_sql('SELECT * FROM airlines', 'result_table')
            >>> session.destroy()

        :param sql_query: sql query
        :type sql_query: str
        :param result_table_name: *Optional.* The table name of sql query result.
        :type result_table_name: str
        :param table_prefix: *Optional.* The prefix to add to all intermediate tables
                    of sql query execution.
        :type table_prefix: str
        :param query_name: *Optional.* Query name used to track sql query status.
        :type query_name: str
        :param drop_as_you_go: *Optional.* Drop intermediate tables in query execution.
                    By default, it is False
        :type drop_as_you_go: bool
        :param drop_src_tables: *Optional.* Drop source tables of the query.
        :type drop_src_tables: bool
        :param random_cross_join: *Optional.* Apply index operation before cross join.
        :type random_cross_join: bool
        :param push_to_select: *Optional.* To push the project and filter operations
                    into imd select operation.
        :type push_to_select: bool
        :param get_ordered_columns: *Optional.* Set it to True to get the list of ordered
            columns of the final table
        :type get_ordered_columns: bool

        :Returns: Resultant table name XXX:Should return table object
        :return_type: str
        """
        if (not isinstance(sql_query, str)):
            raise TypeError("sql_query must be str, not '{}'".format(
                type(sql_query)))
        sql_query_request = SQLQueryRequest()
        optimizations = SQLQueryRequest.Optimizations()
        optimizations.dropAsYouGo = drop_as_you_go
        optimizations.dropSrcTables = drop_src_tables
        optimizations.randomCrossJoin = random_cross_join
        optimizations.pushToSelect = push_to_select
        sql_query_request.optimizations.CopyFrom(optimizations)
        sql_query_request.userName = self.username
        sql_query_request.userId = self._user_id
        sql_query_request.sessionName = self._session_name
        if result_table_name:
            sql_query_request.resultTableName = result_table_name
        if table_prefix:
            sql_query_request.tablePrefix = table_prefix
        if query_name:
            sql_query_request.queryName = query_name
        else:
            sql_query_request.queryName = "XcalarSDK-{}-{}-sql{}".format(
                self.username, self.session_id, uuid.uuid4())
        sql_query_request.queryString = sql_query
        resp = self.client._sql_service.executeSQL(sql_query_request)
        if get_ordered_columns:
            ordered_columns = {(col.colName if not col.rename else col.rename):
                               col.colType
                               for col in resp.orderedColumns}
            return resp.tableName, ordered_columns
        return resp.tableName

    def destroy(self):
        """
        This method destroys the session. Once deleted, the session cannot be
        recovered.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> session = client.get_session("session_demo")
            >>> session.delete()

        :Returns: None
        """
        workItem = WorkItemSessionInact(
            name=self.name, userName=self.username, userIdUnique=self._user_id)
        self._execute(workItem)
        if not self._created_via_workbook_activate:
            # A session created via workbook.activate() shouldn't be
            # deleted in the backend as the workbook still needs it.
            workItem = WorkItemSessionDelete(
                namePattern=self._session_name,
                userName=self.username,
                userIdUnique=self._user_id)
            self._execute(workItem)

    def list_tables(self, pattern='*', globally_shared_only=False):
        """
        This method lists the tables available in the session.
        if globally_shared_only is set to True, list all tables of the
        session that are published (which can be accessed from
        another sessions) from this session,
        else list all the tables(private and global) of this session.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> session = client.get_session("session_demo")
            >>> tables = session.list_tables()
        """
        if not isinstance(pattern, str):
            raise TypeError("pattern must be str, not '{}'".format(
                type(pattern)))
        list_req = ListTablesRequest()
        list_req.pattern = pattern
        list_req.scope.CopyFrom(self.scope)
        list_res = self._client._table_service.listTables(list_req)
        tables = [
            xcalar.external.table.Table._table_from_proto_response(
                self, tab_name, tab_meta)
            for tab_name, tab_meta in list_res.table_meta_map.items()
            if not globally_shared_only or tab_meta.attributes.shared
        ]
        return tables

    def get_table(self, table_name):
        """
        This method returns the table matching the specified name.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> session = client.get_session("session_demo")
            >>> table = session.get_table("my_table_name")

        :param table_name: *Required.* Unique name that identifies the table to
            get
        :type table_name: str
        :raises TypeError: Incorrect parameter type
        """
        if not isinstance(table_name, str):
            raise TypeError("table_name must be str, not '{}'".format(
                type(table_name)))

        tables = self.list_tables(table_name)
        if len(tables) == 0:
            raise ValueError("No such table: '{}'".format(table_name))

        if len(tables) > 1:
            raise ValueError(
                "Multiple tables found matching: '{}'".format(table_name))

        return tables[0]

    def drop_tables(self, name_pattern, delete_completely=False):
        """
        This method drops tables matching the specified name pattern.
        Note this skips dropping pinned tables.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> session = client.get_session("session_demo")
            >>> session.drop_tables("my_table_*")

        :param name_pattern: *Required.* name pattern to regex match tables to drop
        :type table_name: str
        :param delete_completely: If true, deletes all metadata too
        :type table_name: bool
        """
        workItem = WorkItemDeleteDagNode(
            name_pattern,
            SourceTypeT.SrcTable,
            userName=self.username,
            userIdUnique=self._user_id,
            deleteCompletely=delete_completely)
        try:
            self._execute(workItem)
        except LegacyXcalarApiStatusException as ex:
            if ex.status != StatusT.StatusTablePinned:
                raise ex

    def list_imd_table_groups(self):
        """
        This method lists the imd table groups available in the session.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> session = client.get_session("session_demo")
            >>> table_groups = session.list_imd_table_groups("my_group_name")
        """
        imd_store = xcalar.external.imd.IMDMetaStore(self)
        group_names = imd_store.get_group_names()
        return [
            self.get_imd_table_group(group_name) for group_name in group_names
        ]

    def get_imd_table_group(self, group_name):
        """
        This method returns the imd table group matching the specified name

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> session = client.get_session("session_demo")
            >>> table_group = session.get_imd_table_group("my_group_name")

        :param group_name: *Required.* Unique name that identifies the imd
            table group to get
        :type group_name: str
        :raises TypeError: Incorrect parameter type
        """
        if not isinstance(group_name, str):
            raise TypeError("group_name must be str, not '{}'".format(
                type(group_name)))
        imd_table_group = xcalar.external.imd.ImdTableGroup(self, group_name)
        return imd_table_group

    def create_imd_table_group(self,
                               group_name,
                               imd_tables,
                               path_to_back_store,
                               target_name=ImdConstant.Default_target):
        """
        This method creates the imd table group using the specified name, imd tables and path

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> imd_tables = [{
                    "table_name":
                        "tab_1",
                    "primary_keys": ['id'],
                    "schema": [{
                        "name": "id",
                        "type": "INTEGER"
                    }, {
                        "name": "float_val",
                        "type": "FLOAT"
                    }]
                }]
            >>> path = "/tmp/"
            >>> session = client.get_session("session_demo")
            >>> group = session.create_imd_table_group("session_demo", imd_tables, path)

        :param group_name: *Required.* Unique name that identifies the imd
            table group
        :type group_name: str
        :param imd_tables: *Required.* Imd tables to be created as part of this group.
            It should be list of dictionary entries with information of each imd table.
            imd table should have 'table_name', 'primary_keys' and 'schema' entries.
            schema is list of dictionary entries with information of columns like col_name
            and col_type.
        :type imd_tables: list
        :param path_to_back_store: *Required.* Path to backing store to store snapshots and deltas
                note: path_to_back_store should be relative to the mountpoint of the target used.
        :type group_name: str
        :param target_name: target to use to read/write deltas and snapshots
        :type target_name: str
        :raises TypeError: Incorrect parameter type
        """
        imd_store = xcalar.external.imd.IMDMetaStore(self)
        group_info = {
            'backing_store_path': path_to_back_store,
            'target_name': target_name,
        }
        imd_store._create_group_info(
            group_name, group_info=group_info, tables_info=imd_tables)
        imd_table_group = xcalar.external.imd.ImdTableGroup(self, group_name)
        return imd_table_group

    # Provided for backwards compatibility of old tests
    def _download(self, session_name, path_to_additional_files):
        work_item = WorkItemSessionDownload(
            session_name,
            path_to_additional_files,
            userName=self.username,
            userIdUnique=self._user_id)

        return self._execute(work_item)

    # provided for backwards compatibility of old tests
    def _upload(self, session_name, session_content, path_to_additional_files):
        work_item = WorkItemSessionUpload(
            session_name,
            session_content,
            path_to_additional_files,
            userName=self.username,
            userIdUnique=self._user_id)

        return self._execute(work_item)

    # Provided for backwards compatibility of old tests (destroy)
    def _cleanup(self, session_name):
        work_item = WorkItemSessionInact(
            session_name, userName=self.username, userIdUnique=self._user_id)

        self._execute(work_item)

        work_item = WorkItemSessionDelete(
            namePattern=session_name,
            userName=self.username,
            userIdUnique=self._user_id)

        return self._execute(work_item)

    # Provided for backwards compatibility of old tests
    # (reuseExistingSession)
    def _reuse(self):
        work_item = WorkItemSessionActivate(
            self.name, userName=self.username, userIdUnique=self._user_id)

        self._execute(work_item)

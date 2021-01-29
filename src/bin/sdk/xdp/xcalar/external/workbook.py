# Copyright 2018 - 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import json
import os
import re

from xcalar.compute.localtypes.Workbook_pb2 import ConvertKvsToQueryRequest
from xcalar.compute.localtypes.Workbook_pb2 import WorkbookScope
from .LegacyApi.WorkItem import (
    WorkItemSessionInact, WorkItemSessionActivate, WorkItemSessionRename,
    WorkItemSessionPersist, WorkItemSessionDownload, WorkItemSessionDelete,
    WorkItemSessionList, WorkItemSessionNew, WorkItemUdfAdd, WorkItemListXdfs)
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException as LegacyXcalarApiStatusException
from xcalar.compute.coretypes.Status.ttypes import StatusT

from .dataset_builder import DatasetBuilder
from .data_target import DataTarget
import xcalar.external.session
import xcalar.external.dataflow
import xcalar.external.udf
import xcalar.external.kvstore
from xcalar.external.app import App

# dataflows are always owned by a special user (same one that XD uses)
dataflow_user_name = ".xcalar.published.df"


class WorkbookStateException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class Workbook():
    """
    |br|
    In order to get a detailed description of workbook concepts and
    terminology, refer to `XD Workbooks documentation
    <https://lamport.int.xcalar.com/assets/help/user/Content/B_CommonTasks/A1_ManageWorkbook.htm?>`_.
    It is important to understand how workbooks work in Xcalar Design as the same
    concepts will apply when using the SDK.

        **Note**: *In order to create a new workbook, the*
        :meth:`create_workbook <xcalar.external.client.Client.create_workbook>`
        *method should be used.*

        *In order to get a Workbook instance to represent a specific existing
        workbook, the* :meth:`get_workbook
        <xcalar.external.client.Client.get_workbook>` *method should be used,
        which returns a Workbook instance representing the desired workbook.*

        *It is important to note that the Workbook object is simply a
        representation of an existing workbook, with which useful tasks can be
        performed.*

    To :meth:`create <xcalar.external.client.Client.create_workbook>` a \
        new workbook:
        >>> from xcalar.external.client import Client
        >>> client = Client()
        >>> workbook = client.create_workbook("workbook_name")

    To :meth:`get <xcalar.external.client.Client.get_workbook>` a specific \
        workbook:
        >>> from xcalar.external.client import Client
        >>> client = Client()
        >>> workbook = client.get_workbook("workbook_name")

    To :meth:`list <xcalar.external.client.Client.list_workbooks>` all \
        workbooks:
        >>> from xcalar.external.client import Client
        >>> client = Client()
        >>> workbooks = client.list_workbooks()

    **Attributes**

    The following attributes are properties associated with the Workbook
    object. They cannot be directly set.

    * **name** (:class:`str`) - Unique name that identifies the workbook.
    * **username** (:class:`str`) - Unique name to identify the user.
    * **client** (:class:`xcalar.external.client.Client`) - Cluster client

    **Methods**

    These are the list of available methods:

    * :meth:`rename`
    * :meth:`inactivate`
    * :meth:`activate`
    * :meth:`delete`
    * :meth:`download`
    * :meth:`persist`
    * :meth:`is_active`
    * :meth:`fork`
    * :meth:`build_dataset`

     **Note**: *The Workbook class contains operations that get and list
     dataflows*.

    * :meth:`get_dataflow`
    * :meth:`list_dataflows`

     **Note**: *The Workbook class contains operations that create, get, and
     list UDF modules in the workbook, returning UDF class object(s). Similar
     operations for UDF modules in the shared cluster-wide namespace are in
     the* :class:`Client <xcalar.external.client.Client>` *class*.
     The methods to operate on the returned UDF class
     object are in the :class:`UDF <xcalar.external.udf.UDF>` *class*.

    * :meth:`create_udf_module`
    * :meth:`get_udf_module`
    * :meth:`list_udf_modules`

    .. testsetup::

        from xcalar.external.client import Client
        client = Client()
        client.create_workbook("delete_demo")
        client.create_workbook("persist_demo")
        client.create_workbook("activate_demo")
        client.create_workbook("inactivate_demo")
        client.create_workbook("fork_demo")
        client.create_workbook("import_demo")

    .. testcleanup::

        client.get_workbook("rename_demo").delete()
        client.get_workbook("persist_demo").delete()
        client.get_workbook("activate_demo").delete()
        client.get_workbook("inactivate_demo").delete()
        client.get_workbook("fork_demo").delete()
        client.get_workbook("fork_demo_copy").delete()
        client.get_workbook("import_demo").delete()
        client.get_workbook("workbook_name").delete()

    """
    _list_df_regex = "^DF2.*"

    def __init__(self,
                 client,
                 workbook_name,
                 workbook_id,
                 workbook_user=None,
                 info=None,
                 active_node=None):
        self._client = client
        self._workbook_id = workbook_id
        self.info = info
        self.active_node = active_node
        scope = WorkbookScope()
        scope.workbook.name.workbookName = workbook_name
        if workbook_user:
            # If the workbook_user is specified it must be one of the special
            # user names (currently only one possible) as we dont' want users
            # to access other's workbooks.
            assert (workbook_user == dataflow_user_name)
            scope.workbook.name.username = workbook_user
        else:
            scope.workbook.name.username = self._client.username
        self._scope = scope

        # this is needed as XcalarApi excepts a session to have userIdUnique
        self.userIdUnique = self.client.user_id
        self._kvstore = None

    def _execute(self, work_item):
        assert (self._client is not None)
        # "self" is a workbook object and so it seems inappropriate to call
        # setSession(self) below since the latter expects a session object.
        # ...but this works since the references to the session's fields yield
        # references to the same fields in the workbook object. This works for
        # now until the major cleanup when we get rid of legacy_xcalar_api
        self._client._legacy_xcalar_api.setSession(self)
        ret = self._client._execute(work_item)
        self._client._legacy_xcalar_api.setSession(None)
        return ret

    def is_active(self):
        """
        This method returns True if the workbook is Active, else False.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.create_workbook("test_workbook")
            >>> if (workbook.is_active()): workbook.inactivate()

        :Returns: Boolean value indicating whether or not the workbook is
            active
        :return_type: :class:`bool`
        :raises: WorkbookStateException: unexpected number of matches
        """
        work_item = WorkItemSessionList(
            pattern=self.name,
            userName=self.username,
            userIdUnique=self.client.user_id)
        workbooks = self._execute(work_item).sessions
        if (len(workbooks) != 1):
            raise WorkbookStateException(
                "Unexpected number of workbooks found matching name")

        state = workbooks[0].state
        active = state == "Active"
        return active

    @property
    def client(self):
        return self._client

    @property
    def name(self):
        return self.scope.workbook.name.workbookName

    @property
    def username(self):
        return self.scope.workbook.name.username

    @property
    # internal attribute ; not to be exposed to users. Use of double underscore
    # in front mangles the name and internal users must invoke it using the
    # mangled name: "_Workbook__id"
    def __id(self):
        return self._workbook_id

    @property
    def scope(self):
        return self._scope

    @property
    def kvstore(self):
        if self._kvstore is None:
            self._kvstore = xcalar.external.kvstore.KvStore(
                self.client, self.scope)
        return self._kvstore

    @name.setter
    def name(self, name):
        self.scope.workbook.name.workbookName = name

    @username.setter
    def username(self, user_name):
        self.scope.workbook.name.username = user_name

    def rename(self, new_name):
        """
        This method changes the workbook name.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("test_workbook")
            >>> workbook.rename("rename_demo")

        :param new_name: *Required.* New name you want to set for the workbook
        :type new_name: str
        :Returns: None
        :raises TypeError: if new_name is not a str
        :raises XcalarApiStatusException: if workbook already exists.
        """
        if (not isinstance(new_name, str)):
            raise TypeError("new_name must be str, not '{}'".format(
                type(new_name)))

        work_item = WorkItemSessionRename(
            newName=new_name,
            oldName=self.name,
            userName=self.username,
            userIdUnique=self.client.user_id)
        result = self._execute(work_item)
        self.name = new_name

    # XXX This is not defined in the new SDK doc. Should we make it protected/private?
    def inactivate(self):
        """
        This method inactivates the workbook.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("inactivate_demo")
            >>> workbook.inactivate()

        :Returns: None
        """
        work_item = WorkItemSessionInact(
            name=self.name,
            userName=self.username,
            userIdUnique=self.client.user_id)
        self._execute(work_item)

    def persist(self):
        """
        This method saves the current Workbook content.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("persist_demo")
            >>> workbook.persist()

        :Returns: None
        """
        work_item = WorkItemSessionPersist(
            name=self.name,
            userName=self.username,
            userIdUnique=self.client.user_id)
        self._execute(work_item)

    def download(self):
        """
        This method downloads the current workbook and returns a byte stream of
        the workbook file, in the Consolidated Unix Archive format (aka TAR format)
        that has also been compressed using GNU gzip compression. Such files have the
        ".tar.gz" file extension on UNIX/LINUX systems.

        The ".tar.gz" file contains a collection of files in open formats such as
        Google's protobuf, JSON and python. These formats have been put into a TAR archive
        and then compressed.

        It is important to note that the contents of a workbook file aren't in some
        proprietary form, but rather are transparent and easily readable. Once
        the downloaded ".tar.gz" file is uncompressed and un-tarred, the contents
        are available to be read in any of several open formats ( Google's protobuf, JSON, python etc.)

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbooks = client.list_workbooks()
            >>> downloads = [workbook.download() for workbook in workbooks]

        :Returns: The downloaded workbook
        :return_type: A byte stream of the workbook file, in the Consolidated Unix
                    Archive format (aka TAR format) that has also been compressed using
                    GNU gzip compression.
        """
        # XXX This still needs some more thought. This assumes that the path to addtional files
        #     will always be in a specific format (a format that XD has decided on).
        #     However, if XD decides to change the additional file directory layout,
        #     this would break the API since the path_to_addtional_files would point to
        #     some inexistant directory.
        path_to_additional_files = "jupyterNotebooks/{}-{}/".format(
            self.username, self.name)
        work_item = WorkItemSessionDownload(
            name=self.name,
            pathToAdditionalFiles=path_to_additional_files,
            userName=self.username,
            userIdUnique=self.client.user_id)

        result = self._execute(work_item)
        return result

    def download_to_target(self, connector_name, path):
        """
        This method downloads the current workbook to the given path using connector.
        The workbook file, in the Consolidated Unix Archive format (aka TAR format)
        that has also been compressed using GNU gzip compression. Such files have the
        ".tar.gz" file extension on UNIX/LINUX systems.

        The ".tar.gz" file contains a collection of files in open formats such as
        Google's protobuf, JSON and python. These formats have been put into a TAR archive
        and then compressed.

        It is important to note that the contents of a workbook file aren't in some
        proprietary form, but rather are transparent and easily readable. Once
        the downloaded ".tar.gz" file is uncompressed and un-tarred, the contents
        are available to be read in any of several open formats ( Google's protobuf, JSON, python etc.)

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.list_workbooks()[0]
            >>> workbook.download_to_connector("Default Shared Root", "/tmp/wb1.tar.gz")

        :Returns: None
        """
        app_mgr = App(self.client)
        app_input = {
            "op": "download_to_target",
            "notebook_name": self.name,
            "connector_name": connector_name,
            "notebook_path": path,
            "user_name": self.username
        }
        response = app_mgr.run_py_app(app_mgr.NotebookAppName, False,
                                      json.dumps(app_input))
        error = json.loads(response[1])[0][0]
        if error:
            raise RuntimeError(error)

    def activate(self):
        """
        This method activates the workbook and return a new session

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("activate_demo")
            >>> session = workbook.activate()

        :Returns: None
        """
        activateWorkItem = WorkItemSessionActivate(
            name=self.name,
            userName=self.username,
            userIdUnique=self.client.user_id)

        session = xcalar.external.session.Session(
            client=self.client,
            session_name=self.name,
            session_id=self._workbook_id,
            username=self.username,
            created_via_workbook_activate=True)

        self._execute(activateWorkItem)

        return session

    def get_session(self):
        """
        This method return the session the workbook contains

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("activate_demo")
            >>> session = workbook.get_session()
        """
        raise NotImplementedError()

    def delete(self):
        """
        This method deletes the workbook. Once deleted, the workbook cannot be recovered.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("delete_demo")
            >>> workbook.delete()

        :Returns: None
        """
        if (self.is_active()):
            self.inactivate()

        work_item = WorkItemSessionDelete(
            namePattern=self.name,
            userName=self.username,
            userIdUnique=self.client.user_id)
        self._execute(work_item)

    def fork(self, workbook_name, active=True):
        """
        This method creates a copy of the workbook and returns a
        :class:`Workbook <xcalar.external.workbook.Workbook>`
        instance representing this
        newly forked workbook, with which useful tasks can be performed on the workbook.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook(workbook_name = "fork_demo")
            >>> workbook_copy = workbook.fork("fork_demo_copy")

        :param workbook_name: *Required*. Unique name that identifies the newly forked workbook.
        :param active: *Optional*. Indicates whether or not the new workbook will be active.
                            Default value is True.
        :type workbook_name: str
        :type active: bool
        :Returns: A :class:`Workbook <xcalar.external.workbook.Workbook>` instance, which
                represents the newly forked workbook.
        :return_type: :class:`Workbook <xcalar.external.workbook.Workbook>`
        :raises TypeError: if workbook_name is not a str or active is not a bool
        :raises XcalarApiStatusException: if workbook already exists or cluster out of resources.
        """
        if (not isinstance(workbook_name, str)):
            raise TypeError("workbook_name must be str, not '{}'".format(
                type(workbook_name)))

        if (not isinstance(active, bool)):
            raise TypeError("active must be bool, not '{}'".format(
                type(active)))
        work_item = WorkItemSessionNew(
            name=workbook_name,
            fork=True,
            forkedSession=self.name,
            userName=self.username,
            userIdUnique=self.client.user_id)
        result = self._execute(work_item)
        workbook_id = result.output.outputResult.sessionNewOutput.sessionId
        workbook_obj = Workbook(
            client=self._client,
            workbook_name=workbook_name,
            workbook_id=workbook_id)
        if (active):
            workbook_obj.activate()

        return workbook_obj

    def build_dataset(self,
                      dataset_name,
                      data_target,
                      path,
                      import_format,
                      recursive=False,
                      file_name_pattern="",
                      parser_name=None,
                      parser_args={}):
        """
        This method returns a :class:`DatasetBuilder <xcalar.external.dataset_builder.DatasetBuilder>` object,
        that can be used to load a dataset.
        A dataset load is tied to a workbook and can only use udf's present in that workbook (since
        udf's are not shared across workbooks).
        Thus even though datasets are global with respect to workbooks (i.e. shared across
        workbooks for a user), loading a dataset is local to a workbook.

        Example Usage to load a dataset:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("import_demo")
            >>> db = workbook.build_dataset("sample_dataset", "Default Shared Root", "/netstore/datasets/XcalarTraining/reviews.csv", "csv")
            >>> db.option("fieldDelim", ",") # doctest: +SKIP
            >>> db.load() # doctest: +SKIP

        :param dataset_name: *Required.* The name of the dataset to be imported
        :type dataset_name: str
        :param data_target: *Required.* The name of the data target, or a :class:`DataTarget <xcalar.external.data_target.DataTarget>`
                            object representing the data target to be used.
        :type data_target: Union([:class:`str`, :class:`DataTarget <xcalar.external.data_target.DataTarget>`])
        :param path: *Required.* The path to the relevant file/folder containing the data to be imported
        :type path: str
        :param import_format: *Required.* The type of data source. Must be "csv", "json" or "udf"
        :type import_format: str
        :param recursive: *Optional.* Default value is False.
        :type recursive: bool
        :param file_name_pattern: *Optional.* Default value is "", which means only the file specified in the path will be a data source.
        :type file_name_pattern: str
        :param parser_name: *Optional.* The name of the udf file that will be used to parse the data.
                            Only relevant (and required) for "udf" imports. Must be None for "csv" and "json" import formats.
        :type parser_name: str
        :param parser_args: *Optional.* Arguements passed in to the import udf file that will be used to parse
                            the data. Only relevant for "udf" imports. Must be {} for "csv" and "json" imports.
        :type parser_args: dict

        :Returns: A :class:`DatasetBuilder <xcalar.external.dataset_builder.DatasetBuilder>` instance,
                that can be used to load a dataset.
        :return_type: :class:`DatasetBuilder <xcalar.external.dataset_builder.DatasetBuilder>`
        :raises TypeError: if any of the parameters are of incorrect type.
        :raises ValueError: if incorrect import_format, parser_name or parser_args.
         """
        if (not isinstance(data_target, (str, DataTarget))):
            raise TypeError(
                "data_target must be str or a DataTarget instance, not '{}'".
                format(type(data_target)))

        if (not isinstance(dataset_name, str)):
            raise TypeError("dataset_name must be str, not '{}'".format(
                type(dataset_name)))

        if (not isinstance(path, str)):
            raise TypeError("path must be str, not '{}'".format(type(path)))

        if (not isinstance(recursive, bool)):
            raise TypeError("recursive must be bool, not '{}'".format(
                type(recursive)))

        if (not isinstance(file_name_pattern, str)):
            raise TypeError("file_name_pattern must be str, not '{}'".format(
                type(file_name_pattern)))

        if (not isinstance(import_format, str)):
            raise TypeError(
                "import_format must be of type str, not '{}'".format(
                    type(import_format)))

        import_format = import_format.lower()

        if (import_format not in ["csv", "json", "udf"]):
            raise ValueError(
                "Invalid import format: '{}'".format(import_format))

        if (parser_name is not None and (not isinstance(parser_name, str))):
            raise TypeError("parser_name must be str, not '{}'".format(
                type(parser_name)))

        if (parser_args != {} and (not isinstance(parser_args, dict))):
            raise TypeError("parser_args must be dict, not '{}'".format(
                type(parser_args)))

        if (import_format in ["csv", "json"]):
            if (parser_name is not None):
                raise ValueError("parser_name cannot be set for '{}' imports".
                                 format(import_format))
            if (parser_args != {}):
                raise ValueError("parser_args cannot be set for '{}' imports".
                                 format(import_format))

        if (import_format == "udf"):
            if (parser_name is None):
                raise ValueError(
                    "parser_name must be a valid str representing import udf file, not 'None'"
                )

        if (isinstance(data_target, DataTarget)):
            target_name = data_target.name
        elif (isinstance(data_target, str)):
            # make sure get_data_target works, and doesn't raise an error to confirm target_name exists
            self._client.get_data_target(data_target)
            target_name = data_target

        return DatasetBuilder(
            self,
            name=dataset_name,
            target_name=target_name,
            path=path,
            import_format=import_format,
            recursive=recursive,
            file_name_pattern=file_name_pattern,
            parser_name=parser_name,
            parser_args=parser_args)

    def list_dataflows(self):
        """
        This method returns a list of dataflows contained in the workbook.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("workbook_name")
            >>> dataflow_list = workbook.list_dataflows()

        :Returns: list of dataflow names contained in the workbook
        :return_type: list
        """

        # The dataflows stored in kvstore all have keys starting with "DF2".
        # We have to look into the value to determine the dataflow name.
        dataflow_names = []
        keys = self.kvstore.list(self._list_df_regex)
        for key in keys:
            kvs_query = self.kvstore.lookup(key)
            kvs_json = json.loads(kvs_query)
            dataflow_names.append(kvs_json['name'])

        # XXX: should return a list of Dataflow objects.  But that means
        # converting the DF2 values into query_string and optimized_query_string
        # (sharing the code in get_dataflow) but then conversion errors on one
        # dataflow would sabotage the list operation.

        return dataflow_names

    def get_dataflow(self, dataflow_name, params=None):
        """
        This method returns the dataflow with the specified name.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("workbook_name")
            >>> dataflow_list = workbook.list_dataflows()
            >>> dataflow = workbook.get_dataflow(dataflow_list[0])

        :param dataflow_name: *Required.* Name of the workbook containing the dataflows
        :type dataflow_name: str
        :param params: *Optional.* Parameters for the dataflow.
        :type params: dict
        :Returns: the dataflow with the specified name
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>` object
        :raises: TypeError: if dataflow_name is not a str
        :raises: WorkbookStateException: if dataflow cannot be obtained
        """
        if (not isinstance(dataflow_name, str)):
            raise TypeError("dataflow_name must be str, not '{}'".format(
                type(dataflow_name)))
        if (params and not isinstance(params, dict)):
            raise TypeError("params must be dict, not '{}'".format(
                type(params)))

        # The dataflow is stored in the KvStore of the session.  Each dataflow's
        # key starts with "DF2".  So we have to list all the keys and then look
        # within the value for each keyxs to find the matching dataflow name.
        kvs_query = None
        # Build a list of all the queries in the workbook.  This is needed if
        # the dataflow that is being looked up contains linkin/linkout nodes
        # which require all related dataflows to resolve.  Since we don't know
        # which ones are related we'll have to send all in that case.  We don't
        # want to unconditionally send all of them so as to avoid any unneeded
        # performance penalty.
        all_kvs_queries = []
        keys = self.kvstore.list(self._list_df_regex)
        for key in keys:
            value = self.kvstore.lookup(key)
            kvs_json = json.loads(value)
            if kvs_json['name'] == dataflow_name:
                assert (kvs_query is None)
                kvs_query = value
            all_kvs_queries.extend([value])

        if kvs_query is None:
            raise WorkbookStateException(
                "Failed to find dataflow '{}' in workbook '{}'".format(
                    dataflow_name, self.name))

        # Build a string containing the listXdfs output to include with the
        # request.  This is needed by the expServer in some cases to convert
        # the query (e.g. SQL)
        xdfs = self._client._udf.list("*", "*")
        xdfs_json = self._client._thrift_to_json(xdfs)
        xdfs_json_string = json.dumps(xdfs_json)

        # Send a request to the workbook service manager to convert the
        # KvStore value into a query string.
        req = ConvertKvsToQueryRequest()
        req.kvsString.extend([kvs_query])
        req.dataflowName = dataflow_name
        req.optimized = False
        req.listXdfsOutput = xdfs_json_string
        req.userName = self.username
        req.sessionId = self.__id
        req.workbookName = self.name
        if params:
            param_messages = []
            for key, val in params.items():
                param_message = ConvertKvsToQueryRequest.Parameter()
                # parameter key and value will be treated as string types
                param_message.name = str(key)
                param_message.value = str(val)
                param_messages.append(param_message)
            req.parameters.extend(param_messages)

        resp = self._client._workbook_service.convertKvsToQuery(req)
        if resp.converted is False:
            if "DagTabManager is not defined" in resp.resultString or \
                    "Cannot find linked graph" in resp.resultString or \
                    "Cannot find linked function" in resp.resultString:
                # Try again except pass all the dataflows in the workbook.  This
                # syntax overwrites a protobuf repeated field.
                req.kvsString[:] = all_kvs_queries
                resp = self._client._workbook_service.convertKvsToQuery(req)
                if resp.converted is False:
                    raise WorkbookStateException(
                        "Failed to convert workbook dataflows for '{}' into query: {}"
                        .format(dataflow_name, resp.resultString))
            # having publish-table node in dataflow gives empty non-optimized string, and
            # which is fine, as we don't allow non-optimized execution of such dataflows
            elif resp.resultString:
                raise WorkbookStateException(
                    "Failed to convert dataflow '{}' into query: {}".format(
                        dataflow_name, resp.resultString))

        # The query string sometimes has an extra "," which leads it to being
        # an invalid json string.  Thus we replace it with a " ".
        empty_query_string = '[]'
        query_string = empty_query_string
        if resp.resultString:
            query_string = resp.resultString
            len_query_string = len(query_string)
            if (query_string[len_query_string - 2] == ","):
                query_string = query_string[:len_query_string - 2] + "]"

        # Also get the optimized query string
        req.optimized = True
        resp = self._client._workbook_service.convertKvsToQuery(req)
        if resp.converted is False:
            raise WorkbookStateException(
                "Failed to convert dataflow '{}' into optimized query: {}".
                format(dataflow_name, resp.resultString))

        optimized_query_string = resp.resultString

        # check if we got both queries as empty and fail
        if optimized_query_string == '' and query_string == empty_query_string:
            raise WorkbookStateException(
                f"Failed to convert dataflow '{dataflow_name}' into query,"
                " yielded empty query!")

        df = xcalar.external.dataflow.Dataflow.create_dataflow_from_query_string(
            client=self._client,
            query_string=query_string,
            optimized_query_string=optimized_query_string,
            dataflow_name=dataflow_name)

        # XXX hack to get publish node information of XD dataflow
        df.set_publish_result_map(resp.publishResultMap)

        return df

    def create_udf_module(self, udf_module_name, udf_module_source):
        """
        This method creates a UDF module in the workbook, returning the UDF
        class object for the newly created UDF module.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("test_workbook")
            >>> udf_source = "def foo():\\n return 'foo'"
            >>> myudf = workbook.create_udf_module("udf_test_mod", udf_source)

        :param udf_module_name: *Required.* Name of the UDF module
        :type udf_module_name: str
        :param udf_module_source: *Required.* Source code string for the UDF
        :type udf_module_source: str
        :raises TypeError: if udf_module_name or udf_module_source is not a str
        :raises XcalarApiStatusException: if UDF already exists
        :raises XcalarApiStatusException: if UDF name is invalid
        :NOTE: Valid characters in udf_module_name are: alphanumeric, '-', '_'
        :raises XcalarApiStatusException: if UDF source has invalid syntax
        """
        if (not isinstance(udf_module_name, str)):
            raise TypeError("udf_module_name must be str, not '{}'".format(
                type(udf_module_name)))

        if (not isinstance(udf_module_source, str)):
            raise TypeError("udf_module_source must be str, not '{}'".format(
                type(udf_module_source)))

        self.activate()
        workItem = WorkItemUdfAdd(udf_module_name, udf_module_source)
        try:
            result = self._execute(workItem)
            assert result.moduleName == udf_module_name
        except LegacyXcalarApiStatusException as e:
            if e.status == StatusT.StatusUdfModuleLoadFailed:
                print("udf error:\n{}".format(
                    e.output.udfAddUpdateOutput.error.message))
            raise
        # return object from new UDF_module class
        udf_module_obj = xcalar.external.udf.UDF(
            client=self._client,
            workbook=self,
            udf_module_name=udf_module_name)
        return udf_module_obj

    def get_udf_module(self, udf_module_name):
        """
        This method returns a UDF class object for the named UDF module in
        the workbook.

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.workbook import Workbook
            >>> client = Client()
            >>> workbook = client.get_workbook("test_workbook")
            >>> udf = workbook.get_udf_module("udf_test_mod")

        :param udf_module_name: *Required.* Name of the UDF module
        :type udf_module_name: str
        :raises TypeError: if udf_module_name is not a str
        :raises ValueError: if UDF does not exist or multiple modules match
        """
        if (not isinstance(udf_module_name, str)):
            raise TypeError("udf_module_name must be str, not '{}'".format(
                type(udf_module_name)))

        umos = self.list_udf_modules(modNamePattern=udf_module_name)
        if (len(umos) == 0):
            raise ValueError(
                "No such UDF module: '{}'".format(udf_module_name))
        if (len(umos) > 1):
            raise ValueError("Multiple modules found matching: '{}'".format(
                udf_module_name))
        return umos[0]

    def list_udf_modules(self, modNamePattern="*"):
        """
        This method returns a list of UDF class objects from the workbook- one
        for each UDF module whose name matches the specified pattern.

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.workbook import Workbook
            >>> client = Client()
            >>> workbook = client.get_workbook("test_workbook")
            >>> udf_mods = workbook.list_udf_modules("*modFoo*")
        """
        udf_full_path = "/workbook/{}/{}/udf/{}:*".format(
            self.username, self._workbook_id, modNamePattern)
        workItem = WorkItemListXdfs(udf_full_path, "User-defined*")
        listOut = self._execute(workItem)
        mod_names_list = []
        umod_list = []
        if listOut.numXdfs >= 1:
            for uix in range(0, listOut.numXdfs):
                fq_fname = listOut.fnDescs[uix].fnName
                fq_mname, fname = re.match(r'(.*):(.*)', fq_fname).groups()
                mod_name = os.path.basename(fq_mname)
                if mod_name not in mod_names_list:
                    mod_names_list.append(mod_name)
                    umo = xcalar.external.udf.UDF(
                        client=self._client,
                        workbook=self,
                        udf_module_name=mod_name)
                    umod_list.append(umo)
        return umod_list

    # This is marked internal for now since it can be built over the
    # add and update methods, and is in general just a convenience routine
    # which is somewhat dangerous from an API point of view since its use
    # may replace a module unintentionally.
    # XXX: This could be called "_set()" instead of "_create_or_update()"...
    #      Need to decide
    # XXX:
    # Alternatively, we could make this an option to create_udf_module
    # (e.g. add --force) but then what's the difference b/w add --force and
    # update? That'd be confusing. With create and update the assumption made
    # by the caller is clear:
    #
    # create: assume udf_module_name doesn't exist and request is to create it
    # update: assume udf_module_name exists and request is to update it
    #
    # those two are sufficient for all use cases and we shouldn't add any
    # more APIs than are absolutely needed.
    #
    # Also, tests which use this today generally need to do this at start
    # up and so should just first remove UDFs (cleanup) and then do creates
    # instead of the sloppier createOrUpdate - so even for tests, the use case
    # is unclear
    def _create_or_update_udf_module(self, udf_module_name, udf_module_source):
        """
        If the UDF module exists in the workbook, it's updated; otherwise
        it's created, and a UDF class object is returned for the module

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("test_workbook")
            >>> udf_source = "def foo():\n return 'foo'"
            >>> workbook._create_or_update_udf_module("test_mod", udf_source)

        :param udf_module_name: *Required.* Name of the UDF module
        :type udf_module_name: str
        :param udf_module_source: *Required.* Source code string for the UDF
        :type udf_module_source: str
        :raises TypeError: if udf_module_name or udf_module_source is not a str
        :raises XcalarApiStatusException: if UDF name is invalid
        :NOTE: Valid characters in udf_module_name are: alphanumeric, '-', '_'
        :raises XcalarApiStatusException: if UDF source has invalid syntax
        """
        if (not isinstance(udf_module_name, str)):
            raise TypeError("udf_module_name must be str, not '{}'".format(
                type(udf_module_name)))

        if (not isinstance(udf_module_source, str)):
            raise TypeError("udf_module_source must be str, not '{}'".format(
                type(udf_module_source)))

        self.activate()
        try:
            udf_module_obj = self.create_udf_module(udf_module_name,
                                                    udf_module_source)
        except LegacyXcalarApiStatusException as e:
            if e.status == StatusT.StatusUdfModuleAlreadyExists:
                udf_module_obj = xcalar.external.udf.UDF(
                    client=self._client,
                    workbook=self,
                    udf_module_name=udf_module_name)
                udf_module_obj.update(udf_module_source)
            else:
                raise
        return udf_module_obj

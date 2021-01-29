# Copyright 2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

from xcalar.external.LegacyApi.WorkItem import (
    WorkItemUdfUpdate, WorkItemUdfGet, WorkItemUdfDelete,
    WorkItemListXdfs)
from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiWorkbookScopeT
from xcalar.compute.localtypes.UDF_pb2 import GetResolutionRequest

class UDF():
    """
    |br|
    In order to get a detailed description of UDF concepts and
    terminology, refer to the Xcalar Design help documentation.
    It is important to understand how UDFs work in Xcalar Design as similar
    concepts will apply when using the SDK. UDFs can reside in workbooks or in
    the cluster-wide shared namespace (i.e. shared between users and workbooks).
    There are 3 methods which return an instance of a UDF object (create, get
    and list) in each of the two namespaces (workbook and the cluster-wide
    shared namespace).

    **Note**: *In order to create a new UDF module, either the*
    :meth:`workbook's create_udf_module
    <xcalar.external.workbook.Workbook.create_udf_module>` method OR the
    :meth:`shared create_udf_module
    <xcalar.external.client.Client.create_udf_module>` method should be used.

    *In order to get an instance of a UDF object representing an existing UDF,
    either the* :meth:`workbook's get_udf_module
    <xcalar.external.workbook.Workbook.get_udf_module>` *method OR the*
    :meth:`shared get_udf_module <xcalar.external.client.Client.get_udf_module>`
    *method should be used.*

    *It is important to note that the UDF object is simply a representation of
    an existing UDF, with which useful tasks can be performed.*

    To create a new UDF module (in a :meth:`workbook
    <xcalar.external.workbook.Workbook.create_udf_module>` OR in the
    :meth:`shared space <xcalar.external.client.Client.create_udf_module>`):

        >>> from xcalar.external.client import Client
        >>> client = Client()
        >>> workbook = client.create_workbook("workbook_name")
        >>> udf = workbook.create_udf_module("udf_name", "udf_source")
        >>> # following creates a shared UDF module
        >>> shared_udf = client.create_udf_module("shared_udf_name", "shared_udf_source")

    To get a specific UDF module (from a :meth:`workbook
    <xcalar.external.workbook.Workbook.get_udf_module>` OR from the
    :meth:`shared space <xcalar.external.workbook.Workbook.get_udf_module>`):

        >>> from xcalar.external.client import Client
        >>> client = Client()
        >>> workbook = client.get_workbook("workbook_name")
        >>> udf = workbook.get_udf_module("udf_name")
        >>> # following gets a shared UDF module (if one exists with the name)
        >>> shared_udf = client.get_udf_module("shared_udf_name")

    To list all UDF modules (in a :meth:`workbook
    <xcalar.external.workbook.Workbook.list_udf_modules>` OR in the
    :meth:`shared space <xcalar.external.client.Client.list_udf_modules>`):

        >>> from xcalar.external.client import Client
        >>> client = Client()
        >>> workbook = client.get_workbook("workbook_name")
        >>> udfs = workbook.list_udf_modules()
        >>> # following lists all shared UDF modules
        >>> shared_udfs = client.list_udf_modules()

    **Attributes**

    The following attributes are properties associated with the UDF
    object. They cannot be directly set. Note that the cluster client is used
    only when the workbook is empty (this indicates shared UDFs)

    * **name** (:class:`str`) - Unique name that identifies the UDF.
    * **workbook** (:class:`xcalar.external.workbook.Workbook`) - Workbook (set to None if UDF object is from the shared space)

    **Methods**

    These are the list of available methods:

    * :meth:`delete`
    * :meth:`update`

    """

    def __init__(self, client, workbook, udf_module_name):
        self._client = client
        self._workbook = workbook
        self._udf_module_name = udf_module_name

    def _execute(self, workItem):
        self._client._legacy_xcalar_api.setSession(self._workbook)
        apiOut = self._client._legacy_xcalar_api.execute(workItem)
        self._client._legacy_xcalar_api.setSession(None)
        return apiOut

    @property
    def workbook(self):
        return self._workbook

    @property
    def name(self):
        return self._udf_module_name

    def delete(self):
        """
        This method deletes the UDF module (from the workbook or shared space).
        WARNING: the UDF is deleted even though some operation in a dataflow
        may reference it - if the deletion is from a workbook, the reference
        may resolve to the shared space - if the deletion results in the last
        resolvable copy being deleted, the dataflow execution will fail during
        an attempt to retrieve the (now fully deleted) UDF.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("test_workbook")
            >>> udf = workbook.get_udf_module("test_mod")
            >>> udf.delete()
            >>> shared_udf = client.get_udf_module("test_shared_mod")
            >>> shared_udf.delete()

        :raises XcalarApiStatusException: if UDF does not exist
        """
        workItem = WorkItemUdfDelete(self._udf_module_name)
        return self._execute(workItem)

    def update(self, udf_module_source):
        """
        This method updates a UDF module (in the workbook or in shared space)
        with the supplied source

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("test_workbook")
            >>> udf = workbook.get_udf_module("udf_test_mod")
            >>> wkbook_new_udf_source = 'def bar():\\n return 10'
            >>> udf.update(wkbook_new_udf_source)
            >>> shared_udf = client.get_udf_module("udf_test_shared_mod")
            >>> new_shared_udf_source = 'def shared_bar():\\n return 100'
            >>> shared_udf.update(new_shared_udf_source)

        :param udf_module_source: *Required.* Source code string for the UDF
        :type udf_module_source: str
        :raises TypeError: if udf_module_source is not a str
        :raises XcalarApiStatusException: if UDF does not exist
        :raises XcalarApiStatusException: if UDF source has invalid syntax
        """

        if (not isinstance(udf_module_source, str)):
            raise TypeError("udf_module_source must be str, not '{}'".format(
                type(udf_module_source)))
        workItem = WorkItemUdfUpdate(self._udf_module_name, udf_module_source)
        result = self._execute(workItem)
        assert result.moduleName == self._udf_module_name

    # Internal methods below: needed to test functionality needed by XD

    def _get(self):
        """
        This method gets a UDF module's source code - returns it as a string

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("test_workbook")
            >>> udf = workbook.get_udf_module("test_mod")
            >>> source = udf._get()

        :raises XcalarApiStatusException: if UDF does not exist
        """

        workItem = WorkItemUdfGet(self._udf_module_name)
        result = self._execute(workItem)
        return result.source

    def _get_path(self):
        """
        This method gets a UDF module's path. This is needed to help detect
        the path a UDF is coming from (e.g. from workbook or shared space?)
        since the same module name may exist in multiple spaces

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("test_workbook")
            >>> udf = workbook.get_udf_module("test_mod")
            >>> udfpath = udf._get_path()

        :raises XcalarApiStatusException: if UDF does not exist
        """

        request = GetResolutionRequest()
        request.udfModule.name = self._udf_module_name

        if self._workbook is not None:
            request.udfModule.scope.workbook.name.username = self._workbook.username
            request.udfModule.scope.workbook.name.workbookName = self._workbook.name
        else:
            request.udfModule.scope.globl.SetInParent()
        result = self._client._udf_service.getResolution(request)
        return result.fqModName.text

    def _list(self, fnNamePattern="*"):
        """
        This method lists the functions in the UDF module which match
        a function name pattern - it returns a list of function descriptions-
        one for each function in the UDF module.

        XXX: The return type is of type 'struct XcalarApiListXdfsOutput'
        defined in src/include/libapis/LibApisCommon.h. If this is ever made
        public, we should add a new public type and return that from here. For
        now, this is just for internal tests...

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("test_workbook")
            >>> fnList = workbook._list("*myFuncName*")

        :param fnNamePattern: *Required.* Pattern for function name
        :raises TypeError: if fnNamePattern is not a str
        """

        if (not isinstance(fnNamePattern, str)):
            raise TypeError("fnNamePattern must be str, not '{}'".format(
                type(fnNamePattern)))

        if self._workbook is not None:
            udf_func_full_path = "/workbook/{}/{}/udf/{}:{}".format(
                self._workbook.username, self._workbook._workbook_id,
                self._udf_module_name, fnNamePattern)
        else:
            udf_func_full_path = "/sharedUDFs/{}:{}".format(
                self._udf_module_name, fnNamePattern)

        workItem = WorkItemListXdfs(udf_func_full_path, "User-defined*")
        listOut = self._execute(workItem)
        return listOut

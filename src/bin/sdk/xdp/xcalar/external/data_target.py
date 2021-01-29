# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import json

from xcalar.external.exceptions import XDPException
from xcalar.external.LegacyApi.WorkItem import (WorkItemDeleteTarget2,
                                                WorkItemPreview)

from xcalar.compute.coretypes.LibApisCommon.ttypes import DataSourceArgsT
from xcalar.compute.localtypes.Connectors_pb2 import ListFilesRequest


class DataTarget():
    """
    |br|
    In order to get a detailed description of data target concepts and terminology, refer to
    `XD Data Targets documentation <https://www.xcalar.com/documentation/help/XD/1.4.0/Content/A_GettingStarted/B1_MakingDataAccessible.htm>`_ .
    It is import to understand how data targets work in Xcalar Design as
    the same concepts will apply when using the SDK.

        **Note**: *In order to add a new data target in Xcalar, the*
        :meth:`add_data_target <xcalar.external.client.Client.add_data_target>`
        *method should be used, which will return a DataTarget instance representing the newly
        added data target.*

        *In order to get a DataTarget instance to represent a specific existing data target in Xcalar,
        the* :meth:`get_data_target <xcalar.external.client.Client.get_data_target>`
        *method should be used, which returns a DataTarget instance representing the desired data target.*

        *It is important to note that the DataTarget object is
        simply a representation of an existing data target, with which useful tasks can be performed.*

    To :meth:`add <xcalar.external.client.Client.add_data_target>` a data target:

        >>> from xcalar.external.client import Client
        >>> client = Client()
        >>> data_target = client.add_data_target(target_name = "target_demo",
        ...                                      target_type_id = "shared",
        ...                                      params = {"mountpoint":"/"})

    To :meth:`get <xcalar.external.client.Client.get_data_target>` a data target:

        >>> from xcalar.external.client import Client
        >>> client = Client()
        >>> data_target = client.get_data_target(target_name = "target_demo")

    To :meth:`list <xcalar.external.client.Client.list_data_targets>` all data targets:

        >>> from xcalar.external.client import Client
        >>> client = Client()
        >>> data_targets = client.list_data_targets()

    **Attributes**

    The following attributes are properties associated with the DataTarget object. They cannot be
    modified.

    * **name** (:class:`str`) - Name of the Data Target
    * **type_id** (:class:`str`) - The type_id that identifies the data target type
    * **type_name** (:class:`str`) - The type_name for the data target type
    * **params** (:class:`dict`) - A dictionary, where the key is the parameter name and value
        is the parameter value. Refer to :ref:`data_target_types_list_ref`
        to get a list of data target types and information about their respective
        optional/required parameters.

    **Methods**

    These are the available methods:

    * :meth:`preview`
    * :meth:`list_files`
    * :meth:`delete`

    """

    def __init__(self, client, target_name, target_type_id, target_type_name,
                 params):
        self._client = client
        self._target_name = target_name
        self._target_type_id = target_type_id
        self._params = params
        self._target_type_name = target_type_name

    def _execute(self, workItem):
        return self._client._execute(workItem)

    @property
    def name(self):
        return self._target_name

    @property
    def type_id(self):
        return self._target_type_id

    @property
    def type_name(self):
        return self._target_type_name

    @property
    def params(self):
        return self._params

    def list_files(self,
                   path,
                   pattern="*",
                   recursive=False,
                   paged=False,
                   token=""):
        """
        This method lists the files in a given path.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> data_target = client.get_data_target("target_demo")
            >>> data_target.list_files(path = "/somePath") # doctest: +SKIP

        Paged Example:
            >>> token = ""
            >>> all_files = []
            >>> while(true):
            >>>     resp = data_target.list_files(path="/somePath", paged=True, token=token)
            >>>     all_files = all_files + resp['files']
            >>>     token = resp['continuationToken']
            >>>     if token == "":
            >>>         break

        :param path: *Required.* The path to directory from which files will be listed.
        :type path: str
        :param pattern: *Optional* Search-pattern for files to list. The default value is
                    "*", which lists all the files.
        :type pattern: str
        :param recursive: *Optional.* Whether or not the search is to be recursive. The default value is False.
        :type recursive: bool
        :param paged: *Optional.* Whether or not the results should be returned in pages with a accompanying continuationToken to retrieve the next portion of the list. The default value is False.
        :type paged: bool
        :param token: *Optional.* The continuationToken returned by a previous list_files() call used to retrieve the next page of results.
        :type token: str
        :raises TypeError: if any of the parameters are of incorrect type.
        :raises ValueError: if any of the parameters are invalid.
        :raises <xcalar.external.exceptions.XDPException>: if there was a problem with the request.

        :Returns: A dict containing a list of dicts info about the files and a continuationToken if a paged list was requested.
        :return_type: list

        **Response Syntax**:

            ::

                {
                    'files': [
                        {
                            'name': 'comma.csv'
                            'isDir': False,
                            'size': 47,
                            'mtime': 1527200450
                        }
                    ],
                    'continuationToken': '18nRC25i7yjM2Z9q+EgNW2...ehXlz8FDVNbQp5BUA=='
                }


        :Response Structure:
            * (**dict**) -

              * **files** (**list**) -

                * (**dict**) -

                  * **name** (*str*)
                  * **mtime** (*int*)
                  * **size** (*int*)
                  * **isDir** (*bool*)

              * **continuationToken** (*str*)

        """
        if (not isinstance(path, str)):
            raise TypeError(f"path must be str, not '{type(path)}'")
        if (not isinstance(pattern, str)):
            raise TypeError(f"pattern must be str, not '{type(pattern)}'")
        if (not isinstance(recursive, bool)):
            raise TypeError(f"recursive must be bool, not '{type(recursive)}'")
        if (not isinstance(paged, bool)):
            raise TypeError(f"paged must be bool, not '{type(paged)}'")
        if (not isinstance(token, str)):
            raise TypeError(f"token must be str, not '{type(token)}'")

        req = ListFilesRequest()
        req.sourceArgs.targetName = self._target_name
        req.sourceArgs.path = path
        req.sourceArgs.fileNamePattern = pattern
        req.sourceArgs.recursive = recursive
        req.paged = paged
        req.continuationToken = token

        try:
            resp = self._client._connectors_service.listFiles(req)
        except XDPException as e:
            # XXX TTUCKER, look more into resolving this further up the stack.
            if "No files found" in str(e):
                return {"files": []}
            raise

        result = {}
        # MessageToDict doesn't cast int64 fields correctly.
        # https://github.com/protocolbuffers/protobuf/issues/2954
        result['files'] = [{
            "name": file.name,
            "size": file.size,
            "mtime": file.mtime,
            "isDir": file.isDir
        } for file in resp.files]
        result['continuationToken'] = resp.continuationToken
        return result

    def preview(self,
                path,
                pattern="",
                recursive=False,
                num_bytes=100,
                offset=0):
        """
        This method shows a preview of the raw bytes
        (not the parsed records) from the data at the desired location.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> data_target = client.get_data_target("target_demo")
            >>> data_target.preview(path = "/somePath", num_bytes = 10) # doctest: +SKIP

        :param path: *Required.* The path to directory/file to preview.
        :type path: str
        :param pattern: *Optional.* Search-pattern for files to preview. Default is "",
                        which means only the file specified in the path will be previewed.
        :type pattern: str
        :param recursive: *Optional.* Whether or not the search for files to preview is
                        to be recursive. Default value if False.
        :type recursive: bool
        :param num_bytes: *Optional.* The number of bytes to preview. Default value is 100 bytes.
        :type num_bytes: int
        :param offset: *Optional.* The offset at which to begin previewing the data source.
                        Default value is 0, which indicates no offset.
        :return_type: int
        :raises TypeError: if any of the parameters are of incorrect type.
        :raises ValueError: if any of the parameters are invalid.


        **Responce Syntax**:

            ::

                {
                    'fileName': 'reviews.csv',
                    'relPath': 'reviews.csv',
                    'fullPath': '/netstore/datasets/XcalarTraining/reviews.csv',
                    'base64Data': 'cmV2aWV3SUQscg==',
                    'thisDataSize': 10,
                    'totalDataSize': 72140109
                }

        :Response Structure:
            * (**dict**) -

              * **fileName** (*str*)
              * **relPath** (*str*)
              * **fullPath** (*str*)
              * **base64Data** (*str*)
              * **thisDataSize** (*int*)
              * **totalDataSize** (*int*)

        """
        if (not isinstance(path, str)):
            raise TypeError("path must be str, not '{}'".format(type(path)))
        if (not isinstance(pattern, str)):
            raise TypeError("pattern must be str, not '{}'".format(
                type(pattern)))
        if (not isinstance(recursive, bool)):
            raise TypeError("recursive must be bool, not '{}'".format(
                type(recursive)))
        if (not isinstance(num_bytes, int)):
            raise TypeError("num_bytes must be int, not '{}'".format(
                type(num_bytes)))
        if (not isinstance(offset, int)):
            raise TypeError("offset must be int, not '{}'".format(
                type(offset)))

        sourceArgs = DataSourceArgsT()
        sourceArgs.targetName = self.name
        sourceArgs.fileNamePattern = pattern
        sourceArgs.path = path
        sourceArgs.recursive = recursive

        workItem = WorkItemPreview(sourceArgs, num_bytes, offset)
        return json.loads(self._execute(workItem).outputJson)

    def _to_dict(self):
        details = dict()
        details['target_name'] = self.name
        details['target_type_id'] = self.type_id
        details['target_type_name'] = self.type_name
        details['params'] = self.params
        return details

    def delete(self):
        """
        This method deletes the data target.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> data_target = client.get_data_target("target_demo")
            >>> data_target.delete() # doctest: +SKIP
        """
        workItem = WorkItemDeleteTarget2(targetName=self.name)
        self._execute(workItem)

    def __str__(self):
        return json.dumps(self._to_dict())

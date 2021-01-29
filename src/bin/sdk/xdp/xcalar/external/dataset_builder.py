# Copyright 2018 Xcalar, Inc. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import json

from xcalar.external.LegacyApi.WorkItem import WorkItemLoad, WorkItemDatasetCreate
from xcalar.external.LegacyApi.WorkItem import WorkItemDatasetDelete, WorkItemDatasetGetMeta
from xcalar.external.LegacyApi.WorkItem import WorkItemDatasetUnload
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException as LegacyXcalarApiStatusException

from xcalar.external.data_target import DataTarget

from xcalar.compute.coretypes.LibApisCommon.constants import XcalarApiDefaultRecordDelimT
from xcalar.compute.coretypes.LibApisCommon.constants import XcalarApiDefaultFieldDelimT
from xcalar.compute.coretypes.LibApisCommon.constants import XcalarApiDefaultQuoteDelimT
from xcalar.compute.coretypes.LibApisCommon.constants import XcalarApiDefaultCsvParserNameT
from xcalar.compute.coretypes.LibApisCommon.constants import XcalarApiDefaultJsonParserNameT
from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiDfLoadArgsT
from xcalar.compute.coretypes.LibApisCommon.ttypes import DataSourceArgsT
from xcalar.compute.coretypes.LibApisCommon.ttypes import ParseArgsT
from xcalar.compute.coretypes.QueryStateEnums.ttypes import QueryStateT


class DatasetBuilder():
    """
    |br|

    .. testsetup::

        from xcalar.external.client import Client
        client = Client()
        client.create_workbook("import_dataset")

    .. testcleanup::

        client.get_dataset("sample_dataset").delete()
        client.get_dataset("sample_dataset_2").delete()
        client.get_workbook("import_dataset").delete()

    A DatasetBuilder object is used to import a dataset into xcalar.
    In order to get a DatasetBuilder object, the
    :meth:`build_dataset <xcalar.external.workbook.Workbook.build_dataset>`
    method should be used, which will return a DatasetBuilder instance that
    can be used to load the dataset.

    Like in Xcalar Design, multiple import formats are available,
    including "csv", "json" and "udf". Since udf's are local to a
    workbook (and not shared in all workbooks for a user), importing
    datasets requires a workbook.

    Example Usage to import a dataset:
        >>> from xcalar.external.client import Client
        >>> client = Client()
        >>> workbook = client.get_workbook("import_dataset")
        >>> db = workbook.build_dataset("sample_dataset",
        ...                            "Default Shared Root",
        ...                            "/netstore/datasets/XcalarTraining/reviews.csv",
        ...                            "csv")
        >>> db = db.option("fieldDelim", ",")
        >>> dataset = db.load()

    It is important to note that even though importing a dataset is tied to
    a workbook, once imported the dataset is shared and available for use
    in all workbooks for that user.


    **Attributes**

    The following attributes are properties associated with the DatasetBuilder object.
    import_format, workbook_name, username and source_args cannot be modified.

    * **dataset_name** (:class:`str`)
    * **workbook_name** (:class:`str`)
    * **import_format** (:class:`str`)
    * **source_args** (:class:`list`) - A list of dicts containing data source information (target_name and path).
    * **parser_name** (:class:`str`)
    * **parser_args** (:class:`dict`)
    * **username** (:class:`str`)

    **Methods**

    These are the list of available methods:

    * :meth:`option`
    * :meth:`load`
    * :meth:`list_import_options`
    * :meth:`add_source`

    """

    def __init__(self,
                 workbook,
                 name,
                 target_name,
                 path,
                 import_format,
                 recursive=False,
                 file_name_pattern="",
                 parser_name=None,
                 parser_args={}):

        self._args = {}
        self.csv_args = {}

        # set args to default values
        self.csv_args["recordDelim"] = XcalarApiDefaultRecordDelimT
        self.csv_args["fieldDelim"] = XcalarApiDefaultFieldDelimT
        self.csv_args["quoteDelim"] = XcalarApiDefaultQuoteDelimT
        self.csv_args["linesToSkip"] = 0
        self.csv_args["isCRLF"] = False
        self.csv_args["emptyAsFnf"] = False
        self.csv_args["schemaMode"] = "header"
        self.csv_args["schemaFile"] = ""
        self.csv_args["dialect"] = "default"
        self._args["sampleSize"] = 0
        self._args["schema"] = []
        self._args["allowFileErrors"] = False
        self._args["allowRecordErrors"] = False
        self._args["fileNameField"] = ""
        self._args["recordNumField"] = ""

        self._client = workbook._client
        self._legacy_xc_api = self._client._legacy_xcalar_api

        self._dataset_name = name
        self._username = self._client.username
        self._user_id = self._client.user_id
        self._import_format = import_format
        self._workbook = workbook
        self._workbook_name = workbook.name
        self._source_args = []
        source_arg = {}
        source_arg["target_name"] = target_name
        source_arg["path"] = path
        source_arg["recursive"] = recursive
        source_arg["fileNamePattern"] = file_name_pattern
        self._source_args.append(source_arg)

        self._loadArgs = XcalarApiDfLoadArgsT()
        self._loadArgs.parseArgs = ParseArgsT()

        self._parser_args = {}

        if (self.import_format == "csv"):
            self._parser_name = XcalarApiDefaultCsvParserNameT
        elif (self.import_format == "json"):
            self._parser_name = XcalarApiDefaultJsonParserNameT
        else:
            self._parser_name = parser_name
            self._parser_args = parser_args

    def _execute(self, workItem):
        assert (self._client is not None)
        return self._client._execute(workItem)

    @property
    def workbook_name(self):
        return self._workbook_name

    @property
    def username(self):
        return self._username

    @property
    def import_format(self):
        return self._import_format

    @property
    def parser_name(self):
        return self._parser_name

    @property
    def parser_args(self):
        return self._parser_args

    @parser_name.setter
    def parser_name(self, parser_name):
        if (not isinstance(parser_name, str)):
            raise TypeError("parser_name must be str, not '{}'".format(
                type(parser_name)))

        if (self.import_format in ["csv", "json"]):
            raise ValueError(
                "parser_name cannot be set for '{}' imports".format(
                    self.import_format))
        self._parser_name = parser_name

    @parser_args.setter
    def parser_args(self, parser_args):
        if (not isinstance(parser_args, dict)):
            raise TypeError("parser_args must be dict, not '{}'".format(
                type(parser_args)))

        if (self.import_format in ["csv", "json"]):
            raise ValueError(
                "parser_args cannot be set for '{}' imports".format(
                    self.import_format))
        self._parser_args = parser_args

    @property
    def dataset_name(self):
        return self._dataset_name

    @property
    def source_args(self):
        return self._source_args

    @dataset_name.setter
    def dataset_name(self, dataset_name):
        if (not isinstance(dataset_name, str)):
            raise TypeError("dataset_name must be str, not '{}'".format(
                type(dataset_name)))
        self._dataset_name = dataset_name

    def list_import_options(self):
        """
        This method lists the current import options.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("import_dataset")
            >>> db = workbook.build_dataset("sample_dataset", "Default Shared Root", "/somePath/reviews.csv", import_format = "csv")
            >>> import_options = db.list_import_options()

        :Returns: A dict showing the current import option values. Output depends
                on import format.
        :return_type: dict

        Below is an example response for a "csv" import:

            ::

                {
                    'sampleSize': 0,
                    'schema': [],
                    'allowFileErrors': False,
                    'allowRecordErrors': False,
                    'fileNameField': '',
                    'recordNumField': '',

                    # The rest are only relevant to csv imports
                    'recordDelim': '\\n',
                    'fieldDelim': '\\t',
                    'quoteDelim': '"',
                    'linesToSkip': 0,
                    'isCRLF': False,
                    'schemaMode': 'header',
                    'schemaFile': ''
                }

        """
        if (self.import_format == "csv"):
            args = self._args
            args.update(self.csv_args)
            return args
        return self._args

    def option(self, param, value):
        """
        This method allows you to set import options.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("import_dataset")
            >>> db = workbook.build_dataset("sample_dataset", "Default Shared Root", "/somePath/reviews.csv", "csv")
            >>> db.option("fieldDelim", ",").option("allowFileErrors", False) # doctest: +SKIP

        The following import options can be set for udf, json and csv files:

            ==================  ==============
            Import Option       Default Value
            ==================  ==============
            sampleSize          0
            schema              []
            allowFileErrors     False
            allowRecordErrors   False
            fileNameField       ""
            recordNumField      ""
            ==================  ==============

        The following additional import options are only relevant for csv file imports:

            ==================  ==============
            Import Option       Default Value
            ==================  ==============
            recordDelim         "\\\\n"
            fieldDelim          "\\\\t"
            quoteDelim          "\\\\""
            linesToSkip         0
            isCRLF              False
            emptyAsFnf          False
            schemaMode          "header"
            ==================  ==============


        :param param: The import option that will be set to the specified value.
                    Refer to the list above, which lists all the valid import options
                    that can be set.
        :type param: str
        :param value:  The value to set the import option to. (type depends on the import option)
        :raises TypeError: if param or value is of incorrect type.
        :raises ValueError: if param or value are invalid.
        """
        # check if csv_arg
        if (self.import_format == "csv"):
            if (param in self.csv_args):
                if (not isinstance(value, type(self.csv_args[param]))):
                    raise TypeError(
                        "value for '{}' must be {}, not '{}'".format(
                            param, type(self.csv_args[param]), type(value)))

                self.csv_args[param] = value
                return self

        if (param not in self._args):
            raise ValueError("No such param: '{}'".format(param))

        if (not isinstance(value, type(self._args[param]))):
            raise TypeError("value for '{}' must be {}, not '{}'".format(
                param, type(self._args[param]), type(value)))

        self._args[param] = value
        return self

    def xc_query(self):
        sourceArgList = []
        sourceArgListJson = []
        for source_arg in self.source_args:
            sourceArgs = DataSourceArgsT()
            sourceArgs.targetName = source_arg["target_name"]
            sourceArgs.fileNamePattern = source_arg["fileNamePattern"]
            sourceArgs.path = source_arg["path"]
            sourceArgs.recursive = source_arg["recursive"]
            sourceArgList.append(sourceArgs)
            sourceArgListJson.append(self._client._thrift_to_json(sourceArgs))

        if (self.import_format == "csv"):
            self._parser_args = self.csv_args

        self._loadArgs.sourceArgsList = sourceArgList
        self._loadArgs.parseArgs.parserFnName = self.parser_name
        self._loadArgs.parseArgs.parserArgJson = json.dumps(self.parser_args)
        self._loadArgs.parseArgs.fileNameFieldName = self._args[
            "fileNameField"]
        self._loadArgs.parseArgs.recordNumFieldName = self._args[
            "recordNumField"]
        self._loadArgs.parseArgs.allowFileErrors = self._args[
            "allowFileErrors"]
        self._loadArgs.parseArgs.allowRecordErrors = self._args[
            "allowRecordErrors"]
        self._loadArgs.parseArgs.schema = self._args["schema"]

        self._loadArgs.size = self._args["sampleSize"]
        return [{
            "operation": "XcalarApiBulkLoad",
            "args": {
                "dest": self.dataset_name,
                "loadArgs": {
                    "sourceArgsList": sourceArgListJson,
                    "parseArgs": {
                        "parserFnName":
                            self.parser_name,
                        "parserArgJson":
                            self._loadArgs.parseArgs.parserArgJson,
                        "fileNameFieldName":
                            self._loadArgs.parseArgs.fileNameFieldName,
                        "recordNumFieldName":
                            self._loadArgs.parseArgs.recordNumFieldName,
                        "allowFileErrors":
                            self._loadArgs.parseArgs.allowFileErrors,
                        "allowRecordErrors":
                            self._loadArgs.parseArgs.allowRecordErrors,
                    },
                    "size": self._loadArgs.size,
                },
            }
        }]

    def load(self, as_df=False):
        """
        This method loads the data and returns a
        :class:`Dataset <xcalar.external.dataset.Dataset>` instance which
        represents the sucessfully loaded dataset.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("import_dataset")
            >>> db = workbook.build_dataset("sample_dataset_2",
            ...                         "Default Shared Root",
            ...                         "/netstore/datasets/XcalarTraining/reviews.csv",
            ...                         "csv")
            >>> db = db.option("fieldDelim", ",")
            >>> dataset = db.load()

        :Returns: A :class:`Dataset <xcalar.external.dataset.Dataset>` instance which
                represents the sucessfully loaded dataset.
        :return_type: :class:`Dataset <xcalar.external.dataset.Dataset>`
        """
        load_query = self.xc_query()
        if as_df is True:
            # XXX use session.execute_dataflow api
            query_name = "XcalarSDK-{}-{}-{}-dataset-load".format(
                self.username, self._workbook._Workbook__id, self.dataset_name)
            self._legacy_xc_api.submitQuery(
                json.dumps(load_query),
                sessionName=self.workbook_name,
                queryName=query_name,
                isAsync=True,
                userName=self.username,
                userIdUnique=self._user_id,
                collectStats=True)
            query_output = self._legacy_xc_api.waitForQuery(
                query_name, userName=self.username, userIdUnique=self._user_id)
            if query_output.queryState == QueryStateT.qrError or query_output.queryState == QueryStateT.qrCancelled:
                raise LegacyXcalarApiStatusException(query_output.queryStatus,
                                                     query_output)
            self._legacy_xc_api.deleteQuery(
                query_name, userName=self.username, userIdUnique=self._user_id)
        else:
            workItem = WorkItemLoad(
                self.dataset_name,
                self._loadArgs,
                userName=self.username,
                userIdUnique=self._user_id,
                sessionName=self.workbook_name)
            result = None
            try:
                result = self._execute(workItem)
            except LegacyXcalarApiStatusException as e:
                # XXX When exception e is "Connection reset by peer",
                #     e.output is the following  string: "unknown output".
                #     Thus in this case, e.output.loadOut would raise
                #     an exception: 'str' object has no attribute 'loadOutput'.
                #     To avoid this, the line below was added.
                if isinstance(e.output, str):
                    raise
                if e.output.loadOutput is not None:
                    result = e.output.loadOutput
                    print("Load error in '{}':\n{}".format(
                        result.errorFile,
                        result.errorString.replace("\\n", "\n")))
                    raise

        if not self.dataset_name.startswith(".XcalarDS."):
            self.dataset_name = ".XcalarDS." + self.dataset_name
        dataset = self._client.get_dataset(self.dataset_name)
        return dataset

    def add_source(self,
                   data_target,
                   path,
                   recursive=False,
                   file_name_pattern=""):
        """
        This method adds data sources to the datasetBuilder.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("import_dataset")
            >>> db = workbook.build_dataset("sample_dataset", "Default Shared Root", "/somePath/reviews.csv", import_format = "csv")
            >>> db.add_source("Default Shared Root", "/SomeOtherPath/airlines.csv")
            >>> db.load() # doctest: +SKIP

        :param data_target: *Required.* The name of the data target, or a :class:`DataTarget <xcalar.external.data_target.DataTarget>`
                            object representing the data target to be used.
        :type data_target: Union([:class:`str`, :class:`DataTarget <xcalar.external.data_target.DataTarget>`])
        :param path: *Required.* The path to the relevant file/folder containing the data to be imported
        :type path: str
        :param recursive: *Optional.* Default value is False.
        :type recursive: bool
        :param file_name_pattern: *Optional.* Default value is "", which means only the file specified in the path will be a data source.
        :type file_name_pattern: str
        :raises TypeError: if any of the parameters are of incorrect type.
        """
        if (not isinstance(data_target, (str, DataTarget))):
            raise TypeError(
                "data_target must be str or a DataTarget instance, not '{}'".
                format(type(data_target)))

            if (not isinstance(path, str)):
                raise TypeError("path must be str, not '{}'".format(
                    type(path)))

        if (not isinstance(recursive, bool)):
            raise TypeError("recursive must be bool, not '{}'".format(
                type(recursive)))

            if (not isinstance(file_name_pattern, str)):
                raise TypeError(
                    "file_name_pattern must be str, not '{}'".format(
                        type(file_name_pattern)))

                if (isinstance(data_target, DataTarget)):
                    target_name = data_target.name
        elif (isinstance(data_target, str)):
            # make sure get_data_target works, and doesn't raise an error to confirm target_name exists
            self._client.get_data_target(data_target)
            target_name = data_target

        source_arg = {}
        source_arg["target_name"] = target_name
        source_arg["path"] = path
        source_arg["recursive"] = recursive
        source_arg["fileNamePattern"] = file_name_pattern

        self._source_args.append(source_arg)

    def dataset_create(self):
        """
        This method creates the dataset meta on the backend but doesn't actually load
        it.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook("create_dataset")
            >>> db = workbook.build_dataset("sample_dataset",
            ...                         "Default Shared Root",
            ...                         "/netstore/datasets/XcalarTraining/reviews.csv",
            ...                         "csv")
            >>> dataset = db.dataset_create()
        """

        # XXX: the desired, eventual, outcome is to have clients use
        # dataset_create to specify the arguments to be used in a subsequent
        # load.  The load() function (up above) would then simply take the name
        # of the dataset and use the arguments supplied here.
        #

        sourceArgList = []
        for source_arg in self.source_args:
            sourceArgs = DataSourceArgsT()
            sourceArgs.targetName = source_arg["target_name"]
            sourceArgs.fileNamePattern = source_arg["fileNamePattern"]
            sourceArgs.path = source_arg["path"]
            sourceArgs.recursive = source_arg["recursive"]
            sourceArgList.append(sourceArgs)

        if (self.import_format == "csv"):
            self._parser_args = self.csv_args

        self._loadArgs.sourceArgsList = sourceArgList
        self._loadArgs.parseArgs.parserFnName = self.parser_name
        self._loadArgs.parseArgs.parserArgJson = json.dumps(self.parser_args)
        self._loadArgs.parseArgs.fileNameFieldName = self._args[
            "fileNameField"]
        self._loadArgs.parseArgs.recordNumFieldName = self._args[
            "recordNumField"]
        self._loadArgs.parseArgs.allowFileErrors = self._args[
            "allowFileErrors"]
        self._loadArgs.parseArgs.allowRecordErrors = self._args[
            "allowRecordErrors"]
        self._loadArgs.parseArgs.schema = self._args["schema"]

        self._loadArgs.size = self._args["sampleSize"]

        workItem = WorkItemDatasetCreate(
            self.dataset_name,
            self._loadArgs,
            userName=self.username,
            userIdUnique=self._user_id,
            sessionName=self.workbook_name)
        result = None

        try:
            result = self._execute(workItem)
        except LegacyXcalarApiStatusException as e:
            # XXX When exception e is "Connection reset by peer",
            #     e.output is the following  string: "unknown output".
            #     Thus in this case, e.output.loadOut would raise
            #     an exception: 'str' object has no attribute 'loadOutput'.
            #     To avoid this, the line below was added.
            if isinstance(e.output, str):
                raise

    def dataset_unload(self):
        """
        This method unloads the dataset from memory.

        Example:
            >>> db.dataset_unload()

        """

        workItem = WorkItemDatasetUnload(
            self.dataset_name,
            userName=self.username,
            userIdUnique=self._user_id,
            sessionName=self.workbook_name)
        result = None

        try:
            result = self._execute(workItem)
        except LegacyXcalarApiStatusException as e:
            if isinstance(e.output, str):
                raise

    def dataset_delete(self):
        """
        This method deletes the dataset meta on the backend.

        Example:
            >>> db.dataset_delete()

        """

        workItem = WorkItemDatasetDelete(
            self.dataset_name,
            userName=self.username,
            userIdUnique=self._user_id,
            sessionName=self.workbook_name)
        result = None

        try:
            result = self._execute(workItem)
        except LegacyXcalarApiStatusException as e:
            if isinstance(e.output, str):
                raise

    def dataset_getMeta(self):
        """
        This method gets the dataset meta specified in the dataset create

        Example:
            >>> db.dataset_getMeta()

        """

        workItem = WorkItemDatasetGetMeta(
            self.dataset_name,
            userName=self.username,
            userIdUnique=self._user_id,
            sessionName=self.workbook_name)
        result = None

        try:
            result = self._execute(workItem)
        except LegacyXcalarApiStatusException as e:
            if isinstance(e.output, str):
                raise

        return result

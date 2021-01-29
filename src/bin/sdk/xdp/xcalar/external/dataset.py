# Copyright 2018-2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import sys

from xcalar.external.LegacyApi.WorkItem import (
    WorkItemDeleteDagNode, WorkItemDatasetUnload, WorkItemGetTableMeta,
    WorkItemListDataset, WorkItemGetDatasetsInfo)
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException as LegacyXcalarApiStatusException
from xcalar.compute.coretypes.SourceTypeEnum.ttypes import SourceTypeT
from .result_set import ResultSet, DEFAULT_BATCH_SIZE


class Dataset():
    """
        **Note**: *In order to import a new dataset, the*
        :meth:`build_dataset <xcalar.external.workbook.Workbook.build_dataset>`
        *method must be used, which will return a*
        :class:`DatasetBuilder <xcalar.external.dataset_builder.DatasetBuilder>`
        *instance that can be used to load the dataset*

        *In order to get a Dataset instance to represent a specific existing dataset,
        the* :meth:`get_dataset <xcalar.external.client.Client.get_dataset>`
        *method should be used, which returns a Dataset instance representing the desired dataset.*

        *It is important to note that the Dataset object is
        simply a representation of an existing dataset, with which useful tasks can be performed on the dataset.*

    .. testsetup::

        from xcalar.external.client import Client
        client = Client()
        workbook = client.create_workbook("import_dataset")
        db = workbook.build_dataset("sample_dataset", "Default Shared Root", "/netstore/datasets/XcalarTraining/reviews.csv", "csv")
        db.option("fieldDelim", ",")
        db.load()

    .. testcleanup::

        client.get_dataset("sample_dataset").delete()
        client.get_workbook("import_dataset").delete()

    To :meth:`list <xcalar.external.client.Client.list_datasets>` all datasets:
        >>> from xcalar.external.client import Client
        >>> client = Client()
        >>> datasets = client.list_datasets()

    To :meth:`get <xcalar.external.client.Client.get_dataset>` a specific dataset:
        >>> from xcalar.external.client import Client
        >>> client = Client()
        >>> dataset = client.get_dataset("sample_dataset")

    To import a new dataset:
        >>> from xcalar.external.client import Client
        >>> client = Client()
        >>> workbook = client.get_workbook("import_dataset")
        >>> db = workbook.build_dataset("sample_dataset",
        ...                            "Default Shared Root",
        ...                            "/netstore/datasets/XcalarTraining/reviews.csv",
        ...                            "csv")
        >>> db = db.option("fieldDelim", ",")
        >>> dataset = db.load() # doctest: +SKIP

    **Attributes**

    The following attributes are properties associated with the Dataset object. They cannot be directly set.

    * **name** (:class:`str`) - Name of the Dataset this class represents

    **Methods**

    These are the list of available methods:

    * :meth:`delete`
    * :meth:`get_info`
    * :meth:`records`
    * :meth:`get_row`
    * :meth:`errors`
    * :meth:`record_count`

    """

    def __init__(self, client, name):
        self._client = client
        self._name = name
        self._username = self._client.username
        self._user_id = self._client.user_id

    @property
    def name(self):
        return self._name

    def _execute(self, workItem):
        assert (self._client is not None)
        return self._client._execute(workItem)

    def delete(self, delete_completely=False):
        """
        This method drops the dataset. It is important to note, however,
        that the dataset will not be completely deleted until all
        users cease to use the dataset. While other workbooks/users still use
        the dataset, it is marked as *pending delete*.

        :param delete_completely: *Optional.* If set to True, remove dataset metadata backing the dropped dataset.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataset = client.get_dataset("sample_dataset")
            >>> dataset.delete() # doctest: +SKIP

        """
        # delete SrcDataset Dag node from all workbooks for this user
        for sess in self._client._list_sessions():
            if sess.state != "Active":
                continue
            workItem = WorkItemDeleteDagNode(
                self.name,
                SourceTypeT.SrcDataset,
                userName=self._username,
                userIdUnique=self._user_id,
                sessionName=sess.name,
                deleteCompletely=delete_completely)
            try:
                self._execute(workItem)
            except LegacyXcalarApiStatusException as e:
                # any error here is OK - the attempt to delete the dag node
                # isn't critical to the act of trying to delete the DS
                pass
        workItem = WorkItemDatasetUnload(self.name)
        try:
            self._execute(workItem)
        except LegacyXcalarApiStatusException as e:
            if e.message != "Dataset not found":
                raise e

    def get_info(self):
        """
        This method returns a json containing information about the dataset.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataset = client.get_dataset("reviews")
            >>> info = dataset.get_info()

        :return_type: dict

        **Response Syntax**:

            ::

                {
                    "loadArgs": {
                        "sourceArgsList": [
                            {
                                "targetName": "Default Shared Root",
                                "path": "/somePath/reviews.csv",
                                "fileNamePattern": "",
                                "recursive": false
                            }
                        ],
                        "parseArgs": {
                            "parserFnName": "default:parseCsv",
                            "parserArgJson": {
                                "recordDelim": "\\n",
                                "fieldDelim": ",",
                                "isCRLF": true,
                                "linesToSkip": 1,
                                "quoteDelim": "\"",
                                "hasHeader": true,
                                "schemaFile": "",
                                "schemaMode": "loadInput"
                            },
                            "fileNameFieldName": "",
                            "recordNumFieldName": "",
                            "allowRecordErrors": false,
                            "allowFileErrors": false,
                            "schema": [
                                {
                                    "sourceColumn": "reviewID",
                                    "destColumn": "review_id",
                                    "columnType": "DfString"
                                },
                                {
                                    "sourceColumn": "reviewText",
                                    "destColumn": "review_text",
                                    "columnType": "DfString"
                                }
                            ]
                        },
                        "maxSize": 10737418240
                    },
                    "datasetId": "6660",
                    "name": ".XcalarDS.reviews",
                    "loadIsComplete": true,
                    "isListable": true,
                    "udfName": ""
                    "downSampled": False,
                    "totalNumErrors": 0,
                    "datasetSize": 73531392,
                    "numColumns": 2,
                    "columns": [
                        {
                            "name": "review_id",
                            "type": 1
                        },
                        {
                            "name": "review_text",
                            "type": 3
                        }
                    ]
                }

        :Response Structure:
            * (*dict*) -

              * **loadArgs** (*dict*) -

                * **sourceArgsList** (*list*) -

                  * (*dict*) -

                    * **targetName** (*str*)
                    * **path** (*str*) - path to file
                    * **fileNamePattern** (*str*)
                    * **recursive** (*bool*)

                * **parseArgs** (*dict*) -

                  * **parserFnName** (*str*)
                  * **parserArgJson** (*dict*)
                  * **fileNameFieldName** (*str*)
                  * **recordNumFieldName** (*str*)
                  * **allowRecordErrors** (*bool*)
                  * **allowFileErrors** (*bool*)
                  * **schema** (*list*) -

                    * (*dict*) -

                      * **sourceColumn** (*str*)
                      * **destColumn** (*str*)
                      * **columnType** (*str*)

                * **maxsize** (*int*)

              * **datasetId** (*str*)
              * **name** (*str*)
              * **loadIsComplete** (*bool*)
              * **isListable** (*bool*)
              * **udfName** (*str*)
              * **downSampled** (*bool*)
              * **totalNumErrors** (*int*)
              * **datasetSize** (*int*)
              * **numColumns** (*int*)
              * **columns** (*list*) -

                * (*dict*) -

                  * **name** (*str*)
                  * **type** (*int*)

        """
        workItem = WorkItemListDataset()
        datasets = self._execute(workItem).datasets

        for dataset in datasets:
            if (dataset.name == self.name):
                summary = self._summary()
                summary.pop('datasetName')
                res = self._client._thrift_to_json(dataset)
                res.update(summary)
                return res

    def record_count(self):
        """
        Return the number of rows in the dataset.

        :return_type: :obj:`int`

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataset = client.get_dataset("sample_dataset")
            >>> num_rows = dataset.record_count()
        """
        meta = self._get_dataset_meta()
        total = 0
        for x in meta["metas"]:
            total += x["numRows"]
        return total

    def _summary(self):
        workItem = WorkItemGetDatasetsInfo(self.name)
        datasets_info = self._execute(workItem).getDatasetsInfoOutput

        dataset = datasets_info.datasets[0]
        return self._client._thrift_to_json(dataset)

    def summary(self):
        return str(self._summary())

    def __str__(self):
        return self.summary()

    def records(self,
                start_row=0,
                num_rows=sys.maxsize,
                batch_size=DEFAULT_BATCH_SIZE):
        """
        This method returns an iterator over rows of the dataset. This will
        continuously fetch batches in multiple parts if needed.  The first
        record will be the record with index `start_row` and the last record
        will have index (`start_row`+`num_rows`-1).

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataset = client.get_dataset("sample_dataset")
            >>> iterator = dataset.records()
            >>> row_0 = next(iterator) # Returns row 0
            >>> for record in dataset.records():
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

    def errors(self,
               start_row=0,
               num_rows=sys.maxsize,
               batch_size=DEFAULT_BATCH_SIZE):
        """
        This method returns an iterator over the errors encountered when
        loading this dataset. This will continuously fetch batches in multiple
        parts if needed.  The first record will be the record with index
        `start_row` and the last record will have index
        (`start_row`+`num_rows`-1).

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataset = client.get_dataset("sample_dataset")
            >>> iterator = dataset.errors()
            >>> row_0 = next(iterator) # Returns row 0
            >>> for record in dataset.errors():
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

        result_set = self._error_result_set()

        yield from result_set.record_iterator(
            start_row=start_row, num_rows=num_rows, batch_size=batch_size)

    def get_row(self, row):
        """
        Fetch the specified row in the dataset. If you need many rows, prefer
        to use :meth:`records`.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataset = client.get_dataset("sample_dataset")
            >>> row_2 = dataset.get_row(2):

        :param row: *Required.* The row from which data is returned.
        :type row: int
        """
        if (not isinstance(row, int)):
            raise TypeError("row must be int, not '{}'".format(type(row)))

        result_set = self._result_set()
        return result_set.get_row(row)

    def show(self):
        """
        Print the dataset. Only use this on small datasets!
        """
        result_set = self._result_set()
        result_set.print_rs()

    def _result_set(self):
        """
        This method returns a :class:`ResultSet <xcalar.external.result_set.ResultSet>` instance.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataset = client.get_dataset("sample_dataset")
            >>> result_set = dataset._result_set()

        :Returns: A :class:`ResultSet <xcalar.external.result_set.ResultSet>` instance.
        :return_type: :class:`ResultSet <xcalar.external.result_set.ResultSet>`
        """
        return ResultSet(self._client, dataset_name=self.name)

    def _error_result_set(self):
        """
        This method returns a :class:`ResultSet <xcalar.external.result_set.ResultSet>` instance,
        which can be used to view the errors in the dataset.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataset = client.get_dataset("sample_dataset")
            >>> error_result_set = dataset._error_result_set()

        :Returns: A :class:`ResultSet <xcalar.external.result_set.ResultSet>` instance.
        :return_type: :class:`ResultSet <xcalar.external.result_set.ResultSet>`
        """
        return ResultSet(self._client, dataset_name=self.name, error_ds=True)

    def _get_dataset_meta(self):
        # XXX system workbook per user: SDK-826
        self._client._create_system_workbook()

        workItem = WorkItemGetTableMeta(
            False,
            self.name,
            False,
            userName=self._username,
            userIdUnique=self._user_id,
            sessionName=self._client._sys_wkbk_name)
        res = self._client._thrift_to_json(self._execute(workItem))
        return res

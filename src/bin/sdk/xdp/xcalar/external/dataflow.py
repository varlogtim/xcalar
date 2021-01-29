# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import json
from collections import OrderedDict

from xcalar.compute.localtypes.Dataflow_pb2 import (
    FilterRequest, MapRequest, AggregateRequest, GenRowNumRequest, SortRequest,
    GroupByRequest, AggColInfo, JoinRequest, UnionRequest, UnionTableInfo,
    UnionColInfo, JoinTableInfo, JoinOptions, SynthesizeRequest, ColRenameInfo,
    ProjectRequest)

from xcalar.compute.coretypes.JoinOpEnums.ttypes import JoinOperatorT
from xcalar.compute.coretypes.UnionOpEnums.ttypes import UnionOperatorT
from xcalar.compute.coretypes.OrderingEnums.constants import XcalarOrderingT

import xcalar.external.table
from xcalar.external.dataset import Dataset

# Prevent circular import
import xcalar.external.client


class Dataflow:
    """
    |br|
    A Xcalar dataflow which can perform transformation on table data

        **Note**: *In order to create a new dataflow from a dataset, the*
        :meth:`create_dataflow_from_dataset \
            <xcalar.external.dataflow.Dataflow.create_dataflow_from_dataset>`
        *method should be used.*

        **Note**: *In order to create a new dataflow from another dataflow, the*
        :meth:`create_dataflow_from_dataflow \
            <xcalar.external.dataflow.Dataflow.create_dataflow_from_dataflow>`
        *method should be used.*

        **Note**: *In order to create a new dataflow from a table, the*
        :meth:`create_dataflow_from_table \
            <xcalar.external.dataflow.Dataflow.create_dataflow_from_table>`
        *method should be used.*

    To :meth:`create <xcalar.external.dataflow.Dataflow.create_dataflow_from_dataset>` \
        a dataflow from a dataset:
        >>> from xcalar.external.client import Client
        >>> from xcalar.external.dataflow import Dataflow
        >>> client = Client()
        >>> dataset = client.get_dataset("reviews")
        >>> df = Dataflow.create_dataflow_from_dataset(client=client,
                dataset=dataset)

    To :meth:`create <xcalar.external.dataflow.Dataflow.create_dataflow_from_dataflow>` \
        a dataflow from another dataflow:
        >>> from xcalar.external.client import Client
        >>> from xcalar.external.dataflow import Dataflow
        >>> client = Client()
        >>> dataset = client.get_dataset("reviews")
        >>> df1 = Dataflow(client=client, dataset=dataset)
        >>> df = Dataflow.create_dataflow_from_dataflow(client=client,
        ...     dataflow = df1) # create dataflow from another dataflow

    To :meth:`create <xcalar.external.dataflow.Dataflow.create_dataflow_from_table>` \
        a dataflow from a table:
        >>> from xcalar.external.client import Client
        >>> from xcalar.external.dataflow import Dataflow
        >>> client = Client()
        >>> session = client.get_workbook("test_workbook").activate()
        >>> table = session.get_table("reviews_table")
        >>> df = Dataflow.create_dataflow_from_table(client=client,
        ...     table=table) # create dataflow from a table

    **Attributes**

    The following attributes are properties associated with the Dataflow object.
    They cannot be directly set

    * **client** (:class:`xcalar.external.client.Client`) - Cluster client
    * **query_string** (:class:`str`) - The transformations performed
    * **final_table_name** (:class:`str`) - The final table name created by \
        this dataflow

    **Methods**

    These are the list of available methods:

    * :meth:`create_dataflow_from_dataset`
    * :meth:`create_dataflow_from_dataflow`
    * :meth:`create_dataflow_from_table`
    * :meth:`create_dataflow_from_query_string`
    * :meth:`filter`
    * :meth:`map`
    * :meth:`aggregate`
    * :meth:`groupby`
    * :meth:`gen_row_num`
    * :meth:`custom_sort`
    * :meth:`sort`
    * :meth:`join`
    * :meth:`union`
    * :meth:`intersect`
    * :meth:`except_`
    * :meth:`synthesize`
    * :meth:`project`

    .. testsetup::

        from xcalar.external.client import Client
        client = Client()
        w = client.create_workbook("test_workbook")
        w.build_dataset("reviews", "Default Shared Root",
            "/netstore/datasets/XcalarTraining/reviews.csv", "csv")\
            .option("fieldDelim", ",").load()
        w.build_dataset("airlines", "Default Shared Root",
            "/netstore/datasets/XcalarTraining/airlines.csv", "csv")\
            .option("fieldDelim", "\t").load()
        d = w.build_dataset("reviews", "Default Shared Root",
            "/netstore/datasets/XcalarTraining/carriers.json", "json")\
            .load()
        df = Dataflow.create_dataflow_from_dataset(client, d, \
            result_table_name="reviews_table")
        s = w.activate()
        s.execute_dataflow(df)

    .. testcleanup::

        s.get_table("reviews_table").drop()
        client.get_workbook("test_workbook").delete()
        client.get_dataset("reviews").delete()
        client.get_dataset("airlines").delete()
        client.get_dataset("carriers").delete()
    """

    def __init__(self,
                 client=None,
                 dataset=None,
                 dataflow=None,
                 dataflow_name=None,
                 table=None,
                 project_columns=[],
                 query_string=None,
                 optimized_query_string=None,
                 result_table_name=None):
        self._client = client
        if dataflow_name is not None:
            if not isinstance(dataflow_name, str):
                raise TypeError("dataflow_name must be a str, not '{}'".format(
                    type(dataflow_name)))

        self._dataflow_name = dataflow_name
        self._final_table_name = None
        self._table_columns = OrderedDict()
        self._optimized_query_string = optimized_query_string
        if dataset:
            if not isinstance(dataset, Dataset):
                raise TypeError(
                    "dataset must be Dataset object, not '{}'".format(
                        type(dataset)))
            if not isinstance(client, xcalar.external.client.Client):
                raise TypeError(
                    "client must be Client object, not '{}'".format(
                        type(xcalar.external.client.Client)))
            self._query_list = []
            self._synthesize_from_dataset(dataset, project_columns,
                                          result_table_name)
        elif dataflow:
            if (not isinstance(dataflow, Dataflow)):
                raise TypeError(
                    "dataflow must be Dataflow object, not '{}'".format(
                        type(dataflow)))
            if self._dataflow_name is None:
                self._dataflow_name = dataflow.dataflow_name
            self._client = dataflow.client
            self._final_table_name = dataflow.final_table_name
            self._query_list = dataflow._query_list[:]
            self._table_columns = dataflow._table_columns.copy()
            self._optimized_query_string = dataflow._optimized_query_string
        elif table:
            if not isinstance(table, xcalar.external.table.Table):
                raise TypeError("table must be Table object, not '{}'".format(
                    type(table)))
            self._query_list = []
            self._synthesize_from_table(table, project_columns,
                                        result_table_name)
        elif query_string:
            if not isinstance(query_string, str):
                raise TypeError("query_string must be str, not '{}'".format(
                    type(query_string)))
            json_query = json.loads(query_string)
            dest = None
            # Find the last table
            for op in json_query[::-1]:
                # Not all operations have 'dest'
                if "args" in op and "dest" in op["args"]:
                    dest = op['args']['dest']
                    break
            self._query_list = json_query
            self._final_table_name = dest
        else:
            raise ValueError(
                "Either dataset_name or dataflow or query_string must be provided"
            )

        self._publish_result_map = {}

    @property
    def client(self):
        return self._client

    @property
    def query_string(self):
        # query string is returned without bulk load operation
        # this is a non-optimised query string, datasets should be loaded
        # prior running the query string
        query_list = filter(lambda op: op['operation'] != 'XcalarApiBulkLoad',
                            self._query_list)
        return json.dumps(list(query_list))

    @property
    def table_columns(self):
        return list(self._table_columns.keys())

    @property
    def final_table_name(self):
        return self._final_table_name

    @property
    def dataflow_name(self):
        return self._dataflow_name

    @property
    def optimized_query_string(self):
        if self._optimized_query_string is not None:
            return self._optimized_query_string
        # build a optimized query string
        export_operations = filter(
            lambda op: op['operation'] == 'XcalarApiExport', self._query_list)
        if not (export_operations or self.table_columns):
            # cannot construct optimized dataflow without export operations
            # or destination table columns
            return
        temp_tables_info = []    # list of tuples with table and columns
        for export_op in export_operations:
            temp_tables_info.append((export_op['args']['source'],
                                     export_op['args']['columns']))
        if not temp_tables_info:
            columns = []
            for col in self.table_columns:
                columns.append({
                    "columnName": col,
                    "headerName": self._table_columns.get(col)
                })
            temp_tables_info.append((self.final_table_name, columns))

        # make the tables info as backend retina format
        tables_info = []
        for tab_name, tab_cols in temp_tables_info:
            mod_tab_cols = []
            for col in tab_cols:
                tab_col = {
                    'columnName': col['columnName'],
                    'headerAlias': col['headerName']
                }
                mod_tab_cols.append(tab_col)
            tables_info.append({"name": tab_name, "columns": mod_tab_cols})

        dataflow_info = {
            "query": json.dumps(self._query_list),
            "tables": tables_info
        }
        self._optimized_query_string = json.dumps({
            "retina": json.dumps(dataflow_info)
        })
        return self._optimized_query_string

    @classmethod
    def create_dataflow_from_dataset(cls,
                                     client,
                                     dataset,
                                     project_columns=None,
                                     result_table_name=None):
        """
        This method creates a new dataflow from a dataset

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> dataset = client.get_dataset("reveiws")
            >>> df = Dataflow.create_dataflow_from_dataset(client=client,
            ...     dataset=dataset)

        :param client: The cluster client to bind your dataflow
        :type client: :class:`Client <xcalar.external.client.Client>`
        :param dataset: The dataset from where you want to create \
            dataflow
        :type dataset: :class:`Dataset <xcalar.external.dataset.Dataset>`
        :param project_columns: *Optional.* The columns you want to project
        :type project_columns: list
        :param result_table_name: *Optional.* The final table name you want to create from \
            this dataset
        :type result_table_name: str

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        if project_columns is None:
            project_columns = {}
        return cls(
            client=client,
            dataset=dataset,
            project_columns=project_columns,
            result_table_name=result_table_name)

    @classmethod
    def create_dataflow_from_table(cls,
                                   client,
                                   table,
                                   project_columns=None,
                                   result_table_name=None):
        """
        This method creates a new dataflow from a table

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> session = client.get_workbook("test_workbook").activate()
            >>> table = session.get_table("reveiws_table")
            >>> df = Dataflow.create_dataflow_from_table(client=client,\
            ...     table=table)

        :param client: The cluster client to bind your dataflow
        :type client: :class:`Client <xcalar.external.client.Client>`
        :param table: The table from where you want to create \
            dataflow
        :type table: :class:`Table <xcalar.external.table.Table>`

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        if project_columns is None:
            project_columns = {}
        return cls(
            client,
            table=table,
            project_columns=project_columns,
            result_table_name=result_table_name)

    @classmethod
    def create_dataflow_from_dataflow(cls, dataflow, result_table_name=None):
        """
        This method creates a new dataflow from another dataflow

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> dataset = client.get_dataset("reveiws")
            >>> df1 = Dataflow.create_dataflow_from_dataset(client=client,\
            ...     dataset=dataset)
            >>> df = Dataflow.create_dataflow_from_dataflow(client=client,\
            ...     dataflow=df1)

        :param client: The cluster client to bind your dataflow
        :type client: :class:`Client <xcalar.external.client.Client>`
        :param dataflow: The dataflow from where you want to create another\
            dataflow
        :type dataflow: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        :param result_table_name: *Optional.* The final table name you want to create from \
            this dataset
        :type result_table_name: str

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        return cls(dataflow=dataflow, result_table_name=result_table_name)

    @classmethod
    def create_dataflow_from_query_string(cls,
                                          client,
                                          query_string,
                                          columns_to_export=None,
                                          optimized_query_string=None,
                                          dataflow_name=None):
        """
        This method creates a new dataflow from a query string

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> df = Dataflow.create_dataflow_from_query_string(\
            ...     client=client, query_string=query)

        :param client: The cluster client to bind your dataflow
        :type client: :class:`Client <xcalar.external.client.Client>`
        :param query_string: The query string from where you want to create \
            dataflow
        :type query_string: str
        :param columns_to_export: *Optional.* The list of columns information which will be used
                to construct an optmized dataflow. This should be a list of dictionaries
                with each dictionary object having keys columnName and optionally headerAlias
        :type columns_to_export: list

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        if columns_to_export is None:
            columns_to_export = []
        if not isinstance(columns_to_export, list):
            raise TypeError(
                "columns_to_export must be list object, not '{}'".format(
                    type(columns_to_export)))

        df = cls(
            client,
            dataflow_name=dataflow_name,
            query_string=query_string,
            optimized_query_string=optimized_query_string)

        for col in columns_to_export:
            if not isinstance(col, dict):
                raise ValueError(
                    "columns_to_export must be list of dictionary objects, not '{}'"
                    .format(type(col)))
            if 'columnName' not in col:
                raise ValueError(
                    "columns_to_export must be list of dictionary objects, with 'columnName' as a key"
                )
            # table columns
            # key is column name and value is it's alias
            df._table_columns[col['columnName']] = col.get(
                'headerAlias', col['columnName'])
        return df

    @final_table_name.setter
    def final_table_name(self, table_name):
        for op in self._query_list[::-1]:
            # Not all operations have a source/dest
            if "args" in op and "dest" in op["args"] and op["args"][
                    "dest"] == self.final_table_name:
                op["args"]["dest"] = table_name
            if "args" in op and "source" in op["args"] and op["args"][
                    "source"] == self.final_table_name:
                op["args"]["source"] = table_name
        self._final_table_name = table_name

    def _synthesize_from_dataset(self, dataset, project_columns,
                                 result_table_name):
        req = SynthesizeRequest()
        ds_col_name_type_map = OrderedDict()
        for col in dataset.get_info()['columns']:
            ds_col_name_type_map[col['name']] = col['type']
        list_columns_renames = []
        # If doesn't specify the project columns, keep them all
        if not project_columns:
            for col in ds_col_name_type_map:
                rename_map = ColRenameInfo()
                rename_map.orig = col
                rename_map.new = col
                rename_map.type = ds_col_name_type_map[col]
                list_columns_renames.append(rename_map)
        # Otherwise we should only keep the project columns
        else:
            list_columns_renames = self._build_synthesize_column_renames(
                project_columns)
        for col_map in list_columns_renames:
            if col_map.orig.split("[")[0].split(
                    ".")[0] not in ds_col_name_type_map:
                raise KeyError(
                    "column {} is not in your dataset, the dataset columns contains only {}"
                    .format(col_map.orig, ds_col_name_type_map))

        req.colInfos.extend(list_columns_renames)
        req.sameSession = True
        req.srcTableName = dataset.name
        resp = self.client._dataflow_service.synthesize(req)

        # Get rid of '.XcalarDS.' first. Backend will treat it as dataset
        load_args = dataset.get_info()['loadArgs']
        load_args['parseArgs']['parserArgJson'] = json.dumps(
            load_args['parseArgs']['parserArgJson'])
        load_query = {
            'operation': 'XcalarApiBulkLoad',
            'args': {
                'dest': dataset.name,
                'loadArgs': load_args
            },
            'state': 'created'
        }
        query = json.loads(resp.queryStr.strip(','))
        dst_table_name = query['args']['dest'].replace('.XcalarDS.', '')
        query['args']['dest'] = dst_table_name

        if isinstance(query, dict):
            query = [load_query, query]
        elif isinstance(query, list):
            query.insert(0, load_query)

        resp.queryStr = json.dumps(query)
        resp.newTableName = dst_table_name
        self._populate_columns(
            [rename_info.new for rename_info in list_columns_renames])
        self._finalize_query_str(resp, result_table_name)

    def _synthesize_from_table(self, table, project_columns,
                               result_table_name):
        req = SynthesizeRequest()
        list_columns_renames = []

        # If doesn't specify the project columns, keep them all
        if not project_columns:
            for col in table.schema:
                rename_map = ColRenameInfo()
                rename_map.orig = col
                rename_map.new = col
                rename_map.type = table.schema[col]
                list_columns_renames.append(rename_map)
        # Otherwise we should only keep the project columns
        else:
            list_columns_renames = self._build_synthesize_column_renames(
                project_columns)
        for col_map in list_columns_renames:
            if col_map.orig not in table.columns:
                raise KeyError(
                    "column {} is not in your dataset, the dataset columns contains only {}"
                    .format(col_map.orig, table.columns))

        req.colInfos.extend(list_columns_renames)
        req.sameSession = False
        req.srcTableName = table.name
        resp = self.client._dataflow_service.synthesize(req)

        self._final_table_name = table.name
        self._populate_columns(
            [rename_info.new for rename_info in list_columns_renames])
        self._finalize_query_str(resp, result_table_name)

    def _finalize_query_str(self, resp, result_table_name, agg_op=False):
        query = resp.queryStr.strip(",")

        # Convert queries to a list
        if query[0] != '[' and query[-1] != ']':
            query = "[{}]".format(query)
        query = json.loads(query)

        # Drop all the intermediate table
        if not result_table_name:
            for idx in range(len(query)):
                query[idx]['state'] = 'Dropped'
        else:
            for idx in range(len(query) - 1):
                query[idx]['state'] = 'Dropped'
            query[-1]['args']['dest'] = result_table_name

        self._query_list.extend(query)
        if not agg_op:
            self._final_table_name = query[-1]['args']['dest']

    # populates the datflow from a collection
    # alias will be same as column name
    def _populate_columns(self, columns):
        assert self._table_columns is not None
        for col in columns:
            self._table_columns[col] = col

    def _merge_dataflows(self, dataflows):
        if (not isinstance(dataflows, list)):
            raise TypeError("dataflows must be list object, not '{}'".format(
                type(dataflows)))
        all_tables = {op["args"]["dest"] for op in self._query_list}
        for dataflow in dataflows:
            for op in dataflow._query_list:
                dest_table = op["args"]["dest"]
                if dest_table in all_tables:
                    continue
                self._query_list.append(op)
                all_tables.add(op["args"]["dest"])

    def filter(self, filter_str, result_table_name=None):
        """
        This method performs a filter operation

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> dataset = client.get_dataset("reviews")
            >>> df = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset)
            >>> filter_df = df.filter('ge(stars, "4")')

        :param filter_str: *Required.* The filter query string you want to apply
        :type filter_str: str

        :param result_table_name: *Optional.* The table name for the result. Will \
            use an auto-generated name if not specify
        :type result_table_name: str

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        req = FilterRequest()
        req.filterStr = filter_str
        req.srcTableName = self.final_table_name
        if result_table_name:
            req.dstTableName = result_table_name
        resp = self.client._dataflow_service.filter(req)
        new_df = self.create_dataflow_from_dataflow(self)
        new_df._finalize_query_str(resp, result_table_name)
        return new_df

    def map(self, evals_list, result_table_name=None, icv_mode=False):
        """
        This method performs a map operation

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> dataset = client.get_dataset("reviews")
            >>> df = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset)
            >>> map_df = df.map([("ceil(stars)", "stars_ceil")])

        :param evals_list: *Required.* Arguments to perform the map operation. It's \
            a list of 2-element tuple. The 1st element is the map query string while \
            the 2nd element is a string for new column name. \
            [("query1", "new_col1"), ("query2", "new_col2")...]
        :type evals_list: list
        :param result_table_name: *Optional.* The table name for the result. Will \
            use an auto-generated name if not specify
        :type result_table_name: str
        :param icv_mode: *Optional.* Set it to **True** to include only the erroneous \
            rows. Default to False
        :type icv_mode: bool

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        if (not isinstance(evals_list, list)):
            raise TypeError("evals_list must be list object, not '{}'".format(
                type(evals_list)))
        try:
            map_strs, new_col_names = list(zip(*evals_list))
            assert len(map_strs) == len(new_col_names)
        except ValueError:
            raise ValueError("evals_list should be a list of tuple objects")
        except AssertionError as e:
            raise ValueError(
                "evals_list should be list of two element tuple") from e
        req = MapRequest()
        req.srcTableName = self.final_table_name
        req.icvMode = icv_mode
        if result_table_name:
            req.dstTableName = result_table_name
        req.mapStrs.extend(map_strs)
        req.newColNames.extend(new_col_names)
        resp = self.client._dataflow_service.map(req)
        new_df = self.create_dataflow_from_dataflow(self)
        new_df._populate_columns(new_col_names)
        new_df._finalize_query_str(resp, result_table_name)
        return new_df

    def aggregate(self, aggr_func, col_name, result_aggr_name=None):
        """
        This method performs a aggregate operation

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> dataset = client.get_dataset("reviews")
            >>> df = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset)
            >>> aggregate_df = df.aggregate('avg', 'stars', 'avg_stars')

        :param aggr_func: *Required.* The aggregate function you want to apply
        :type aggr_func: str
        :param col_name: *Required.* The column name where you want to apply your \
            aggregate function on
        :type col_name: str
        :param result_aggr_name: *Optional.* The name for the aggregate result. Will \
            use an auto-generated name if not specify
        :type result_aggr_name: str

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        req = AggregateRequest()
        req.aggOp = aggr_func
        req.colName = col_name
        req.srcTableName = self.final_table_name
        if result_aggr_name:
            req.dstAggName = result_aggr_name
        resp = self.client._dataflow_service.aggregate(req)
        new_df = self.create_dataflow_from_dataflow(self)
        new_df._finalize_query_str(resp, None, True)
        return new_df

    def gen_row_num(self, new_col_name, result_table_name=None):
        """
        This method generate row number for each row

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> dataset = client.get_dataset("reviews")
            >>> df = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset)
            >>> row_num_df = df.gen_row_num("row_num")

        :param new_col_name: *Required.* The new column name for the row number result
        :type new_col_name: str
        :param result_table_name: *Optional.* The table name for the result. Will \
            use an auto-generated name if not specify
        :type result_table_name: str

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        req = GenRowNumRequest()
        req.newColName = new_col_name
        req.srcTableName = self.final_table_name
        if result_table_name:
            req.dstTableName = result_table_name
        resp = self.client._dataflow_service.genRowNum(req)
        new_df = self.create_dataflow_from_dataflow(dataflow=self)
        if new_col_name in new_df._table_columns:
            raise ValueError(
                "new column name shouldn't be existing column names '{}'".
                format(new_col_name))
        new_df._table_columns[new_col_name] = new_col_name
        new_df._finalize_query_str(resp, result_table_name)
        return new_df

    def custom_sort(self, keys, result_table_name=None, dht_name=None):
        """
        This method let you specify the keys to use and performs a cutomized \
            sort operation

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> from xcalar.compute.coretypes.OrderingEnums.constants import XcalarOrderingT
            >>> client = Client()
            >>> dataset = client.get_dataset("reviews")
            >>> df = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset)
            >>> keys = [{"name": "stars", "ordering": XcalarOrderingT.XcalarOrderingAscending}]
            >>> custom_sort_df = df.custom_sort(keys)

        :param keys: *Required.* The keys you want to specify for the sorting. \
            It's a list of dictionary where you specify the column name and the \
            ordering you want to use
        :type keys: list
        :param result_table_name: *Optional.* The table name for the result. Will \
            use an auto-generated name if not specify
        :type result_table_name: str
        :param dht_name: *Optional.*. Distribute hash table name. It determines how \
            you want to distribute your data
        :type dnt_name: str

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """

        if (not isinstance(keys, list)):
            raise TypeError("keys must be list object, not '{}'".format(
                type(keys)))
        req = SortRequest()
        key_info_reqs = []
        for key_info in keys:
            key_info_req = SortRequest.keyInfo()
            for key in key_info:
                # converting dictionary object to protobuf message
                setattr(key_info_req, key, key_info[key])
            key_info_reqs.append(key_info_req)
        req.keyInfos.extend(key_info_reqs)
        req.srcTableName = self.final_table_name
        if result_table_name:
            req.dstTableName = result_table_name
        if dht_name:
            req.dhtName = dht_name
        resp = self.client._dataflow_service.sort(req)
        new_df = self.create_dataflow_from_dataflow(self)
        new_df._finalize_query_str(resp, result_table_name)
        return new_df

    def sort(self, col_names, result_table_name=None, reverse=False):
        """
        This method performs a sort operation in ascending order

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> dataset = client.get_dataset("reviews")
            >>> df = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset)
            >>> sort_df = df.sort(["stars", "price_range"])

        :param col_names: *Required.* A list of column names where you want to perform \
            the sort. When the value of the first column is equal, it will compare the \
            second and so on.
        :type col_names: list
        :param result_table_name: *Optional.* The table name for the result. Will \
            use an auto-generated name if not specify
        :type result_table_name: str
        :param reverse: *Optional.* Set it to **True** to sort in descending order. \
            Default to False

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        if (not isinstance(col_names, list)):
            raise TypeError("col_names must be list object, not '{}'".format(
                type(col_names)))
        keys_info = []
        ordering = (XcalarOrderingT.XcalarOrderingDescending
                    if reverse else XcalarOrderingT.XcalarOrderingAscending)
        for col_name in col_names:
            keys_info.append({"name": col_name, "ordering": ordering})
        return self.custom_sort(keys_info, result_table_name)

    def groupby(self,
                aggrs_info,
                group_on,
                result_table_name=None,
                group_all=False,
                include_sample_columns=False):
        """
        This method performs a groupby operation

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> dataset = client.get_dataset("reviews")
            >>> df = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset)
            >>> groupby_df = df.groupby([{
            ...     "operator": "count",
            ...     "aggColName": "reviewID",
            ...     "newColName": "count_review",
            ...     "isDistinct": True
            ... }], ["price_range"])

        :param aggrs_info: *Required.* The aggregation function to apply over the groupby. \
            It's a list of dict which specify the aggregation function, the column \
            to apply aggregate, the new column name for the result, and if it's distinct
        :type aggrs_info: list
        :param group_on: *Required.* The column names where you want to group on. It's a \
            list of string which specify all the columns you want to group on
        :type group_on: list
        :param result_table_name: *Optional.* The table name for the result. Will \
            use an auto-generated name if not specify
        :type result_table_name: str

        :param group_all : *Optional.* Set it to **True** to group on all columns. \
            Default to False
        :type group_all: bool

        :param include_sample_columns : *Optional.* Set it to **True** to include random \
            sample of each group. Default to False
        :type group_all: bool

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        if (not isinstance(aggrs_info, list)):
            raise TypeError(
                "aggrs_info must be list of dictionary objects, not '{}'".
                format(type(aggrs_info)))
        if (not isinstance(group_on, list)):
            raise TypeError("group_on must be list, not '{}'".format(
                type(group_on)))
        req = GroupByRequest()
        aggr_args = []
        new_col_names = []
        for aggr in aggrs_info:
            aggColInfo = AggColInfo()
            for key in aggr:
                setattr(aggColInfo, key, aggr[key])
            aggr_args.append(aggColInfo)
            new_col_names.append(aggr['newColName'])
        req.groupByCols.extend(group_on)
        req.aggArgs.extend(aggr_args)
        req.srcTableName = self.final_table_name
        if result_table_name:
            req.options.newTableName = result_table_name
        req.options.groupAll = group_all
        req.options.isIncSample = include_sample_columns
        resp = self.client._dataflow_service.groupBy(req)
        new_df = self.create_dataflow_from_dataflow(self)
        col_name_collision = set(new_col_names) & set(group_on)
        if col_name_collision:
            raise ValueError(
                "new column names shouldn't be the same as group_on column names '{}'"
                .format(col_name_collision))
        else:
            if group_all or include_sample_columns:
                new_df._populate_columns(new_col_names)
            else:
                # reset columns
                new_df._table_columns = OrderedDict()
                new_df._populate_columns(group_on + new_col_names)
        new_df._finalize_query_str(resp, result_table_name)
        return new_df

    def _join_operation_helper(self, right_table_name, left_table_columns,
                               right_table_columns, left_cols_joinon,
                               right_cols_joinon, lrename_map, rrename_map):
        # Left tab info
        l_table_info = JoinTableInfo()
        l_table_info.tableName = self.final_table_name
        l_table_info.columns.extend(left_cols_joinon)
        left_rename_info = []
        try:
            for lrename in lrename_map:
                rename_map = ColRenameInfo()
                rename_map.orig = lrename.get('name')
                rename_map.new = lrename.get('rename')
                left_rename_info.append(rename_map)

                if rename_map.orig not in left_table_columns:
                    raise ValueError(
                        "Invalid rename, '{}' column doesn't exist in the table"
                        .format(rename_map.orig))
                left_table_columns.pop(rename_map.orig)
                if rename_map.new in left_table_columns:
                    raise ValueError(
                        "Invalid rename, new name '{}' already exists".format(
                            rename_map.new))
                left_table_columns[rename_map.new] = rename_map.new
        except KeyError as e:
            raise ValueError("Invalid key in column_renames item") from e
        l_table_info.rename.extend(left_rename_info)

        # Right tab info
        r_table_info = JoinTableInfo()
        r_table_info.tableName = right_table_name
        r_table_info.columns.extend(right_cols_joinon)
        right_rename_info = []
        try:
            for rrename in rrename_map:
                rename_map = ColRenameInfo()
                rename_map.orig = rrename.get('name')
                rename_map.new = rrename.get('rename')
                right_rename_info.append(rename_map)

                if rename_map.orig not in right_table_columns:
                    raise ValueError(
                        "Invalid rename, '{}' column doesn't exist in the table"
                        .format(rename_map.orig))
                right_table_columns.pop(rename_map.orig)
                if rename_map.new in right_table_columns or rename_map.new in left_table_columns:
                    raise ValueError(
                        "Invalid rename, new name '{}' already exists".format(
                            rename_map.new))
                right_table_columns[rename_map.new] = rename_map.new
        except KeyError as e:
            raise ValueError("Invalid key in column_renames item") from e
        r_table_info.rename.extend(right_rename_info)

        # Deal with name collision, only rename the right table columns
        set_left_table_columns = set(left_table_columns.keys())
        name_collision = [
            col for col in right_table_columns if col in set_left_table_columns
        ]
        right_collision_resolve = []
        for old_name in name_collision:
            rename_map = ColRenameInfo()
            rename_map.orig = old_name
            rename_map.new = "{}_r_{}".format(old_name, right_table_name)
            right_collision_resolve.append(rename_map)
            right_table_columns.pop(rename_map.orig)
            right_table_columns[rename_map.new] = rename_map.new
        r_table_info.rename.extend(right_collision_resolve)

        return (l_table_info, r_table_info)

    # XXX: Also need to take columns to keep from left and right tables
    def join(self,
             join_with,
             join_on,
             join_type=JoinOperatorT.InnerJoin,
             lrename_map=[],
             rrename_map=[],
             result_table_name=None,
             filter_str="",
             keep_all_cols=True):
        """
        This method performs a join operation

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> dataset1 = client.get_dataset("airlines")
            >>> df1 = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset1)
            >>> dataset2 = client.get_dataset("carriers")
            >>> df2 = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset2)
            >>> join_df = df1.join(df2, [("Carrier", "Carrier")])

        :param join_with: *Required.* The other dataflow you want to join with
        :type join_with: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        :param join_on: *Required.* The column names you want to join on. It's \
            a list of 2-element tuples. The 1st element is the column name from \
            the left join dataflow. The 2nd element is the column name from the \
            right join dataflow
        :type join_on: list
        :param join_type: *Optional*: The join type you want to use. Default to \
            inner join
        :type join_type: str
        :param lrename_map: *Optional*: A list of dictionaries with which you can specify the \
            rename map for the left table. Each dictionary contains 2 keys. **name** \
            indicates the original name and **rename** indicates the new name you want \
            to rename
        :type lrename_map: list
        :param rrename_map: *Optional*: A list of dictionaries with which you can specify the \
            rename map for the right table. Each dictionary contains 2 keys. **name** \
            indicates the original name and **rename** indicates the new name you want \
            to rename
        :type rrename_map: list
        :type result_table_name: str
        :param filter_str: *Optional.* The filter query string you want to apply \
            after the join. Only works with cross join, left semi join, and left \
            anti join
        :type filter_str: str
        :param keep_all_cols: *Optional.* Set it to **True** to keep all columns in the \
            join result. Default to True
        :type keep_all_cols: bool

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        if (not isinstance(join_on, list)):
            raise TypeError("join_on must be list object, not '{}'".format(
                type(join_on)))
        if (not isinstance(join_with, Dataflow)):
            raise TypeError(
                "join_with must be Dataflow object, not '{}'".format(
                    type(join_with)))
        if not isinstance(lrename_map, list):
            raise TypeError("rename_map must be list object, not '{}'".format(
                type(lrename_map)))
        if not isinstance(rrename_map, list):
            raise TypeError("rename_map must be list object, not '{}'".format(
                type(rrename_map)))
        try:
            left_cols_joinon, right_cols_joinon = list(zip(*join_on))
            assert len(left_cols_joinon) == len(right_cols_joinon)
        except ValueError:
            raise ValueError("join_on should be a list of tuple objects")
        except AssertionError as exc:
            raise ValueError(
                "evals_list should be list of two element tuple") from exc
        # Deal with name collision
        left_table_columns = OrderedDict(self._table_columns)
        right_table_columns = OrderedDict(join_with._table_columns)
        l_table_info, r_table_info = self._join_operation_helper(
            join_with.final_table_name, left_table_columns,
            right_table_columns, left_cols_joinon, right_cols_joinon,
            lrename_map, rrename_map)

        # Other options
        options = JoinOptions()
        if result_table_name:
            options.newTableName = result_table_name
        options.evalStr = filter_str
        options.keepAllColumns = keep_all_cols

        req = JoinRequest()
        req.joinType = join_type
        req.lTableInfo.CopyFrom(l_table_info)
        req.rTableInfo.CopyFrom(r_table_info)
        req.options.CopyFrom(options)
        resp = self.client._dataflow_service.join(req)
        new_df = self.create_dataflow_from_dataflow(self)
        new_df._merge_dataflows([join_with])

        # reset columns and fill with the results
        new_df._table_columns = OrderedDict()
        if join_type in {
                JoinOperatorT.LeftSemiJoin, JoinOperatorT.LeftAntiJoin
        }:
            new_df._populate_columns(left_table_columns)
        else:
            new_df._populate_columns(
                list(left_table_columns.keys()) +
                list(right_table_columns.keys()))
        new_df._finalize_query_str(resp, result_table_name)
        return new_df

    @staticmethod
    def _set_operation_helper(source_dfs, source_dfs_cols, dest_df_cols,
                              result_table_name, dedup, set_op_type):
        if (not isinstance(source_dfs, list)):
            raise TypeError("source_dfs must be list object, not '{}'".format(
                type(source_dfs)))
        if (not isinstance(source_dfs_cols, list)):
            raise TypeError("source_cols must be list object, not '{}'".format(
                type(source_dfs_cols)))
        if (not isinstance(dest_df_cols, list)):
            raise TypeError(
                "dest_df_cols must be list object, not '{}'".format(
                    type(dest_df_cols)))

        if (len(source_dfs) == 0):
            raise ValueError("length of source_dfs must be greater than 0")
        if (len(source_dfs_cols) != len(source_dfs)):
            raise ValueError(
                "source_dfs and source_cols must be of equal length")

        req = UnionRequest()
        table_infos = []
        for tab, cols in zip(source_dfs, source_dfs_cols):
            if (not isinstance(tab, Dataflow)):
                raise TypeError(
                    "source_dfs must contain Dataflow objects, not '{}'".
                    format(type(tab)))
            if (not isinstance(cols, list)):
                raise TypeError(
                    "source_cols must contain list objects, not '{}'".format(
                        type(cols)))
            if (len(dest_df_cols) != len(cols)):
                raise ValueError(
                    "each source_df_cols must be equal length of dest_df_cols")
            table_info = UnionTableInfo()
            cols_info = []
            for src_col_info, dest_col in zip(cols, dest_df_cols):
                unionColInfo = UnionColInfo()
                unionColInfo.name = src_col_info["name"]
                unionColInfo.type = src_col_info["type"]
                unionColInfo.rename = dest_col
                cols_info.append(unionColInfo)
            table_info.tableName = tab.final_table_name
            table_info.columns.extend(cols_info)
            table_infos.append(table_info)
        req.tableInfos.extend(table_infos)
        req.dedup = dedup
        req.unionType = set_op_type
        if result_table_name:
            req.newTableName = result_table_name
        first_df = source_dfs[0]
        resp = first_df.client._dataflow_service.unionOp(req)
        new_df = Dataflow.create_dataflow_from_dataflow(first_df)
        new_df._merge_dataflows(source_dfs[1:])
        new_df._finalize_query_str(resp, result_table_name)
        # parse columns from set operation json query
        new_df._table_columns = OrderedDict()
        set_op_query = new_df._query_list[-1]
        new_df._populate_columns([
            col['destColumn'] for inner in set_op_query['args']['columns']
            for col in inner if 'destColumn' in col
        ])
        return new_df

    @classmethod
    def union(cls,
              source_dfs,
              source_dfs_cols,
              dest_df_cols,
              result_table_name=None,
              dedup=False):
        """
        This method performs a union operation

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> dataset1 = client.get_dataset("airlines")
            >>> df1 = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset1)
            >>> dataset2 = client.get_dataset("carriers")
            >>> df2 = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset2)
            >>> columns = [
                    [{"name": "Carrier", "type": "string"}],
                    [{"name": "Carrier", "type": "string"}]
                ]
            >>> union_df = Dataflow.union([df1, df2], columns, ["carrier"])

        :param source_dfs: *Required.* The dataflows you want to perfrom union \
            over them. It's a list of Dataflow instance
        :type source_dfs: list
        :param source_dfs_cols: *Reqruied.* The columns within the dataflow you \
            want to union
        :type source_dfs_cols: list
        :param dest_df_cols: *Required.* The new column names for the union result
        :type dest_df_cols: list
        :param result_table_name: *Optional.* The table name for the result. Will \
            use an auto-generated name if not specify
        :type result_table_name: str
        :param dedup: *Optional.* Set it to **True** to remove duplicate result. Default \
            to False
        :type dedup: bool

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        return cls._set_operation_helper(source_dfs, source_dfs_cols,
                                         dest_df_cols, result_table_name,
                                         dedup, UnionOperatorT.UnionStandard)

    @classmethod
    def intersect(cls,
                  source_dfs,
                  source_dfs_cols,
                  dest_df_cols,
                  result_table_name=None,
                  dedup=False):
        """
        This method performs a intersect operation

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> dataset1 = client.get_dataset("airlines")
            >>> df1 = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset1)
            >>> dataset2 = client.get_dataset("carriers")
            >>> df2 = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset2)
            >>> columns = [
                    [{"name": "Carrier", "type": "string"}],
                    [{"name": "Carrier", "type": "string"}]
                ]
            >>> intersect_df = Dataflow.intersect([df1, df2], columns, ["carrier"])

        :param source_dfs: *Required.* The dataflows you want to perfrom intersect\
            over them. It's a list of Dataflow instance
        :type source_dfs: list
        :param source_dfs_cols: *Reqruied.* The columns within the dataflow you \
            want to intersect
        :type source_dfs_cols: list
        :param dest_df_cols: *Required.* The new column names for the intersect result
        :type dest_df_cols: list
        :param result_table_name: *Optional.* The table name for the result. Will \
            use an auto-generated name if not specify
        :type result_table_name: str
        :param dedup: *Optional.* Set it to **True** to remove duplicate result. Default \
            to False
        :type dedup: bool

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        return cls._set_operation_helper(source_dfs, source_dfs_cols,
                                         dest_df_cols, result_table_name,
                                         dedup, UnionOperatorT.UnionIntersect)

    @classmethod
    def except_(cls,
                source_dfs,
                source_dfs_cols,
                dest_df_cols,
                result_table_name=None,
                dedup=False):
        """
        This method performs a intersect operation. We name it with a suffix \
            underscore because **except** is a reserved keyword for Python

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> dataset1 = client.get_dataset("airlines")
            >>> df1 = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset1)
            >>> dataset2 = client.get_dataset("carriers")
            >>> df2 = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset2)
            >>> columns = [
                    [{"name": "Carrier", "type": "string"}],
                    [{"name": "Carrier", "type": "string"}]
                ]
            >>> except_df = Dataflow.except_([df1, df2], columns, ["carrier"])

        :param source_dfs: *Required.* The dataflows you want to perfrom except\
            over them. It's a list of Dataflow instance
        :type source_dfs: list
        :param source_dfs_cols: *Reqruied.* The columns within the dataflow you \
            want to perform except
        :type source_dfs_cols: list
        :param dest_df_cols: *Required.* The new column names for the except result
        :type dest_df_cols: list
        :param result_table_name: *Optional.* The table name for the result. Will \
            use an auto-generated name if not specify
        :type result_table_name: str
        :param dedup: *Optional.* Set it to **True** to remove duplicate result. Default \
            to False
        :type dedup: bool

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        return cls._set_operation_helper(source_dfs, source_dfs_cols,
                                         dest_df_cols, result_table_name,
                                         dedup, UnionOperatorT.UnionExcept)

    def synthesize(self,
                   project_columns,
                   result_table_name=None,
                   sameSession=True):
        """
        This method performs a synthesize operation which let you do \
            column project, rename, and type casting

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> dataset = client.get_dataset("reviews")
            >>> df = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset)
            >>> synthesize_df = df.synthesize([{
            ...     'name': 'stars',
            ...     'rename': 'string_stars',
            ...     'type': 'string'
            ... }])

        :param column_renames: *Required.* A list of dictionaries which you can specify \
            the synthesize info. Each dictionary contains three keys: **name** indicates \
            the original column name, **rename** indicates the new column name, **type** is \
            the new type info for that column
        :type column_renames: list
        :param sameSession: *Optional.* Indicate whether the dataflow is in the same session
        :type sameSession: bool

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        req = SynthesizeRequest()
        list_column_renames = self._build_synthesize_column_renames(
            project_columns)
        req.colInfos.extend(list_column_renames)
        req.sameSession = sameSession
        req.srcTableName = self.final_table_name
        if result_table_name:
            req.dstTableName = result_table_name
        resp = self.client._dataflow_service.synthesize(req)
        new_df = self.create_dataflow_from_dataflow(self)
        new_df._table_columns = OrderedDict()
        new_df._populate_columns(
            [rename_info.new for rename_info in list_column_renames])
        new_df._finalize_query_str(resp, result_table_name)
        return new_df

    def project(self, columns, result_table_name=None):
        """
        This method performs a project operation

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.external.dataflow import Dataflow
            >>> client = Client()
            >>> dataset = client.get_dataset("reviews")
            >>> df = Dataflow.create_dataflow_from_dataset(
            ...     client=client, dataset=dataset)
            >>> project_df = df.project(['stars', 'business_name', 'price_range'])

        :param columns: *Required.* The columns you want to project on
        :type columns: list
        :param result_table_name: *Optional.* The table name for the result. Will \
            use an auto-generated name if not specify
        :type result_table_name: str

        :Returns: A :class:`Dataflow <xcalar.external.dataflow.Dataflow>` instance
        :return_type: :class:`Dataflow <xcalar.external.dataflow.Dataflow>`
        """
        if (not isinstance(columns, list)):
            raise TypeError("columns must be a list object, not '{}'".format(
                type(columns)))
        for col in columns:
            if col not in self._table_columns:
                raise ValueError(
                    "The column {} is not in the table, the table only contains {}"
                    .format(col, self._table_columns))
        req = ProjectRequest()
        req.columns.extend(columns)
        req.srcTableName = self.final_table_name
        if result_table_name:
            req.dstTableName = result_table_name
        resp = self.client._dataflow_service.project(req)
        new_df = self.create_dataflow_from_dataflow(self)
        new_df._table_columns = OrderedDict()
        new_df._populate_columns(columns)
        new_df._finalize_query_str(resp, result_table_name)
        return new_df

    def _build_synthesize_column_renames(self, project_columns):
        if (not isinstance(project_columns, list)):
            raise TypeError(
                "project_columns must be a list object, not '{}'".format(
                    type(project_columns)))

        list_column_renames = []
        new_table_columns = set()
        for column_rename_map in project_columns:
            if (not isinstance(column_rename_map, dict)):
                raise TypeError(
                    "project_columns must contain dict objects, not '{}'".
                    format(type(column_rename_map)))
            try:
                rename_map = ColRenameInfo()
                rename_map.orig = column_rename_map.get("name")
                if "rename" in column_rename_map:
                    rename_map.new = column_rename_map.get("rename")
                else:
                    rename_map.new = column_rename_map.get("name")
                rename_map.type = column_rename_map.get("type").lower()
                list_column_renames.append(rename_map)

                if rename_map.new in new_table_columns:
                    raise ValueError(
                        "project_columns leads to duplicate columns, invalid rename '{}'"
                        .format(column_rename_map))
                new_table_columns.add(rename_map.new)
            except KeyError as e:
                raise ValueError("Invalid key in project_columns item: '{}'".
                                 format(column_rename_map)) from e
        return list_column_renames

    # XXX see ENG-8807 for more details.
    # set_publish_result_map and get_publish_result_map
    # these apis are for a hack to workaround the publish node in XD dataflow,
    # to publish the dataflow execution result, which is not supported
    # as a operator in query language.
    def set_publish_result_map(self, publish_result_map):
        self._publish_result_map = publish_result_map

    def get_publish_result_map(self):
        return self._publish_result_map

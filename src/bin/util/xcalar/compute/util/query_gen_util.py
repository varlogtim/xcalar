#
# Issue FASJ operators using QueryGenerate.
#

import logging
import random

from xcalar.external.dataflow import Dataflow
from xcalar.external.query_generate import QueryGenerate
from xcalar.external.runtime import Runtime

# Exceptions
from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.external.exceptions import XDPException

# Enums
from xcalar.compute.coretypes.DataTargetTypes.ttypes import ExColumnNameT
from xcalar.compute.coretypes.LibApisCommon.ttypes import (
    XcalarApiExportColumnT, XcalarApiRetinaDstT)
from xcalar.compute.coretypes.OrderingEnums.constants import XcalarOrderingT, XcalarOrderingTFromStr, XcalarOrderingTStr
from xcalar.compute.coretypes.DagTypes.ttypes import XcalarApiNamedInputT
from xcalar.compute.coretypes.QueryStateEnums.ttypes import QueryStateT
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.DataFormatEnums.constants import DfFieldTypeTStr, DfFieldTypeTFromStr
from xcalar.compute.coretypes.DataFormatEnums.ttypes import DfFieldTypeT
from xcalar.compute.coretypes.LibApisEnums.ttypes import XcalarApisT
from xcalar.compute.coretypes.LibApisEnums.constants import XcalarApisTStr, XcalarApisTFromStr
from xcalar.compute.coretypes.JoinOpEnums.ttypes import JoinOperatorT
from xcalar.compute.coretypes.JoinOpEnums.constants import JoinOperatorTStr, JoinOperatorTFromStr
from xcalar.compute.coretypes.UnionOpEnums.ttypes import UnionOperatorT
from xcalar.compute.coretypes.UnionOpEnums.constants import UnionOperatorTStr, UnionOperatorTFromStr

#
# Contants
#
DfFatptrPrefixDelimiter = "::"
DfFatptrPrefixDelimiterReplaced = "--"
OperatorsAggregateTag = "^"
DfNestedDelimiter = "."
DfArrayIndexStartDelimiter = "["
DfArrayIndexEndDelimiter = "]"
EscapeEscape = "\\"
DsDefaultDatasetKeyName = "xcalarRecordNum"


class QueryGenHelper:
    def __init__(self, client, session, logger=None):
        self.client = client
        self.session = session
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger()
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s %(name)s %(process)d %(levelname)s %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
            self.logger.propagate = False

    # Issue XcalarQuery and wait of it's exection if is_async=False.
    def execute_query(self,
                      query_string,
                      table_name,
                      optimized=False,
                      is_async=False,
                      columns_to_export=[],
                      clean_job=True,
                      parallel_ops=False,
                      sched_name=None):
        df = Dataflow.create_dataflow_from_query_string(
            client=self.client,
            query_string=query_string,
            columns_to_export=columns_to_export)

        df_name = self.session.execute_dataflow(
            df,
            table_name=table_name,
            optimized=optimized,
            is_async=True,
            parallel_operations=parallel_ops,
            clean_job_state=clean_job,
            sched_name=sched_name)

        self.logger.info(
            "execute_query: df_name: {}, table_name: {}, optimized: {}, is_async: {}, user_name: {}, session_name: {},  clean_job {}, parallel_ops {}, sched_name {}, query: '{}'"
            .format(df_name, table_name, optimized, is_async,
                    self.client.username, self.session.name, clean_job,
                    parallel_ops, sched_name, query_string))
        if not is_async:
            self.wait_for_async_query(df_name)
        return df_name

    # Wait for an async dispatched query
    def wait_for_async_query(self, query_name, wait_poll_interval_secs=5):
        try:
            query_output = self.client._legacy_xcalar_api.waitForQuery(
                queryName=query_name,
                pollIntervalInSecs=wait_poll_interval_secs)
            if query_output.queryState == QueryStateT.qrError or query_output.queryState == QueryStateT.qrCancelled:
                raise XcalarApiStatusException(query_output.queryStatus,
                                               query_output)
        except XcalarApiStatusException as ex:
            if ex.status not in [
                    StatusT.StatusQrQueryNotExist,
                    StatusT.StatusQrQueryAlreadyDeleted,
                    StatusT.StatusDatasetAlreadyDeleted
            ]:
                self.logger.exception(ex)
                raise ex

    # Wait for a list of async dispatched queries
    def wait_for_async_queries(self, query_names, wait_poll_interval_secs=5):
        for q in query_names:
            self.wait_for_async_query(q, wait_poll_interval_secs)

    # Issue Index operator.
    def issue_index(self,
                    source,
                    dest,
                    keys_map,
                    optimized=False,
                    prefix='',
                    is_async=False,
                    clean_job=True,
                    parallel_ops=False,
                    sched_name=None,
                    columns_to_export=[]):
        q = QueryGenerate(query=[])
        q.op_index(source=source, dest=dest, keys_map=keys_map, prefix=prefix)
        df_name = self.execute_query(
            query_string=q.get_query_string,
            table_name=dest,
            optimized=optimized,
            is_async=is_async,
            clean_job=clean_job,
            parallel_ops=parallel_ops,
            sched_name=sched_name,
            columns_to_export=columns_to_export)
        return df_name

    # Issue Project operator
    def issue_project(self,
                      source,
                      dest,
                      column_names,
                      optimized=False,
                      is_async=False,
                      clean_job=True,
                      parallel_ops=False,
                      sched_name=None,
                      columns_to_export=[]):
        q = QueryGenerate(query=[])
        q.op_project(source=source, dest=dest, column_names=column_names)
        df_name = self.execute_query(
            query_string=q.get_query_string,
            table_name=dest,
            optimized=optimized,
            is_async=is_async,
            columns_to_export=columns_to_export,
            clean_job=clean_job,
            parallel_ops=parallel_ops,
            sched_name=sched_name)
        return df_name

    # Issue Filter operator
    def issue_filter(self,
                     source,
                     dest,
                     eval_string,
                     optimized=False,
                     is_async=False,
                     clean_job=True,
                     parallel_ops=False,
                     sched_name=None,
                     columns_to_export=[]):
        q = QueryGenerate(query=[])
        q.op_filter(source=source, dest=dest, eval_string=eval_string)
        df_name = self.execute_query(
            query_string=q.get_query_string,
            table_name=dest,
            optimized=optimized,
            is_async=is_async,
            clean_job=clean_job,
            parallel_ops=parallel_ops,
            sched_name=sched_name,
            columns_to_export=columns_to_export)
        return df_name

    # Issue Aggregate operator
    def issue_aggregate(self,
                        source,
                        dest,
                        eval_string,
                        optimized=False,
                        is_async=False,
                        columns_to_export=[],
                        clean_job=True,
                        parallel_ops=False,
                        sched_name=None):
        q = QueryGenerate(query=[])
        q.op_aggregate(source=source, dest=dest, eval_string=eval_string)
        df_name = self.execute_query(
            query_string=q.get_query_string,
            table_name=dest,
            optimized=optimized,
            is_async=is_async,
            clean_job=clean_job,
            parallel_ops=parallel_ops,
            sched_name=sched_name,
            columns_to_export=columns_to_export)
        return df_name

    # Issue Map operator
    def issue_map(self,
                  source,
                  dest,
                  evals_list,
                  optimized=False,
                  is_async=False,
                  columns_to_export=[],
                  clean_job=True,
                  parallel_ops=False,
                  sched_name=None):
        q = QueryGenerate(query=[])
        q.op_map(source=source, dest=dest, evals_list=evals_list)
        df_name = self.execute_query(
            query_string=q.get_query_string,
            table_name=dest,
            optimized=optimized,
            is_async=is_async,
            clean_job=clean_job,
            parallel_ops=parallel_ops,
            sched_name=sched_name,
            columns_to_export=columns_to_export)
        return df_name

    # Issue GroupBy operator
    def issue_groupby(self,
                      source,
                      dest,
                      eval_list,
                      group_all,
                      include_sample,
                      keys_map,
                      optimized=False,
                      is_async=False,
                      columns_to_export=[],
                      clean_job=True,
                      parallel_ops=False,
                      sched_name=None):
        q = QueryGenerate(query=[])
        q.op_groupby(
            source=source,
            dest=dest,
            eval_list=eval_list,
            group_all=group_all,
            include_sample=include_sample,
            keys_map=keys_map)
        df_name = self.execute_query(
            query_string=q.get_query_string,
            table_name=dest,
            optimized=optimized,
            is_async=is_async,
            clean_job=clean_job,
            parallel_ops=parallel_ops,
            sched_name=sched_name,
            columns_to_export=columns_to_export)
        return df_name

    # Issue Join operator
    def issue_join(self,
                   source_left,
                   source_right,
                   dest,
                   left_rename_maps,
                   right_rename_maps,
                   join_type,
                   left_keys=[],
                   right_keys=[],
                   eval_string='',
                   keep_all_columns=False,
                   null_safe=False,
                   optimized=False,
                   is_async=False,
                   clean_job=True,
                   parallel_ops=False,
                   sched_name=None,
                   columns_to_export=[]):
        q = QueryGenerate(query=[])
        q.op_join(
            source_left=source_left,
            source_right=source_right,
            dest=dest,
            left_rename_maps=left_rename_maps,
            right_rename_maps=right_rename_maps,
            join_type=join_type,
            left_keys=left_keys,
            right_keys=right_keys,
            eval_string=eval_string,
            keep_all_columns=keep_all_columns,
            null_safe=null_safe)
        df_name = self.execute_query(
            query_string=q.get_query_string,
            table_name=dest,
            optimized=optimized,
            is_async=is_async,
            clean_job=clean_job,
            parallel_ops=parallel_ops,
            sched_name=sched_name,
            columns_to_export=columns_to_export)
        return df_name

    # Issue Union operator
    def issue_union(self,
                    sources,
                    dest,
                    table_rename_maps,
                    union_type,
                    keys=[],
                    dedup=False,
                    optimized=False,
                    is_async=False,
                    clean_job=True,
                    parallel_ops=False,
                    sched_name=None,
                    columns_to_export=[]):
        q = QueryGenerate(query=[])
        q.op_union(
            sources=sources,
            dest=dest,
            table_rename_maps=table_rename_maps,
            union_type=union_type,
            keys=keys,
            dedup=dedup)
        df_name = self.execute_query(
            query_string=q.get_query_string,
            table_name=dest,
            optimized=optimized,
            is_async=is_async,
            clean_job=clean_job,
            parallel_ops=parallel_ops,
            sched_name=sched_name,
            columns_to_export=columns_to_export)
        return df_name

    # Issue XcalarApiGetRowNum operator
    def issue_row_num(self,
                      source,
                      dest,
                      new_field,
                      optimized=False,
                      is_async=False,
                      clean_job=True,
                      parallel_ops=False,
                      sched_name=None,
                      columns_to_export=[]):
        q = QueryGenerate(query=[])
        q.op_row_num(source=source, dest=dest, new_field=new_field)
        df_name = self.execute_query(
            query_string=q.get_query_string,
            table_name=dest,
            optimized=optimized,
            is_async=is_async,
            clean_job=clean_job,
            parallel_ops=parallel_ops,
            sched_name=sched_name,
            columns_to_export=columns_to_export)
        return df_name

    # Issue XcalarApiSynthesize operator
    def issue_synthesize(self,
                         source,
                         dest,
                         table_rename_map,
                         same_session=True,
                         optimized=False,
                         is_async=False,
                         clean_job=True,
                         parallel_ops=False,
                         sched_name=None,
                         columns_to_export=[]):
        q = QueryGenerate(query=[])
        q.op_synthesize(
            source=source,
            dest=dest,
            table_rename_map=table_rename_map,
            same_session=same_session)
        df_name = self.execute_query(
            query_string=q.get_query_string,
            table_name=dest,
            optimized=optimized,
            is_async=is_async,
            clean_job=clean_job,
            parallel_ops=parallel_ops,
            sched_name=sched_name,
            columns_to_export=columns_to_export)
        return df_name

    def is_column_with_fatptr_prefix(self, col_name):
        return len(col_name.split(DfFatptrPrefixDelimiter)) == 2

    def is_column_with_fatptr_prefix_replaced(self, col_name):
        return len(col_name.split(DfFatptrPrefixDelimiterReplaced)) == 2

    # Get random columns from a given table schema including Faptrs
    def get_rand_columns(self, table):
        schema = self.get_schema(table)
        return dict(
            random.sample(
                population=schema.items(), k=random.randint(1, len(schema))))

    # Get schema from a given table schema including Faptrs
    def get_schema(self, table):
        row = table.get_row(row=0)
        columns = list(row.keys())
        schema = table.schema
        ret_schema = {}
        for col in columns:
            if self.is_column_with_fatptr_prefix(col):
                ret_schema[col] = DfFieldTypeTStr[DfFieldTypeT.DfFatptr]
            else:
                ret_schema[col] = schema[col]
        return ret_schema

    # Get a random column from given a table schema including Fatptrs
    def get_rand_column(self, table):
        col_name, col_type = random.choice(
            list(self.get_rand_columns(table).items()))
        return {col_name: col_type}

    # Get key map for a given table
    def get_keys_map(self, table):
        keys = table._get_meta().keys
        key_names = []
        key_field_names = []
        key_types = []
        key_orderings = []
        for k, v in keys.items():
            key_names.append(k)
            key_field_names.append(k.replace(DfFatptrPrefixDelimiter, "__"))
            key_types.append(DfFieldTypeTFromStr[v["type"]])
            key_orderings.append(XcalarOrderingTFromStr[v["ordering"]])
        return QueryGenerate().get_key_maps(key_names, key_field_names,
                                            key_types, key_orderings)

    # Get a radom key map for a given schema
    def get_rand_keys_map(
            self,
            schema,
            ordering_choices=[XcalarOrderingT.XcalarOrderingUnordered],
            keys_count=None):
        if keys_count is None:
            keys_count = random.randint(1, len(schema.keys()))
        keys_schema = random.sample(
            population=list(schema.items()), k=keys_count)
        key_names = []
        key_field_names = []
        key_types = []
        key_orderings = []
        for field in keys_schema:
            kfn = field[0].replace(DfFatptrPrefixDelimiter, "__")
            if kfn not in key_field_names:
                key_names.append(field[0])
                key_field_names.append(kfn)
                key_types.append(DfFieldTypeT.DfUnknown)
                key_orderings.append(random.choice(ordering_choices))
        return QueryGenerate().get_key_maps(key_names, key_field_names,
                                            key_types, key_orderings)

    def get_rand_sched_name(self):
        return random.choice(Runtime.get_dataflow_scheds())

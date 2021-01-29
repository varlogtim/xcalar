# Test suite to drive randomized operators

import logging
import uuid
import random
import time
import tarfile
import io
import os
import re
import glob
import json

from test_base import TestDriver, TestWorker
from udf import type_map

# New SDK
from xcalar.external.client import Client
from xcalar.external.dataflow import Dataflow
from xcalar.external.result_set import ResultSet
from xcalar.external.query_generate import QueryGenerate
from xcalar.external.runtime import Runtime

# Legacy SDK
from xcalar.external.LegacyApi.WorkItem import (
    WorkItemDeleteDagNode, WorkItemExport, WorkItemMakeRetina,
    WorkItemGetRetinaJson, WorkItemDeleteRetina)
from xcalar.external.LegacyApi.XcalarApi import XcalarApi

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

# Exceptions
from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.external.exceptions import XDPException

# QA & Utils.
import xcalar.compute.util.query_gen_util as QgenUtil


class OperatorsTestError(Exception):
    pass


class OperatorsTestVerificationError(OperatorsTestError):
    pass


class OperatorsTestRowCountMismatch(OperatorsTestVerificationError):
    pass


class OperatorsTestAggrMismatch(OperatorsTestVerificationError):
    pass


#
# Contants
#
# XXX TODO need to understand the limits here for GroupBy. But for now
# pick a smallish number.
GroupByEvalsCountMax = 16
DataflowFileName = "dataflowInfo.json"
JoinRightTableRecordCount = 100
UnionMaxSourceTables = 16

#
# Targets
#
GeneratedTargetName = "memory"


class TestOperators(TestDriver):
    def __init__(self, test_mgr):
        self.setup_class(test_mgr)

    @classmethod
    def setup_class(cls, test_mgr):
        super().setup_class(test_mgr)
        cls.test_plan = test_mgr.test_plan
        cls.user_name = cls.test_plan["user_name_prefix"]
        cls.session_name = cls.test_plan["session_name_prefix"]
        cls.logger.info("Create userName: '{}', sessionName: '{}'".format(
            cls.user_name, cls.session_name))

        os.environ["XLR_PYSDK_VERIFY_SSL_CERT"] = "false"
        if test_mgr.cliargs.host:
            url = "https://" + test_mgr.cliargs.host + ":" + str(test_mgr.cliargs.apiport)
        else:
            url = None
        cls.username = test_mgr.cliargs.uname
        cls.password = test_mgr.cliargs.password
        cls.xcUrl = url
        cls.client_secrets = {'xiusername': cls.username, 'xipassword': cls.password}
        cls.client = Client(url=cls.xcUrl, client_secrets=cls.client_secrets)

        # XXX TODO override the client user_name because the client user_name
        # is picked up from Auth. Need a way to do this in a clean way for
        # tests.
        cls.client._user_name = cls.user_name
        cls.client.add_data_target(GeneratedTargetName, "memory", {})

        try:
            cls.client.get_workbook(cls.session_name).delete()
        except Exception as ex:
            cls.logger.exception(ex)
            pass
        cls.workbook = cls.client.create_workbook(cls.session_name)
        cls.session = cls.workbook.activate()
        cls.client._legacy_xcalar_api.setSession(cls.session)
        cls.test_mgr.import_udfs(cls.client, cls.workbook,
                                 cls.test_plan["udfs"])

    @classmethod
    def teardown_class(cls, success):
        super().teardown_class(success)
        if success and not cls.test_mgr.cliargs.keep:
            cls.logger.info("Session destroy: {}".format(cls.session_name))
            cls.session.destroy()
            cls.workbook.delete()
        for ds in cls.client.list_datasets():
            try:
                cls.logger.info("Dataset delete: {} records: {}".format(
                    ds.name, ds.record_count()))
                ds.delete()
            except Exception as ex:
                cls.logger.exception(ex)
                pass

    def num_workers(self):
        return (self.test_plan["users_count"] *
                self.test_plan["sessions_per_user_count"])

    def scan_dfs(self):
        self.df_files = []
        for f in os.listdir(self.test_plan["df_restore_path"]):
            if self.test_plan["df_name_prefix"] in f:
                self.df_files.append(f)
        self.logger.info("Restore dataflows: {}".format(self.df_files))
        self.dataflows = {}
        for ii in range(self.num_workers()):
            self.dataflows[ii] = []

        count = 0
        for f in self.df_files:
            with open(f, 'rb') as fobj:
                with tarfile.open(fileobj=fobj, mode='r:gz') as tar:
                    for item in tar:
                        iobuf = tar.extractfile(item)
                        buf = iobuf.readlines()[0].decode('utf-8')
                        self.dataflows[count % self.num_workers()].append(buf)
                        self.logger.info("Restore dataflows[{}]: {}".format(
                            count, buf))
                        count += 1

    def work(self):
        self.test_mgr.log_test_header("{}".format(self.__class__.__name__))

        if os.path.exists(self.test_plan["df_restore_path"]):
            self.restore_dfs = True
            self.scan_dfs()
        else:
            self.restore_dfs = False

        # Create datasets with random schemas
        dataset_names = []
        for ii in range(self.test_plan["datasets_count"]):
            dataset_names.append("{}-{}".format(
                self.test_plan["dataset_name_prefix"], str(uuid.uuid4())))
        self.datasets, self.dss_schemas = self.test_mgr.gen_random_datasets(
            self.test_plan["datasets_count"],
            self.test_plan["dataset_min_rows"],
            self.test_plan["dataset_max_rows"], dataset_names,
            self.test_plan["schema_min_columns"],
            self.test_plan["schema_max_columns"],
            self.test_plan["column_name_prefix"], self.workbook)

        # Materialize tables. These could be used for shared table operations.
        self.source_tables = []
        table_names = []
        for ii in range(self.test_plan["datasets_count"]):
            table_names.append("{}-{}".format(
                self.test_plan["table_name_prefix"], str(uuid.uuid4())))
        source_tables = self.test_mgr.tables_materialize_from_dss(
            self.client, self.session, self.datasets, self.dss_schemas,
            table_names)

        self.test_mgr.start_worker_helper(
            tclass=TestWorkerOperator,
            num_workers=self.num_workers(),
            args={"{}".format(self.__class__.__name__): self},
            worker_timeout_sec=(self.test_mgr.cliargs.test_duration_in_sec +
                                self.test_plan["quiece_timeout_in_seconds"]))
        self.test_mgr.log_test_footer("{}".format(self.__class__.__name__))


# Test Operators worker
class TestWorkerOperator(TestWorker):
    def __init__(self, test_mgr, tnum, args):
        self.setup_class(test_mgr, tnum, args)

    @classmethod
    def setup_class(cls, test_mgr, tnum, args):
        super().setup_class(test_mgr, tnum, args)
        cls.test_plan = test_mgr.test_plan
        cls.user_name = "{}-{}".format(
            cls.test_plan["user_name_prefix"],
            int(tnum / cls.test_plan["users_count"]))
        cls.session_name = "{}-{}".format(cls.test_plan["session_name_prefix"],
                                          tnum)
        cls.logger.info(
            "Create tnum: {}, userName: '{}', sessionName: '{}'".format(
                tnum, cls.user_name, cls.session_name))

        cls.args = args
        cls.test_operators = args["TestOperators"]
        cls.barrier = args["barrier"]
        cls.client = Client(url=cls.test_operators.xcUrl, client_secrets=cls.test_operators.client_secrets)

        # XXX TODO override the client user_name because the client user_name
        # is picked up from Auth. Need a way to do this in a clean way for
        # tests.
        cls.client._user_name = cls.user_name
        try:
            cls.client.get_workbook(cls.session_name).delete()
        except Exception as ex:
            cls.logger.exception(ex)
            pass
        cls.workbook = cls.client.create_workbook(cls.session_name)
        cls.session = cls.workbook.activate()
        cls.client._legacy_xcalar_api.setSession(cls.session)
        test_mgr.import_udfs(cls.client, cls.workbook, cls.test_plan["udfs"])

        cls.qgh = QgenUtil.QueryGenHelper(
            client=cls.client, session=cls.session, logger=cls.logger)

        # XXX TODO Add weightage here per operator and make it bias random
        # op generation.
        cls.ops_helper = {
            XcalarApisT.XcalarApiIndex: {
                "driver": "worker_index",
                "weightage": 1,
                "opsCount": 0,
                "recordCount": 0,
                "sizeInBytes": 0
            },
            XcalarApisT.XcalarApiJoin: {
                "driver": "worker_join",
                "weightage": 1,
                "opsCount": 0,
            },
            XcalarApisT.XcalarApiProject: {
                "driver": "worker_project",
                "weightage": 1,
                "opsCount": 0,
                "recordCount": 0,
                "sizeInBytes": 0
            },
            XcalarApisT.XcalarApiGetRowNum: {
                "driver": "worker_get_rownum",
                "weightage": 1,
                "opsCount": 0,
            },
            XcalarApisT.XcalarApiFilter: {
                "driver": "worker_filter",
                "weightage": 1,
                "opsCount": 0,
                "recordCount": 0,
                "sizeInBytes": 0
            },
            XcalarApisT.XcalarApiGroupBy: {
                "driver": "worker_groupby",
                "weightage": 1,
                "opsCount": 0,
            },
            XcalarApisT.XcalarApiMap: {
                "driver": "worker_map",
                "weightage": 1,
                "opsCount": 0,
            },
            XcalarApisT.XcalarApiAggregate: {
                "driver": "worker_aggregate",
                "weightage": 1,
                "opsCount": 0,
            },
            XcalarApisT.XcalarApiUnion: {
                "driver": "worker_union",
                "weightage": 1,
                "opsCount": 0,
            },
            XcalarApisT.XcalarApiSynthesize: {
                "driver": "worker_synthesize",
                "weightage": 1,
                "opsCount": 0,
            },
            XcalarApisT.XcalarApiDeleteObjects: {
                "driver": "worker_delete_objs",
                "weightage": 1,
                "opsCount": 0,
            }
        }

    @classmethod
    def teardown_class(cls, success):
        if success and not cls.test_mgr.cliargs.keep:
            cls.logger.info("Destroy session: {}".format(cls.session_name))
            cls.session.destroy()
            cls.workbook.delete()
        super().teardown_class(success)

    def worker_bulkload(self, stats):
        pass

    # Trim the Table memory by dropping tables.
    def worker_delete_objs(self, stats):
        empty_tables = self.get_empty_tables()
        for tab in empty_tables:
            self.logger.info("worker_delete_objs: Drop empty table: {}".format(
                tab.name))
            tab.drop()
        tables_sorted = self.get_tables_with_size(ascending=False)
        self.logger.debug(
            "worker_delete_objs: tables_sorted: {}".format(tables_sorted))
        if tables_sorted is None:
            return
        num_tables_to_drop = int(
            (len(tables_sorted) * self.test_plan["table_drop_percent"]) / 100)
        if not num_tables_to_drop:
            return
        self.logger.debug("worker_delete_objs: Drop tables count: {}".format(
            num_tables_to_drop))
        dropped_count = 0
        for tname, size in tables_sorted.items():
            tab = self.session.get_table(tname)
            self.logger.info(
                "worker_delete_objs: Drop table: {}, records: {} size in bytes: {}"
                .format(tab.name, tab.record_count(),
                        tab._get_meta().total_size_in_bytes))
            tab.drop()
            dropped_count += 1
            if dropped_count == num_tables_to_drop:
                break

    # Verify row count matches
    def compare_tables(self, tables, df_names):
        record_counts = []
        for table, df_name in zip(tables, df_names):
            record_counts.append(self.session.get_table(table).record_count())

        # Verify if row count is the same for all the tables
        if len(set(record_counts)) != 1:
            for table, df_name in zip(tables, df_names):
                self.persist_dataflow_from_table(
                    self.session.get_table(table), df_name)
            raise OperatorsTestRowCountMismatch(
                "Row Count Mismatch: tables: '{}', df_names: '{}', record_counts: '{}'"
                .format(tables, df_names, record_counts))

    # For index, verification involves running the same index twice and
    # comparing the table row count.
    def worker_index_helper(self, stats, source, schema, keys_map, prefix=''):
        # Issue index queries twice
        df_names = []
        dst_names = []
        for ii in range(2):
            dst_table_name = "{}-{}".format(
                self.test_plan["table_name_prefix"], str(uuid.uuid4()))
            df_name = self.qgh.issue_index(
                source=source,
                dest=dst_table_name,
                keys_map=keys_map,
                prefix=prefix,
                is_async=True,
                clean_job=random.choice([True, False]),
                parallel_ops=random.choice([True, False]),
                sched_name=self.qgh.get_rand_sched_name())
            self.logger.info(
                "worker_index_helper: source {}, dest: {}, keys_map: {}, prefix: {}, df_name: {}, user_name: {}, session_name: {}"
                .format(source, dst_table_name, keys_map, prefix, df_name,
                        self.user_name, self.session_name))

            df_names.append(df_name)
            dst_names.append(dst_table_name)

        # Wait for async queues to complete.
        self.qgh.wait_for_async_queries(df_names)

        # Verify tables match
        self.compare_tables(dst_names, df_names)

        return dst_names

    # Use Unordered for dataset index keys
    def worker_dataset_index(self, stats):
        src_ds = self.get_rand_dataset()
        schema = self.get_dataset_schema(src_ds)
        keys_map = self.qgh.get_rand_keys_map(schema)
        self.worker_index_helper(
            stats,
            src_ds.name,
            schema,
            keys_map,
            prefix=self.test_plan["dataset_prefix"])

    # Use Unordered for tabl eindex keys
    def worker_table_index(self, stats):
        src_table = self.get_rand_table()
        schema = self.qgh.get_schema(src_table)
        keys_map = self.qgh.get_rand_keys_map(schema)
        self.worker_index_helper(stats, src_table.name, schema, keys_map)

    # For table sort keys, use Unordered, Ascending and Descending.
    def worker_table_sort(self, stats):
        src_table = self.get_rand_table()
        schema = self.qgh.get_schema(src_table)
        keys_map = self.qgh.get_rand_keys_map(schema, [
            XcalarOrderingT.XcalarOrderingUnordered,
            XcalarOrderingT.XcalarOrderingPartialAscending,
            XcalarOrderingT.XcalarOrderingPartialDescending
        ])
        dst_names = self.worker_index_helper(stats, src_table.name, schema,
                                             keys_map)

        # Verification.
        gb_tables = []
        df_names = []
        col_name = list(self.qgh.get_rand_column(src_table).keys())[0]
        eval_strings = []
        new_col_names = []
        evals = []
        for ii in range(random.randint(1, GroupByEvalsCountMax)):
            eval_string = "count({})".format(col_name)
            new_col_name = "{}-gb-{}".format(
                self.test_plan["column_name_prefix"], str(uuid.uuid4()))
            new_col_names.append(new_col_name)
            evals.append((eval_string, new_col_name))

        for dst_name in dst_names:
            gb_name = "{}-{}".format(self.test_plan["table_name_prefix"],
                                     str(uuid.uuid4()))
            df_names.append(
                self.worker_groupby_helper(
                    src_table_name=dst_names[0],
                    dst_table_name=gb_name,
                    eval_list=evals))
            gb_tables.append(gb_name)

        # Verify tables match
        self.compare_tables(gb_tables, df_names)

    def worker_index(self, stats):
        index_choices = [
            "worker_dataset_index", "worker_table_index", "worker_table_sort"
        ]
        getattr(self, random.choice(index_choices))(stats)

    def gen_tab_rename_map(self, table):
        col_names = []
        new_col_names = []
        col_types = []
        keys = table._get_meta().keys
        for k, v in table.schema.items():
            col_names.append(k)
            col_types.append(v)
            new_col_names.append("{}-{}".format(
                self.test_plan["column_name_prefix"], str(uuid.uuid4())))
        return QueryGenerate().get_table_rename_maps(
            source_columns=col_names,
            dest_columns=new_col_names,
            column_types=col_types)

    def worker_join(self, stats):
        # Creater Join left table
        src_table = self.get_rand_table()
        schema = self.qgh.get_schema(src_table)
        keys_map = self.qgh.get_rand_keys_map(schema)
        lt_table_name = "{}-lt-{}".format(self.test_plan["table_name_prefix"],
                                          str(uuid.uuid4()))
        df_name = self.qgh.issue_index(
            source=src_table.name,
            dest=lt_table_name,
            keys_map=keys_map,
            clean_job=random.choice([True, False]),
            parallel_ops=random.choice([True, False]),
            sched_name=self.qgh.get_rand_sched_name())

        self.logger.info(
            "worker_join: src table: {}, left table: {}, keys_map: {}, df_name: {}, user_name: {}, session_name: {}"
            .format(src_table.name, lt_table_name, keys_map, df_name,
                    self.user_name, self.session_name))
        lt_table = self.session.get_table(lt_table_name)
        lt_rename_map = self.gen_tab_rename_map(lt_table)

        # Create the join right table
        col_name = "{}-{}".format(self.test_plan["column_name_prefix"],
                                  str(uuid.uuid4()))
        row_num_table_name = "{}-rn-{}".format(
            self.test_plan["table_name_prefix"], str(uuid.uuid4()))
        df_name = self.qgh.issue_row_num(
            source=lt_table.name,
            dest=row_num_table_name,
            new_field=col_name,
            clean_job=random.choice([True, False]),
            parallel_ops=random.choice([True, False]),
            sched_name=self.qgh.get_rand_sched_name())

        self.logger.info(
            "worker_join: src table: {}, rownum table: {}, df_name: {}, user_name: {}, session_name: {}"
            .format(lt_table.name, row_num_table_name, df_name, self.user_name,
                    self.session_name))

        filter_str = "not(mod({}, {}))".format(
            col_name,
            (self.session.get_table(row_num_table_name).record_count() /
             JoinRightTableRecordCount))
        rt_table_name = "{}-rt-{}".format(self.test_plan["table_name_prefix"],
                                          str(uuid.uuid4()))
        df_name = self.qgh.issue_filter(
            source=lt_table.name,
            dest=rt_table_name,
            eval_string=filter_str,
            is_async=False,
            clean_job=random.choice([True, False]),
            parallel_ops=random.choice([True, False]),
            sched_name=self.qgh.get_rand_sched_name())
        self.logger.info(
            "worker_join: src table: {}, right table: {}, eval_str: {}, df_name: {}, user_name: {}, session_name: {}"
            .format(lt_table.name, rt_table_name, filter_str, df_name,
                    self.user_name, self.session_name))
        rt_table = self.session.get_table(rt_table_name)
        rt_rename_map = self.gen_tab_rename_map(rt_table)

        # Issue Join
        for join_type in JoinOperatorTStr.keys():
            join_table_name = "{}-join-{}".format(
                self.test_plan["table_name_prefix"], str(uuid.uuid4()))
            self.logger.info(
                "worker_join: type: {}, left table: {}, left rename map: {}, right table: {}, right rename map: {},  user_name: {}, session_name: {}"
                .format(JoinOperatorTStr[join_type], lt_table.name,
                        lt_rename_map, rt_table.name, rt_rename_map,
                        self.user_name, self.session_name))
            self.qgh.issue_join(
                source_left=lt_table_name,
                source_right=rt_table_name,
                dest=join_table_name,
                left_rename_maps=lt_rename_map,
                right_rename_maps=rt_rename_map,
                join_type=join_type,
                left_keys=[],
                right_keys=[],
            # XXX TODO Need to exercise Join filter eval code path as well
                eval_string='',
                keep_all_columns=random.choice([True, False]),
                null_safe=random.choice([True, False]),
                clean_job=random.choice([True, False]),
                parallel_ops=random.choice([True, False]),
                sched_name=self.qgh.get_rand_sched_name())

    # Project random columns
    def worker_project(self, stats):
        src_table = self.get_rand_table()
        dst_name = "{}-{}".format(self.test_plan["table_name_prefix"],
                                  str(uuid.uuid4()))
        df_name = "{}-{}".format(self.test_plan["query_name_prefix"],
                                 dst_name.split("-"))[1]
        schema = self.qgh.get_rand_columns(src_table)
        df_name = self.qgh.issue_project(
            source=src_table.name,
            dest=dst_name,
            column_names=list(schema.keys()),
            clean_job=random.choice([True, False]),
            parallel_ops=random.choice([True, False]),
            sched_name=self.qgh.get_rand_sched_name())
        self.logger.info(
            "worker_project: src table: {}, dst table: {}, df_name: {}, schema: {}, user_name: {}, session_name: {}"
            .format(src_table.name, dst_name, schema, df_name, self.user_name,
                    self.session_name))

    def worker_get_rownum(self, stats):
        src_table = self.get_rand_table()
        dst_name = "{}-{}".format(self.test_plan["table_name_prefix"],
                                  str(uuid.uuid4()))
        col_name = "{}-{}".format(self.test_plan["column_name_prefix"],
                                  str(uuid.uuid4()))
        df_name = self.qgh.issue_row_num(
            source=src_table.name,
            dest=dst_name,
            new_field=col_name,
            clean_job=random.choice([True, False]),
            parallel_ops=random.choice([True, False]),
            sched_name=self.qgh.get_rand_sched_name())

        self.logger.info(
            "worker_get_rownum: src table: {}, dst table: {}, new field: {}, df_name: {}, user_name: {}, session_name: {}"
            .format(src_table.name, dst_name, col_name, df_name,
                    self.user_name, self.session_name))

    # For Filter operator, verfication strategy involves running the same
    # filter eval twice and comparing the table row count.
    def worker_filter(self, stats):
        src_table = self.get_rand_table()
        scalar = self.get_rand_scalar()
        column = self.qgh.get_rand_column(src_table)
        col_name = list(column.items())[0][0]
        col_type = list(column.items())[0][1]
        filter_ops = ["gt", "lt", "default:multiJoin"]
        eval_str = "{}(float({}), float({}{}))".format(
            random.choice(filter_ops), col_name,
            QgenUtil.OperatorsAggregateTag, scalar.name)

        # Issue filter queries twice
        df_names = []
        dst_names = []
        for ii in range(2):
            dst_name = "{}-{}".format(self.test_plan["table_name_prefix"],
                                      str(uuid.uuid4()))
            df_name = self.qgh.issue_filter(
                source=src_table.name,
                dest=dst_name,
                eval_string=eval_str,
                is_async=True,
                clean_job=random.choice([True, False]),
                parallel_ops=random.choice([True, False]),
                sched_name=self.qgh.get_rand_sched_name())
            df_names.append(df_name)
            dst_names.append(dst_name)
            self.logger.info(
                "worker_filter: source table: {}, dest table: {}, eval_str: {}, df_name: {}, user_name: {}, session_name: {}"
                .format(src_table.name, dst_name, eval_str, df_name,
                        self.user_name, self.session_name))

        # Wait for async queues to complete.
        self.qgh.wait_for_async_queries(df_names)

        # Verify tables match
        self.compare_tables(dst_names, df_names)

    def worker_groupby_helper(self,
                              src_table_name,
                              dst_table_name,
                              eval_list,
                              include_sample=False,
                              icv_mode=False,
                              group_all=False):
        src_table = self.session.get_table(src_table_name)
        table_keys = src_table._get_meta().keys
        table_float_keys = {}
        for k, v in table_keys.items():
            if v["type"] in ["DfFloat32", "DfFloat64"]:
                table_float_keys[k] = v

        # If keys are of type float, cast it to int and re-index on the new
        # set of keys.
        if len(table_float_keys.keys()):
            map_table_name = "{}-map-{}".format(
                self.test_plan["table_name_prefix"], str(uuid.uuid4()))
            map_eval_strings = []
            index_key_names = []
            index_key_field_names = []
            index_key_types = []
            index_key_orderings = []
            for k, v in table_float_keys.items():
                map_col_name = "{}-{}".format(
                    self.test_plan["column_name_prefix"], str(uuid.uuid4()))
                map_eval_string = "int({})".format(self.escape_nested_delim(k))
                map_eval_strings.append((map_eval_string, map_col_name))
                index_key_names.append(map_col_name)
                index_key_orderings.append(
                    XcalarOrderingT.XcalarOrderingUnordered)
                index_key_types.append(DfFieldTypeT.DfUnknown)
                index_key_field_names.append(
                    map_col_name.replace(QgenUtil.DfFatptrPrefixDelimiter,
                                         "__"))

            df_name = self.qgh.issue_map(
                source=src_table.name,
                dest=map_table_name,
                evals_list=map_eval_strings,
                clean_job=random.choice([True, False]),
                parallel_ops=random.choice([True, False]),
                sched_name=self.qgh.get_rand_sched_name())
            self.logger.info(
                "worker_groupby_helper: src table: {}, map table: {}, evals_list: {}, df_name: {}, user_name: {}, session_name: {}"
                .format(src_table.name, map_table_name, map_eval_strings,
                        df_name, self.user_name, self.session_name))

            index_table_name = "{}-idx-{}".format(
                self.test_plan["table_name_prefix"], str(uuid.uuid4()))
            index_keys_map = QueryGenerate().get_key_maps(
                index_key_names, index_key_field_names, index_key_types,
                index_key_orderings)
            df_name = self.qgh.issue_index(
                source=map_table_name,
                dest=index_table_name,
                keys_map=index_keys_map,
                clean_job=random.choice([True, False]),
                parallel_ops=random.choice([True, False]),
                sched_name=self.qgh.get_rand_sched_name())
            self.logger.info(
                "worker_groupby_helper: src table: {}, index table: {}, keys_map: {}, df_name: {}, user_name: {}, session_name: {}"
                .format(map_table_name, index_table_name, index_keys_map,
                        df_name, self.user_name, self.session_name))

            # Now this becomes the new source table on which to do groupBy
            src_table = self.session.get_table(index_table_name)

        elif QgenUtil.DsDefaultDatasetKeyName in table_keys.keys():
            schema = self.qgh.get_schema(src_table)
            keys_map = self.qgh.get_rand_keys_map(schema)
            index_table_name = "{}-idx-{}".format(
                self.test_plan["table_name_prefix"], str(uuid.uuid4()))
            df_name = self.qgh.issue_index(
                source=src_table.name,
                dest=index_table_name,
                keys_map=keys_map,
                clean_job=random.choice([True, False]),
                parallel_ops=random.choice([True, False]),
                sched_name=self.qgh.get_rand_sched_name())
            self.logger.info(
                "worker_groupby_helper: src table: {}, index table: {}, keys_map: {}, df_name: {}, user_name: {}, session_name: {}"
                .format(src_table.name, index_table_name, keys_map, df_name,
                        self.user_name, self.session_name))

            # Now this becomes the new source table on which to do groupBy
            src_table = self.session.get_table(index_table_name)

        # Issue groupby
        keys_map = self.qgh.get_keys_map(
            self.session.get_table(src_table.name))
        df_name = self.qgh.issue_groupby(
            source=src_table.name,
            dest=dst_table_name,
            eval_list=eval_list,
            group_all=group_all,
            include_sample=include_sample,
            keys_map=keys_map,
            clean_job=random.choice([True, False]),
            parallel_ops=random.choice([True, False]),
            sched_name=self.qgh.get_rand_sched_name())
        self.logger.info(
            "worker_groupby_helper: src table: {}, groupby table: {}, eval_list: {}, keys_map: {}, df_name: {}, user_name: {}, session_name: {}"
            .format(src_table.name, dst_table_name, eval_list, keys_map,
                    df_name, self.user_name, self.session_name))
        return df_name

    # Issue aggregate eval as groupBy operator.
    def issue_aggr_eval_as_groupby(self,
                                   src_name,
                                   eval_string,
                                   new_col_name=None,
                                   dst_name=None):
        if dst_name is None:
            dst_name = "{}-{}".format(self.test_plan["table_name_prefix"],
                                      str(uuid.uuid4()))
        if new_col_name is None:
            new_col_name = "{}-{}".format(self.test_plan["column_name_prefix"],
                                          str(uuid.uuid4()))
        eval_list = [(eval_string, new_col_name)]
        keys_map = self.qgh.get_keys_map(self.session.get_table(src_name))
        df_name = self.qgh.issue_groupby(
            source=src_name,
            dest=dst_name,
            eval_list=eval_list,
            group_all=True,
            include_sample=False,
            keys_map=keys_map,
            clean_job=random.choice([True, False]),
            parallel_ops=random.choice([True, False]),
            sched_name=self.qgh.get_rand_sched_name())
        self.logger.info(
            "issue_aggr_eval_as_groupby: src_name: {}, eval_list: {}, dst_name: {}, keys_map: {}, df_name: {}, user_name: {}, session_name: {}"
            .format(src_name, eval_list, dst_name, keys_map, df_name,
                    self.user_name, self.session_name))
        return df_name, dst_name

    # For groupby verification,
    # * Choose a random source table and then choose to run count() Evals
    #   against a randomly chosen table column.
    # * Issue groupBy Operarator on the Evals.
    # * To verify the result, run aggregate on one of the evals and verify sum
    #   against table row count.
    def worker_groupby(self, stats):
        src_table = self.get_rand_table()
        schema = self.qgh.get_rand_column(src_table)
        col_name = list(schema.keys())[0]
        eval_strings = []
        new_col_names = []
        evals = []
        for ii in range(random.randint(1, GroupByEvalsCountMax)):
            eval_string = "count({})".format(col_name)
            new_col_name = "{}-gb-{}".format(
                self.test_plan["column_name_prefix"], str(uuid.uuid4()))
            new_col_names.append(new_col_name)
            evals.append((eval_string, new_col_name))

        # Issue groupby
        gb_table_name = "{}-gb-{}".format(self.test_plan["table_name_prefix"],
                                          str(uuid.uuid4()))
        group_all = random.choice([True, False])
        include_sample = not group_all
        self.logger.info(
            "worker_groupby: src table: {}, groupby table: {}, evals: {}, user_name: {}, session_name: {}"
            .format(src_table.name, gb_table_name, evals, self.user_name,
                    self.session_name))

        # XXX TODO Exercise ICV mode
        self.worker_groupby_helper(
            src_table_name=src_table.name,
            dst_table_name=gb_table_name,
            eval_list=evals,
            include_sample=include_sample,
            icv_mode=False,
            group_all=group_all)

        # Do verification
        agg_eval_str = "sum({})".format(random.choice(new_col_names))
        aggr_name = "{}-{}".format(self.test_plan["scalar_name_prefix"],
                                   str(uuid.uuid4()))
        df_name = self.qgh.issue_aggregate(
            source=gb_table_name,
            dest=aggr_name,
            eval_string=agg_eval_str,
            clean_job=random.choice([True, False]),
            parallel_ops=random.choice([True, False]),
            sched_name=self.qgh.get_rand_sched_name())
        self.logger.info(
            "worker_groupby: src table: {}, aggregate: {}, eval_str: {}, df_name: {}, user_name: {}, session_name: {}"
            .format(gb_table_name, aggr_name, agg_eval_str, df_name,
                    self.user_name, self.session_name))
        aggr_sum = int(self.get_scalar(aggr_name)['constant'])
        source_count = src_table.record_count()

        self.logger.info(
            "worker_groupby: source table: {}, record count: {}, aggregate: {}, sum: {}, user_name: {}, session_name: {}"
            .format(src_table.name, source_count, aggr_name, aggr_sum,
                    self.user_name, self.session_name))

        if aggr_sum != source_count:
            # There may be empty rows, so count them
            filter_str = "not(exists({}))".format(col_name)
            dst_name = "{}-{}".format(self.test_plan["table_name_prefix"],
                                      str(uuid.uuid4()))
            df_name = self.qgh.issue_filter(
                source=src_table.name,
                dest=dst_name,
                eval_string=filter_str,
                is_async=False,
                clean_job=random.choice([True, False]),
                parallel_ops=random.choice([True, False]),
                sched_name=self.qgh.get_rand_sched_name())
            self.logger.info(
                "worker_groupby: src table: {}, dst table: {}, eval_str: {}, df_name: {}, user_name: {}, session_name: {}"
                .format(src_table.name, dst_name, filter_str, df_name,
                        self.user_name, self.session_name))

            source_count -= self.session.get_table(dst_name).record_count()

        if aggr_sum != source_count:
            src_df_name = self.persist_dataflow_from_table(src_table)
            aggr_df_name, aggr_table_name = self.issue_aggr_eval_as_groupby(
                gb_table_name, agg_eval_str)
            self.persist_dataflow_from_table(
                self.session.get_table(aggr_table_name), aggr_df_name)
            raise OperatorsTestAggrMismatch(
                "GroupBy source: '{}', df_name: '{}', record count: {}, aggregate: '{}', sum: {}, df_name: '{}', user_name: '{}', session_name: '{}'"
                .format(src_table.name, src_df_name, source_count, aggr_name,
                        aggr_sum, aggr_df_name, self.user_name,
                        self.session_name))

    def escape_nested_delim(self, field_name):
        for delim in [
                QgenUtil.DfNestedDelimiter,
                QgenUtil.DfArrayIndexStartDelimiter,
                QgenUtil.DfArrayIndexEndDelimiter
        ]:
            field_name.replace(delim, QgenUtil.EscapeEscape)
        return field_name

    def worker_map(self, stats):
        src_table = self.get_rand_table()
        schema = self.qgh.get_rand_columns(src_table)
        self.logger.info("worker_map: src table: {}, schema: {}".format(
            src_table.name, schema))

    def worker_union(self, stats):
        source_table = self.get_rand_table()
        source_count = random.randint(1, UnionMaxSourceTables)

        # Issue index
        schema = self.qgh.get_schema(self.session.get_table(source_table.name))
        keys_map = self.qgh.get_rand_keys_map(
            schema=schema, keys_count=len(schema))
        index_table_name = "{}-id-{}".format(
            self.test_plan["table_name_prefix"], str(uuid.uuid4()))
        df_name = self.qgh.issue_index(
            source=source_table.name,
            dest=index_table_name,
            keys_map=keys_map,
            clean_job=random.choice([True, False]),
            parallel_ops=random.choice([True, False]),
            sched_name=self.qgh.get_rand_sched_name())
        self.logger.info(
            "worker_union: src table: {}, dst table: {}, keys_map: {}, df_name: {}, user_name: {}, session_name: {}"
            .format(source_table.name, index_table_name, keys_map, df_name,
                    self.user_name, self.session_name))

        # Add row num
        col_name = "{}-{}".format(self.test_plan["column_name_prefix"],
                                  str(uuid.uuid4()))
        row_num_table_name = "{}-rn-{}".format(
            self.test_plan["table_name_prefix"], str(uuid.uuid4()))
        df_name = self.qgh.issue_row_num(
            source=index_table_name,
            dest=row_num_table_name,
            new_field=col_name,
            clean_job=random.choice([True, False]),
            parallel_ops=random.choice([True, False]),
            sched_name=self.qgh.get_rand_sched_name())
        self.logger.info(
            "worker_union: src table: {}, dst table: {}, keys_map: {}, df_name: {}, user_name: {}, session_name: {}"
            .format(
                index_table_name, row_num_table_name,
                self.qgh.get_keys_map(
                    self.session.get_table(row_num_table_name)), df_name,
                self.user_name, self.session_name))
        rnum_row_count = self.session.get_table(
            row_num_table_name).record_count()

        # Convert Fatptrs to immediates for Union source tables
        synth_name = "{}-sy-{}".format(self.test_plan["table_name_prefix"],
                                       str(uuid.uuid4()))
        table_rename_map = self.gen_tab_rename_map(
            self.session.get_table(row_num_table_name))
        keys_map = self.session.get_table(row_num_table_name)._get_meta().keys
        for field in table_rename_map:
            if field['columnType'] == 'DfFatptr':
                field['columnType'] = 'DfUnknown'
            # XXX TODO For some reason, synthesize does not seem to allow keys column renames
            if field['sourceColumn'] in keys_map.keys():
                field['destColumn'] = field['sourceColumn']
        df_name = self.qgh.issue_synthesize(
            source=row_num_table_name,
            dest=synth_name,
            table_rename_map=table_rename_map,
            clean_job=random.choice([True, False]),
            parallel_ops=random.choice([True, False]),
            sched_name=self.qgh.get_rand_sched_name())
        self.logger.info(
            "worker_union: src table: {}, dst table: {}, table_rename_map: {}, keys_map: {}, df_name: {}, user_name: {}, session_name: {}"
            .format(row_num_table_name, synth_name, table_rename_map,
                    self.qgh.get_keys_map(self.session.get_table(synth_name)),
                    df_name, self.user_name, self.session_name))

        source_table_names = []
        source_rename_maps = []
        for ii in range(source_count):
            filter_str = "not(mod({}, {}))".format(
                col_name, (rnum_row_count / random.randint(1, rnum_row_count)))
            src_table_name = "{}-rt-{}".format(
                self.test_plan["table_name_prefix"], str(uuid.uuid4()))
            df_name = self.qgh.issue_filter(
                source=synth_name,
                dest=src_table_name,
                eval_string=filter_str,
                is_async=False,
                clean_job=random.choice([True, False]),
                parallel_ops=random.choice([True, False]),
                sched_name=self.qgh.get_rand_sched_name())
            self.logger.info(
                "worker_union: src table: {}, dst table: {}, eval_str: {}, df_name: {}, user_name: {}, session_name: {}"
                .format(synth_name, src_table_name, filter_str, df_name,
                        self.user_name, self.session_name))
            source_table = self.session.get_table(src_table_name)
            source_table_names.append(source_table.name)
            source_rename_maps.append(self.gen_tab_rename_map(source_table))

        for union_type in UnionOperatorTStr.keys():
            union_table_name = "{}-un-{}".format(
                self.test_plan["table_name_prefix"], str(uuid.uuid4()))
            df_name = self.qgh.issue_union(
                sources=source_table_names,
                dest=union_table_name,
                table_rename_maps=source_rename_maps,
                union_type=union_type,
                dedup=random.choice([True, False]),
                clean_job=random.choice([True, False]),
                parallel_ops=random.choice([True, False]),
                sched_name=self.qgh.get_rand_sched_name())
            self.logger.info(
                "worker_union: src tables: {}, union table: {}, rename_maps: {}, df_name: {}, user_name: {}, session_name: {}"
                .format(source_table_names, union_table_name,
                        source_rename_maps, df_name, self.user_name,
                        self.session_name))

    def worker_synthesize(self, stats):
        source_table = self.get_rand_table()
        synth_name = "{}-sy-{}".format(self.test_plan["table_name_prefix"],
                                       str(uuid.uuid4()))
        table_rename_map = self.gen_tab_rename_map(
            self.session.get_table(source_table.name))
        keys_map = source_table._get_meta().keys
        for field in table_rename_map:
            if field['columnType'] == 'DfFatptr':
                field['columnType'] = 'DfUnknown'
            # XXX TODO For some reason, synthesize does not seem to allow keys column renames
            if field['sourceColumn'] in keys_map.keys():
                field['destColumn'] = field['sourceColumn']

        df_name = self.qgh.issue_synthesize(
            source=source_table.name,
            dest=synth_name,
            table_rename_map=table_rename_map,
            clean_job=random.choice([True, False]),
            parallel_ops=random.choice([True, False]),
            is_async=True,
            sched_name=self.qgh.get_rand_sched_name())
        self.qgh.wait_for_async_query(df_name)
        self.logger.info(
            "worker_synthesize: src table: {}, dst table: {}, table_rename_map: {}, keys_map: {}, df_name: {}, user_name: {}, session_name: {}"
            .format(source_table.name, synth_name, table_rename_map,
                    self.qgh.get_keys_map(self.session.get_table(synth_name)),
                    df_name, self.user_name, self.session_name))

        df_names = []
        df_names.append("{}-{}".format(self.test_plan["df_name_prefix"],
                                       uuid.uuid4()))
        df_names.append(df_name)

        # Verify tables match
        dst_names = []
        dst_names.append(source_table.name)
        dst_names.append(synth_name)
        self.compare_tables(dst_names, df_names)

    # For Aggregate operator, verification strategy involves running the same
    # aggregate eval twice and comparing the results.
    def worker_aggregate(self, stats):
        src_table = self.get_rand_table()
        column = self.qgh.get_rand_column(src_table)
        col_name = list(column.items())[0][0]
        col_type = list(column.items())[0][1]
        eval_str = "count({})".format(col_name)

        # Issue aggregate queries twice
        df_names = []
        dst_names = []
        for ii in range(2):
            dst_name = "{}-{}".format(self.test_plan["scalar_name_prefix"],
                                      str(uuid.uuid4()))
            df_name = self.qgh.issue_aggregate(
                source=src_table.name,
                dest=dst_name,
                eval_string=eval_str,
                is_async=True,
                clean_job=random.choice([True, False]),
                parallel_ops=random.choice([True, False]),
                sched_name=self.qgh.get_rand_sched_name())

            self.logger.info(
                "worker_aggregate: src table: {}, dst: {}, eval_str: {}, df_name: {}, user_name: {}, session_name: {}"
                .format(src_table.name, dst_name, eval_str, df_name,
                        self.user_name, self.session_name))

            df_names.append(df_name)
            dst_names.append(dst_name)

        self.qgh.wait_for_async_queries(df_names)

        # Verify the results are the same
        results = []
        for ii in range(2):
            results.append(self.get_scalar(dst_names[ii]))

        if results[0] != results[1]:
            aggr_df_name, aggr_table_name = self.issue_aggr_eval_as_groupby(
                src_table.name, eval_str)
            self.persist_dataflow_from_table(
                self.session.get_table(aggr_table_name), aggr_df_name)
            raise OperatorsTestAggrMismatch(
                "aggr_df_name: {}, result scalar-1: {}, record count: {}, result scalar-2: {}, record count: {}, user_name: {}, session_name: {}"
                .format(aggr_df_name, dst_names[0], results[0], dst_names[1],
                        results[1], self.user_name, self.session_name))

    def get_scalar(self, scalar_name):
        return ResultSet(
            self.client,
            table_name=scalar_name,
            session_name=self.session_name).get_row(0)

    def get_dataset_schema(self, dataset_name):
        return self.get_dataset_schema(self, self.client.get_dataset())

    def get_dataset_schema(self, dataset):
        info = dataset.get_info()
        schema = info["columns"]
        ret_schema = {}
        for col in schema:
            ret_schema[col["name"]] = col["type"]
        return ret_schema

    def get_rand_dataset(self):
        return self.get_rand_datasets(dataset_count=1)[0]

    def get_rand_datasets(self, dataset_count):
        valid_datasets = [
            ds for ds in self.client.list_datasets()
            if self.test_plan["dataset_name_prefix"] in ds.name
        ]
        if len(valid_datasets) < dataset_count:
            return None
        else:
            return random.sample(population=valid_datasets, k=dataset_count)

    # Prune empty tables and just return a table with non-zero records
    def get_rand_table(self):
        return self.get_rand_tables(table_count=1)[0]

    def get_empty_tables(self):
        return [
            table for table in self.session.list_tables(
                pattern="{}*".format(self.test_plan["table_name_prefix"]))
            if not table.record_count()
        ]

    def get_non_empty_tables(self):
        return [
            table for table in self.session.list_tables(
                pattern="{}*".format(self.test_plan["table_name_prefix"]))
            if table.record_count()
        ]

    def get_tables_with_size(self, ascending=True):
        tables_with_size = {}
        for table in self.session.list_tables(
                pattern="{}*".format(self.test_plan["table_name_prefix"])):
            tables_with_size[table.
                             name] = table._get_meta().total_size_in_bytes
        return dict(
            sorted(
                tables_with_size.items(),
                key=lambda x: x[1],
                reverse=not ascending))

    # Prune empty tables and just return tables with non-zero records
    def get_rand_tables(self, table_count):
        valid_tables = [
            table for table in self.session.list_tables(
                pattern="{}*".format(self.test_plan["table_name_prefix"]))
            if table.record_count()
        ]
        self.logger.debug(
            "get_rand_tables: valid_tables {}, table_count: {}".format(
                valid_tables, table_count))
        if len(valid_tables) < table_count:
            return None
        else:
            return random.sample(population=valid_tables, k=table_count)

    # Get random scalar
    def get_rand_scalar(self):
        return self.get_rand_scalars(scalar_count=1)[0]

    # Get random collection of scalars
    def get_rand_scalars(self, scalar_count):
        scalars = [
            scalar for scalar in self.session.list_tables(
                pattern="{}*".format(self.test_plan["scalar_name_prefix"]))
        ]

        if len(scalars) < scalar_count:
            return None
        else:
            return random.sample(population=scalars, k=scalar_count)

    # Idea is to create a bunch of source tables that have fatPtrs, so
    # we get to exercise demystification codepaths. Also create scalars
    # to be used by tests downstream.
    def create_source_tables_and_scalars(self):
        df_names = []
        src_table_names = []

        # For each Dataset, pick some keys to index on and dispatch
        # async queries.
        for ii in range(self.test_plan["datasets_count"]):
            table_name = "{}-{}".format(self.test_plan["table_name_prefix"],
                                        str(uuid.uuid4()))
            dataset = self.test_operators.datasets[ii]
            ds_schema = self.test_operators.dss_schemas[ii]
            keys_map = self.qgh.get_rand_keys_map(ds_schema["columns"])
            df_name = self.qgh.issue_index(
                source=dataset.name,
                dest=table_name,
                keys_map=keys_map,
                prefix=self.test_plan["dataset_prefix"],
                optimized=False,
                is_async=True,
                clean_job=random.choice([True, False]),
                parallel_ops=random.choice([True, False]),
                sched_name=self.qgh.get_rand_sched_name())
            self.logger.info(
                "Create source table: {}, dataset: {}, schema: {}, keys_map: {}, df_name: {}, user_name: {}, session_name: {}"
                .format(table_name, dataset.name, ds_schema, keys_map, df_name,
                        self.user_name, self.session_name))

            df_names.append(df_name)
            src_table_names.append(table_name)

        # Wait for async queues to complete.
        self.qgh.wait_for_async_queries(df_names)

        if self.test_mgr.is_test_timeout():
            return

        df_names = []
        for src_table_name in src_table_names:
            src_table = self.session.get_table(src_table_name)
            column = self.qgh.get_rand_column(src_table)
            col_name = list(column.items())[0][0]
            col_type = list(column.items())[0][1]
            eval_str = "count({})".format(col_type)

            dst = "{}-{}".format(self.test_plan["scalar_name_prefix"],
                                 str(uuid.uuid4()))
            df_name = self.qgh.issue_aggregate(
                source=src_table_name,
                dest=dst,
                eval_string=eval_str,
                is_async=True,
                clean_job=random.choice([True, False]),
                parallel_ops=random.choice([True, False]),
                sched_name=self.qgh.get_rand_sched_name())
            self.logger.info(
                "Create src scalars: {}, src table: {}, eval_str: {}, df_name: {}, user_name: {}, session_name: {}"
                .format(dst, src_table_name, eval_str, df_name, self.user_name,
                        self.session_name))
            df_names.append(df_name)

        # Wait for async queues to complete.
        self.qgh.wait_for_async_queries(df_names)

    def get_rand_op(self):
        return random.choice(list(self.ops_helper.keys()))

    # Randomly issue operator
    def execute_rand_op(self):
        next_op = self.get_rand_op()
        self.logger.info("Execute op: {}".format(XcalarApisTStr[next_op]))
        try:
            getattr(self, self.ops_helper[next_op]["driver"])(
                self.ops_helper[next_op])
            self.ops_helper[next_op]["opsCount"] += 1
        except XcalarApiStatusException as ex:
            self.logger.exception(ex)
            if ex.status not in [
                    StatusT.StatusNoMem, StatusT.StatusNoXdbPageBcMem
            ]:
                raise ex
        except XDPException as ex:
            self.logger.exception(ex)
            if ex.statusCode not in [
                    StatusT.StatusNoMem, StatusT.StatusNoXdbPageBcMem
            ]:
                raise ex
            # XXX Collect stats about failed operators due to various soft errors

    def execute_legacy_api(self, work_item):
        self.client._legacy_xcalar_api.setSession(self.session)
        try:
            ret = self.client._execute(work_item)
        except XcalarApiStatusException as ex:
            self.logger.error(ex, exc_info=True)
            raise
        finally:
            self.client._legacy_xcalar_api.setSession(None)
        return ret

    # Given a table, persist it's DF
    def persist_dataflow_from_table(self, table, dfname=None):
        df = self.get_df_from_table(table)
        if dfname is None:
            dfname = os.path.join(
                self.test_plan["df_persist_path"], "{}-{}.tar.gz".format(
                    self.test_plan["df_name_prefix"], uuid.uuid4()))
        else:
            dfname = "{}.tar.gz".format(dfname)
        return self.persist_dataflow(df, dfname)

    # Given a DF, persist it as regular tar.gz file
    def persist_dataflow(self, df, dfname=None):
        data = df.encode('utf-8')
        tarinfo = tarfile.TarInfo(DataflowFileName)
        tarinfo.size = len(data)
        if dfname is None:
            dfname = os.path.join(
                self.test_plan["df_persist_path"], "{}-{}.tar.gz".format(
                    self.test_plan["df_name_prefix"], uuid.uuid4()))
        tar = tarfile.open(name=dfname, mode='w:gz')
        tar.addfile(tarinfo=tarinfo, fileobj=io.BytesIO(data))
        tar.close()
        return dfname

    # Return DF json given a table
    def get_df_from_table(self, table):
        retina_dst = XcalarApiRetinaDstT()
        retina_dst.target = XcalarApiNamedInputT()
        retina_dst.target.name = table.name
        retina_dst.numColumns = len(table.columns)

        retina_dst.columns = []
        for col_name in table.columns:
            column = ExColumnNameT()
            column.name = col_name
            column.headerAlias = col_name
            retina_dst.columns.append(column)

        # Make Retina could fail because the shared UDF is busy and it's
        # comparison with Retina UDF failed, so try again.
        done = False
        while not done:
            try:
                # Make the retina
                rname = "{}-{}".format(self.test_plan["df_name_prefix"],
                                       uuid.uuid4())
                work_item = WorkItemMakeRetina(rname, [retina_dst], [])
                self.execute_legacy_api(work_item)
                done = True
            except XcalarApiStatusException as ex:
                self.logger.error(ex, exc_info=True)
                if ex.status == StatusT.StatusBusy:
                    time.sleep(1)
                else:
                    raise

        # Get the retina
        work_item = WorkItemGetRetinaJson(rname)
        out = self.execute_legacy_api(work_item)

        # Delete the retina made in the backend
        work_item = WorkItemDeleteRetina(rname)
        self.execute_legacy_api(work_item)
        return out.retinaJson

    # Persist dataflows based on tables in session
    def persist_dataflows(self):
        if not os.path.exists(self.test_plan["df_persist_path"]):
            return

        total_dfs_count = int(
            self.test_plan["limit_saved_dfs_in_teardown"] / self.num_workers())
        remainer = self.test_plan[
            "limit_saved_dfs_in_teardown"] % self.num_workers()
        if (remainer and (remainer >= self.tnum + 1)):
            total_dfs_count += 1
        self.logger.info("Persist dataflows: {}".format(total_dfs_count))

        dfs_count = 0
        tables = self.get_non_empty_tables()
        for tab in tables:
            if dfs_count >= total_dfs_count:
                break
            df = self.get_df_from_table(tab)
            dfname = self.persist_dataflow(df)
            self.logger.info("Persisted df name: {}, df: {}".format(
                dfname, df))
            dfs_count += 1

    def num_workers(self):
        return (self.test_plan["users_count"] *
                self.test_plan["sessions_per_user_count"])

    # Restore queries
    def restore_dataflows(self):
        if not self.test_operators.restore_dfs:
            return

        total_dfs_count = int(
            self.test_plan["limit_restored_dfs_in_setup"] / self.num_workers())
        remainer = self.test_plan[
            "limit_restored_dfs_in_setup"] % self.num_workers()
        if (remainer and (remainer >= self.tnum + 1)):
            total_dfs_count += 1
        self.logger.info("Restore dataflows: {}".format(total_dfs_count))

        dfs_count = 0
        for df in self.test_operators.dataflows[self.tnum]:
            if self.test_mgr.is_test_timeout():
                return
            if dfs_count >= total_dfs_count:
                break
            dst_table_name = "{}-{}".format(
                self.test_plan["table_name_prefix"], str(uuid.uuid4()))
            self.logger.info("Restore df: {}, table: {}".format(
                df, dst_table_name))
            retina_dict = json.loads(df)
            query_string = json.dumps(retina_dict['query'][:-1])
            columns_to_export = retina_dict['tables'][0]['columns']
            self.qgh.execute_query(
                query_string=query_string,
                table_name=dst_table_name,
                optimized=True,
                is_async=False,
                columns_to_export=columns_to_export)
            dfs_count += 1

    # Driver method for the worker
    def work(self):
        self.test_mgr.log_test_header("{}: {}".format(self.__class__.__name__,
                                                      self.tnum))

        # Create a bunch of source tables and scalars
        self.create_source_tables_and_scalars()

        # Restore dataflows
        self.restore_dataflows()

        # Issue individual operators in this session until timeout
        while not self.test_mgr.is_test_timeout():
            self.execute_rand_op()

        # Persist dataflows
        self.persist_dataflows()

        if self.barrier.wait() == 0:
            self.logger.info("{} past barrier".format(self.__class__.__name__))

        # Dump stats
        # XXX TODO Need to be more thorough about stats collection
        self.test_mgr.log_test_footer("{}: {} Stats: {}".format(
            self.__class__.__name__, self.tnum, self.ops_helper))

        return (True, {})

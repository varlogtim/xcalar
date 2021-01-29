#!/usr/bin/env python
#
# XCE test suite.
# 1. Create users parameterized by 'user_name_prefix' and 'user_count'.
#    This covers for multi-user config.
# 2. Create sessions per user parameterized by 'session_name_prefix' and
#    'session_count'. This covers for multi-session config per user.
# 3. Use 'dataset_seed' to pseudo-random generate Datasets with different
#    schemas and row count.
# 4. Use 'df_seed' to pseudo-random generate operators for dataflows.
# 5. Use 'duration' in seconds to specify test time in seconds. Upon expiry,
#    test cleans up after itself.
# 6. Use 'keep' flag to retain test state upon it's completion.
#
# XXX TODO
# 1. Legacy published table:
#    Add Legacy published table coverage. We need to maintain this as long
#    as this code is in production.
# 2. SQL:
#    Add SQL coverage? We could somehow integrate this with test_jdbc.py, so
#    we can get SQL coverage.
# 3. Control path scaling:
#    Includes Sessions, UDFs, Tables, Dataflows etc.
# 4. IMD:
#    IMD with diffent data verification strategies.
# 5. Shared tables:
#    Demonstrate shared tables lifecyle correctness.
# 6. Dataset load:
#    Various connectors, parsers, schemas (nested, array, object types).
#

import time
import logging
import os
import sys
import argparse
import time
import traceback
import json
import multiprocessing as mp
import multiprocessing.pool
import random
import signal

from xcalar.external.dataflow import Dataflow

from udf import type_map
from test_base import TestDriver, TestWorker
from test_shared_tables import TestSharedTables, TestWorkerSharedTables
from test_dataset_load import TestDatasetLoad, TestWorkerDatasetLoad
from test_imd import TestIMD, TestWorkerIMD
from test_legacy_published_tables import TestLegacyPublishedTables, TestWorkerLegacyPublish
from test_control_path_scaling import TestControlPathScaling, TestWorkerControlPathScaling
from test_operators import TestOperators, TestWorkerOperator
from test_operators import OperatorsTestError, OperatorsTestVerificationError, OperatorsTestRowCountMismatch

#
# Constants
#
TEST_DURATION_IN_SECS = 60 * 60

#
# Env
#
XLRDIR = os.environ['XLRDIR']

#
# Logger
#
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)s %(process)d %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False


#
# Exceptions
#
class XceTestError(Exception):
    pass


#
# Test plan
#
test_plan_default_dir = os.path.abspath(
    os.path.join(XLRDIR, 'src/bin/tests/xceTests/'))

# Tests
TEST_OPERATORS = "test_operators"
TEST_LEGACY_PUBLISHED_TABLES = "test_legacy_published_tables"
TEST_CONTROL_PATH_SCALING = "test_control_path_scaling"
TEST_IMD = "test_imd"
TEST_DATASET_LOAD = "test_dataset_load"
TEST_SHARED_TABLES = "test_shared_tables"
TEST_PLAN_KEY = "test_plan"
TEST_DRIVER_KEY = "test_driver"

TEST_CONFIG_MAP = {
    TEST_OPERATORS: {
        TEST_PLAN_KEY: 'OperatorsTestPlan.json',
        TEST_DRIVER_KEY: 'TestOperators'
    },
    TEST_LEGACY_PUBLISHED_TABLES: {
        TEST_PLAN_KEY: 'LegacyPublishedTablesTestPlan.json',
        TEST_DRIVER_KEY: 'TestLegacyPublishedTables'
    },
    TEST_CONTROL_PATH_SCALING: {
        TEST_PLAN_KEY: 'ControlPathScalingTestPlan.json',
        TEST_DRIVER_KEY: 'TestControlPathScaling'
    },
    TEST_IMD: {
        TEST_PLAN_KEY: 'ImdTestPlan.json',
        TEST_DRIVER_KEY: 'TestIMD'
    },
    TEST_DATASET_LOAD: {
        TEST_PLAN_KEY: 'DatasetLoadTestPlan.json',
        TEST_DRIVER_KEY: 'TestDatasetLoad'
    },
    TEST_SHARED_TABLES: {
        TEST_PLAN_KEY: 'SharedTablesTestPlan.json',
        TEST_DRIVER_KEY: 'TestSharedTables'
    },
}

#
# Targets
#
GeneratedTargetName = "memory"

#
# Arg parsing
#
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument(
    '--duration',
    dest='test_duration_in_sec',
    required=False,
    default=TEST_DURATION_IN_SECS,
    type=int,
    help='Test duration in seconds')
arg_parser.add_argument(
    '--dataset_seed',
    dest='test_dataset_seed',
    required=False,
    default=int(time.time()),
    type=int,
    help='Test dataset seed for random number generator')
arg_parser.add_argument(
    '--df_seed',
    dest='test_df_seed',
    required=False,
    default=int(time.time()),
    type=int,
    help='Test dataflow seed for random number generator')
# Test plan filename is TEST_PLAN_PATH
arg_parser.add_argument(
    '--test_plan_path',
    dest='test_plan_path',
    required=False,
    default=test_plan_default_dir,
    type=str,
    help='Path to JSON test plan directory')
arg_parser.add_argument(
    '--test_name',
    dest='test_name',
    required=False,
    default=TEST_OPERATORS,
    type=str,
    help="Here are the list names to choose from. [{}, {}, {}, {}, {}, {}]".
    format(TEST_OPERATORS, TEST_LEGACY_PUBLISHED_TABLES,
           TEST_CONTROL_PATH_SCALING, TEST_IMD, TEST_DATASET_LOAD,
           TEST_SHARED_TABLES))
arg_parser.add_argument(
    '--keep',
    dest='keep',
    required=False,
    action='store_true',
    help='Keep state after exit')
arg_parser.add_argument(
    '--uname',
    dest='uname',
    required=False,
    default="admin",
    type=str,
    help='Xcalar username')
arg_parser.add_argument(
    '--passwd',
    dest='password',
    required=False,
    default="admin",
    type=str,
    help='Xcalar password')
arg_parser.add_argument(
    '--apiport',
    dest='apiport',
    required=False,
    default=int(os.getenv('XCE_HTTPS_PORT','8443')),
    type=int,
    help='Xcalar API port (8443 for some dockers)')
arg_parser.add_argument(
    '--host',
    dest='host',
    required=False,
    default='localhost',
    type=str,
    help='hostname')

cliargs = arg_parser.parse_args()

#
# Exception white list
#
IGNORE_EXCEPTION = {}

#
# Signal handler
#
worker_pool = None
is_running = True


def worker_cleanup(signum, frame):
    logger.info("worker_cleanup called with signal: {}".format(signum))
    if worker_pool is not None:
        logger.info("worker_cleanup terminating workers")
        worker_pool.terminate()
        logger.info("worker_cleanup joining workers")
        worker_pool.join()
    logger.info("worker_cleanup exit")
    sys.exit()


def sigint_handler(signum, frame):
    global is_running

    logger.info("sigint_handler called")
    is_running = False
    raise KeyboardInterrupt


signal.signal(signal.SIGHUP, worker_cleanup)
signal.signal(signal.SIGTERM, worker_cleanup)


#
# To support cancel testing we need to run the subprocesses non-deamon so we're
# allowed to have them fork their own subprocesses.
#
class NdProcess(mp.Process):
    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)


class NdPool(multiprocessing.pool.Pool):
    Process = NdProcess


#
# Test driver class
#
class TestMgr():
    def __init__(self, test_plan, cliargs, logger):
        self.setup_class(test_plan, cliargs, logger)

    @classmethod
    def setup_class(cls, test_plan, cliargs, logger):
        cls.test_plan = test_plan
        cls.cliargs = cliargs
        cls.logger = logger
        cls.start_time = time.time()

    @classmethod
    def teardown_class(cls, success):
        pass

    def get_test_elapsed_time(self):
        return time.time() - self.start_time

    def is_test_timeout(self):
        return self.get_test_elapsed_time() > self.cliargs.test_duration_in_sec

    def log_test_header(self, name):
        logger.info("================= START {} ==============".format(name))

    def log_test_footer(self, name):
        logger.info("================= END {} ==============".format(name))

    def import_udfs(self, client, workbook, udfs):
        logger.info("Import UDFs")
        for udfArgs in udfs:
            name = udfArgs['name']
            logger.info("Adding UDF {}".format(name))
            with open(udfArgs['sourcePath'].replace("$XLRDIR", XLRDIR)) as f:
                source = f.read()
            if udfArgs.get('type', 'private') == 'public':
                try:
                    client.get_udf_module(name).delete()
                except Exception:
                    pass
                client.create_udf_module(name, source)
            else:
                workbook.create_udf_module(name, source)

    def load_udf_dataset(self, workbook, data_target, ds_name, import_udf,
                         path, parser_args):
        ds = workbook.build_dataset(
            ds_name,
            data_target,
            path,
            "udf",
            parser_name=import_udf,
            parser_args=parser_args)
        dataset = ds.load()
        return dataset

    # XXX TODO Add keys?
    def table_materialize_from_ds(self, client, session, dataset, schema,
                                  table_name):
        project_columns = []
        for col_name, col_type in schema["columns"].items():
            project_columns.append({
                'name': col_name,
                'rename': col_name,
                'type': col_type.lower()
            })
        logger.info(
            "Materialize table name: '{}', dataset: '{}', size in rows: {}, schema: '{}', userName: '{}', sessionName: '{}'"
            .format(table_name, dataset.name, dataset.record_count(),
                    project_columns, client.username, session.name))
        df = Dataflow.create_dataflow_from_dataset(
            client, dataset, project_columns=project_columns)
        session.execute_dataflow(
            df, is_async=False, table_name=table_name, optimized=True)
        return session.get_table(table_name)

    def tables_materialize_from_dss(self, client, session, datasets,
                                    dss_schemas, table_names):
        source_tables = []
        for (ds, schema, table_name) in zip(datasets, dss_schemas,
                                            table_names):
            col_count = len(schema["columns"])
            key_count = random.randint(1, col_count)
            schema["keys"] = self.get_random_keys_from_schema(
                schema, key_count)
            source_tables.append(
                test_mgr.table_materialize_from_ds(client, session, ds, schema,
                                                   table_name))
        return source_tables

    def dataset_gen(self, workbook, ds_size, schema, ds_name):
        logger.info(
            "Generate Datset name: '{}', records: {}, schema: {}".format(
                ds_name, ds_size, schema))
        parse_args = {
            "config": {
                "schema": schema,
                "seed": cliargs.test_dataset_seed
            }
        }
        udf_ds = self.load_udf_dataset(
            workbook, GeneratedTargetName, ds_name,
            test_plan['dataSource']['udf']['function'], str(ds_size),
            parse_args)
        return udf_ds

    def gen_random_schema(self, col_name_prefix, col_count, key_count=0):
        schema = {}
        schema["keys"] = []
        schema["columns"] = {}
        for ii in range(col_count):
            col_type = random.choice(list(type_map.TABLE_COL_TYPES_MAP.keys()))
            col_name = "{}-{}".format(col_name_prefix, ii)
            schema["columns"][col_name] = col_type
        if key_count:
            schema["keys"] = self.get_random_keys_from_schema(
                schema, key_count)
        return schema

    def get_random_keys_from_schema(self, schema, key_count):
        return list(random.sample(schema["columns"].keys(), key_count))

    def gen_random_datasets(self, ds_count, ds_min_rows, ds_max_rows, ds_names,
                            schema_min_columns, schema_max_columns,
                            col_name_prefix, workbook):
        dss = []
        schemas = []
        for ii in range(ds_count):
            ds_size = random.randint(ds_min_rows, ds_max_rows)
            col_count = random.randint(schema_min_columns, schema_max_columns)
            schema = self.gen_random_schema(col_name_prefix, col_count)
            dss.append(
                self.dataset_gen(workbook, ds_size, schema, ds_names[ii]))
            schemas.append(schema)
        return (dss, schemas)

    def worker_wrap(self, tnum, tclass, test_mgr, args):
        try:
            worker = tclass(test_mgr, tnum, args)
            ret = getattr(worker, "work")()
            worker.teardown_class(ret[0])
            return ret
        except Exception:
            traceback.print_exc()
            return (False, {})

    def start_worker_helper(self,
                            tclass,
                            num_workers,
                            args,
                            worker_timeout_sec,
                            non_demonic=False):
        if non_demonic:
            worker_pool = NdPool(processes=num_workers)
        else:
            worker_pool = multiprocessing.pool.Pool(processes=num_workers)

        args["barrier"] = multiprocessing.Manager().Barrier(
            num_workers, timeout=worker_timeout_sec)
        workers = []
        for i in range(num_workers):
            tnum = i
            workers.append(
                worker_pool.apply_async(self.worker_wrap,
                                        (tnum, tclass, self, args)))
            logger.info("Started worker tnum: {} tclass: {}".format(
                tnum, tclass.__name__))

        rets = [w.get(worker_timeout_sec) for w in workers]
        worker_pool.close()
        worker_pool.join()

        if not all([ret[0] for ret in rets]):
            raise XceTestError("Worker returned failed status")
        return rets


#
# Entry point to kick tests.
#
if __name__ == "__main__":
    status = 0
    test_mgr = None
    test_driver = None

    logger.info("CLI Args: '{}'".format(cliargs))

    # Scan Test plan
    with open(
            os.path.join(cliargs.test_plan_path,
                         TEST_CONFIG_MAP[cliargs.test_name][TEST_PLAN_KEY]),
            'r') as f:
        test_plan = json.load(f)
        logger.info("Test plan: '{}'".format(str(test_plan)))

    # Init random number gen
    random.seed(cliargs.test_df_seed)

    try:
        test_mgr = TestMgr(test_plan, cliargs, logger)
        test_driver = globals()[TEST_CONFIG_MAP[cliargs.test_name]
                                [TEST_DRIVER_KEY]](test_mgr)
        getattr(test_driver, "work")()
    except Exception:
        traceback.print_exc()
        status = 1
    finally:
        if test_driver is not None:
            test_driver.teardown_class(status == 0)
            test_driver = None
        if test_mgr is not None:
            test_mgr.teardown_class(status == 0)
            test_mgr = None
    sys.exit(status)

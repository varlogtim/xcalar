#!/usr/bin/env python

import logging
import tempfile
import os
import re
import sys
import argparse
import shutil
import time
import traceback
import json
import psycopg2
import multiprocessing as mp
import multiprocessing.pool
import uuid

from xcalar.compute.coretypes.OrderingEnums.constants import XcalarOrderingT
from xcalar.compute.coretypes.QueryStateEnums.ttypes import QueryStateT
from xcalar.compute.coretypes.Status.ttypes import StatusT

from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException

from xcalar.external.client import Client
from xcalar.external.dataflow import Dataflow
from xcalar.external.imd import IMDMetaStore

import xcalar.compute.util.imd.imd_constants as ImdConstant
from xcalar.external.exceptions import ImdGroupSnapshotException, IMDGroupException

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-5s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

XLRDIR = os.environ['XLRDIR']
plan_default_dir = os.path.abspath(
    os.path.join(XLRDIR, 'src/bin/tests/imdTests/'))

arg_parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
arg_parser.add_argument(
    '-p',
    dest='plan',
    required=False,
    default=plan_default_dir,
    type=str,
    help='Path to JSON test plan directory')
arg_parser.add_argument(
    '-t',
    dest='test',
    required=False,
    default=None,
    type=str,
    help='Run only specific suite (test_merge_txn_boundary)')
arg_parser.add_argument(
    '-u',
    dest='url',
    required=False,
    default="https://localhost:{}".format(os.getenv('XCE_HTTPS_PORT', '8443')),
    type=str,
    help='url of the Xcalar cluster')
arg_parser.add_argument(
    '--db',
    dest="db",
    help="what cube data to connect to",
    required=False,
    default="imd_test_db")
arg_parser.add_argument(
    '--dbHost',
    help="Host name of postgresql",
    required=False,
    dest="dbHost",
    default="localhost")
arg_parser.add_argument(
    '--dbPort',
    dest="dbPort",
    help="port number of postgresql",
    required=False,
    default=5432)
arg_parser.add_argument(
    '--dbUser',
    dest="dbUser",
    help="username of database",
    required=False,
    default="jenkins")
arg_parser.add_argument(
    '--dbPass',
    dest="dbPass",
    help="password of database",
    required=False,
    default="jenkins")
arg_parser.add_argument(
    '--restoreRate',
    dest="restoreRate",
    help="rate of restore imd group",
    required=False,
    default=0,
    type=int)
arg_parser.add_argument(
    '--snapshotRate',
    dest="snapshotRate",
    help="rate of snapshot imd group",
    required=False,
    default=0,
    type=int)
arg_parser.add_argument(
    '--validationRate',
    dest="validationRate",
    help="rate of validation of imd group with an external database",
    required=False,
    default=5,
    type=int)
arg_parser.add_argument(
    '--run_boundary_tests',
    dest='run_boundary_tests',
    required=False,
    action='store_true',
    help='do not run the boundry tests on the imd base tables')
arg_parser.add_argument(
    '--no_ext_db',
    dest='verify_with_db',
    required=False,
    action='store_false',
    help='do not compare results with external database(postgres)')
arg_parser.add_argument(
    '-K',
    dest='keep',
    required=False,
    action='store_true',
    help='Keep published tables after exit')

cliargs = arg_parser.parse_args()

GeneratedTargetName = "memory"
PGDBTargetName = "PGDB_Target"
DB_DATATYPE_MAP = {
    "integer": "integer",
    "string": "text",
    "float": "float8",
    "money": "decimal(34, 2)"
}

IGNORE_EXCEPTION = {
    ImdGroupSnapshotException("Group is inactive!"),
    ImdGroupSnapshotException("No new data to snapshot"),
    ImdGroupSnapshotException("No data to snapshot"),
    IMDGroupException(
        "cannot deactivate group 'test_imd_group', transaction in progress"),
    IMDGroupException(
        "cannot deactivate group 'test_imd_group', snapshot in progress")
}


# To support cancel testing we need to run the subprocesses non-deamon so we're
# allowed to have them fork their own subprocesses.
class ndProcess(mp.Process):
    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)


class ndPool(multiprocessing.pool.Pool):
    Process = ndProcess


class Util():
    @staticmethod
    def worker_wraper(target, test_mgr, args, kwds):
        try:
            worker = TestImdWorker(test_mgr)
            f = getattr(worker, target)
            return f(*args, **kwds)
        except Exception:
            traceback.print_exc()
            return (False, {})

    @staticmethod
    def convert_db_type(type_):
        return DB_DATATYPE_MAP.get(type_.lower(), "text")

    @staticmethod
    def ignore_exception(ex):
        for e in IGNORE_EXCEPTION:
            if type(e) == type(ex) and e.args == ex.args:
                return True
        return False


class TestMgr():
    def __init__(self):
        self.setup_class()

    @classmethod
    def setup_class(cls):
        cls.client = Client(url=cliargs.url)
        try:
            cls.client.get_workbook("sdk_imd_exec").delete()
        except Exception:
            pass
        cls.workbook = cls.client.create_workbook("sdk_imd_exec")
        cls.session = cls.workbook.activate()
        cls.imd_meta_store = IMDMetaStore(cls.session)
        cls.client.add_data_target(GeneratedTargetName, "memory", {})
        cls.db_conn = None

    @classmethod
    def teardown_class(cls, success):
        if cls.db_conn and success:
            cur = cls.db_conn.cursor()
            try:
                cur.execute("DROP OWNED BY {};".format(cliargs.dbUser))
            except Exception:
                pass
            finally:
                cur.close()
                cls.db_conn.close()
                cls.db_conn = None
        for ds in cls.client.list_datasets():
            try:
                ds.delete()
            except Exception:
                pass
        if success and not cliargs.keep:
            cls.session.destroy()
            cls.workbook.delete()

    def set_log_header(self, name):
        logger.info("================= START {} ==============".format(name))

    def set_log_footer(self, name):
        logger.info("================= END {} ==============".format(name))

    def import_udfs(self, udfs):
        for udfArgs in udfs:
            name = udfArgs['name']
            logger.info("Adding UDF {}".format(name))
            with open(udfArgs['sourcePath'].replace("$XLRDIR", XLRDIR)) as f:
                source = f.read()
            if udfArgs.get('type', 'private') == 'public':
                try:
                    self.client.get_udf_module(name).delete()
                except Exception:
                    pass
                self.client.create_udf_module(name, source)
            else:
                self.workbook.create_udf_module(name, source)

    def reset_db(self, tables):
        cur = self.db_conn.cursor()
        try:
            for table in tables:
                cur.execute("DROP TABLE IF EXISTS {};".format(table))
        finally:
            cur.close()
        self.db_conn.commit()

    def prepare_imd_group(self, name, path_to_back_store, schema, tables):
        imd_tables = []
        try:
            self.session.get_imd_table_group(name).delete_group()
        except Exception:
            pass
        for idx, table in enumerate(tables):
            imd_table = {}
            imd_table['table_name'] = table
            pk = "PK{}".format(idx)
            pk_type = schema[idx]['type']
            imd_table['primary_keys'] = [pk]
            imd_table['schema'] = [{"name": pk, "type": pk_type}] + schema[:]
            imd_tables.append(imd_table)
        return self.session.create_imd_table_group(name, imd_tables,
                                                   path_to_back_store)

    @classmethod
    def postgresq_conn(cls):
        # create posgresql db connection
        try:
            cls.db_conn = psycopg2.connect(
                user=cliargs.dbUser,
                password=cliargs.dbPass,
                host=cliargs.
                dbHost,    # Might need to connect to remote PostgreSQL
                port=cliargs.dbPort,
                database=cliargs.db)
        except psycopg2.Error as e:
            raise RuntimeError("Error while connection to PostgreSQL", e)
        # create postgres db target
        params = {
            "dbname": cliargs.db,
            "dbtype": "PG",
            "host": cliargs.dbHost,
            "port": cliargs.dbPort,
            "psw_arguments": cliargs.dbPass,
            "psw_provider": 'plaintext',
            'uid': cliargs.dbUser,
            'auth_mode': 'None'
        }
        cls.client.add_data_target(PGDBTargetName, "dsn", params)

    def prepare_db_tables(self, schema, tables):
        self.reset_db(tables)
        cur = self.db_conn.cursor()
        try:
            for idx, table in enumerate(tables):
                pk = "PK{}".format(idx)
                pk_type = schema[idx]['type']
                statement = """
                    create table {table_name} ({cols}, PRIMARY KEY ({pk}));
                """.format(
                    table_name=table,
                    cols=', '.join([
                        "{} {}".format(s['name'],
                                       Util.convert_db_type(s['type']))
                        for s in schema[:] + [{
                            "name": pk,
                            "type": pk_type
                        }]
                    ]),
                    pk=pk)
                cur.execute(statement)
        finally:
            self.db_conn.commit()
            cur.close()

    def run_merge_txn_boundary_tests(self, test_case, test_timeout):
        update_size = int(
            os.environ.get("UPDATE_SIZE", None) or test_case['updateSize'])
        iteration = int(
            os.environ.get('ITERATION', None) or test_case['testIteration'])
        tables = test_case['tables']
        schema = test_case['schema']
        rules = test_case['rules']
        if len(tables) != len(schema):
            return (False, {
                'details':
                    "The length of the tables is not same as schema in the test plan file"
            })
        verify_with_db = cliargs.verify_with_db
        boundary_queries = test_case.get('boundary_queries', [])
        back_store = test_case.get("backStore", tempfile.mkdtemp())
        logger.info("back store is: {}".format(back_store))
        try:
            imd_group = self.prepare_imd_group("test_imd_group", back_store,
                                               schema, tables)
            if verify_with_db:
                self.postgresq_conn()
                self.prepare_db_tables(schema, tables)
        except Exception as e:
            shutil.rmtree(back_store)
            traceback.print_exc()
            return (False, {"details": str(e)})

        success = True
        manager = mp.Manager()
        lock = manager.Lock()
        exits = manager.Event()
        retry_count = 10
        workers = []
        workers.append({
            "name":
                "merge",
            "args": (iteration, imd_group, test_case, update_size, rules,
                     schema, tables, lock, exits, verify_with_db)
        })
        if cliargs.restoreRate > 0:
            workers.append({
                "name":
                    "restore",
                "args": (iteration, imd_group, schema, tables, lock, exits,
                         verify_with_db)
            })
            if cliargs.snapshotRate > 0:
                workers.append({
                    "name": "snapshot",
                    "args": (iteration, imd_group, exits)
                })
        if cliargs.run_boundary_tests:
            workers.append({
                "name": "verify_boundary_queries",
                "args": (boundary_queries, retry_count, exits)
            })

        worker_names = [
            "merge", "restore", "snapshot", "verify_boundary_queries"
        ]
        try:
            pool = ndPool(processes=len(workers))
            test_workers = []
            for worker_info in workers:
                worker_process = pool.apply_async(
                    Util.worker_wraper, (worker_info.get("name"), self,
                                         worker_info.get("args", ()), {}))
                test_workers.append(worker_process)
            worker_rets = [w.get(test_timeout) for w in test_workers]
            worker_runs = [ret[1].get('run', 0) for ret in worker_rets]
            for idx, ret in enumerate(worker_rets):
                if not ret[0]:
                    err_details = ret[1].get('details')
                    logger.error(err_details)
                    raise RuntimeError(
                        "{} worker returned failed status, with msg {}".format(
                            worker_names[idx], err_details))
        except Exception as e:
            success = False
            traceback.print_exc()
            return (False, {"details": str(e)})
        finally:
            logger.info('============Test Status==============')
            logger.info(
                '{} iterations of merge in this test case'.format(iteration))
            logger.info('{} update size / iteration'.format(update_size))
            for idx, worker_run in enumerate(worker_runs):
                logger.info('{} worker ran -> {} iterations'.format(
                    worker_run, worker_names[idx]))
            logger.info('=====================================')
            pool.close()
            pool.join()
            logger.info(
                "==========run_merge_txn_boundary_tests success {}=============="
                .format(success))
            if success:
                # XXX SDK-864 create a new Client object to retrieve a new session
                # For more info, see https://github.com/psf/requests/issues/4323
                client = Client(url=cliargs.url)
                session = client.get_workbook("sdk_imd_exec").activate()
                imd_group = session.get_imd_table_group("test_imd_group")
                imd_group.delete_group()
                logger.info("========imd group deleted========")
                shutil.rmtree(back_store)
                logger.info("========back_store path removed========")
        return (True, {})


class TestImd(TestMgr):
    def test_merge_txn_boundary(self, test_timeout):
        with open(os.path.join(cliargs.plan, "IMDTestPlan.json"), 'r') as f:
            test_plan = json.load(f)
        test_group = test_plan["merge_txn_tests"]

        if 'udfs' in test_group:
            self.import_udfs(test_group['udfs'])

        for name, test_case in test_group['testCases'].items():
            self.set_log_header("IMD Merge and Txn Boundary: " + name)
            ret = self.run_merge_txn_boundary_tests(test_case, test_timeout)
            if not ret[0]:
                return ret
            self.set_log_footer("IMD Merge and Txn Boundary: " + name)
        return (True, {})


class TestImdWorker():
    def __init__(self, test_mgr):
        # Can't inherit the client object from the test_mgr.
        # Cause we can't share the request.session across multiple processes.
        # For more info, see https://github.com/psf/requests/issues/4323
        self.client = Client(url=cliargs.url)
        self.workbook = self.client.get_workbook(test_mgr.workbook.name)
        self.session = self.workbook.activate()
        self.imd_meta_store = test_mgr.imd_meta_store
        legacy_xcalar_api = XcalarApi(auth_instance=self.client.auth)
        legacy_xcalar_api.setSession(self.session)
        self._operator_obj = Operators(legacy_xcalar_api)

    def get_table_from_db(self, table, project_columns):
        ds_name = "db_table_ds"
        import_udf = 'default:ingestFromDatabase'
        args = {"query": "select * from {}".format(table)}
        path = '/db'
        try:
            db_ds = self.load_udf_dataset(PGDBTargetName, ds_name, import_udf,
                                          path, args)
            df = Dataflow.create_dataflow_from_dataset(
                self.client, db_ds, project_columns=project_columns)
            table_name = "{}_{}".format(ds_name, table)
            self.session.execute_dataflow(
                df, is_async=False, table_name=table_name)
            return self.session.get_table(table_name)
        finally:
            try:
                db_ds.delete()
            except Exception:
                pass

    def validate_data(self, imd_group, schema, tables):
        tables_delete = []
        try:
            for idx, table in enumerate(tables):
                logger.info(
                    '--> Merge: verifing the correctness of IMD table: {}'.
                    format(table))
                pk = "PK{}".format(idx)
                pk_type = schema[idx]['type']
                project_columns = [{
                    'name': pk.lower(),
                    'rename': pk.upper(),
                    'type': pk_type.lower()
                }]
                for s in schema:
                    col = {
                        'name': s['name'].lower(),
                        'rename': s['name'].upper(),
                        'type': s['type'].lower()
                    }
                    project_columns.append(col)
                db_table = self.get_table_from_db(table, project_columns)
                imd_table = imd_group.get_table(table)

                assert db_table.record_count() == imd_table.record_count()

                df_db = Dataflow.create_dataflow_from_table(
                    self.client, db_table)
                df_imd = Dataflow.create_dataflow_from_table(
                    self.client, imd_table)
                float_round_evals = []
                for s in schema:
                    if s['type'].lower() == "float":
                        float_round_evals.append(
                            ('round({}, 3)'.format(s['name']), s['name']))

                if float_round_evals:
                    df_db = df_db.map(float_round_evals)
                    df_imd = df_imd.map(float_round_evals)

                columns = [[{
                    'name': pk.upper(),
                    'type': pk_type
                }] + [{
                    'name': s['name'].upper(),
                    'type': s['type']
                } for s in schema], [{
                    'name': pk,
                    'type': pk_type
                }] + schema[:]]
                new_columns = [pk + "_except"] + [
                    col + '_except' for col in [s['name'] for s in schema]
                ]
                df = Dataflow.except_([df_db, df_imd], columns, new_columns)
                table_name = "{}_except_validate".format(table)
                self.session.execute_dataflow(
                    df, table_name=table_name, is_async=False)
                validate_table = self.session.get_table(table_name)

                assert validate_table.record_count() == 0, validate_table.show(
                )
                tables_delete.extend([db_table, validate_table])
        except AssertionError as e:
            logger.error("Validation failed!")
            traceback.print_exc()
            return False
        except Exception:
            raise
        finally:
            for t in tables_delete:
                t.drop(delete_completely=True)
        return True

    def generate_delta_table(self, dataset, schema, rules, tables):
        project_columns = []
        keys_columns = []
        for idx, col in enumerate(schema):
            pk = "PK{}".format(idx)
            pk_type = schema[idx]['type'].lower()
            keys_columns.append({'name': pk, 'rename': pk, 'type': pk_type})
            project_columns.append({
                'name': col['name'],
                'rename': col['name'],
                'type': col['type'].lower()
            })
        project_columns = keys_columns + project_columns
        project_columns.append({'name': ImdConstant.Opcode, 'type': 'integer'})
        project_columns.append({
            'name': ImdConstant.Rankover,
            'type': 'integer'
        })
        df0 = Dataflow.create_dataflow_from_dataset(
            self.client, dataset, project_columns=project_columns)
        dfs = []

        delta_table_names = []
        for idx in range(len(tables)):
            pk = "PK{}".format(idx)
            columns = [pk] + [s['name'] for s in schema]
            columns.extend([ImdConstant.Opcode, ImdConstant.Rankover])
            df = df0.project(columns)
            imd_table_name = tables[idx]

            # Round up float type primary key
            if schema[idx]['type'].lower() == 'float':
                df = df.map([('round({}, 1)'.format(pk), pk)])

            # Generate map evals based on rules
            if rules and imd_table_name in rules:
                rule = rules[imd_table_name]
                evals_list = []
                for col_name, conversion in rule.items():
                    evals_list.append((conversion, col_name))
                df = df.map(evals_list)

            index_keys = [{
                "name": pk,
                "ordering": XcalarOrderingT.XcalarOrderingPartialAscending
            }]
            delta_table_names.append("imd_test_delta_table{}".format(idx))
            dfs.append(
                df.custom_sort(
                    index_keys, result_table_name=delta_table_names[-1]))

        df = dfs[0]
        df._merge_dataflows(dfs[1:])
        self.session.execute_dataflow(df, is_async=False)
        delta_tables = [
            self.session.get_table(delta_name)
            for delta_name in delta_table_names
        ]
        return delta_tables

    def load_udf_dataset(self, data_target, name, import_udf, path,
                         parser_args):
        db = self.workbook.build_dataset(
            name,
            data_target,
            path,
            "udf",
            parser_name=import_udf,
            parser_args=parser_args)
        dataset = db.load()
        return dataset

    def insert_into_db(self, tables, delta_tables):
        tmp_str = str(uuid.uuid4()).replace('-', '_')
        queries = []
        for idx, t in enumerate(delta_tables):
            pk = "PK{}".format(idx)
            aggr_info = [{
                "operator": "max",
                "aggColName": ImdConstant.Rankover,
                "newColName": ImdConstant.Rankover + "_tmp",
                "isDistinct": False
            }]
            df = Dataflow.create_dataflow_from_table(self.client, t).groupby(
                aggr_info, [pk, ImdConstant.Rankover],
                include_sample_columns=True).project(t.columns)

            driver_params = {
                "db_user": cliargs.dbUser,
                "db_pass": cliargs.dbPass,
                "db_host": cliargs.dbHost,
                "db_port": cliargs.dbPort,
                "db": cliargs.db,
                "db_table": tables[idx],
                "db_pk": pk
            }

            export_tab_name = "{}_export_tab".format(df.final_table_name)
            export_op = {
                'operation': 'XcalarApiExport',
                'args': {
                    'source':
                        df.final_table_name,
                    'dest':
                        export_tab_name,
                    'columns': [{
                        'columnName': col,
                        'headerName': col
                    } for col in t.columns],
                    'driverName':
                        'pg_imd',
                    'driverParams':
                        json.dumps(driver_params)
                },
                'comment': 'db export',
                'state': 'Dropped'
            }
            df._query_list[-1]['state'] = 'Dropped'
            df._final_table_name = export_tab_name
            df._query_list.append(export_op)
            df._query_list.pop(0)
            df._query_list[0]['args']['source'] = t.name
            queryName = str(uuid.uuid4()).replace('-', '_')
            queries.append(
                self.session.execute_dataflow(
                    df, query_name=queryName, is_async=True))

        # wait for queries to complete
        for query in queries:
            try:
                query_output = self.client._legacy_xcalar_api.waitForQuery(
                    query)
                if query_output.queryState == QueryStateT.qrError or query_output.queryState == QueryStateT.qrCancelled:
                    raise XcalarApiStatusException(query_output.queryStatus,
                                                   query_output)
            except XcalarApiStatusException as ex:
                if ex.status not in [
                        StatusT.StatusQrQueryNotExist,
                        StatusT.StatusQrQueryAlreadyDeleted
                ]:
                    raise ex

    def data_preparation(self, idx, test_case, update_size, schema, tables,
                         rules):
        ds_name = "test_ds_{}".format(idx)
        parse_args = {"config": {"schema": schema}}
        udf_ds = self.load_udf_dataset(
            GeneratedTargetName,
            ds_name, test_case['dataSource']['udf']['function'],
            str(update_size), parse_args)
        delta_tables = self.generate_delta_table(udf_ds, schema, rules, tables)

        return (udf_ds, delta_tables)

    def restore(self,
                iters,
                imd_group,
                schema,
                tables,
                match_database=True,
                lock=None,
                exits=None):
        run = 0
        logger.info('--> Restore worker: Started..')
        while True:
            if exits and exits.is_set():
                return (True, {"run": run})
            info_ = imd_group.info_
            idx = info_[ImdConstant.Current_txn_counter]
            if idx > iters:
                return (True, {"run": run})
            if idx % cliargs.restoreRate != 0:
                continue

            if lock:
                lock.acquire()
            try:
                if exits and exits.is_set():
                    return (True, {"run": run})
                logger.info('--> Restore: IMD restore at imd counter@{} <--'.
                            format(idx))
                imd_group.deactivate()
                begin = time.time()
                imd_group.restore()
                end = time.time()
                logger.info('--> Restore: IMD finishes restoring within {}s'.
                            format(end - begin))
                if match_database and not self.validate_data(
                        imd_group, schema, tables):
                    raise ValueError("Restore Error!")
                run += 1
            except Exception as e:
                if Util.ignore_exception(e):
                    time.sleep(1)
                else:
                    traceback.print_exc()
                    if exits:
                        exits.set()
                    return (False, {'details': str(e), "run": run})
            finally:
                try:
                    lock.release()
                except Exception:
                    pass

    def snapshot(self, iters, imd_group, exits=None):
        run = 0
        retry = False
        logger.info('--> Snapshot worker: Started..')
        while True:
            try:
                if exits and exits.is_set():
                    return (True, {'run': run})

                info_ = imd_group.info_
                idx = info_[ImdConstant.Current_txn_counter]
                if idx > iters:
                    return (True, {'run': run})
                if idx % (cliargs.snapshotRate * (run + 1)) != 0:
                    continue
                if retry:
                    logger.info(
                        '--> Snapshot: retrying for counter@{}..'.format(idx))
                else:
                    logger.info(
                        '--> Snapshot: IMD snapshot at imd counter@{} <--'.
                        format(idx))
                begin = time.time()
                imd_group.snapshot()
                end = time.time()
                logger.info(
                    '--> Snapshot: IMD finishes snapshoting within {}s\n'.
                    format(end - begin))
                run += 1
                retry = False
            except Exception as e:
                if Util.ignore_exception(e):
                    logger.info(
                        '--> Snapshot: Failed with {}, will retry..'.format(e))
                    time.sleep(10)
                    retry = True
                    continue
                traceback.print_exc()
                if exits:
                    exits.set()
                return (False, {'details': str(e), 'run': run})

    def merge(self,
              iters,
              imd_group,
              test_case,
              update_size,
              rules,
              schema,
              tables,
              lock=None,
              exits=None,
              update_db=True):
        run = 0
        num_rows = update_size * 100
        # if restore process is not running just don't persist deltas as they won't be used
        persist_deltas = False
        if cliargs.restoreRate > 0:
            persist_deltas = True
        for idx in range(1, iters):
            if lock:
                lock.acquire()
            try:
                if exits and exits.is_set():
                    return (True, {'run': run})
                logger.info('--> IMD Merge iteration@{} <--'.format(idx))
                logger.info(
                    '--> Loading deltas for tables {} with num rows {} each'.
                    format(list(tables), num_rows))
                begin = time.time()
                udf_ds, delta_tables = self.data_preparation(
                    idx, test_case, num_rows, schema, tables, rules)
                end = time.time()
                logger.info('--> Done in {}s\n'.format(end - begin))
                # IMD Merge
                logger.info('--> Merging deltas..')
                delta_tables_info = {}
                for table, delta_table in zip(tables, delta_tables):
                    delta_tables_info[table] = delta_table.name
                begin = time.time()
                imd_group.merge(
                    delta_tables_info, persist_deltas=persist_deltas)
                end = time.time()
                num_rows = update_size
                logger.info(
                    '--> Merge: IMD finishes merging within {}s'.format(end -
                                                                        begin))
                if update_db:
                    # DB merge
                    logger.info(
                        '--> Merge: DB Merge iteration@{} <--'.format(idx))
                    begin = time.time()
                    self.insert_into_db(tables, delta_tables)
                    end = time.time()
                    logger.info('--> Merge: DB finishes merging within {}s\n'.
                                format(end - begin))

                    if (idx % cliargs.validationRate == 0 or idx == iters - 1):
                        begin = time.time()
                        logger.info(
                            '--> Merge: Data validation of iter@{} with database begin'
                            .format(idx))
                        if not self.validate_data(imd_group, schema, tables):
                            raise ValueError("Merge Error!")
                        end = time.time()
                        logger.info('--> Merge: Data validation done in {}s\n'.
                                    format(end - begin))
                for t in delta_tables:
                    t.drop(delete_completely=True)
                udf_ds.delete()
                run += 1
            except Exception as e:
                if Util.ignore_exception(e):
                    time.sleep(1)
                else:
                    traceback.print_exc()
                    if exits:
                        exits.set()
                    return (False, {"details": str(e), "run": run})
            finally:
                try:
                    lock.release()
                except Exception:
                    pass
        if exits:
            exits.set()
        return (True, {"run": run})

    def execute_xc_sql_query(self, sql_query):
        xc_session = self.session
        table_name, ordered_columns = xc_session.execute_sql(
            sql_query, get_ordered_columns=True)

        table = xc_session.get_table(table_name)
        return table, ordered_columns

    def get_back_xcalar_tables(self, imd_tables, retry_count=10):
        imd_group_map = {}
        for imd_tab in imd_tables:
            src_table_name = imd_tab.upper()
            tab_info = self.imd_meta_store.get_table_info(
                imd_table_name=src_table_name)
            group_name = self.imd_meta_store._get_group_info_by_id(
                group_id=tab_info[ImdConstant.Group_id])[ImdConstant.
                                                         Group_name]
            imd_group_map.setdefault(group_name, set()).add(
                tab_info[ImdConstant.Table_name])

        back_tables_map = {}
        for group_name, tables in imd_group_map.items():
            # check if group is active
            imd_group_obj = self.session.get_imd_table_group(group_name)
            retries = 0
            while not imd_group_obj.is_active() and retries < retry_count:
                logger.info("--> Boundary test: Group {} is inactive..".format(
                    group_name))
                time.sleep(10)
                retries += 1
            group_back_tables = self.imd_meta_store.get_back_xcalar_tables(
                group_name, table_names=tables)
            back_tables_map = {**back_tables_map, **group_back_tables}

        return back_tables_map

    def verify_boundry_test_query(self, query, retry_count, exits):
        sql_tables = re.findall(r"{(\w+)}", query)
        if not sql_tables:
            logger.warning("No imd tables found in the sql query")
        retries = 0
        table = None
        while retries < retry_count:
            if exits and exits.is_set():
                return
            try:
                modified_query = query
                if sql_tables:
                    back_xcalar_tables_map = self.get_back_xcalar_tables(
                        sql_tables, retry_count)
                    modified_query = query.format(**back_xcalar_tables_map)
                table, _ = self.execute_xc_sql_query(modified_query)
            except Exception as ex:
                retries += 1
                if retries == retry_count:
                    msg = 'query {}, exceeded retry count {}'.format(
                        query, retry_count)
                    logger.error('--> {}'.format(msg))
                    raise RuntimeError(msg)
                logger.info('--> Error {}..retrying..'.format(str(ex)))
                continue

            if table.record_count() != 0:
                table.show()
                msg = 'query {}, failed, returned {} records instead of 0'.format(
                    query, table.record_count())
                logger.error('--> {}'.format(msg))
                raise RuntimeError(msg)
            else:
                table.drop(delete_completely=True)
            break

    def verify_boundary_queries(self, queries, retry_count=10, exits=None):
        logger.info('--> IMD Boundary verification started <--')
        run = 1
        while len(queries) > 0:
            for query in queries:
                logger.info(
                    '--> Boundary test run {}: verifying query {}\n'.format(
                        run, query))
                try:
                    if exits and exits.is_set():
                        return (True, {'run': run})
                    self.verify_boundry_test_query(query, retry_count, exits)
                except Exception as ex:
                    if exits:
                        exits.set()
                    return (False, {'details': str(ex), 'run': run})
            run += 1
        return (True, {'run': run})


if __name__ == "__main__":
    status = 0
    setup = None
    try:
        pool = None
        setup = TestImd()
        if cliargs.test:
            getattr(setup, cliargs.test)()
        else:
            timeout = int(os.environ.get("IMDTEST_TIMEOUT", None) or 100000)
            pool = ndPool(processes=1)
            workers = [
                pool.apply_async(setup.test_merge_txn_boundary, (timeout, )),
            ]
            rets = [w.get(timeout) for w in workers]
            for idx, ret in enumerate(rets):
                if not ret[0]:
                    err_details = ret[1].get('details')
                    logger.error(err_details)
                    raise RuntimeError(
                        "Worker returned failed status, with msg '{}'".format(
                            err_details))
            if not all([ret[0] for ret in rets]):
                raise RuntimeError("Worker returned failed status")
    except Exception:
        traceback.print_exc()
        status = 1
    finally:
        if pool:
            pool.close()
            pool.join()
        if setup:
            setup.teardown_class(status == 0)
    sys.exit(status)

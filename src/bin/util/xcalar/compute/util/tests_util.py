import json
import os
import tarfile
from decimal import Decimal
import datetime
import re
import subprocess
import time
import random
import logging
from IPython.utils.io import capture_output
import sys

from xcalar.external.result_set import ResultSet as RS
from xcalar.external.dataflow import Dataflow
import xcalar.compute.util.imd.imd_constants as ImdConstant
from xcalar.compute.util.utils import XcUtil
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.LegacyApi.XcalarApi import XcalarApi

logger = logging.getLogger("xcalar")


class JdbcTestError(Exception):
    pass


class JdbcTestVerificationError(JdbcTestError):
    pass


class JdbcTestSparkVerificationError(JdbcTestError):
    pass


class JdbcTestSDKVerificationError(JdbcTestError):
    pass


class JdbcTestSharedVerificationError(JdbcTestError):
    pass


def noex(f, *args):
    try:
        f(*args)
    except Exception:
        pass


class Util(object):
    # java numeric type constructors require a numeric python type
    @staticmethod
    def coerce(v):
        if type(v) == bool:
            return v
        if v == "false":
            return False
        if v == "true":
            return True
        try:
            return int(str(v))
        except Exception:
            try:
                return float(str(v))
            except Exception:
                return v

    @staticmethod
    def coerceDt(v):
        try:
            dt = datetime.datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%fZ")
        except Exception:
            try:
                dt = datetime.datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                try:
                    dt = datetime.datetime.strptime(v, "%Y-%m-%d %H:%M:%S.%f")
                except Exception:
                    dt = datetime.datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
        return (dt.isoformat())

    @staticmethod
    def iseq(testCol, ansCol, floatTolerance=0.0200001):
        # Increase float tolearance to fit the deciaml precision issue
        # for sdk test verification
        if testCol is None:
            return ansCol is None

        try:
            ansColCoerced = Util.coerceDt(ansCol)
            testCol = Util.coerceDt(testCol)
        except Exception:

            try:
                ansColCoerced = type(testCol)(Util.coerce(ansCol))
            except ValueError:
                ansColCoerced = Util.coerce(ansCol)
                testCol = type(ansCol)(Util.coerce(testCol))

        if testCol != ansColCoerced:
            try:
                diff = abs(testCol - ansColCoerced)
                if diff > floatTolerance:
                    return False
            except Exception:
                return False
        return True

    @staticmethod
    def dumpRows(dbName, qname, rows):
        with open("rows-{}-{}.txt".format(dbName, qname), 'w') as fh:
            fh.write('\n'.join([str(x) for x in rows]))
            fh.write('\n')

    @staticmethod
    def formatResultSet(rows, schema):
        xlrToPythonMap = {
            "int": int,
            "float": float,
            "numeric": Decimal,
            "money": Decimal,
            "DfInt64": int,
            "DfFloat64": float,
            "DfMoney": Decimal
        }
        excludeList = {
            'XCALAROPCODE', 'XCALARRANKOVER', 'XCALARBATCHID', 'XCALARROWNUM'
        }
        ret = []
        if isinstance(rows, RS):
            rows = rows.record_iterator()
        for row in rows:
            r = []
            for key in schema:
                # assert key in row
                """ We comment the assertation out because for some row
                in a result_set, some field may be FNF, which will not
                show up in result_set. We will need to find a way to deal
                with FNF field
                """
                if any(key.startswith(k) for k in excludeList):
                    continue
                elif key in row:
                    r.append(xlrToPythonMap.get(schema[key], str)(row[key]))
                else:
                    r.append(None)
            ret.append(r)
        return ret

    @staticmethod
    def logWarning(logger, msg, tnumStr):
        logger.warning(tnumStr + ' ' + str(msg))

    @staticmethod
    def verifyAnswer(qname,
                     rows,
                     answer,
                     opts,
                     sdkVerify=False,
                     logger=None,
                     tnumStr="",
                     shared_verify=False):
        if 'numOfRows' in answer:
            numRowsStr = re.sub(r",", r"", answer['numOfRows'])
            expectedRows = int(numRowsStr)
            if len(rows) != expectedRows:
                failedRowStr = str(len(rows)) + " != " + str(expectedRows)
                failStr = "Rowcount Mismatch: {}: {}".format(
                    qname, failedRowStr)
                if opts['enforce']:
                    if sdkVerify:
                        raise JdbcTestSDKVerificationError(failStr)
                    elif shared_verify:
                        raise JdbcTestSharedVerificationError(failStr)
                    else:
                        raise JdbcTestVerificationError(failStr)
                else:
                    Util.logWarning(logger, "UNENFORCED " + failStr, tnumStr)

        if 'rowVerificationFn' in answer:
            for row in rows:
                valid = eval(answer['rowVerificationFn'])
                if not valid:
                    raise JdbcTestVerificationError(
                        "row: {} failed verification fn: {}".format(
                            row, answer['rowVerificationFn']))

        if opts['resort']:
            # Some queries fail to verify due to sort stability issues, so resort
            # them here prior to verification
            # XXX: answer will have to be adjusted to match sort order
            rows = sorted(rows, key=lambda x: str(x))

        for (k, v) in answer.items():
            m = re.match(r"row(\d+)", k)
            if m:
                rowIdx = int(m.group(1))
                for (testCol, ansCol) in zip(rows[rowIdx], v):
                    if not Util.iseq(testCol, ansCol):
                        failedElmStr = str(testCol) + " != " + str(ansCol)
                        failedRowStr = str(rows[rowIdx]) + " != " + str(v)
                        if sdkVerify:
                            failStr = "FAILED SDK verification: {}: {}: Row: {}".format(
                                qname, failedElmStr, failedRowStr)
                        elif shared_verify:
                            failStr = "FAILED shared table verification: {}: {}: Row: {}".format(
                                qname, failedElmStr, failedRowStr)
                        else:
                            failStr = "FAILED verification: {}: {}: Row: {}".format(
                                qname, failedElmStr, failedRowStr)
                        if opts['enforce']:
                            Util.logWarning(logger, failStr, tnumStr)
                            if sdkVerify:
                                raise JdbcTestSDKVerificationError(failStr)
                            elif shared_verify:
                                raise JdbcTestSharedVerificationError(failStr)
                            else:
                                raise JdbcTestVerificationError(failedElmStr)
                        else:
                            Util.logWarning(logger, "UNENFORCED " + failStr,
                                            tnumStr)

    @staticmethod
    def getQueryOpts(optDict):
        opts = {'enable': True, 'enforce': True, 'resort': False}
        if optDict:
            opts['enable'] = optDict.get('enable', opts['enable'])
            opts['enforce'] = optDict.get('enforce', opts['enforce'])
            opts['resort'] = optDict.get('resort', opts['resort'])
        return opts


class TpcTestUtil(object):
    @staticmethod
    def build_optimized_dataflow(client,
                                 table_name,
                                 target_name,
                                 path,
                                 table_info,
                                 parser_name,
                                 parser_args,
                                 custom_index=False):
        prefix = "p"
        schema = table_info["schema"]
        schema_json_array = []
        export_schema_array = []
        pkeys = table_info.get("primary_keys", [])
        for col in schema:
            col_info = {
                'sourceColumn':
                    col[ImdConstant.Col_name],
                'destColumn':
                    col[ImdConstant.Col_name],
                'columnType':
                    ImdConstant.Table_col_types_map[
                        col[ImdConstant.Col_type].upper()]
            }
            schema_json_array.append(col_info)
            export_schema_array.append({
                'columnName':
                    "{}::{}".format(prefix, col[ImdConstant.Col_name]),
                'headerAlias':
                    col[ImdConstant.Col_name]
            })

        load_query = {
            'operation': 'XcalarApiBulkLoad',
            'comment': '',
            'tag': '',
            'args': {
                'dest': '.XcalarDS.Optimized.ds',
                'loadArgs': {
                    'sourceArgsList': [{
                        'targetName': target_name,
                        'path': path,
                        'fileNamePattern': '',
                        'recursive': True
                    }],
                    'parseArgs': {
                        'parserFnName': parser_name,
                        'parserArgJson': json.dumps(parser_args),
                        'fileNameFieldName': '',
                        'recordNumFieldName': '',
                        'allowFileErrors': False,
                        'allowRecordErrors': False,
                        'schema': schema_json_array
                    },
                    'size':
                        10737418240
                }
            },
            'annotations': {}
        }

        if not custom_index:
            index_query = {
                "operation": "XcalarApiIndex",
                "args": {
                    "source":
                        ".XcalarDS.Optimized.ds",
                    "dest":
                        "tab-1",
                    "prefix":
                        prefix,
                    "dhtName":
                        "systemRandomDht",
                    "key": [{
                        "name": "xcalarRecordNum",
                        "ordering": "Unordered",
                        "keyFieldName": "",
                        "type": "DfInt64"
                    }],
                },
            }
        else:
            keys_info = []
            keys_type = [
                ImdConstant.Table_col_types_map[col["type"].upper()]
                for col in schema if col["name"] in pkeys
            ]
            for key, key_type in zip(pkeys, keys_type):
                keys_info.append({
                    "name": key,
                    "ordering": "PartialAscending",
                    "keyFieldName": key,
                    "type": key_type
                })
            index_query = {
                "operation": "XcalarApiIndex",
                "args": {
                    "source": ".XcalarDS.Optimized.ds",
                    "dest": "tab-1",
                    "prefix": prefix,
                    "key": keys_info
                }
            }

        xc_query = json.dumps([load_query, index_query])
        df = Dataflow.create_dataflow_from_query_string(
            client,
            xc_query,
            columns_to_export=export_schema_array,
            dataflow_name=table_name)
        return df

    @staticmethod
    def load_df_from_retina(client, retina_path):
        df_name = os.path.basename(retina_path).split(".tar.gz")[0]
        df = None
        with tarfile.open(retina_path, mode="r:gz") as tar:
            df_mem = tar.getmember("dataflowInfo.json")
            df_file = tar.extractfile(df_mem)
            ret = json.load(df_file)
            optimized_ret = json.dumps({"retina": json.dumps(ret)})
            df = Dataflow.create_dataflow_from_query_string(
                client,
                json.dumps(ret["query"]),
                optimized_query_string=optimized_ret,
                dataflow_name=df_name)
        return df

    @staticmethod
    # replace the source synthesize tables to FQN of the source session
    def update_source_tables_scope(dataflow, global_tab_names_lookup):
        source_op_names = {'XcalarApiSynthesize'}
        for query in dataflow._query_list:
            op = query['operation']
            if op not in source_op_names:
                continue
            if op == 'XcalarApiSynthesize' and query['args'][
                    'sameSession'] is False:
                source_name = query['args']['source']
                query['args']['source'] = global_tab_names_lookup.get(
                    source_name, source_name)
        # now fix the optimized retina string if exists in dataflow
        if not dataflow.optimized_query_string:
            return dataflow
        json_obj = json.loads(dataflow.optimized_query_string)
        retina_obj = json.loads(json_obj['retina'])
        query_list = json.loads(retina_obj['query'])

        # now fix this query list and jsonify the retina
        for query in query_list:
            op = query['operation']
            if op not in source_op_names:
                continue
            if op == 'XcalarApiSynthesize' and query['args'][
                    'sameSession'] is False:
                source_name = query['args']['source']
                query['args']['source'] = global_tab_names_lookup.get(
                    source_name, source_name)
        retina_obj['query'] = json.dumps(query_list)
        json_obj['retina'] = json.dumps(retina_obj)
        dataflow._optimized_query_string = json.dumps(json_obj)
        return dataflow

    # the dataflows in the queries_workbook should be of the form linkIn -> sqlNode -> linkOut.
    # When we invoke get_dataflow on queries_workbook object, it makes a request to expServer
    # to compile the sqlNode to a xcalar query(which triggers sqldf code paths) and returns
    # the dataflow containing optimized query string.
    # We will cache this optimized query string in kvstore instead of recompiling every time.
    @staticmethod
    def get_tpc_optimized_df(queries_workbook, query_name):
        optimized_query_key = f"tpc_optimized_{query_name}"
        optimized_query_val = None
        query_wb_kvstore = queries_workbook.kvstore
        df = None
        with XcUtil.ignored(Exception):
            optimized_query_val = query_wb_kvstore.lookup(optimized_query_key)
        if optimized_query_val is None:
            logger.info(
                f"Compiling query {query_name} from {queries_workbook.name} workbook"
            )
            # this will compile the sqlNode and returns dataflow containing optimized query string
            df = queries_workbook.get_dataflow(query_name)
            optimized_query_val = df.optimized_query_string
            # cache the optimized query string in workbook kvstore to avoid recompilation
            query_wb_kvstore.add_or_replace(
                optimized_query_key, optimized_query_val, persist=False)
        else:
            logger.info(
                f"Reusing compiled dataflow for {query_name} from {queries_workbook.name} kvstore"
            )
            df = Dataflow.create_dataflow_from_query_string(
                queries_workbook.client,
                json.loads(json.loads(optimized_query_val)['retina'])['query'],
                optimized_query_string=optimized_query_val,
                dataflow_name=query_name)

        return df


def upload_stats_test_util(client, xcalar_api, workbook_path, dataflow_name,
                           shell):
    test_wb_name = "TestUploadWorkbook_{}".format(int(time.time()))
    query_name = "TestExecuteQuery_{}_{}".format(dataflow_name,
                                                 int(time.time()))
    ts = int(time.time())
    before = datetime.datetime.fromtimestamp(ts)
    date_before = "{}-{}-{}".format(before.year, before.month, before.day)
    with open(workbook_path, 'rb') as wb_file:
        wb_content = wb_file.read()

        workbook = client.upload_workbook(test_wb_name, wb_content)
        dataflow = workbook.get_dataflow(dataflow_name=dataflow_name)

        sess = workbook.activate()
        # Dataflows can only be executed optimized as there are no loaded
        # datasets for the unoptimized version to find.
        sess.execute_dataflow(
            dataflow,
            optimized=True,
            query_name=query_name,
            is_async=False,
            parallel_operations=random.choice([True, False]))

    # Adding 120 seconds here just to be conservative. Test will fail if
    # the upload_stats command happens on a different date (which only happens
    # if date changes immediately after job finishes).
    ts_after = int(time.time()) + 120

    after = datetime.datetime.fromtimestamp(ts_after)
    date_after = "{}-{}-{}".format(after.year, after.month, after.day)

    if date_after == date_before:
        if shell:
            with capture_output() as captured:
                shell.run_line_magic(
                    magic_name='statsmart',
                    line="-upload --date {} --upload_statsmart_project".format(
                        date_before))
            assert "Sucessfully loaded new published table CG_USRNODE_MEM" in captured.stdout
        else:
            cmd = "{} {}/scripts/upload_stats.py".format(
                sys.executable, os.environ["XLRDIR"])
            cmd += " --date " + date_after
            cmd += " -w StatsMart"
            resp = subprocess.run(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            stdout, sterr, retcode = resp.stdout, resp.stderr, resp.returncode
            print(stdout)
            # For debugging purposes, we want to print stdout here because
            # an unsuccessful upload will not necessarily have a non-zero retcode
            print(stdout)
            if retcode != 0:
                raise Exception(
                    "upload stats failed with the following error: {}".format(
                        sterr))

        xcalar_api.setSession(sess)
        op = Operators(xcalar_api)
        publish_tables_names = [
            "JOB_STATS", "OPERATOR_STATS", "OS_CPU_STATS", "OS_MISC_STATS",
            "XCALAR_INTERNAL_STATS", "OS_IO_STATS", "CG_USRNODE_CPU",
            "CG_USRNODE_MEM", "CG_XPU_MEM", "CG_XPU_CPU", "CG_MW_CPU",
            "CG_MW_MEM"
        ]
        for pt in publish_tables_names:
            print(pt)
            list_out = op.listPublishedTables(pt)
            assert len(list_out.tables) == 1

        # test StatsMart workbook
        wb = client.get_workbook("StatsMart")
        mart_sess = wb.activate()
        dataflow_names = [
            "A - Job Execution Summary",
            "B - Operator Breakdown For Longest Running Job"
        ]
        """
        Unable to run dataflows with link-in nodes, so leaving
        this out for now.
        dataflow_name.extend([
            "C - CPU Stats For Longest Running Job",
            "D - Xcalar System Stats For Longest Running Job",
            "E - Xcalar Internal Stats For Longest Running Job"
        ])
        """
        tables_to_drop = []
        for dataflow_name in dataflow_names:
            dataflow = wb.get_dataflow(dataflow_name)
            final_table_name = "statsTable_{}_{}".format(
                dataflow.dataflow_name[0], int(time.time()))
            qname = mart_sess.execute_dataflow(
                dataflow,
                optimized=True,
                table_name=final_table_name,
                is_async=False)
            table = mart_sess.get_table(table_name=final_table_name)

            # Just some sanity checks to make sure the dataflows are running as expected
            assert table.record_count() > 0
            for row in table.records(num_rows=10):
                for k, v in row.items():
                    assert k is not None
                    assert v is not None
            tables_to_drop.append(table)

        for table in tables_to_drop:
            table.drop()

        for pt in publish_tables_names:
            op.unpublish(pt)

    workbook.inactivate()
    workbook.delete()

import json
import os
import sys
import uuid
import time
import traceback
import re
import random
import concurrent.futures

from xcalar.container.parent import Client
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
import xcalar.container.context as ctx
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.external.exceptions import XDPException
from xcalar.external.dataflow import Dataflow

from xcalar.compute.util.tests_util import *

# Set up logging
logger = ctx.get_logger()


def doNoOpIMD(source_tabs_sess, baseTables, tnum):
    num_workers = len(
        baseTables)    # processes will be waiting mostly on apis responses
    worker_args = [(tnum, source_tabs_sess, tab) for tab in baseTables]
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=num_workers) as executor:
        outputs = executor.map(lambda args: workerNoOpImd(*args), worker_args)
    success = all(out[0] for out in outputs)
    if not success:
        raise RuntimeError("Merge returned failure status")


def workerNoOpImd(tnum, sess_name, base_tab_name):
    logger.info('Applying IMD on {}..'.format(base_tab_name))
    # orginal delta
    client = Client()
    session = client.get_session(sess_name)
    table = session.get_table(base_tab_name)
    q_name_prefix = "{}-{}".format(table.name, tnum)
    base_rec_count = table.record_count()
    delta_name = "table{}".format(uuid.uuid4().hex)
    rand_num = random.randint(base_rec_count // 2, base_rec_count)
    df = Dataflow.create_dataflow_from_table(
        client, table).gen_row_num("rowNum").filter(
            'lt(rowNum, {})'.format(rand_num)).project(table.columns).map(
                [("int(1)", "XcalarOpCode"), ("int(1)", "XcalarRankOver")])
    q_name = session.execute_dataflow(
        df,
        table_name=delta_name,
        query_name=q_name_prefix + "-MW-1",
        optimized=True,
        is_async=False)

    delta_tab = session.get_table(delta_name)
    delta_rec_count = delta_tab.record_count()
    # do all deletes with the delta
    # XXX do union to have more than one row per pk
    tmp_tab_name = "table{}".format(uuid.uuid4().hex)
    df = Dataflow.create_dataflow_from_table(
        client, delta_tab).map([("int(0)", "XcalarOpCode"),
                                ("genRandom(1, 100000)", "XcalarRankOver")])
    q_name = session.execute_dataflow(
        df,
        table_name=tmp_tab_name,
        query_name=q_name_prefix + "-MW-2",
        optimized=True,
        is_async=False)
    tmp_tab = session.get_table(tmp_tab_name)
    begin1 = time.monotonic()
    table.merge(tmp_tab)
    end1 = time.monotonic()
    tmp_tab.drop(delete_completely=True)

    # do all random inserts and deletes with the delta
    df = Dataflow.create_dataflow_from_table(client, delta_tab).map(
        [("genRandom(0, 1)", "XcalarOpCode"),
         ("genRandom(1, 100000)", "XcalarRankOver")])
    q_name = session.execute_dataflow(
        df,
        table_name=tmp_tab_name,
        query_name=q_name_prefix + "-MW-3",
        optimized=True,
        is_async=False)
    tmp_tab = session.get_table(tmp_tab_name)
    begin2 = time.monotonic()
    table.merge(tmp_tab)
    end2 = time.monotonic()
    tmp_tab.drop(delete_completely=True)

    # do all inserts with the orginal delta
    begin3 = time.monotonic()
    table.merge(delta_tab)
    end3 = time.monotonic()
    delta_tab.drop(delete_completely=True)
    imd_end = time.monotonic()
    logger.info(
        'Table {}, base size ({:,}), delta size ({:,}), deletes ({:.2f}), inserts/deletes ({:.2f}), inserts ({:.2f}) secs.'
        .format(base_tab_name, base_rec_count, delta_rec_count,
                (end1 - begin1), (end2 - begin2), (end3 - begin3)))
    return (True, {})


def worker_run_xd_dataflows(tname, queries, retinaBasePath, answers, wbName,
                            tables, seed, tablesSessName, testMgrSessionName,
                            cliargsCont, cliargsTestMergeOp, tnum, username):
    client = Client(user_name=username)

    queries_wb = client.get_workbook(wbName)

    dataflows = {}
    for df_name in queries_wb.list_dataflows():
        df = TpcTestUtil.get_tpc_optimized_df(queries_wb, df_name)
        df._client = None
        dataflows[df_name] = df

    failed = []
    perfTrackerList = []
    subQueryList = []
    qnames = list(queries.keys())
    if seed:
        r = random.Random(seed + tnum)
        r.shuffle(qnames)

    worker_session_name = "{}_worker_{}".format(testMgrSessionName, tnum)
    try:
        worker_session = client.get_session(worker_session_name)
    except ValueError:
        worker_session = client.create_session(worker_session_name)

    tables_sess = None
    if tablesSessName:
        tables_sess = client.get_session(tablesSessName)
    else:
        tables_sess = client.get_session(testMgrSessionName)

    retry_count = 100
    busy_statuses = [StatusT.StatusAccess, StatusT.StatusTableNameNotFound]
    while retry_count > 0:
        try:
            tab_names_loopkup = {
                tab.name: tab.fqn_name
                for tab in tables_sess.list_tables(globally_shared_only=True)
            }
            break
        except XDPException as ex:
            if ex.statusCode not in busy_statuses:
                raise ex
            logger.warning("Failed to list tables from session {}".format(
                worker_session_name))
        retry_count -= 1
        time.sleep(random.randint(1, 5))

    for qname in qnames:
        qlist = queries[qname]
        qAnswerList = None
        if qname in answers:
            qAnswerList = answers[qname]
        perfTracker = []

        for (subquery, q) in enumerate(qlist):
            perf = {
                'qname': qname,
                'q': q.get("query"),
                'xcalarDfs': {
                    'queryExecStart': 0,
                    'queryExecEnd': 0,
                    'getDfStart': 0,
                    'getDfEnd': 0
                }
            }
            perfDf = perf['xcalarDfs']
            dfname = qname if len(qlist) == 1 else '{}_{}'.format(
                qname, subquery)
            perfDf['getDfStart'] = time.monotonic()
            df = dataflows.get(dfname)
            if tablesSessName:
                TpcTestUtil.update_source_tables_scope(df, tab_names_loopkup)
            perfDf['getDfEnd'] = time.monotonic()
            qanswer = None
            if qAnswerList and subquery < len(qAnswerList):
                qanswer = qAnswerList[subquery]
            rows = None
            passed = True
            perfTracker.append(perf)
            table = None
            try:
                logger.info('EXECUTE query: {}'.format(qname))
                if cliargsTestMergeOp:
                    doNoOpIMD(tablesSessName, tables, tnum)
                sdkOpts = Util.getQueryOpts(q.get("xcalarOpts", None))
                delStrFmt = '{:7.3f}s'
                naStr = '{:^8s}'.format('N/A')
                xcalarDfStr = naStr
                rowsReturnedStr = '{:5d}r'
                rowsReturned = -1

                tabName = "table{}".format(uuid.uuid4().hex)
                perfDf['queryExecStart'] = time.monotonic()
                qName = "{}-{}-thr{}".format(worker_session.name,
                                             df.dataflow_name, tnum)
                qName = worker_session.execute_dataflow(
                    df,
                    table_name=tabName,
                    query_name=qName,
                    optimized=True,
                    is_async=False)
                perfDf['queryExecEnd'] = time.monotonic()
                table = worker_session.get_table(tabName)
                orderedColumns = table.schema

                rowsReturned = table.record_count()
                xcalarDfExecDelta = perfDf['queryExecEnd'] - perfDf[
                    'queryExecStart']
                xcalarDfGetDelta = perfDf['getDfEnd'] - perfDf['getDfStart']
                xcalarDfExecDeltaStr = delStrFmt.format(xcalarDfExecDelta)
                xcalarDfGetDeltaStr = delStrFmt.format(xcalarDfGetDelta)
                if qanswer:
                    rows = Util.formatResultSet(table._result_set(),
                                                orderedColumns)
                    Util.verifyAnswer(qname, rows, qanswer, sdkOpts, True,
                                      logger, '({:4d})'.format(tnum))
                logger.info('PASSED ({}, {}): {}: {}: {}\n'.format(
                    xcalarDfGetDeltaStr, xcalarDfExecDeltaStr,
                    rowsReturnedStr.format(rowsReturned), qname,
                    q.get("query")))
            except Exception as e:
                if isinstance(e, JdbcTestSDKVerificationError):
                    Util.dumpRows('xcalarDfs', qname, table.records())
                bt = traceback.format_exc()
                failed.append({
                    "name": qname,
                    "exception": str(e),
                    "trace": bt
                })
                logger.info('FAILED {}: {}: {}\n'.format(
                    qname, str(e), q.get("query")))
                if not cliargsCont:
                    # Immediately failing, so return immediately
                    # instead of waiting to collect other backtraces
                    logger.error("{}".format(bt))
                    worker_session.destroy()
                    return (len(failed) == 0, {
                        "failed": failed,
                        "perf": perfTrackerList,
                        "tid": tnum
                    })
            finally:
                if table:
                    noex(table.drop, True)
        perfTrackerList.append(perfTracker)

    worker_session.destroy()
    return (len(failed) == 0, {
        "failed": failed,
        "perf": perfTrackerList,
        "tid": tnum
    })


def main(inBlob):
    params = json.loads(inBlob)
    res = worker_run_xd_dataflows(
        params["tname"], params["queries"], params["retinaBasePath"],
        params["answers"], params["wbName"], params["tables"], params["seed"],
        params["tables_sess_name"], params["testMgrSessName"],
        params["cliargs_cont"], params["cliargs_testMergeOp"], params["tnum"],
        params["username"])
    return json.dumps(res)

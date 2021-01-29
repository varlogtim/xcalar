# Copyright 2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import os
import pytest
import json
import time

from xcalar.compute.util.cluster import DevCluster
from xcalar.compute.util.Qa import verifyRowCount
from xcalar.external.client import Client
from xcalar.external.dataflow import Dataflow
from xcalar.external.exceptions import XDPException
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.LegacyApi.Target2 import Target2
from xcalar.external.LegacyApi.XcalarApi import (XcalarApi,
                                                 XcalarApiStatusException)
from xcalar.external.LegacyApi.Session import Session
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.LegacyApi.Dataset import Dataset, UdfDataset
from xcalar.compute.coretypes.OrderingEnums.ttypes import XcalarOrderingT
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.QueryStateEnums.ttypes import QueryStateT

pytestmark = [
    pytest.mark.last(
        "Execute this test as late as possible since it manages its own clusters"
    ),
    pytest.mark.usefixtures("config_backup")
]

TestPagingUserName = "TestPagingUser"
TestPagingSessionName = "TestPagingSession"

datasetPrefix = ".XcalarDS."

DEFAULT_NUM_NODES = 3

# We deliberately start the cluster with very low BufferCachePercentOfTotalMem
# in order to induce paging sooner
# If you have a test that needs to run while the cluster is paging,
# consider adding the test here
# Any lower than 50, we fail due to OOM instead of paging
BufferCachePercentOfTotalMem = 50


@pytest.fixture(scope="module")
def setup(request):
    config_params = None
    bufferCachePercentOfTotalMem_previous = None
    num_nodes = os.environ.get("NumNodes", DEFAULT_NUM_NODES)
    needReconfig = True
    reconfiged = False
    while needReconfig:
        needReconfig = False
        with DevCluster(num_nodes=num_nodes):
            client = Client()
            config_params = client.get_config_params()
            config_params = {
                config_param["param_name"]: config_param["param_value"]
                for config_param in config_params
            }

            if int(config_params.get("BufferCachePercentOfTotalMem",
                                     100)) != BufferCachePercentOfTotalMem:
                bufferCachePercentOfTotalMem_previous = config_params.get(
                    "BufferCachePercentOfTotalMem", None)
                client.set_config_param(
                    param_name="BufferCachePercentOfTotalMem",
                    param_value=BufferCachePercentOfTotalMem)
                needReconfig = True
                reconfiged = True
                continue

            client._user_name = TestPagingUserName
            xcApi = XcalarApi()
            session = Session(
                xcApi,
                owner=None,
                username=TestPagingUserName,
                sessionName=TestPagingSessionName)
            xcApi.setSession(session)
            session.activate()

            yield (client, xcApi)

            queries = xcApi.listQueries("*").queries
            for query in queries:
                # Otherwise there should be no outstanding queries
                assert (request.session.testsfailed > 0)
                try:
                    xcApi.cancelQuery(query.name)
                except XcalarApiStatusException as e:
                    if (e.status not in [
                            StatusT.StatusOperationHasFinished,
                            StatusT.StatusOperationInError,
                            StatusT.StatusOperationCancelled
                    ]):
                        raise e
                xcApi.waitForQuery(query.name)
                xcApi.deleteQuery(query.name, deferredDelete=False)
                with pytest.raises(XcalarApiStatusException) as e:
                    xcApi.queryState(query.name, detailedStats=False)
                    assert e.status == StatusT.StatusQrQueryNotExist

            operators = Operators(xcApi)
            operators.dropTable("*", deleteCompletely=True)
            operators.dropConstants("*", deleteCompletely=True)

            try:
                operators.dropDatasets("*", deleteCompletely=True)
            except XcalarApiStatusException as e:
                if (e.status != StatusT.StatusDsNotFound):
                    raise

            Dataset.bulkDelete(xcApi, "*", deleteCompletely=True)

            session.destroy()
            xcApi.setSession(None)

            # We need to restore previous value
            if reconfiged:
                client.set_config_param(
                    param_name="BufferCachePercentOfTotalMem",
                    param_value=bufferCachePercentOfTotalMem_previous)


# Here's how the fat-ass table is generated:
# 1) We have a load UDF generate <numDatasetRows> number of rows, with each
# row with the field "col", of the value "0,1,2,...,<explosionFactor>"
# 2) We then call explodeString on "col" 3 times. As a result, the size of
# the fat-ass table == <numDatasetRows> * (<explosionFactor> ** 3)
# The above is a fast way to generate large tables since generating purely
# from UDF runs too slow in Python, and explosive join causes skew
def genSourceTable(client, xcApi, testPrefix, explosionFactor, numDatasetRows):
    udfMgr = Udf(xcApi)
    operators = Operators(xcApi)
    targetMgr = Target2(xcApi)

    memoryTargetName = "QA test_paging memory"
    targetMgr.add("memory", memoryTargetName, {})

    # Start by loading a small dataset and doing a series of explosion

    explodeString = ",".join(map(str, [ii for ii in range(explosionFactor)]))
    udfSource = """
import json
import random
def load(inp, ins):
    inObj = json.loads(ins.read())
    for ii in range(inObj["startRow"], inObj["startRow"] + inObj["numRows"]):
        yield {"x": ii, "y": 1, "col": "%s", "constant": "someconstantvalue"}
""" % explodeString

    udfName = "{}-udf".format(testPrefix)
    udfMgr.addOrUpdate(udfName, udfSource)

    # Load the small dataset with the pre-defined explosion string
    path = str(numDatasetRows)
    dsName = "{}-ds".format(testPrefix)
    dataset = UdfDataset(xcApi, memoryTargetName, path, dsName,
                         "{}:load".format(udfName))
    dataset.load()

    loadTable = "{}-load".format(testPrefix)
    operators.indexDataset(
        "{}{}".format(datasetPrefix, dsName),
        loadTable,
        "xcalarRecordNum",
        fatptrPrefixName="p")

    startTable = "{}-start".format(testPrefix)

    # We want to explode on an immediate for faster explosion
    operators.map(
        loadTable, startTable,
        ["string(p::col)", "int(p::x)", "string(p::constant)", "int(p::y)"],
        ["explodeCol", "intCol", "constantCol", "constantInt"])

    # Repeatedly explode until we get the desired size
    for ii in range(3):
        intermediateTable = "{}-map-{}".format(testPrefix, ii)
        operators.map(startTable, intermediateTable,
                      'explodeString(explodeCol, ",")', "col-{}".format(ii))
        verifyRowCount(client, xcApi, intermediateTable,
                       numDatasetRows * (explosionFactor**(ii + 1)))
        startTable = intermediateTable

    return (startTable, dataset)


# Witness to ENG-6691 and ENG-7165
# Here's how the test works. We first generate a fat-ass table as the
# starting point, make several copies of it, then do groupBys
# concurrently on the fat-ass tables, and occasionally cancel some of the
# groupBys in order to trigger bcScanCleanout, while the groupBys trigger
# demand-paging logic, that not only tests cursors paging, but also paging
# on groupMeta. In unoptimized mode, the groupBy also runs on a fatpointer
# in order to test paging on the fatptr demystification path.
#
# We first generate a fatass table using genSourceTable
# Next, we make <numCopiesOfSrcTable> copies of the above table.
# We're done generating the tables we need, and are ready for the groupBys.
@pytest.mark.parametrize(
    "explosionFactor, numDatasetRows, numCopiesOfSrcTable, indexCol", [
        pytest.param(12, 2243, 9, "constantInt"),
        pytest.param(19, 599, 17, "intCol"),
    ])
# For each iteration, for <numIterations> number of iterations,
# We run <numGroupBys> number of groupBys concurrently. This is done by
# submitting the groupBys as async queries.
# And then for every <intervalToCancel> other groupBys, we cancel it, in
# order to trigger txn recovery and bcScanCleanout.
# Preferably, <intervalToCancel> should be less than the square root of
# <numGroupBys>
# Once all the groupBys have run to completion, only then are we done with
# an iteration and can commence the next iteration
# If we had done <numGroupBys> * <numIterations> number of groupBys all at
# once, it's more likely for them to all fail with OOM, and hence we stagger
# them like this, after empirically finding a sweet spot that keeps us in
# the demand-paging path for as long as possible
@pytest.mark.parametrize(
    "numGroupBys, numIterations, intervalToCancel",
    [pytest.param(31, 1, 6),
     pytest.param(97, 1, 10, marks=pytest.mark.slow)])
# groupBy can be run as part of optimized dataflow execution or unoptimized
# dataflow execution. This is parameterized via <queryModes>
@pytest.mark.parametrize(
    "queryModes",
    [
    # Run both optimized and unoptimized together
    # Unoptimized dataflow with fatptr required to reproduce ENG-6691
    # Optimized dataflow with maxInteger required to reproduce ENG-7165
        pytest.param(["unoptimized", "optimized"]),
        pytest.param(["unoptimized"], marks=pytest.mark.slow),
        pytest.param(["optimized"], marks=pytest.mark.slow),
    ])
# While the test is running, we can concurrently perform IMD on the source tables
# This is turned on with the parameter <imdEnabled>
@pytest.mark.parametrize("imdEnabled", [
    pytest.param(False),
    pytest.param(True, marks=pytest.mark.slow),
])
def testGroupByPaging(setup, explosionFactor, numDatasetRows,
                      numCopiesOfSrcTable, numGroupBys, numIterations,
                      intervalToCancel, queryModes, imdEnabled, indexCol):
    (client, xcApi) = setup
    testPrefix = "gbPaging"
    ImdMaxRetries = 3

    (startTable, dataset) = genSourceTable(client, xcApi, testPrefix,
                                           explosionFactor, numDatasetRows)

    session = client.get_session(TestPagingSessionName)
    operators = Operators(xcApi)
    indexTable = "{}-index".format(testPrefix)
    if imdEnabled:
        ordering = XcalarOrderingT.XcalarOrderingPartialAscending
    else:
        ordering = XcalarOrderingT.XcalarOrderingUnordered
    operators.indexTable(startTable, indexTable, indexCol, ordering=ordering)

    # Now let's make several copies of this table
    for ii in range(numCopiesOfSrcTable):
        tableCopy = "{}-copy-{}".format(testPrefix, ii)
        if imdEnabled:
            operators.project(
                indexTable, tableCopy,
                ["explodeCol", "intCol", "constantCol", "constantInt"])
        else:
            operators.map(indexTable, tableCopy, "int(1)", "dummy")

    # Now let's do groupBy until we start paging madly
    for jj in range(numIterations):
        print("Start Iteration: {}".format(jj))
        queryNames = []
        for ii in range(numGroupBys):
            if imdEnabled:
                groupByCol = "intCol"
            else:
                groupByCol = "p::x"
            for mode in queryModes:
                sourceTable = "{}-copy-{}".format(
                    testPrefix,
                    ((jj * numGroupBys) + ii) % numCopiesOfSrcTable)
                query = []
                if mode == "optimized":
                    destTable = "{}-synth{}-{}".format(testPrefix, jj, ii)
                    query.append({
                        "operation": "XcalarApiSynthesize",
                        "args": {
                            "source": sourceTable,
                            "dest": destTable,
                            "sameSession": False,
                            "columns": [],
                            "numColumns": 0
                        }
                    })
                    sourceTable = destTable

                query.append({
                    "operation": "XcalarApiGroupBy",
                    "args": {
                        "source":
                            sourceTable,
                        "dest":
                            "{}-gb{}-{}".format(testPrefix, jj, ii),
                        "eval": [{
                            "evalString": "maxInteger({})".format(groupByCol),
                            "newField": "gbField"
                        }]
                    }
                })

                queryName = "{}-query{}-{}-{}".format(testPrefix, jj, mode, ii)
                print("Schedule {} query: {}".format(mode, queryName))
                if (mode == "optimized"):
                    df = Dataflow.create_dataflow_from_query_string(
                        client,
                        query_string=json.dumps(query),
                        columns_to_export=[{
                            "columnName": "gbField"
                        }, {
                            "columnName": "intCol"
                        }, {
                            "columnName": "explodeCol"
                        }, {
                            "columnName": "constantCol"
                        }, {
                            "columnName": "constantInt"
                        }])
                    dataflowName = session.execute_dataflow(
                        df,
                        table_name="{}-lrq{}-{}".format(testPrefix, jj, ii),
                        optimized=True,
                        query_name=queryName,
                        is_async=True,
                        clean_job_state=False)
                else:
                    xcApi.submitQuery(
                        json.dumps(query), queryName=queryName, isAsync=True)
                    dataflowName = queryName
                queryNames.append((queryName, dataflowName))

        if (imdEnabled):
            for ii in range(numCopiesOfSrcTable):
                baseTableName = "{}-copy-{}".format(testPrefix, ii)
                imdTableName = "{}-imd-{}".format(testPrefix, ii)
                try:
                    operators.map(baseTableName, imdTableName,
                                  ["int(1)", "int(1)"],
                                  ["XcalarRankOver", "XcalarOpCode"])
                except XcalarApiStatusException as e:
                    if (e.status != StatusT.StatusNoXdbPageBcMem):
                        raise
                    else:
                        continue

                imdTable = session.get_table(imdTableName)
                baseTable = session.get_table(baseTableName)

                attempt = 0
                while attempt < ImdMaxRetries:
                    attempt += 1
                    try:
                        baseTable.merge(imdTable)
                        break
                    except XDPException as e:
                        if (e.statusCode == StatusT.StatusAccess or
                                e.statusCode == StatusT.StatusNoXdbPageBcMem):
                            # StatusAccess happens when we try to do IMD on a table
                            # that's being accessed elsewhere
                            print(
                                "(Attempt: {} / {}) Failed to merge {} with {}: {}"
                                .format(attempt, ImdMaxRetries, imdTableName,
                                        baseTableName, str(e)))
                            time.sleep(0.1)
                            continue
                        else:
                            raise

                operators.dropTable(imdTableName, deleteCompletely=True)

        for (idx, (queryName, dataflowName)) in enumerate(queryNames):
            if idx % intervalToCancel == 0:
                print("Cancel query: ({}, {})".format(queryName, dataflowName))
                # In order to kick bcScanCleanout
                try:
                    xcApi.cancelQuery(dataflowName)
                except XcalarApiStatusException as e:
                    if (e.status != StatusT.StatusOperationHasFinished
                            and e.status != StatusT.StatusOperationInError):
                        raise

        for (queryName, dataflowName) in queryNames:
            idx = int(queryName.split("-")[3])
            optimized = (queryName.split("-")[2] == "optimized")
            if optimized:
                tableName = "{}-lrq{}-{}".format(testPrefix, jj, idx)
            else:
                tableName = "{}-gb{}-{}".format(testPrefix, jj, idx)

            try:
                queryState = xcApi.waitForQuery(dataflowName).queryState
                xcApi.deleteQuery(dataflowName, deferredDelete=False)
                xcApi.queryState(dataflowName, detailedStats=False)
            except XcalarApiStatusException as e:
                assert e.status == StatusT.StatusQrQueryNotExist or \
                    e.status == StatusT.StatusQrQueryAlreadyDeleted

            resultTableValid = False
            try:
                # Backend cleans out Query Job status after a timeout,
                # so even if QueryState fails with StatusT.StatusQrQueryNotExist,
                # the resultant Table may be around to do verification.
                resultTable = session.get_table(tableName)
                resultTableValid = True
            except ValueError as e:
                print("Table {} not found for query result: ({}, {})".format(
                    tableName, queryName, dataflowName))
                assert "No such table: '{}'".format(tableName) in str(e)

            if queryState == QueryStateT.qrFinished or resultTableValid:
                print("Validate query result: ({}, {})".format(
                    queryName, dataflowName))
                try:
                    if indexCol == "constantInt":
                        answer = 1
                    else:
                        answer = numDatasetRows
                    verifyRowCount(client, xcApi, tableName, answer)
                except XcalarApiStatusException as e:
                    if (e.status != StatusT.StatusDgDagNodeError
                            and e.status != StatusT.StatusNoMem):
                        raise

                operators.dropTable(tableName, deleteCompletely=True)

    operators.dropTable("{}-*".format(testPrefix), deleteCompletely=True)
    dataset.delete()

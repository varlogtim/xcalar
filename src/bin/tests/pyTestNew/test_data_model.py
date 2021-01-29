import os
import logging
import sys
import pytest

from xcalar.compute.util.cluster import DevCluster
from xcalar.external.dataflow import Dataflow
from xcalar.external.result_set import ResultSet
from xcalar.external.LegacyApi.XcalarApi import XcalarApi as LegacyApi
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.LegacyApi.Operators import Operators

pytestmark = [
    pytest.mark.last(
        "Execute this test as late as possible since it manages its own clusters"
    ),
    pytest.mark.usefixtures("per_function_config_backup")
]

# Set up logging
logger = logging.getLogger("Test Data Model")
try:
    logging_level = getattr(logging, os.environ.get("LOGGING_LEVEL", "INFO"))
except AttributeError as e:
    logging_level = logging.INFO
logger.setLevel(logging_level)

if not logger.handlers:
    log_handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)

#
# UDFs
#
mem_udf_name = "test_data_model"
udf_sum = mem_udf_name + ":sumAll"
flaky_frequency = 10
num_dataset_rows = 100

mem_udf_source_preamble = """
import json
import random
from datetime import datetime, timedelta
import time
import xcalar.container.parent as xce
from xcalar.container.table import Table

def noneTest():
    return None

def cursorTest(tableId):
    table = Table(tableId, [
        {"columnName": "p1-val",
         "headerAlias": "p1-val"}])

    out = 0
    for row in table.all_rows():
        out += row['p1-val']

    return out

def testNone(a):
    if a is None:
        return "Empty"
    return a

def sumAll(*cols):
    sum = 0
    for col in cols:
        sum += int(col)
    return str(sum)

def randdate():
    d1 = datetime.strptime('1/1/1980', '%m/%d/%Y')
    d2 = datetime.strptime('1/1/2017', '%m/%d/%Y')

    d3 = d1 + timedelta(
        seconds=random.randint(0, int((d2 - d1).total_seconds()))
    )

    return d3.strftime("%Y-%m-%d")
"""

mem_udf_source_load = """
def load(fullPath, inStream):
    inObj = json.loads(inStream.read())
    numLocalRows = inObj["numRows"]
    startNum = inObj["startRow"]
    rowGen = random.Random()
    rowGen.seed(537)
    for ii in range(startNum, startNum+numLocalRows):
        rowDict = {"val": ii, "valDiv10": ii//10, "constant": 1, "date": randdate(), "nullVal": None, "mixedType": None}
        if not (ii %% %d):
            rowDict['flaky'] = 1

        rowDict.update({"prn1": rowGen.randint(0, 4), "prn2": rowGen.randint(0, 4), "prn3": rowGen.randint(0, 4)})

        option = random.randint(0, 2)
        if option == 0:
            rowDict['mixedType'] = ii
        elif option == 1:
            rowDict['mixedType'] = str(ii)
        else:
            rowDict['mixedType'] = float(ii)

        yield rowDict
    """ % flaky_frequency

mem_udf_source = mem_udf_source_preamble + mem_udf_source_load


#
# Helper methods
#
def verify_checksum(client, session, operators, src_table, field, answer_table,
                    answer_field):
    t1 = session.get_table(src_table)
    t2 = session.get_table(answer_table)
    t1.show()
    t2.show()
    df1 = Dataflow.create_dataflow_from_table(client, t1)\
        .aggregate('sum', field, 'checksum_src')
    df2 = Dataflow.create_dataflow_from_table(client, t2)\
        .aggregate('sum', answer_field, 'checksum_answer')
    session.execute_dataflow(df1, is_async=False)
    session.execute_dataflow(df2, is_async=False)
    rst = ResultSet(
        client, table_name='checksum_src', session_name=session.name)
    src_sum = rst.get_row(0)
    rst = ResultSet(
        client, table_name="checksum_answer", session_name=session.name)
    answer_sum = rst.get_row(0)

    assert src_sum == answer_sum
    operators.dropConstants("checksum*")


def verify_checksum_value(client, session, operators, src_table, field,
                          answer):
    t = session.get_table(src_table)
    df = Dataflow.create_dataflow_from_table(client, t)\
        .aggregate('sum', field, 'checksum_src')
    session.execute_dataflow(df, is_async=False)
    rst = ResultSet(
        client, table_name='checksum_src', session_name=session.name)
    src_sum = rst.get_row(0)

    assert float(src_sum["constant"]) == float(answer)
    operators.dropConstants("checksum*")


def verify_row_count(session, src_table, answer_rows):
    t = session.get_table(src_table)
    assert t.record_count() == answer_rows


def verify_row_count_match(session, src_table, answer_table):
    t1 = session.get_table(src_table)
    t2 = session.get_table(answer_table)
    assert t1.record_count() == t2.record_count()


def verify_schema(session,
                  src_table,
                  columns_in_schema=[],
                  columns_not_in_schema=[]):
    t = session.get_table(src_table)
    row_0 = t.get_row(0)

    for col in columns_in_schema:
        assert col in row_0

    for col in columns_not_in_schema:
        assert col not in row_0


def verify_column_match(session, src_table, df):
    t = session.get_table(src_table)
    assert set(t.columns) == df.table_columns


def clean_up(operators, tables=['*'], constants=['*']):
    for table in tables:
        operators.dropTable(table)
    for constant in constants:
        operators.dropConstants(constant)


def telecom_dataset(client, workbook, xc_api):
    dataset_name = "testDataflowServiceDs"
    path = str(num_dataset_rows)
    parser_name = "{}:load".format(mem_udf_name)
    target_name = "qa_memory_target"
    target_type_id = "memory"

    # Add target
    data_target = client.add_data_target(target_name, target_type_id)

    # Upload Udf
    udf = Udf(xc_api)
    udf.addOrUpdate(mem_udf_name, mem_udf_source)

    dataset_builder = workbook.build_dataset(
        dataset_name, data_target, path, "udf", parser_name=parser_name)
    dataset = dataset_builder.load()
    return dataset


def imd_table(operators, telecom_dataset):
    # XXX: This method will change to use new dataflow apis
    pt = "testImdTable"
    rt = "RegTable"
    ii = 0
    operators.indexDataset(
        telecom_dataset.name,
        "%s%d" % (rt, ii),
        "valDiv10",
        fatptrPrefixName="p")
    ii = ii + 1
    operators.getRowNum("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii), "rowNum")
    ii = ii + 1
    operators.project("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                      ["rowNum", "p-valDiv10"])
    ii = ii + 1
    operators.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii), ["int(1)"],
                  ["XcalarOpCode"])
    ii = ii + 1
    operators.indexTable("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii), "rowNum")
    operators.publish("%s%d" % (rt, ii), pt)
    return pt


def imd_sql_execution(imd_table, session, operators):
    select_table = "imd_select"
    operators.select(imd_table, select_table)
    sql_table = "sql_table"
    sql_query = "SELECT * FROM {}".format(imd_table)
    session.execute_sql(sql_query, sql_table)
    verify_row_count_match(session, sql_table, select_table)
    clean_up(operators)


#
# Test correctness for different XDB page sizes
#
def test_xdbPage_limits():
    WORKBOOK_NAME = "TestDataModel"
    configDict = None
    defaultXdbPageSize = None

    xdb_page_limits = [131072, 2097152]    # min and max limits
    Xdbpage_param_name = "XdbPageSize"
    ii = -1    # do-while
    default_xdb_page_size = None
    while ii < len(xdb_page_limits):
        with DevCluster() as cluster:
            client = cluster.client()
            workbook = client.create_workbook(WORKBOOK_NAME)
            session = workbook.activate()
            xc_api = LegacyApi()
            xc_api.setSession(workbook)
            operators = Operators(xc_api)

            test_ds = telecom_dataset(client, workbook, xc_api)
            pt_name = imd_table(operators, test_ds)
            imd_sql_execution(pt_name, session, operators)

            if not default_xdb_page_size:
                xdb_page_param = client.get_config_params(
                    [Xdbpage_param_name])[0]
                default_xdb_page_size = int(xdb_page_param['default_value'])

            ii += 1
            if ii < len(xdb_page_limits
                        ) and xdb_page_limits[ii] == default_xdb_page_size:
                # we covered default size already
                ii += 1

            if ii >= len(xdb_page_limits):
                # we are done, set value to default
                client.set_config_param(
                    param_name=Xdbpage_param_name, set_to_default=True)
            else:
                # set next xdb page value
                client.set_config_param(
                    param_name=Xdbpage_param_name,
                    param_value=xdb_page_limits[ii])

            operators.unpublish(pt_name)
            test_ds.delete()
            xc_api.setSession(None)
            session.destroy()
            workbook.delete()


#
# Test different Buffer Cache config interactions. Below confluence page
# has all the settings.
# https://xcalar.atlassian.net/wiki/spaces/EN/pages/23396366/HOWTO+Enable+paging+and+other+Buffer+Cache+Settings
#
"""
    if BufferCacheMlock is True:
        # Buffer Cache memory needs to be pinned.
        # Note that BufferCachePercentOfTotalMem cannot exceed available
        # memory.

        if BuffCacheNonTmpFs is True:
            # Buffer Cache memory uses SHM segments instead of TmpFs.
        else:
            # Buffer Cache memory uses TmpFs. So TmpFs needs to be mounted
            # appropriately with enough available memory as specified by
            # BufferCachePercentOfTotalMem.

        if BufferCacheLazyMemLocking:
            # Pin Buffer Cache Memory in a lazy fashion in chunks
            # specified by XdbPageMlockChunkPct.
    else:
        # Buffer Cache memory to not be pinned.
        # Note that BufferCachePercentOfTotalMem can exceed available
        # memory, i.e. can be > 100.

        if BuffCacheNonTmpFs is True:
            # Buffer Cache memory uses SHM segments
        elif BufferCachePath:
            # Create a regular filesystem file in this Buffer Cache path.
        else:
            # Invalid Buffer Cache params. Fail XCE boot.
"""


class BufferCacheConfig:
    def __init__(self, bufferCacheMlock, bufCacheNonTmpFs,
                 bufferCachePercentOfTotalMem, bufferCacheLazyMemLocking,
                 xdbPageMlockChunkPct, bufferCachePath,
                 bufferCacheMlockPercent, xdbSerDesMode, xdbLocalSerDesPath):
        self.bufferCacheMlock = bufferCacheMlock
        self.bufCacheNonTmpFs = bufCacheNonTmpFs
        self.bufferCachePercentOfTotalMem = bufferCachePercentOfTotalMem
        self.bufferCacheLazyMemLocking = bufferCacheLazyMemLocking
        self.xdbPageMlockChunkPct = xdbPageMlockChunkPct
        self.bufferCachePath = bufferCachePath
        self.bufferCacheMlockPercent = bufferCacheMlockPercent
        self.xdbSerDesMode = xdbSerDesMode
        self.xdbLocalSerDesPath = xdbLocalSerDesPath


def setUpBufCacheConfig(testCase, xc_api, bufCacheConfig):
    logger.info(
        "BufferCache settings: Test Case '{}', BufferCacheMlock = {}, BufCacheNonTmpFs = {}, BufferCachePercentOfTotalMem = {}, BufferCacheLazyMemLocking = {}, XdbPageMlockChunkPct = {}, BufferCachePath = {} BufferCacheMlockPercent = {}, XdbSerDesMode = {}, XdbLocalSerDesPath = {}"
        .format(testCase, bufCacheConfig.bufferCacheMlock,
                bufCacheConfig.bufCacheNonTmpFs,
                bufCacheConfig.bufferCachePercentOfTotalMem,
                bufCacheConfig.bufferCacheLazyMemLocking,
                bufCacheConfig.xdbPageMlockChunkPct,
                bufCacheConfig.bufferCachePath,
                bufCacheConfig.bufferCacheMlockPercent,
                bufCacheConfig.xdbSerDesMode,
                bufCacheConfig.xdbLocalSerDesPath))

    xc_api.setConfigParam("BufferCacheMlock", bufCacheConfig.bufferCacheMlock)
    xc_api.setConfigParam("BufCacheNonTmpFs", bufCacheConfig.bufCacheNonTmpFs)
    xc_api.setConfigParam("BufferCachePercentOfTotalMem",
                          bufCacheConfig.bufferCachePercentOfTotalMem)
    xc_api.setConfigParam("BufferCacheLazyMemLocking",
                          bufCacheConfig.bufferCacheLazyMemLocking)
    xc_api.setConfigParam("XdbPageMlockChunkPct",
                          bufCacheConfig.xdbPageMlockChunkPct)
    xc_api.setConfigParam("BufferCachePath", bufCacheConfig.bufferCachePath)
    xc_api.setConfigParam("BufferCacheMlockPercent",
                          bufCacheConfig.bufferCacheMlockPercent)
    xc_api.setConfigParam("XdbSerDesMode", bufCacheConfig.xdbSerDesMode)
    xc_api.setConfigParam("XdbLocalSerDesPath",
                          bufCacheConfig.xdbLocalSerDesPath)


def getDefaultBufCacheConfig(xc_api):
    getConfigOutput = xc_api.getConfigParams()
    for ii in range(0, getConfigOutput.numParams):
        if getConfigOutput.parameter[ii].paramName == "BufferCacheMlock":
            defBufferCacheMlock = getConfigOutput.parameter[ii].paramValue
        elif getConfigOutput.parameter[ii].paramName == "BufCacheNonTmpFs":
            defBufCacheNonTmpFs = getConfigOutput.parameter[ii].paramValue
        elif getConfigOutput.parameter[
                ii].paramName == "BufferCachePercentOfTotalMem":
            defBufferCachePercentOfTotalMem = getConfigOutput.parameter[
                ii].paramValue
        elif getConfigOutput.parameter[
                ii].paramName == "BufferCacheLazyMemLocking":
            defBufferCacheLazyMemLocking = getConfigOutput.parameter[
                ii].paramValue
        elif getConfigOutput.parameter[ii].paramName == "XdbPageMlockChunkPct":
            defXdbPageMlockChunkPct = getConfigOutput.parameter[ii].paramValue
        elif getConfigOutput.parameter[ii].paramName == "BufferCachePath":
            defBufferCachePath = getConfigOutput.parameter[ii].paramValue
        elif getConfigOutput.parameter[
                ii].paramName == "BufferCacheMlockPercent":
            defBufferCacheMlockPercent = getConfigOutput.parameter[
                ii].paramValue
        elif getConfigOutput.parameter[ii].paramName == "XdbSerDesMode":
            defXdbSerDesMode = getConfigOutput.parameter[ii].paramValue
        elif getConfigOutput.parameter[ii].paramName == "XdbLocalSerDesPath":
            defXdbLocalSerDesPath = getConfigOutput.parameter[ii].paramValue
    return BufferCacheConfig(
        defBufferCacheMlock, defBufCacheNonTmpFs,
        defBufferCachePercentOfTotalMem, defBufferCacheLazyMemLocking,
        defXdbPageMlockChunkPct, defBufferCachePath,
        defBufferCacheMlockPercent, defXdbSerDesMode, defXdbLocalSerDesPath)


def runBufCacheConfigs(testCase, nextBufCacheConfig):
    curBufCacheConfig = None
    WORKBOOK_NAME = "TestDataModel"
    with DevCluster() as cluster:
        client = cluster.client()
        workbook = client.create_workbook(WORKBOOK_NAME)
        session = workbook.activate()
        xc_api = LegacyApi()
        xc_api.setSession(workbook)
        operators = Operators(xc_api)

        test_ds = telecom_dataset(client, workbook, xc_api)
        pt_name = imd_table(operators, test_ds)
        imd_sql_execution(pt_name, session, operators)

        # Get the current buffer cache configuration
        curBufCacheConfig = getDefaultBufCacheConfig(xc_api)

        # Set up the next buffer cache configuration
        setUpBufCacheConfig(testCase, xc_api, nextBufCacheConfig)

        operators.unpublish(pt_name)
        test_ds.delete()
        xc_api.setSession(None)
        session.destroy()
        workbook.delete()
    return curBufCacheConfig


@pytest.mark.slow(
    reason=
    "This test will only run in daily tests. Not necessary for pre-checkin.")
def test_buf_cache_configs():
    configDict = None
    defBufCacheConfig = None

    defBufCacheConfig = runBufCacheConfigs(
        "1. Test default Buffer Cache configuration",
        BufferCacheConfig(
            bufferCacheMlock="true",    # Mlocked
            bufCacheNonTmpFs="true",    # SHM Segments
            bufferCachePercentOfTotalMem="70",
            bufferCacheLazyMemLocking="true",
            xdbPageMlockChunkPct="1",
            bufferCachePath="/var/tmp/xcalar",
            bufferCacheMlockPercent="100",
            xdbSerDesMode="2",
            xdbLocalSerDesPath="/var/tmp/xcalar"))

    runBufCacheConfigs(
        "2. Test Buffer Cache Mlock with SHM segments",
        BufferCacheConfig(
            bufferCacheMlock="true",    # Mlocked
            bufCacheNonTmpFs="false",    # TmpFs
            bufferCachePercentOfTotalMem="70",
            bufferCacheLazyMemLocking="false",
            xdbPageMlockChunkPct="5",
            bufferCachePath="/var/tmp/xcalar",
            bufferCacheMlockPercent="100",
            xdbSerDesMode="2",
            xdbLocalSerDesPath="/var/tmp/xcalar"))

    runBufCacheConfigs(
        "3. Test Buffer Cache Mlock with SHM in Tmpfs",
        BufferCacheConfig(
            bufferCacheMlock="false",    # Non-Mlocked
            bufCacheNonTmpFs="true",    # SHM Segments
            bufferCachePercentOfTotalMem="70",
            bufferCacheLazyMemLocking="true",
            xdbPageMlockChunkPct="1",
            bufferCachePath="/var/tmp/xcalar",
            bufferCacheMlockPercent="100",
            xdbSerDesMode="2",
            xdbLocalSerDesPath="/var/tmp/xcalar"))

    runBufCacheConfigs(
        "4. Test Buffer Cache non-Mlock with SHM segments",
        BufferCacheConfig(
            bufferCacheMlock="false",    # Non-Mlocked
            bufCacheNonTmpFs="false",    # Regular FS file
            bufferCachePercentOfTotalMem="15",
            bufferCacheLazyMemLocking="true",
            xdbPageMlockChunkPct="1",
            bufferCachePath="/var/tmp/xcalar",
            bufferCacheMlockPercent="100",
            xdbSerDesMode="2",
            xdbLocalSerDesPath="/var/tmp/xcalar"))

    runBufCacheConfigs(
        "5. Test Buffer Cache non-Mlock with regular FS file",
        BufferCacheConfig(
            bufferCacheMlock="true",    # Partial Mlocked
            bufCacheNonTmpFs="false",    # Regular FS file
            bufferCachePercentOfTotalMem="30",
            bufferCacheLazyMemLocking="true",
            xdbPageMlockChunkPct="1",
            bufferCachePath="/var/tmp/xcalar",
            bufferCacheMlockPercent="50",
            xdbSerDesMode="2",
            xdbLocalSerDesPath="/var/tmp/xcalar"))

    runBufCacheConfigs(
        "6. Test Buffer Cache partial Mlock with regular FS file",
        BufferCacheConfig(
            bufferCacheMlock="true",    # Partial Mlocked
            bufCacheNonTmpFs="true",    # SHM Segments
            bufferCachePercentOfTotalMem="40",
            bufferCacheLazyMemLocking="true",
            xdbPageMlockChunkPct="1",
            bufferCachePath="/var/tmp/xcalar",
            bufferCacheMlockPercent="50",
            xdbSerDesMode="2",
            xdbLocalSerDesPath="/var/tmp/xcalar"))

    runBufCacheConfigs(
        "7. Test Buffer Cache partial Mlock with SHM segments",
        BufferCacheConfig(
            bufferCacheMlock=defBufCacheConfig.bufferCacheMlock,
            bufCacheNonTmpFs=defBufCacheConfig.bufCacheNonTmpFs,
            bufferCachePercentOfTotalMem=defBufCacheConfig.
            bufferCachePercentOfTotalMem,
            bufferCacheLazyMemLocking=defBufCacheConfig.
            bufferCacheLazyMemLocking,
            xdbPageMlockChunkPct=defBufCacheConfig.xdbPageMlockChunkPct,
            bufferCachePath=defBufCacheConfig.bufferCachePath,
            bufferCacheMlockPercent=defBufCacheConfig.bufferCacheMlockPercent,
            xdbSerDesMode=defBufCacheConfig.xdbSerDesMode,
            xdbLocalSerDesPath=defBufCacheConfig.xdbLocalSerDesPath))

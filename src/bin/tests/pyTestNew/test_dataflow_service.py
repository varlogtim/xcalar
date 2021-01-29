import pytest
import json
import time
import os
import re
import random

from xcalar.compute.util.Qa import freezeRows, getFilteredDicts
from xcalar.compute.coretypes.JoinOpEnums.ttypes import JoinOperatorT
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.QueryStateEnums.ttypes import QueryStateT

from xcalar.external.dataflow import Dataflow
from xcalar.external.result_set import ResultSet
from xcalar.external.runtime import Runtime

from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException, XcalarApi as LegacyApi
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.compute.util.Qa import (XcalarQaDatasetPath, DefaultTargetName)

from xcalar.compute.coretypes.LibApisEnums.ttypes import XcalarApisT
from xcalar.compute.coretypes.LibApisConstants.ttypes import XcalarApisConstantsT

mem_udf_name = "testOperators"
udf_sum = mem_udf_name + ":sumAll"
flaky_frequency = 10
num_dataset_rows = 1000

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
        rowDict = {"val": ii, "valStr": str(ii), "valFloat": float(ii), "valDiv10": ii//10, "constant": 1, "date": randdate(), "nullVal": None, "mixedType": None}
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

sleep_udf_source = """
import time
def sleep_fn():
    time.sleep(10)
    return "Done"
"""


# trace back strings expected in a single eval scenario when funcFails6 is
# invoked on DayOfWeek column in the canonical airlines csv dataset from
# netstore.
#
# The strings appear in the array, from most frequent to least - the order
# depends on input dataset, of course.
#
# NOTE: see NOTE above UDF source code, regarding line numbers in the strings
# below
#

tbsFromFunc6 = [(20518, """Traceback (most recent call last):
  File "map_udf_fail.py", line 14, in funcFails6
    ncol = 2/0
ZeroDivisionError: division by zero"""),
                (20212, """Traceback (most recent call last):
  File "map_udf_fail.py", line 20, in funcFails6
    ncol = 5/0
ZeroDivisionError: division by zero"""),
                (20210, """Traceback (most recent call last):
  File "map_udf_fail.py", line 22, in funcFails6
    ncol = 6/0
ZeroDivisionError: division by zero"""),
                (20130, """Traceback (most recent call last):
  File "map_udf_fail.py", line 18, in funcFails6
    ncol = 4/0
ZeroDivisionError: division by zero"""),
                (20001, """Traceback (most recent call last):
  File "map_udf_fail.py", line 16, in funcFails6
    ncol = 3/0
ZeroDivisionError: division by zero""")]

# tracebacks expected in a multi-eval scenario:
# invoke funcFails6 in each of 2 eval strings: first one on DayOfWeek column,
# and second one on TaxiIn column of the AirlinesCarrier.csv dataset in
# netstore. The tbsFromFunc6MultiE1[] array enumerates the tracebacks expected
# for the first eval string
#
# NOTE: see NOTE above UDF source code, regarding line numbers in the strings
# below
#

tbsFromFunc6MultiE1 = [(20518, """Traceback (most recent call last):
  File "map_udf_fail.py", line 14, in funcFails6
    ncol = 2/0
ZeroDivisionError: division by zero"""),
                       (20212, """Traceback (most recent call last):
  File "map_udf_fail.py", line 20, in funcFails6
    ncol = 5/0
ZeroDivisionError: division by zero"""),
                       (20210, """Traceback (most recent call last):
  File "map_udf_fail.py", line 22, in funcFails6
    ncol = 6/0
ZeroDivisionError: division by zero"""),
                       (20130, """Traceback (most recent call last):
  File "map_udf_fail.py", line 18, in funcFails6
    ncol = 4/0
ZeroDivisionError: division by zero"""),
                       (20001, """Traceback (most recent call last):
  File "map_udf_fail.py", line 16, in funcFails6
    ncol = 3/0
ZeroDivisionError: division by zero""")]

# Same as above, except: tbsFromFunc6MultiE2[] array enumerates the tracebacks
# expected for the second eval string
#
# NOTE: see NOTE above UDF source code, regarding line numbers in the strings
# below
#

tbsFromFunc6MultiE2 = [(55067, """Traceback (most recent call last):
  File "map_udf_fail.py", line 24, in funcFails6
    ncol = funcFails1()
  File "map_udf_fail.py", line 6, in funcFails1
    y = time.time()
NameError: name 'time' is not defined"""),
                       (20917, """Traceback (most recent call last):
  File "map_udf_fail.py", line 20, in funcFails6
    ncol = 5/0
ZeroDivisionError: division by zero"""),
                       (17474, """Traceback (most recent call last):
  File "map_udf_fail.py", line 22, in funcFails6
    ncol = 6/0
ZeroDivisionError: division by zero"""),
                       (15503, """Traceback (most recent call last):
  File "map_udf_fail.py", line 18, in funcFails6
    ncol = 4/0
ZeroDivisionError: division by zero"""),
                       (6264, """Traceback (most recent call last):
  File "map_udf_fail.py", line 16, in funcFails6
    ncol = 3/0
ZeroDivisionError: division by zero""")]

# tracebacks expected in a multi-eval scenario: invoke funcFailsMulti in each
# of 2 eval strings: first one on DayOfWeek column, and second one on TaxiIn
# column of the AirlinesCarrier.csv dataset in netstore. The
# tbsFromFuncMultiE1[] array enumerates the tracebacks expected for the first
# eval string
#
# NOTE: see NOTE above UDF source code, regarding line numbers in the strings
# below
#

tbsFromFuncMultiE1 = [(20518, """Traceback (most recent call last):
  File "map_udf_fail.py", line 44, in funcFailsMulti
    ncol = 2/0
ZeroDivisionError: division by zero"""),
                      (20212, """Traceback (most recent call last):
  File "map_udf_fail.py", line 50, in funcFailsMulti
    ncol = 5/0
ZeroDivisionError: division by zero"""),
                      (20210, """Traceback (most recent call last):
  File "map_udf_fail.py", line 52, in funcFailsMulti
    ncol = 6/0
ZeroDivisionError: division by zero"""),
                      (20130, """Traceback (most recent call last):
  File "map_udf_fail.py", line 48, in funcFailsMulti
    ncol = 4/0
ZeroDivisionError: division by zero"""),
                      (17722, """Traceback (most recent call last):
  File "map_udf_fail.py", line 58, in funcFailsMulti
    ncol = funcFails1()
  File "map_udf_fail.py", line 6, in funcFails1
    y = time.time()
NameError: name 'time' is not defined""")]

# Same as above, except that tbsFromFuncMultiE2[] array enumerates the
# tracebacks expected for the second eval string
#
# NOTE: see NOTE above UDF source code, regarding line numbers in the strings
# below
#

tbsFromFuncMultiE2 = [(47493, """Traceback (most recent call last):
  File "map_udf_fail.py", line 58, in funcFailsMulti
    ncol = funcFails1()
  File "map_udf_fail.py", line 6, in funcFails1
    y = time.time()
NameError: name 'time' is not defined"""),
                      (20917, """Traceback (most recent call last):
  File "map_udf_fail.py", line 50, in funcFailsMulti
    ncol = 5/0
ZeroDivisionError: division by zero"""),
                      (17474, """Traceback (most recent call last):
  File "map_udf_fail.py", line 52, in funcFailsMulti
    ncol = 6/0
ZeroDivisionError: division by zero"""),
                      (15503, """Traceback (most recent call last):
  File "map_udf_fail.py", line 48, in funcFailsMulti
    ncol = 4/0
ZeroDivisionError: division by zero"""),
                      (3568, """Traceback (most recent call last):
  File "map_udf_fail.py", line 44, in funcFailsMulti
    ncol = 2/0
ZeroDivisionError: division by zero""")]

# trace back strings expected for funcFails5 function (from most frequent to
# least - the order depends on input dataset which in this case is the
# canonical airlines csv dataset from netstore)
#
# NOTE: see NOTE above UDF source code, regarding line numbers in the strings
# below
#

tbsFromFunc5 = [(20212, """Traceback (most recent call last):
  File "map_udf_fail.py", line 35, in funcFails5
    ncol = 5/0
ZeroDivisionError: division by zero"""),
                (20210, """Traceback (most recent call last):
  File "map_udf_fail.py", line 37, in funcFails5
    ncol = 6/0
ZeroDivisionError: division by zero"""),
                (20130, """Traceback (most recent call last):
  File "map_udf_fail.py", line 33, in funcFails5
    ncol = 4/0
ZeroDivisionError: division by zero"""),
                (20001, """Traceback (most recent call last):
  File "map_udf_fail.py", line 31, in funcFails5
    ncol = 3/0
ZeroDivisionError: division by zero"""),
                (17722, """Traceback (most recent call last):
  File "map_udf_fail.py", line 39, in funcFails5
    ncol = funcFails1()
  File "map_udf_fail.py", line 6, in funcFails1
    y = time.time()
NameError: name 'time' is not defined""")]

# definitions for the udf_visibility tests (see ENG-657)
num_udf_ds_rows = 5
bump_val_A = 1
bump_val_B = 5
mem_udf_visibility_name = "testUdfVisibility"
mem_udf_visibility_A_src = """
import json

def load(fullPath, inStream):
    inObj = json.loads(inStream.read())
    numLocalRows = inObj["numRows"]
    startNum = inObj["startRow"]
    for ii in range(startNum, startNum+numLocalRows):
        rowDict = {"val": ii, "valBump": ii + %s }
        yield rowDict
""" % bump_val_A

# Traceback for filter udf errors
filterUdfErrors = [
    """Argument 0 failed with the following error:\nTraceback (most recent call last):
  File "filter_udf.py", line 8, in fail
    ncol = 1/0
ZeroDivisionError: division by zero
\nWhile processing the next outer nested level, the following error occurred:\n
Traceback (most recent call last):
  File "filter_udf.py", line 6, in fail
    if (\'most\' in col):
TypeError: argument of type \'NoneType\' is not iterable""",
    "XCE-00000108 Some variables are undefined during evaluation",
    """Argument 0 failed with the following error:
Traceback (most recent call last):
  File \"filter_udf.py\", line 8, in fail
    ncol = 1/0
ZeroDivisionError: division by zero
\nWhile processing the next outer nested level, the following error occurred:
\nXCE-00000108 Some variables are undefined during evaluation""",
    """Argument 0 failed with the following error:
Traceback (most recent call last):
  File \"filter_udf.py\", line 12, in test_div
    return int(col)/0
ZeroDivisionError: division by zero\n
Argument 1 failed with the following error:
Traceback (most recent call last):
  File \"filter_udf.py\", line 12, in test_div
    return int(col)/0
ZeroDivisionError: division by zero\n
While processing the next outer nested level, the following error occurred:\n
XCE-00000108 Some variables are undefined during evaluation""",
    """Argument 0 failed with the following error:
Argument 0 failed with the following error:
Traceback (most recent call last):
  File "filter_udf.py", line 12, in test_div
    return int(col)/0
ZeroDivisionError: division by zero\n
While processing the next outer nested level, the following error occurred:\n
Traceback (most recent call last):
  File "filter_udf.py", line 12, in test_div
    return int(col)/0
TypeError: int() argument must be a string, a bytes-like object or a number, not 'NoneType'\n
While processing the next outer nested level, the following error occurred:\n
Traceback (most recent call last):
  File "filter_udf.py", line 12, in test_div
    return int(col)/0
TypeError: int() argument must be a string, a bytes-like object or a number, not 'NoneType'""",
]

mem_udf_visibility_B_src = """
import json

def load(fullPath, inStream):
    inObj = json.loads(inStream.read())
    numLocalRows = inObj["numRows"]
    startNum = inObj["startRow"]
    for ii in range(startNum, startNum+numLocalRows):
        rowDict = {"val": ii, "valBump": ii + %s}
        yield rowDict
""" % bump_val_B

# Strings below support testing of UDF failure handling. The UDF assumes the
# canonical airlines csv dataset in netstore with the "DayOfWeek" column
# (and the "TaxiIn" column when doing multi-eval), being mapped by the test.
#
# First string is the UDF source code:
#      6 unique failure strings expected to be generated by funcFails6,
#      5 by funcFails5, and
#     11 by funcFailsMulti
#
# NOTE: NOTE: NOTE:
# If any code in this UDF is modified, be cognizant about line number
# impact on traceback strings (see tbsFromFunc6, tbsFromFunc5, etc. below).
# The traceback strings reference the line numbers in this UDF and must
# remain unchanged, or modified to reflect any changes to the UDF
# NOTE: NOTE: NOTE:
#
map_udf_fail_source = """
def funcPasses(col):
    return(int(col)+1)

def funcFails1():
    y = time.time()
    l = []
    x['test'] = l
    z = json.dumps(x)
    return 1

def funcFails6(col):
    if int(col) < 2:
        ncol = 2/0
    elif int(col) < 3:
        ncol = 3/0
    elif int(col) < 4:
        ncol = 4/0
    elif int(col) < 5:
        ncol = 5/0
    elif int(col) < 6:
        ncol = 6/0
    else:
        ncol = funcFails1()
    return(ncol+2)

def funcFails5(col):
    if int(col) < 2:
        ncol = 2/1 # --> one failure fixed
    elif int(col) < 3:
        ncol = 3/0
    elif int(col) < 4:
        ncol = 4/0
    elif int(col) < 5:
        ncol = 5/0
    elif int(col) < 6:
        ncol = 6/0
    else:
        ncol = funcFails1()
    return(ncol+2)

def funcFailsMulti(col):
    if int(col) < 2:
        ncol = 2/0
    elif int(col) < 3:
        ncol = 3/1
    elif int(col) < 4:
        ncol = 4/0
    elif int(col) < 5:
        ncol = 5/0
    elif int(col) < 6:
        ncol = 6/0
    elif int(col) == 9:
        ncol = 9/1
    elif int(col) == 13:
        ncol = 13/0
    else:
        ncol = funcFails1()
    return(ncol+2)

def test_none(col):
    if col is None:
        return "Empty"
    return a

def test_div(col):
    return int(col)/0
"""

map_udf_perf_source = """
def funcPasses(col):
    return(int(col)+1)

def funcFailsAll(col):
    ncol = 1/0
    return (ncol+1)
"""

filter_udf_source = """
def add_one(col):
    return (int(col)+1)

def fail(col):
    if ('most' in col):
        return 1
    ncol = 1/0
    return ncol + 1

def test_div(col):
    return int(col)/0
"""

# The 'str' type isn't supported; used to validate failure
udf_bad_type_str_source = """
def funcUnsuppType(col) -> str:
	return (col)
"""

# Illegal return type annotation should trigger a failure
udf_bad_type_foo_source = """
def funcBadType(col) -> foo:
	return (col)
"""

# Used to validate the supported return type annotations
udf_good_types_source = """
def funcRetInt(col) -> int:
	return (col)

def funcRetInt(col) -> bytes:
	return (col)

def funcRetInt(col) -> float:
	return (col)

def funcRetInt(col) -> bool:
	return (col)
"""

# Check enforcement of return type, validate failure in returning a
# string when return type annotation is int

fdescFromFuncRet =\
    [(98583, """TypeError: an integer is required (got type str)\n""")]

map_udf_ret_type_source = """
import random
def funcRetIntFail(col) -> int:
    if int(col) < 2:
        # this is incompatible return
        return "2"
    elif int(col) < 3:
        # this is incompatible return
        return ",".join([str(random.random()) for _ in range(10)])
    elif int(col) < 4:
        # this is incompatible return
        return "4"
    elif int(col) < 5:
        # this is incompatible return
        return "5"
    elif int(col) < 6:
        # this is compatible/correct
        return 6
    else:
        # this is incompatible return
        return "7"
"""

table_access_xpu_udf = """
import xcalar.container.context as ctx
import xcalar.container.cluster
import xcalar.container.table

from xcalar.external.client import Client

logger = ctx.get_logger()


def duplicate_table(fullPath, inStream, session_name, table_name):
    cluster = xcalar.container.cluster.get_running_cluster()
    tab_obj = None
    node_0_master = cluster.local_master_for_node(0)
    if cluster.my_xpu_id == node_0_master:
        # assuming, session is present on master node
        client = Client(bypass_proxy=True)
        sess = client.get_session(session_name)
        tab = sess.get_table(table_name)
        xdb_id = tab._get_meta().xdb_id
        tab_info = {
            "xdb_id":
                xdb_id,
            "columns": [{
                "columnName": col,
                "headerAlias": col
            } for col in tab.columns]
        }
        cluster.broadcast_msg(tab_info)
    else:
        tab_info = cluster.recv_msg()
    tab_obj = xcalar.container.table.Table(tab_info["xdb_id"],
                                           tab_info["columns"])
    yield from tab_obj.partitioned_rows()
"""


@pytest.fixture(scope="module")
def session(workbook):
    session = workbook.activate()
    yield session
    session.destroy()


@pytest.fixture(scope="module")
def xc_api(workbook):
    xc_api = LegacyApi()
    xc_api.setSession(workbook)
    yield xc_api
    xc_api.setSession(None)


@pytest.fixture(scope="module")
def operators(xc_api):
    operators = Operators(xc_api)
    yield operators


@pytest.fixture(scope="module")
def telecom_dataset(client, workbook):
    dataset_name = "testDataflowServiceDs"
    path = str(num_dataset_rows)
    parser_name = "{}:load".format(mem_udf_name)
    target_name = "qa_memory_target"
    target_type_id = "memory"

    # Add target
    data_target = client.add_data_target(target_name, target_type_id)

    # Upload Udf
    workbook._create_or_update_udf_module(mem_udf_name, mem_udf_source)

    dataset_builder = workbook.build_dataset(
        dataset_name, data_target, path, "udf", parser_name=parser_name)
    dataset = dataset_builder.load()
    yield dataset
    dataset.delete()


def udf_create_visibility(client, workbook, session):
    dataset_name = "testDataflowGS"
    path = str(num_udf_ds_rows)
    parser_name = "{}:load".format(mem_udf_visibility_name)
    target_name = "qa_memory_target"
    target_type_id = "memory"

    # Add target
    data_target = client.add_data_target(target_name, target_type_id)

    # create shared UDF and use it in a load op; testing that created UDF
    # is visible to the load - if not, the load will fail (see ENG-657)
    sudf = client.create_udf_module(mem_udf_visibility_name,
                                    mem_udf_visibility_A_src)
    dataset_builder = workbook.build_dataset(
        dataset_name, data_target, path, "udf", parser_name=parser_name)
    dataset = dataset_builder.load()
    dataset.delete()
    sudf.delete()

    # create workbook UDF and use it in a load op; testing that created UDF
    # is visible to the load - if not, the load will fail (see ENG-657)
    myudf = workbook.create_udf_module(mem_udf_visibility_name,
                                       mem_udf_visibility_A_src)
    dataset_builder = workbook.build_dataset(
        dataset_name, data_target, path, "udf", parser_name=parser_name)
    dataset = dataset_builder.load()
    dataset.delete()
    myudf.delete()


#
# The following test loops over the following two actions (see ENG-657):
#     - update a UDF
#     - use the UDF as a streaming UDF in a load operation
#     - verify that the loaded dataset matches updated UDF's actions
#
# The update toggles between two different UDFs: mem_udf_visibility_A_src and
# mem_udf_visibility_B_src
#
def udf_update_visibility(client, workbook, session):
    dataset_name = "testDataflowGS"
    path = str(num_udf_ds_rows)
    parser_name = "{}:load".format(mem_udf_visibility_name)
    target_name = "qa_memory_target"
    target_type_id = "memory"

    # Add target
    data_target = client.add_data_target(target_name, target_type_id)

    # Create UDF with mem_udf_visibility_A_src: when this UDF is used as an import
    # UDF while loading from a memory target, it results in the creation of
    # a small table with two columns: val and valBump.
    #
    # If udf_gs_A_src is used, valBump is val + val_bump_A, but if
    # udf_gs_B_src is used, valBump is val + val_bump_B. See source code for
    # UDFs above. So the loop toggles between the two UDFs, updating the
    # same UDF module, and then verifies that the update has worked by
    # asserting the results of the UDF execution

    myudf = workbook.create_udf_module(mem_udf_visibility_name,
                                       mem_udf_visibility_A_src)

    for ii in range(0, 10):
        dataset_builder = workbook.build_dataset(
            dataset_name, data_target, path, "udf", parser_name=parser_name)
        dataset = dataset_builder.load()

        df = Dataflow.create_dataflow_from_dataset(client, dataset)
        session.execute_dataflow(df, "gstab", is_async=False)
        t1 = session.get_table("gstab")
        t1.show()
        r0 = t1.get_row(0)
        print("row 0 is {}".format(r0))

        for jj in range(0, t1.record_count()):
            row = t1.get_row(jj)
            if ii % 2 == 0:
                assert row['valBump'] == row['val'] + bump_val_A
            else:
                assert row['valBump'] == row['val'] + bump_val_B
        dataset.delete()
        t1.drop()
        if ii % 2 == 0:
            myudf.update(mem_udf_visibility_B_src)
        else:
            myudf.update(mem_udf_visibility_A_src)

    myudf.delete()


# The following test loops over the following two actions (see ENG-657):
#     - create a UDF
#     - use the UDF as a streaming UDF in a load operation
# The loop is in the outer routine
#
def test_udf_create_visibility_loop(client, workbook, session):
    for i in range(0, 10):
        print("test udf create visibility loop {}".format(i))
        udf_create_visibility(client, workbook, session)


#
# The following test loops over the following two actions (see ENG-657):
#     - update a UDF
#     - use the UDF as a streaming UDF in a load operation
#     - verify that the loaded dataset matches updated UDF's actions
#
# At each loop iteration, The update toggles between two different UDFs
#
# This test helps to repro part of ENG-657 in a multi-node cluster using
# the shared root across NFS since the UDFs are stored persistently in
# the shared root
#
# The loop is in the inner routine but one can bump up the outer loop for
# more longevity style testing
#
def test_udf_update_visibility_loop(client, workbook, session):
    for i in range(0, 1):
        print("test udf update visibility loop {}".format(i))
        udf_update_visibility(client, workbook, session)


@pytest.fixture(scope="module")
def airlines_dataset(client, workbook):
    dataset_name = "airlinesDs"
    dataset_builder = workbook.build_dataset(
        dataset_name,
        data_target=DefaultTargetName,
        path="/netstore/datasets/AirlinesCarrier.csv",
        import_format="csv")
    dataset = dataset_builder.load()
    yield dataset
    dataset.delete()


@pytest.fixture(scope="module")
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

    # Cleanup tables
    operators.dropTable("*")

    yield pt
    operators.unpublish(pt)


# Helper function
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


# Helper function
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


# Helper function
def verify_row_count(session, src_table, answer_rows):
    t = session.get_table(src_table)
    assert t.record_count() == answer_rows


# Helper function
def verify_row_count_match(session, src_table, answer_table):
    t1 = session.get_table(src_table)
    t2 = session.get_table(answer_table)
    assert t1.record_count() == t2.record_count()


# Helper function
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


# Helper function
def verify_column_match(session, src_table, df):
    t = session.get_table(src_table)
    assert t.columns == df.table_columns


# Helper function
def clean_up(operators, tables=['*'], constants=['*']):
    for table in tables:
        operators.dropTable(table)

    for constant in constants:
        operators.dropConstants(constant)


def test_imd_sql_execution(imd_table, session, operators):
    select_table = "imd_select"
    operators.select(imd_table, select_table)
    sql_table = "sql_table"
    sql_query = "SELECT * FROM {}".format(imd_table)
    session.execute_sql(sql_query, sql_table)
    verify_row_count_match(session, sql_table, select_table)
    clean_up(operators)


def test_imd_xdb_sql_execution(imd_table, session, operators):
    select_table = "imd_select"
    operators.select(imd_table, select_table)
    sql_table = "sql_table"
    sql_query = "(SELECT 'P-VALDIV10', rownum FROM TestIMDTable) UNION ALL (SELECT 'P-VALDIV10', rownum FROM Imd_Select)".format(
        imd_table, select_table)
    session.execute_sql(sql_query, sql_table)
    verify_row_count(session, sql_table, 2 * num_dataset_rows)
    clean_up(operators)


def test_sql_name_resolution(imd_table, session, operators):
    operators.select(imd_table, imd_table, filterString="le(rowNum, 10)")
    sql_table = "sql_table"
    sql_query = "(SELECT * FROM TESTIMDTABLE)"
    session.execute_sql(sql_query, sql_table)
    verify_row_count(session, sql_table, 10)
    clean_up(operators)


def test_multiple_map(client, workbook, session, operators, telecom_dataset):
    df = Dataflow.create_dataflow_from_dataset(client, telecom_dataset)
    df1 = df.aggregate("max", "constant", "const")\
            .map([("add(val, valDiv10)", "add1")])
    df2 = df.map([("add(val, add(^const, valDiv10))", "add2")])
    df3 = df.map([("int({}(^const, valDiv10, val))".format(udf_sum), "udf")])

    # Do all three map at once
    df4 = df.map([("add(val, valDiv10)", "add1_"),
                  ("add(val, add(^const, valDiv10))", "add2_"),
                  ("int({}(^const, valDiv10, val))".format(udf_sum), "udf_")])

    session.execute_dataflow(df1, "map1", is_async=False)
    session.execute_dataflow(df2, "map2", is_async=False)
    session.execute_dataflow(df3, "map3", is_async=False)
    session.execute_dataflow(df4, "multipleMap", is_async=False)

    verify_column_match(session, "map1", df1)
    verify_column_match(session, "map2", df2)
    verify_column_match(session, "map3", df3)
    verify_column_match(session, "multipleMap", df4)

    verify_checksum(client, session, operators, "map1", "add1", "multipleMap",
                    "add1_")
    verify_checksum(client, session, operators, "map2", "add2", "multipleMap",
                    "add2_")
    verify_checksum(client, session, operators, "map3", "udf", "multipleMap",
                    "udf_")
    clean_up(operators)


def test_multiple_groupby(client, workbook, session, operators,
                          telecom_dataset):
    df = Dataflow.create_dataflow_from_dataset(client, telecom_dataset)
    df1 = df.aggregate("max", "constant", "const")\
        .groupby(aggrs_info=[{
                'operator': 'sum',
                'aggColName': 'val',
                'newColName': 'sum1'
                            }], group_on=[
                'valDiv10'
        ])
    df2 = df.groupby(
        aggrs_info=[{
            'operator': 'sum',
            'aggColName': '^const',
            'newColName': 'sum2'
        }],
        group_on=['valDiv10'])
    df3 = df.groupby(
        aggrs_info=[{
            'operator': 'count',
            'aggColName': 'valDiv10',
            'newColName': 'count'
        }],
        group_on=['valDiv10'])

    # Do all three aggregate with groupby at once
    df4 = df.groupby(
        aggrs_info=[{
            'operator': 'sum',
            'aggColName': 'val',
            'newColName': 'sum1_'
        }, {
            'operator': 'sum',
            'aggColName': '^const',
            'newColName': 'sum2_'
        },
                    {
                        'operator': 'count',
                        'aggColName': 'valDiv10',
                        'newColName': 'count_'
                    }],
        group_on=['valDiv10'])
    session.execute_dataflow(df1, "groupBy1", is_async=False)
    session.execute_dataflow(df2, "groupBy2", is_async=False)
    session.execute_dataflow(df3, "groupBy3", is_async=False)
    session.execute_dataflow(df4, 'groupByMultiple', is_async=False)
    verify_column_match(session, "groupBy1", df1)
    verify_column_match(session, "groupBy2", df2)
    verify_column_match(session, "groupBy3", df3)
    verify_column_match(session, "groupByMultiple", df4)
    verify_checksum(client, session, operators, "groupBy1", 'sum1',
                    'groupByMultiple', "sum1_")
    verify_checksum(client, session, operators, "groupBy2", 'sum2',
                    'groupByMultiple', "sum2_")
    verify_checksum(client, session, operators, "groupBy3", 'count',
                    'groupByMultiple', "count_")

    # clean up
    clean_up(operators)


def test_union_all(client, workbook, session, operators, telecom_dataset):
    df = Dataflow.create_dataflow_from_dataset(client, telecom_dataset)
    df1 = df.map([("add(1, 0)", "col1")])
    df2 = df.map([("add(1, 0)", "col2")])
    df3 = df.map([("add(1, 0)", "col3")])

    columns = [[{
        "name": "col1",
        "type": "float"
    }], [{
        "name": "col2",
        "type": "float"
    }], [{
        "name": "col3",
        "type": "float"
    }]]

    df4 = Dataflow.union([df1, df2, df3], columns, ["col_"])
    session.execute_dataflow(df4, "union_", is_async=False)
    verify_row_count(session, "union_", 3 * num_dataset_rows)
    verify_checksum_value(client, session, operators, "union_", 'col_',
                          3 * num_dataset_rows)

    clean_up(operators)


def test_union_dedup(client, workbook, session, operators, telecom_dataset):
    df = Dataflow.create_dataflow_from_dataset(client, telecom_dataset)
    df1 = df.map([("add(0, valDiv10)", "col1"), ("add(1, 0)", 'col2'),
                  ("add(0, 0)", "col3")])
    df2 = df.map([("add(0, valDiv10)", "col1"), ("add(1, 0)", 'col2'),
                  ("add(0, 0)", "col3")])
    df3 = df.map([("add(0, valDiv10)", "col1"), ("add(1, 0)", 'col2'),
                  ("add(1, val)", "col3")])
    columns = [[{
        "name": "col1",
        "type": "float"
    }, {
        "name": "col2",
        "type": "float"
    }, {
        "name": "col3",
        "type": "float"
    }],
               [{
                   "name": "col1",
                   "type": "float"
               }, {
                   "name": "col2",
                   "type": "float"
               }, {
                   "name": "col3",
                   "type": "float"
               }],
               [{
                   "name": "col1",
                   "type": "float"
               }, {
                   "name": "col2",
                   "type": "float"
               }, {
                   "name": "col3",
                   "type": "float"
               }]]

    df4 = Dataflow.union([df1, df2, df3],
                         columns, ["col1_", "col2_", "col3_"],
                         dedup=True)
    session.execute_dataflow(df4, "union123", is_async=False)
    verify_row_count(session, "union123",
                     num_dataset_rows + (num_dataset_rows // 10))
    verify_column_match(session, "union123", df4)

    clean_up(operators)


# Helper function
def setup_set_tables(client, col_tweak, telecom_dataset):

    df = Dataflow.create_dataflow_from_dataset(client, telecom_dataset)
    df1 = df.map([("add(0, prn1)", "prn1"),
                  ("add({}, prn2)".format(col_tweak[0]), "prn2"),
                  ("add(0, prn3)", "prn3")])
    df2 = df.map([("add(0, prn1)", "prn1"),
                  ("add({}, prn2)".format(col_tweak[1]), "prn2"),
                  ("add(0, prn3)", "prn3")])
    df3 = df.map([("add(0, prn1)", "prn1"),
                  ("add({}, prn2)".format(col_tweak[2]), "prn2"),
                  ("add(0, prn3)", "prn3")])

    src_columns = [[{
        "name": "prn1",
        "type": "float"
    }, {
        "name": "prn2",
        "type": "float"
    }, {
        "name": "prn3",
        "type": "float"
    }],
                   [{
                       "name": "prn1",
                       "type": "float"
                   }, {
                       "name": "prn2",
                       "type": "float"
                   }, {
                       "name": "prn3",
                       "type": "float"
                   }],
                   [{
                       "name": "prn1",
                       "type": "float"
                   }, {
                       "name": "prn2",
                       "type": "float"
                   }, {
                       "name": "prn3",
                       "type": "float"
                   }]]

    dest_columns = ['prn1r', 'prn2r', 'prn3r']

    return ([df1, df2, df3], src_columns, dest_columns)


def test_intersect_one(client, workbook, session, operators, telecom_dataset):
    src_df, src_columns, dest_columns = setup_set_tables(
        client, [0, 1, 0], telecom_dataset)
    df4 = Dataflow.intersect(src_df, src_columns, dest_columns, dedup=True)
    df1, df2, df3 = src_df
    session.execute_dataflow(df4, 'settab', is_async=False)
    session.execute_dataflow(df1, 'index1', is_async=False)
    session.execute_dataflow(df2, 'index2', is_async=False)
    session.execute_dataflow(df3, 'index3', is_async=False)
    t1 = session.get_table("index1")
    t2 = session.get_table("index2")
    t3 = session.get_table("index3")
    t4 = session.get_table("settab")

    # Helper function
    def get_set(t):
        return freezeRows(
            getFilteredDicts(t._result_set(), ["prn1", "prn2", "prn3"],
                             dest_columns))

    s1 = get_set(t1)
    s2 = get_set(t2)
    s3 = get_set(t3)
    s4 = freezeRows(getFilteredDicts(t4._result_set(), dest_columns))

    assert s4 == s1 & s2 & s3

    clean_up(operators)


def test_intersect_all(client, workbook, session, operators, telecom_dataset):
    src_df, src_columns, dest_columns = setup_set_tables(
        client, [0, 1, 0], telecom_dataset)
    df4 = Dataflow.intersect(src_df, src_columns, dest_columns)
    df1, df2, df3 = src_df
    session.execute_dataflow(df4, 'settab', is_async=False)
    session.execute_dataflow(df1, 'index1', is_async=False)
    session.execute_dataflow(df2, 'index2', is_async=False)
    session.execute_dataflow(df3, 'index3', is_async=False)
    t1 = session.get_table("index1")
    t2 = session.get_table("index2")
    t3 = session.get_table("index3")
    t4 = session.get_table("settab")

    # Helper function
    def get_set(t):
        return freezeRows(
            getFilteredDicts(t._result_set(), ["prn1", "prn2", "prn3"],
                             dest_columns), True)

    s1 = get_set(t1)
    s2 = get_set(t2)
    s3 = get_set(t3)
    s4 = freezeRows(getFilteredDicts(t4._result_set(), dest_columns), True)

    assert s4 == s1 & s2 & s3
    clean_up(operators)


def test_except_one(client, workbook, session, operators, telecom_dataset):
    src_df, src_columns, dest_columns = setup_set_tables(
        client, [0, 1, 1], telecom_dataset)
    df4 = Dataflow.except_(src_df, src_columns, dest_columns)
    df1, df2, df3 = src_df
    session.execute_dataflow(df4, 'settab', is_async=False)
    session.execute_dataflow(df1, 'index1', is_async=False)
    session.execute_dataflow(df2, 'index2', is_async=False)
    session.execute_dataflow(df3, 'index3', is_async=False)
    t1 = session.get_table("index1")
    t2 = session.get_table("index2")
    t3 = session.get_table("index3")
    t4 = session.get_table("settab")

    # Helper function
    def get_set(t):
        return freezeRows(
            getFilteredDicts(t._result_set(), ["prn1", "prn2", "prn3"],
                             dest_columns))

    s1 = get_set(t1)
    s2 = get_set(t2)
    s3 = get_set(t3)
    s4 = freezeRows(getFilteredDicts(t4._result_set(), dest_columns))

    assert s4 == s1 - s2 - s3
    clean_up(operators)


def test_except_all(client, workbook, session, operators, telecom_dataset):
    src_df, src_columns, dest_columns = setup_set_tables(
        client, [0, 1, 1], telecom_dataset)
    df4 = Dataflow.except_(src_df, src_columns, dest_columns, dedup=True)
    df1, df2, df3 = src_df
    session.execute_dataflow(df4, 'settab', is_async=False)
    session.execute_dataflow(df1, 'index1', is_async=False)
    session.execute_dataflow(df2, 'index2', is_async=False)
    session.execute_dataflow(df3, 'index3', is_async=False)
    t1 = session.get_table("index1")
    t2 = session.get_table("index2")
    t3 = session.get_table("index3")
    t4 = session.get_table("settab")
    verify_column_match(session, "settab", df4)
    verify_column_match(session, "index1", df1)
    verify_column_match(session, "index2", df2)
    verify_column_match(session, "index3", df3)

    # Helper function
    def get_set(t):
        return freezeRows(
            getFilteredDicts(t._result_set(), ["prn1", "prn2", "prn3"],
                             dest_columns), True)

    s1 = get_set(t1)
    s2 = get_set(t2)
    s3 = get_set(t3)
    s4 = freezeRows(getFilteredDicts(t4._result_set(), dest_columns), True)

    assert s4 == s1 - s2 - s3
    clean_up(operators)


def test_intercept_except_union(client, workbook, session, operators,
                                telecom_dataset):
    df1 = Dataflow.create_dataflow_from_dataset(client, telecom_dataset)
    df2 = df1.map([("int(add(val, 1))", "val2")])

    columns = [[{
        "name": "val",
        "type": "int"
    }], [{
        "name": "val2",
        "type": "int"
    }]]

    df3 = Dataflow.intersect([df1, df2], columns, ["v"])
    session.execute_dataflow(df3, "intersect", is_async=False)
    verify_row_count(session, "intersect", num_dataset_rows - 1)
    verify_column_match(session, "intersect", df3)

    df4 = Dataflow.except_([df1, df2], columns, ["v"])
    session.execute_dataflow(df4, "except", is_async=False)
    verify_row_count(session, "except", 1)
    verify_column_match(session, "except", df4)

    t3 = session.get_table("intersect")
    t4 = session.get_table("except")
    df5 = Dataflow.create_dataflow_from_table(client, t3)
    df6 = Dataflow.create_dataflow_from_table(client, t4)

    columns = [[{"name": "v", "type": "int"}], [{"name": "v", "type": "int"}]]
    df7 = Dataflow.union([df5, df6], columns, ["v"])
    session.execute_dataflow(df7, "union", is_async=False)
    verify_row_count(session, "union", num_dataset_rows)
    verify_column_match(session, "union", df7)

    clean_up(operators)


def test_cross_join(client, workbook, session, operators, telecom_dataset):
    df = Dataflow.create_dataflow_from_dataset(client, telecom_dataset)
    df1 = df.filter("lt(val, 100)").project(["val"])
    df2 = df.join(df1, [("val", "val")], join_type=JoinOperatorT.CrossJoin)
    session.execute_dataflow(df2, "join1", is_async=False)
    verify_row_count(session, "join1", num_dataset_rows * 100)
    verify_column_match(session, "join1", df2)

    df3 = df.join(
        df1, [("val", "val")],
        join_type=JoinOperatorT.CrossJoin,
        rrename_map=[{
            "name": "val",
            "rename": "r-val"
        }],
        filter_str="neq(valDiv10, r-val)")
    session.execute_dataflow(df3, "join2", is_async=False)
    verify_row_count(session, "join2", num_dataset_rows * 100 - 1000)
    verify_column_match(session, "join2", df3)

    df4 = df.join(
        df1, [("val", "val")],
        rrename_map=[{
            "name": "val",
            "rename": "r-val"
        }],
        join_type=JoinOperatorT.CrossJoin,
        filter_str="lt(valDiv10, r-val)")
    session.execute_dataflow(df4, "join3", is_async=False)
    verify_row_count(session, "join3", 49500)
    verify_column_match(session, "join3", df4)

    df5 = df.filter("eq(1, 0)")
    df6 = df.join(
        df5, [("val", "val")],
        join_type=JoinOperatorT.CrossJoin,
        rrename_map=[{
            "name": "val",
            "rename": "r-val"
        }],
        filter_str="lt(valDiv10, r-val)")
    session.execute_dataflow(df6, "join4", is_async=False)
    verify_row_count(session, "join4", 0)
    verify_column_match(session, "join4", df6)

    clean_up(operators)


def test_anti_and_semi_join(client, workbook, session, operators,
                            telecom_dataset):
    df0 = Dataflow.create_dataflow_from_dataset(client, telecom_dataset)
    df1 = Dataflow.create_dataflow_from_dataset(client, telecom_dataset)
    df2 = df1.join(
        df1, [("valDiv10", "valDiv10")], join_type=JoinOperatorT.LeftSemiJoin)
    session.execute_dataflow(df2, "semi1", is_async=False)
    verify_row_count(session, "semi1", num_dataset_rows)
    verify_column_match(session, "semi1", df2)

    df3 = df1.join(
        df0, [("valDiv10", "val")], join_type=JoinOperatorT.LeftSemiJoin)
    session.execute_dataflow(df3, "semi2", is_async=False)
    session.get_table("semi2").show()
    verify_row_count(session, "semi2", num_dataset_rows)
    verify_column_match(session, "semi2", df3)

    df4 = df0.join(
        df1, [("val", "valDiv10")], join_type=JoinOperatorT.LeftAntiJoin)
    session.execute_dataflow(df4, "anti1", is_async=False)
    verify_row_count(session, "anti1", 9 * num_dataset_rows / 10)
    verify_column_match(session, "anti1", df4)

    df5 = df1.join(
        df0, [("valDiv10", "val")], join_type=JoinOperatorT.LeftAntiJoin)
    session.execute_dataflow(df5, "anti2", is_async=False)
    verify_row_count(session, "anti2", 0)
    verify_column_match(session, "anti2", df5)

    df6 = df1.join(
        df0, [("valDiv10", "valDiv10")], join_type=JoinOperatorT.LeftAntiJoin)
    session.execute_dataflow(df6, "anti3", is_async=False)
    verify_row_count(session, "anti3", 0)
    verify_column_match(session, "anti3", df6)

    clean_up(operators)


@pytest.mark.skip(
    reason="Filter str not working with semi/anti join right now")
def test_anti_and_semi_join_with_filter(client, workbook, session, operators,
                                        telecom_dataset):
    df0 = Dataflow.create_dataflow_from_dataset(client, telecom_dataset)
    df1 = Dataflow.create_dataflow_from_dataset(client, telecom_dataset)
    df2 = df1.join(
        df0, [("valDiv10", "val")],
        join_type=JoinOperatorT.LeftSemiJoin,
        filter_str="lt(valDiv10, 10)")
    session.execute_dataflow(df2, "semi1", is_async=False)
    session.get_table("semi1").show()
    verify_row_count(session, "semi1", 100)

    filter_str = "between(r_val, {}, {})".format(num_dataset_rows / 10 + 1,
                                                 num_dataset_rows - 1)
    df3 = df0.join(
        df1, [("val", "valDiv10")],
        join_type=JoinOperatorT.LeftAntiJoin,
        filter_str=filter_str)
    session.execute_dataflow(df3, "anti1", is_async=False)
    verify_row_count(session, "anti1", 9 * num_dataset_rows / 10 - 1)

    clean_up(operators)


# This test is a sanity test for UDFs in filters - see SDK-814. The SQL 'where'
# clause can reference functions which appear in the 'sql.py' UDF module - these
# appear as UDF invocations in a filter operator.
def test_filter_udfs(client, workbook, session, operators, airlines_dataset):
    udf_module_name = "filter_udf"
    try:
        myudf = workbook.get_udf_module(udf_module_name)
    except ValueError as e:
        if e.args[0] == "No such UDF module: 'filter_udf'":
            myudf = workbook.create_udf_module(udf_module_name,
                                               filter_udf_source)
        else:
            raise
    df = Dataflow.create_dataflow_from_dataset(client, airlines_dataset)

    # adds one to the DayOfWeek column, and filters all results which are less
    # than 7.  For the airlines dataset, this is expected to result in 101071
    # rows

    df1 = df.filter("lt(int(filter_udf:add_one(DayOfWeek)), 7)")
    session.execute_dataflow(df1, "filter_query", is_async=False)
    verify_row_count(session, "filter_query", 101071)

    verify_column_match(session, "filter_query", df1)

    clean_up(operators)


@pytest.mark.parametrize("filter_str, expected_num_rows_failed, err_idx", [
    ("filter_udf:fail(filter_udf:fail(DayOfWeek))", 118793, 0),
    ("lt(None, 0)", 118793, 1),
    ("lt(filter_udf:fail(DayOfWeek), 7)", 118793, 2),
    ("lt(filter_udf:test_div(DayOfWeek), filter_udf:test_div(1))", 118793, 3),
    ("filter_udf:test_div(filter_udf:test_div(filter_udf:test_div(DayOfWeek)))",
     118793, 4),
])
def test_filter_udf_failure(client, workbook, session, operators, xc_api,
                            airlines_dataset, filter_str,
                            expected_num_rows_failed, err_idx):
    err_str = filterUdfErrors[err_idx]
    udf_module_name = "filter_udf"
    try:
        myudf = workbook.get_udf_module(udf_module_name)
    except ValueError as e:
        if e.args[0] == "No such UDF module: 'filter_udf'":
            myudf = workbook.create_udf_module(udf_module_name,
                                               filter_udf_source)
        else:
            raise
    df = Dataflow.create_dataflow_from_dataset(client, airlines_dataset)

    df1 = df.filter(filter_str)

    for optimized in [True, False]:
        query_name = session.execute_dataflow(df1, "filter_query", is_async=False, clean_job_state=False, optimized=optimized)
        qs = xc_api.queryState(query_name)
        print(qs)
        for node in qs.queryGraph.node:
            if node.api == XcalarApisT.XcalarApiFilter:
                assert node.opFailureInfo.numRowsFailedTotal == expected_num_rows_failed
                if optimized:
                    assert fixTb(
                        node.opFailureInfo.opFailureSummary[0].
                        failureSummInfo[0].failureDesc, udf_module_name) == ''
                else:
                    assert fixTb(
                        node.opFailureInfo.opFailureSummary[0].
                        failureSummInfo[0].failureDesc, udf_module_name) == err_str
        session.delete_job_state(query_name, deferred_delete=False)
        verify_column_match(session, "filter_query", df1)
        clean_up(operators)


@pytest.mark.parametrize("eval_str, expected_num_rows_failed", [
    ("lt(map_udf_fail:funcFails6(DayOfWeek), 0)", 118793),
    ("map_udf_fail:funcFails6(map_udf_fail:funcFails6(DayOfWeek))", 118793),
    ("lt(add(DayOfWeek, 1/0), DayOfWeek)", 118793),
    ("explodeString(int(DayOfWeek), \".\")", 118793),
    ("explodeString(map_udf_fail:funcFails1(DayOfWeek), \".\")", 118793)
])
def test_map_xdf_udf_failure(client, workbook, session, operators, xc_api,
                                    airlines_dataset, eval_str,
                                    expected_num_rows_failed):
    udf_module_name = "map_udf_fail"
    try:
        myudf = workbook.get_udf_module(udf_module_name)
    except ValueError as e:
        if e.args[0] == "No such UDF module: 'map_udf_fail'":
            myudf = workbook.create_udf_module(udf_module_name,
                                               map_udf_fail_source)
        else:
            raise
    df = Dataflow.create_dataflow_from_dataset(client, airlines_dataset)

    df1 = df.map([(eval_str, "DofW_xdf")])
    query_name = session.execute_dataflow(df1, "map_query", is_async=False, clean_job_state=False)
    qs = xc_api.queryState(query_name)
    print(qs)
    for node in qs.queryGraph.node:
        if node.api == XcalarApisT.XcalarApiMap:
            assert node.opFailureInfo.numRowsFailedTotal == expected_num_rows_failed

    clean_up(operators)


@pytest.mark.parametrize("eval_str, table_name", [
    ("map_udf_fail:test_none(None)", "res1"),
])
def test_null_udf_arg(client, workbook, session, operators, airlines_dataset,
                      eval_str, table_name):
    udf_module_name = "map_udf_fail"
    try:
        myudf = workbook.get_udf_module(udf_module_name)
    except ValueError as e:
        if e.args[0] == "No such UDF module: 'map_udf_fail'":
            myudf = workbook.create_udf_module(udf_module_name,
                                               map_udf_fail_source)
        else:
            raise
    df = Dataflow.create_dataflow_from_dataset(client, airlines_dataset)
    df1 = df.map([(eval_str, "dummy")], result_table_name=table_name)

    session.execute_dataflow(df1, is_async=False)
    rs = ResultSet(client, table_name=table_name, session_name=session.name)
    it = rs.record_iterator()
    row = next(it)
    assert row["dummy"] == "Empty"
    del rs
    table = session.get_table(table_name)
    table.drop()


@pytest.mark.parametrize("eval_str, table_name", [
    ("int(default:coalesce(map_udf_fail:test_div(DayOfWeek), -1))", "res1"),
])
def test_coalesce(client, workbook, session, operators, airlines_dataset,
                  eval_str, table_name):
    udf_module_name = "map_udf_fail"
    try:
        myudf = workbook.get_udf_module(udf_module_name)
    except ValueError as e:
        if e.args[0] == "No such UDF module: 'map_udf_fail'":
            myudf = workbook.create_udf_module(udf_module_name,
                                               map_udf_fail_source)
        else:
            raise
    df = Dataflow.create_dataflow_from_dataset(client, airlines_dataset)
    df1 = df.map([(eval_str, "dummy")], result_table_name=table_name)

    session.execute_dataflow(df1, is_async=False)
    rs = ResultSet(client, table_name=table_name, session_name=session.name)
    it = rs.record_iterator()
    row = next(it)
    assert row["dummy"] == -1
    del rs
    table = session.get_table(table_name)
    table.drop()


def test_range_filter_optimizations(client, workbook, session, operators,
                                    telecom_dataset):
    df = Dataflow.create_dataflow_from_dataset(client, telecom_dataset)
    df1 = df.filter("lt(val, 10)")
    df2 = df.filter("le(val, 10)")
    df3 = df.filter("gt(val, 10)")
    df4 = df.filter("ge(val, 10)")
    df5 = df.filter("between(val, 5, 10)")
    df6 = df.filter("eq(val, 10)")

    session.execute_dataflow(df1, "filter1", is_async=False)
    session.execute_dataflow(df2, "filter2", is_async=False)
    session.execute_dataflow(df3, "filter3", is_async=False)
    session.execute_dataflow(df4, "filter4", is_async=False)
    session.execute_dataflow(df5, "filter5", is_async=False)
    session.execute_dataflow(df6, "filter6", is_async=False)
    verify_row_count(session, "filter1", 10)
    verify_row_count(session, "filter2", 11)
    verify_row_count(session, "filter3", num_dataset_rows - 11)
    verify_row_count(session, "filter4", num_dataset_rows - 10)
    verify_row_count(session, "filter5", 6)
    verify_row_count(session, "filter6", 1)
    verify_column_match(session, "filter1", df1)
    verify_column_match(session, "filter2", df2)
    verify_column_match(session, "filter3", df3)
    verify_column_match(session, "filter4", df4)
    verify_column_match(session, "filter5", df5)
    verify_column_match(session, "filter6", df6)

    # Test string key
    df1 = df.map([("string(val)", "strVal")])
    df2 = df1.filter("eq(strVal, '10')")
    df3 = df1.filter("between(strVal, '0', '1')")
    session.execute_dataflow(df2, "str_filter1", is_async=False)
    session.execute_dataflow(df3, "str_filter2", is_async=False)
    verify_column_match(session, "str_filter1", df2)
    verify_column_match(session, "str_filter2", df3)
    verify_row_count(session, "str_filter1", 1)
    verify_row_count(session, "str_filter2", 2)

    clean_up(operators)


def test_synthesize_sanity(client, workbook, session, operators,
                           telecom_dataset):
    df = Dataflow.create_dataflow_from_dataset(client, telecom_dataset)
    df1 = df.synthesize([{
        'name': 'val',
        'rename': 'val_rename',
        'type': 'int'
    }, {
        'name': 'valDiv10',
        'rename': 'valDiv10_rename',
        'type': 'string'
    }, {
        'name': 'valStr',
        'rename': 'valFloat_rename',
        'type': 'float'
    }, {
        'name': 'valFloat',
        'rename': 'valStr_rename',
        'type': 'string'
    }])
    session.execute_dataflow(df1, "synthesize", is_async=False)
    verify_schema(
        session,
        'synthesize',
        columns_in_schema=[
            "val_rename", "valDiv10_rename", "valFloat_rename", "valStr_rename"
        ],
        columns_not_in_schema=["val", "valDiv10", "mixedType", "prn1"])
    verify_column_match(session, "synthesize", df1)
    clean_up(operators)


def test_sort_sanity(client, workbook, session, operators, telecom_dataset):
    df = Dataflow.create_dataflow_from_dataset(client, telecom_dataset)
    df1 = df.sort(["val"])
    df2 = df.sort(["val"], reverse=True)
    session.execute_dataflow(df1, 'sort1', is_async=False)
    session.execute_dataflow(df2, 'sort2', is_async=False)
    verify_column_match(session, "sort1", df1)
    verify_column_match(session, "sort2", df2)
    t1 = session.get_table("sort1")
    t2 = session.get_table("sort2")

    assert t1.get_row(0)["val"] == 0
    assert t2.get_row(0)["val"] == num_dataset_rows - 1
    clean_up(operators)


def test_dataflow_optimized_queries(client):
    test_wb_name = "df_sql_parameterized"
    wb_path = XcalarQaDatasetPath + "/dfWorkbookTests/dfSqlParameterized.xlrdf.tar.gz"

    # clean up old session, if any
    try:
        wb = client.get_workbook(test_wb_name)
        wb.delete()
    except ValueError as e:
        assert "No such workbook" in str(e)

    with open(wb_path, 'rb') as wb_file:
        wb_content = wb_file.read()
        workbook = client.upload_workbook(test_wb_name, wb_content)

    df1 = workbook.get_dataflow("df1")

    # generally this query string will be created programmatically
    # this should contain load operations too
    xc_query_str = json.loads(
        json.loads(df1.optimized_query_string)['retina'])['query']
    columns_to_export = [{"columnName": "DAYOFWEEK"}]
    df2 = Dataflow.create_dataflow_from_query_string(
        client, query_string=xc_query_str, columns_to_export=columns_to_export)

    sess = client.create_session("dummy_session")
    sess.execute_dataflow(
        df2,
        table_name="tab#1",
        optimized=True,
        is_async=False,
        parallel_operations=random.choice([True, False]))
    tab2 = sess.get_table("tab#1")
    assert tab2.record_count() == 11024
    row0 = tab2.get_row(0)
    assert len(row0) == 1    # only one field
    assert set(row0.keys()) == {'DAYOFWEEK'}

    columns_to_export = [{
        "columnName": "DAYOFWEEK"
    }, {
        'columnName': 'DEPTIME',
        'headerAlias': 'DEP_TIME'
    }, {
        'columnName': 'ARRTIME',
        'headerAlias': 'ARR_TIME'
    }]
    df2 = Dataflow.create_dataflow_from_query_string(
        client, xc_query_str, columns_to_export=columns_to_export)

    sess.execute_dataflow(
        df2,
        table_name="tab#2",
        optimized=True,
        is_async=False,
        parallel_operations=random.choice([True, False]))
    tab3 = sess.get_table("tab#2")
    assert tab3.record_count() == 11024
    row0 = tab3.get_row(0)
    assert len(row0) == 3    # three fields exported
    assert tab3.columns == ['DAYOFWEEK', 'DEP_TIME', 'ARR_TIME'], tab3.columns
    # cleanup
    sess.destroy()    # session destroys in the cluster but not client object
    workbook.delete(
    )    # workbook gets deleted in the cluster but not workbook object
    assert len(client.list_sessions(sess.name)) == 0
    assert len(client.list_workbooks(workbook.name)) == 0


@pytest.mark.skip(reason="Refer ENG-5013")
@pytest.mark.parametrize("optimized", [(True), (False)])
def test_dataflow_in_progress_session_del(client, telecom_dataset, optimized):
    # build dataflow of just 200 records
    df = Dataflow.create_dataflow_from_dataset(
        client, telecom_dataset).gen_row_num("pk").filter("lt(pk, 200)").map(
            [("test_udf:sleep_fn()", "sleep_col")])

    # workaround, which is not present in 2.0 branch
    if optimized:
        load_args = telecom_dataset.get_info()['loadArgs']
        load_args['parseArgs']['parserArgJson'] = json.dumps(
            load_args['parseArgs']['parserArgJson'])
        load_query = {
            'operation': 'XcalarApiBulkLoad',
            'args': {
                'dest': telecom_dataset.name,
                'loadArgs': load_args
            },
        }
        df._query_list.insert(0, load_query)
        columns = [{"columnName": col} for col in df.table_columns]
        df._build_optimized_query_string(columns)
    else:
        df._query_list[0]['args']['sameSession'] = False

    # create workbook and add udf module to it
    new_wb = client.create_workbook("test_dataflow_in_progress")
    new_wb.create_udf_module("test_udf", sleep_udf_source)
    new_wb._create_or_update_udf_module(mem_udf_name, mem_udf_source)

    # run the dataflow in a session
    sess = new_wb.activate()
    query_name = sess.execute_dataflow(
        df,
        table_name="out_table",
        optimized=optimized,
        is_async=True,
        parallel_operations=random.choice([True, False]))

    xc_api = client._legacy_xcalar_api
    xc_api.setSession(sess)
    max_tries = 10

    # allow some time for query to be in processing state
    for i in range(max_tries):
        rv = xc_api.queryState(query_name)
        if rv.queryState == QueryStateT.qrProcessing:
            break
        time.sleep(2)
    assert rv.queryState == QueryStateT.qrProcessing

    # inactivate and delete the workbook
    with pytest.raises(XcalarApiStatusException) as ex:
        new_wb.delete()
        assert ex.status == StatusT.StatusOperationOutstanding

    xc_api.cancelQuery(query_name)
    rv = xc_api.queryState(query_name)
    # allow some time for query to get cancelled
    for i in range(max_tries):
        rv = xc_api.queryState(query_name)
        if rv.queryStatus == StatusT.StatusCanceled:
            break
        time.sleep(2)
    assert rv.queryStatus == StatusT.StatusCanceled

    # this should succeed, now
    new_wb.delete()


def test_df_exec_all_params(client, session, airlines_dataset):
    # test optimized dataflow pin result
    df = Dataflow.create_dataflow_from_dataset(client, airlines_dataset).map(
        [("int(DayOfWeek)", "DayOfWeek")])
    df1 = df.map([("len(Description)", "len_desc")]).filter("eq(DayOfWeek, 3)")
    result_table = "pin_test_table"
    result_num_rows = 20130
    for pin_result in [False, True]:
        session.execute_dataflow(
            df1,
            table_name=result_table,
            is_async=False,
            optimized=True,
            pin_results=pin_result,
            parallel_operations=random.choice([True, False]))
        res_tab = session.get_table(result_table)
        assert res_tab.record_count() == result_num_rows
        if pin_result:
            assert res_tab.is_pinned()
            with pytest.raises(XcalarApiStatusException) as ex:
                res_tab.drop()
                assert ex.status == StatusT.StatusTablePinned
            res_tab.unpin()
            res_tab.drop()
        else:
            assert not res_tab.is_pinned()
            res_tab.drop()
        with pytest.raises(ValueError) as ex:
            session.get_table(result_table)
            assert "No such table: '{}'".format(result_table) in str(ex)

    # test non-optmized dataflows
    # results
    tab_results = [("tab1", 20130), ("tab2", 20212), ("tab3", 20210)]
    df1 = df.map([("len(Description)", "len_desc")]).filter(
        "eq(DayOfWeek, 3)", result_table_name="tab1")
    df2 = df.map([("len(Description)", "len_desc")]).filter(
        "eq(DayOfWeek, 4)", result_table_name="tab2")
    df3 = df.map([("len(Description)", "len_desc")]).filter(
        "eq(DayOfWeek, 5)", result_table_name="tab3")

    df3._merge_dataflows([df1, df2])

    # intermediate table, to see if we are locking intermediate table
    # by default, we drop all intermediate tables, so changing state to not drop
    df3._query_list[1]['state'] = "Created"
    intermediate_tab_name = df3._query_list[1]['args']['dest']
    intermediate_tab_row_count = 118793

    for pin_result in [False, True]:
        # clean start
        for tab in session.list_tables():
            tab.drop()
        assert len(session.list_tables()) == 0
        session.execute_dataflow(
            df3, is_async=False, optimized=False, pin_results=pin_result)

        # check results of the sink tables of dataflow
        for tab, actual_row_count in tab_results:
            res_tab = session.get_table(tab)
            assert res_tab.record_count() == actual_row_count
            if pin_result:
                assert res_tab.is_pinned()
                with pytest.raises(XcalarApiStatusException) as ex:
                    res_tab.drop()
                    assert ex.status == StatusT.StatusTablePinned
                res_tab.unpin()
                res_tab.drop()
            else:
                assert not res_tab.is_pinned()
                res_tab.drop()
            with pytest.raises(ValueError) as ex:
                session.get_table(result_table)
                assert "No such table: '{}'".format(result_table) in str(ex)

        # intermediate table should be dropped without unpin
        res_tab = session.get_table(intermediate_tab_name)
        assert res_tab.record_count() == intermediate_tab_row_count
        res_tab.drop()
        with pytest.raises(ValueError) as ex:
            session.get_table(intermediate_tab_name)
            assert "No such table: '{}'".format(intermediate_tab_name) in str(
                ex)


@pytest.mark.parametrize("optimized", [True, False])
@pytest.mark.parametrize("sched_name", [
    Runtime.Scheduler.Sched0, Runtime.Scheduler.Sched1,
    Runtime.Scheduler.Sched2
])
@pytest.mark.parametrize("exec_mode", [True, False])
def test_dataflow_execution_with_scheds(client, workbook, airlines_dataset,
                                        optimized, sched_name, exec_mode):
    session = workbook.activate()
    # test optimized dataflow pin result
    df = Dataflow.create_dataflow_from_dataset(client, airlines_dataset).map(
        [("int(DayOfWeek)", "DayOfWeek")])
    df1 = df.map([("len(Description)", "len_desc")]).filter("eq(DayOfWeek, 3)")

    result_table = "pin_test_table"
    result_num_rows = 20130
    clean_job_state = random.choice([True, False])

    job_name = session.execute_dataflow(
        df1,
        table_name=result_table,
        is_async=False,
        optimized=optimized,
        sched_name=sched_name,
        parallel_operations=exec_mode,
        clean_job_state=clean_job_state)
    res_tab = session.get_table(result_table)
    assert res_tab.record_count() == result_num_rows
    assert len(res_tab.columns) == 17
    res_tab.drop()
    if clean_job_state:
        with pytest.raises(XcalarApiStatusException) as ex:
            while True:
                client._legacy_xcalar_api.queryState(job_name)
                time.sleep(0.1)
            assert ex.status in [
                StatusT.StatusQrQueryNotExist,
                StatusT.StatusQrQueryAlreadyDeleted
            ]

# Validate bad return type annotation failures (unsupported and illegal)
@pytest.mark.parametrize("mod_name, mod_src",
    [
        ("udf_bad_type_str", udf_bad_type_str_source),
        ("udf_bad_type_foo", udf_bad_type_foo_source),
    ])
def test_udf_type_bad(client, workbook, session, operators, xc_api,
                      mod_name, mod_src):
    try:
        myudf = workbook.get_udf_module(mod_name)
    except ValueError as e:
        expected_msg = "No such UDF module: '{}'".format(mod_name)
        if e.args[0] == expected_msg:
            try:
                myudf = workbook.create_udf_module(mod_name, mod_src)
            except XcalarApiStatusException as e:
                print("create udf failed as expected, failure {}".\
                      format(e.args[1].udfAddUpdateOutput.error.message))
                pass
        else:
            raise

# Validate no failures if funcs use supported return type annotation
# bool, bytes, int and float
@pytest.mark.parametrize("mod_name, mod_src",
    [
        ("udf_good_type", udf_good_types_source),
    ])
def test_udf_type_good(client, workbook, session, operators, xc_api,
                       mod_name, mod_src):
    try:
        myudf = workbook.get_udf_module(mod_name)
    except ValueError as e:
        expected_msg = "No such UDF module: '{}'".format(mod_name)
        if e.args[0] == expected_msg:
            myudf = workbook.create_udf_module(mod_name, mod_src)
        else:
            raise

# If a function's return value is not compatible with the function's return
# type annotation (e.g. a func with return type int, returns a string), then
# it should fail - otherwise it should pass. Following verifies this.
# It invokes the same function with and without icvMode, and verifies the
# precise failure string expected

@pytest.mark.parametrize(
    "evalStr, dstCol, icvMode",
    [
        ("map_udf_ret_type:funcRetIntFail(DayOfWeek)", "DofW_udf", False),
        ("map_udf_ret_type:funcRetIntFail(DayOfWeek)", "DofW_udf", True),
    ])
def test_udf_type_ret(client, workbook, session, operators, xc_api,
                      airlines_dataset, evalStr, dstCol, icvMode):

    udf_module_name = "map_udf_ret_type"

    # UDF will already exist on subsequent rounds - create it only once
    try:
        myudf = workbook.get_udf_module(udf_module_name)
    except ValueError as e:
        if e.args[0] == "No such UDF module: 'map_udf_ret_type'":
            myudf = workbook.create_udf_module(udf_module_name,
                                               map_udf_ret_type_source)
        else:
            raise

    # execute map with parameterized eval string and icvMode
    df = Dataflow.create_dataflow_from_dataset(client, airlines_dataset)
    dff = df.map([(evalStr, dstCol)], icv_mode=icvMode)
    qn = session.execute_dataflow(
        dff, query_name="retmap_query", is_async=False, clean_job_state=False)
    qs = xc_api.queryState(qn)
    found_map = False
    print("num nodes is {}".format(qs.queryGraph.numNodes))
    for jj in range(0, qs.queryGraph.numNodes):
        node = qs.queryGraph.node[jj]
        print("node {} api is {}".format(jj, node.api))
        if node.api == XcalarApisT.XcalarApiMap:
            found_map = True
            evalList = node.input.mapInput.eval
            opFi = node.opFailureInfo
            # the numbers in the following asserts assume a specific
            # dataset - the canonical airlines csv dataset from netstore
            assert (opFi.numRowsFailedTotal == 98583)
            fSumm = opFi.opFailureSummary[0]
            if icvMode is False:
                print("\nnode {} has {} failures; breakdown below:".format(
                    jj, opFi.numRowsFailedTotal))
                assert (fSumm.failureSummName == "DofW_udf")
            else:
                # icvMode is True- there's no break-down of total failures
                print("\nnode {} has {} failures".format(
                    jj, opFi.numRowsFailedTotal))
                assert (fSumm.failureSummInfo[0].numRowsFailed == 0)
                assert (fSumm.failureSummName == "")
            totalFromBd = 0
            for kk in range(0, XcalarApisConstantsT.XcalarApiMaxFailures):
                frow = opFi.opFailureSummary[0].failureSummInfo[kk]
                if frow.numRowsFailed != 0:
                    totalFromBd += frow.numRowsFailed
                    print("\n\n{} rows failed with:\n{}".format(
                        frow.numRowsFailed, frow.failureDesc))
                    fdesc = frow.failureDesc
                    # for the non-icv mode maps, validate the failure strings
                    # (failure strings aren't emitted for map in icv mode)
                    assert (frow.numRowsFailed == fdescFromFuncRet[kk][0])
                    assert (fdesc == fdescFromFuncRet[kk][1])
                else:
                    break
            if icvMode is False:
                assert (opFi.numRowsFailedTotal == totalFromBd)
            else:
                assert (totalFromBd == 0)
    assert (found_map is True)


# Function to fix-up the Traceback (TB) reported by XCE, to replace the fully
# qualified UDF path name, if it were to appear in the backtrace, with just
# the UDF module name, so that the string can be compared with the expected TB


def fixTb(tb, udf_module_name):
    tb_lines = tb.splitlines()
    for ii in range(0, len(tb_lines)):
        udf_mod_str = "/{}.py".format(udf_module_name)
        # if the line has a path name to the module,
        # replace with just the module name
        if udf_mod_str in tb_lines[ii]:
            user_udf_line = tb_lines[ii]
            matches = re.match(r'(.*)"(.*)"(.*)', user_udf_line)
            if matches is not None:
                udf_pre, udf_path, udf_add_info = matches.groups()
                udf_directory, udf_module = os.path.split(udf_path)
                fixed_udf_line = "{}\"{}\"{}".format(udf_pre, udf_module,
                                                     udf_add_info)
                tb_lines[ii] = fixed_udf_line
    fixed_tb = "\n".join(tb_lines)
    return fixed_tb


# map UDF failure test cases:
# 0. invoke map with UDF with 6 different failure scenarios (1 more than max)
#    validate total number of rows failed, rows per failure, and TB strings
# 1. invoke map with UDF with 5 different failure scenarios (== max == 5)
#    validate total number of rows failed, rows per failure, and TB strings
# 2. invoke map with UDF with 6 failures in ICV-mode
#    validate total number of rows failed
# 3. invoke map with UDF with 5 failures in ICV-mode
#    validate total number of rows failed


@pytest.mark.parametrize(
    "evalStr, dstCol, icvMode",
    [
        ("map_udf_fail:funcFails6(DayOfWeek)", "DofW_udf",
         False),    # scenario 0
        ("map_udf_fail:funcFails5(DayOfWeek)", "DofW_udf",
         False),    # scenario 1
        ("map_udf_fail:funcFails6(DayOfWeek)", "DofW_udf",
         True),    # scenario 2
        ("map_udf_fail:funcFails5(DayOfWeek)", "DofW_udf",
         True),    # scenario 3
    ])
def test_single_udf_failures(client, workbook, session, operators, xc_api,
                             airlines_dataset, evalStr, dstCol, icvMode):
    # this test assumes a max number of failures for which a per-failure
    # row-count break-down is reported - this is asserted below. If this
    # assert fails, then the test needs to be adjusted (the UDFs themselves
    # which generate max+1 and max failures) and the code below
    assert (XcalarApisConstantsT.XcalarApiMaxFailures == 5)

    udf_module_name = "map_udf_fail"

    # UDF will already exist on subsequent rounds - create it only once
    try:
        myudf = workbook.get_udf_module(udf_module_name)
    except ValueError as e:
        if e.args[0] == "No such UDF module: 'map_udf_fail'":
            myudf = workbook.create_udf_module(udf_module_name,
                                               map_udf_fail_source)
            print("created udf")
        else:
            raise

    # execute map with parameterized eval string and icvMode
    df = Dataflow.create_dataflow_from_dataset(client, airlines_dataset)
    dff = df.map([(evalStr, dstCol)], icv_mode=icvMode)
    qn = session.execute_dataflow(
        dff, query_name="fmap_query", is_async=False, clean_job_state=False)

    # Now, check results of the map execution using queryState API
    # XXX:
    # the following uses the legacy QueryState API; replace with new SDK
    # one when it's ready

    qs = xc_api.queryState(qn)
    found_map = False
    print("num nodes is {}".format(qs.queryGraph.numNodes))
    for jj in range(0, qs.queryGraph.numNodes):
        node = qs.queryGraph.node[jj]
        print("node {} api is {}".format(jj, node.api))
        if node.api == XcalarApisT.XcalarApiMap:
            found_map = True
            evalList = node.input.mapInput.eval
            for kk in range(0, len(evalList)):
                print("\n\nnode {} evalString[{}] is {}\n\n".format(
                    jj, kk, evalList[kk].evalString))
                # assert that the eval string has relative UDF name
                assert ('/workbook' not in evalList[kk].evalString)
                assert ('/myaddUdf' not in evalList[kk].evalString)
            opFi = node.opFailureInfo
            # the numbers in the following asserts assume a specific
            # dataset - the canonical airlines csv dataset from netstore
            if "6" in evalStr:
                # for the 6-failure test cases, all rows expected to fail
                assert (opFi.numRowsFailedTotal == 118793)
            else:
                # for the 5-failure test cases, less rows expected to fail
                assert (opFi.numRowsFailedTotal == 98275)
            fSumm = opFi.opFailureSummary[0]
            if icvMode is False:
                print("\nnode {} has {} failures; breakdown below:".format(
                    jj, opFi.numRowsFailedTotal))
                assert (fSumm.failureSummName == "DofW_udf")
            else:
                # icvMode is True- there's no break-down of total failures
                print("\nnode {} has {} failures".format(
                    jj, opFi.numRowsFailedTotal))
                assert (fSumm.failureSummInfo[0].numRowsFailed == 0)
                assert (fSumm.failureSummName == "")
            totalFromBd = 0
            for kk in range(0, XcalarApisConstantsT.XcalarApiMaxFailures):
                frow = opFi.opFailureSummary[0].failureSummInfo[kk]
                if frow.numRowsFailed != 0:
                    totalFromBd += frow.numRowsFailed
                    print("\n\n{} rows failed with:\n{}".format(
                        frow.numRowsFailed, frow.failureDesc))
                    fdesc = frow.failureDesc
                    # for the non-icv mode maps (ii < 2), validate the
                    # traceback strings (failure strings aren't
                    # emitted for map in icv mode)
                    fixedTb = fixTb(fdesc, udf_module_name)
                    if "6" in evalStr:
                        # first map with UDF with 6 failures
                        print("fixedTb {} is {}".format(kk, fixedTb))
                        assert (frow.numRowsFailed == tbsFromFunc6[kk][0])
                        assert (fixedTb == tbsFromFunc6[kk][1])
                    else:
                        # second map with UDF with 5 failures
                        print("fixedTb {} is {}".format(kk, fixedTb))
                        assert (frow.numRowsFailed == tbsFromFunc5[kk][0])
                        assert (fixedTb == tbsFromFunc5[kk][1])
                else:
                    break
            if icvMode is False:
                if "6" in evalStr:
                    # in 6-failure case, the row count breakdown for only
                    # first 5 are reported - following asserts remainder
                    assert (opFi.numRowsFailedTotal - totalFromBd == 17722)
                else:
                    assert (opFi.numRowsFailedTotal == totalFromBd)
            else:
                assert (totalFromBd == 0)
    assert (found_map is True)


# Test UDF failures with multiple eval strings (in this case, 2 eval strings)
# 0. Invoke map with UDF with 6 failure cases in two eval strings on 2 cols
#    validate number of total failures and the breakdown
# 1. Same as 0 above, but with ICV mode
# 2. Invoke map with checker-board success/failure pattern in two evals by
#    using the funcFailsMulti UDF function on 2 cols
#    validate number of total failures and the breakdown
@pytest.mark.parametrize(
    "evalStrs, icvMode",
    [
        ([("map_udf_fail:funcFails6(DayOfWeek)", "DofW_udf"),
          ("map_udf_fail:funcFails6(TaxiIn)", "Taxiin_udf")],
         False),    # scenario 0
        ([("map_udf_fail:funcFails6(DayOfWeek)", "DofW_udf"),
          ("map_udf_fail:funcFails6(TaxiIn)", "Taxiin_udf")],
         True),    # scenario 1
        ([("map_udf_fail:funcFailsMulti(DayOfWeek)", "DofW_udf"),
          ("map_udf_fail:funcFailsMulti(TaxiIn)", "Taxiin_udf")],
         False),    # scenario 2
    ])
def test_multi_udf_failures(client, workbook, session, operators, xc_api,
                            airlines_dataset, evalStrs, icvMode):
    udf_module_name = "map_udf_fail"
    # UDF will already exist on subsequent rounds - create it only once
    try:
        myudf = workbook.get_udf_module(udf_module_name)
    except ValueError as e:
        if e.args[0] == "No such UDF module: 'map_udf_fail'":
            myudf = workbook.create_udf_module(udf_module_name,
                                               map_udf_fail_source)
        else:
            raise

    df = Dataflow.create_dataflow_from_dataset(client, airlines_dataset)
    dff = df.map(evalStrs, icv_mode=icvMode)

    qn = session.execute_dataflow(
        dff, query_name="fmap_query", is_async=False, clean_job_state=False)
    # XXX:
    # the following uses the legacy QueryState API; replace with new SDK
    # one when it's ready
    qs = xc_api.queryState(qn)
    found_map = False
    print("num nodes is {}".format(qs.queryGraph.numNodes))
    for jj in range(0, qs.queryGraph.numNodes):
        node = qs.queryGraph.node[jj]
        print("node {} api is {}".format(jj, node.api))
        if node.api == XcalarApisT.XcalarApiMap:
            found_map = True
            evalList = node.input.mapInput.eval
            for kk in range(0, len(evalList)):
                print("\n\nnode {} evalString[{}] is {}\n\n".format(
                    jj, kk, evalList[kk].evalString))
                # assert that the eval string has relative UDF name
                assert ('/workbook' not in evalList[kk].evalString)
                assert ('/myaddUdf' not in evalList[kk].evalString)
            opFi = node.opFailureInfo

            # the numbers in the following asserts assume a specific
            # dataset - the canonical airlines csv dataset from netstore

            print("\nnode {} has {} failures:".format(jj,
                                                      opFi.numRowsFailedTotal))
            if "funcFails6" in evalStrs[0][0]:
                # for the 6-failure test cases, all rows
                # expected to fail each of 118793 rows fails
                # on 2 evals - so total is 118793x2 = 237586
                assert (opFi.numRowsFailedTotal == 237586)
            else:
                assert (opFi.numRowsFailedTotal == 205663)
            if icvMode is False:
                # assert the total count, and breakdown counts, and the
                # failure description strings (typically the Traceback
                # strings)
                totalFromBd = 0
                maxEvals = XcalarApisConstantsT.XcalarApiMaxFailureEvals
                maxFailures = XcalarApisConstantsT.XcalarApiMaxFailures
                for ll in range(0, maxEvals):
                    fSumm = opFi.opFailureSummary[ll]
                    for kk in range(0, maxFailures):
                        frow = fSumm.failureSummInfo[kk]
                        if frow.numRowsFailed != 0:
                            totalFromBd += frow.numRowsFailed
                            print("\n\n{} rows failed with:\n{}".format(
                                frow.numRowsFailed, frow.failureDesc))
                            fdesc = frow.failureDesc
                            # for the non-icv mode maps, validate the
                            # traceback strings (failure strings aren't
                            # emitted for map in icv mode)
                            fixedTb = fixTb(fdesc, udf_module_name)
                            if "funcFails6" in evalStrs[0][0]:
                                # first map with UDF with 6 failures
                                print("fixedTb {},{} is {}".format(
                                    ll, kk, fixedTb))
                                # Note the sum of all 10 counts doesn't equal
                                # opFi.numRowsFailedTotal since the rest
                                # aren't reported due to the limit of
                                # XcalarApiMaxFailures per eval

                                if ll == 0:
                                    assert (
                                        fSumm.failureSummName == "DofW_udf")
                                    assert (frow.numRowsFailed ==
                                            tbsFromFunc6MultiE1[kk][0])
                                    assert (
                                        fixedTb == tbsFromFunc6MultiE1[kk][1])
                                else:
                                    assert (
                                        fSumm.failureSummName == "Taxiin_udf")
                                    assert (frow.numRowsFailed ==
                                            tbsFromFunc6MultiE2[kk][0])
                                    assert (
                                        fixedTb == tbsFromFunc6MultiE2[kk][1])
                            else:
                                # funcFailsMulti
                                print("fixedTb {},{} is {}".format(
                                    ll, kk, fixedTb))
                                if ll == 0:
                                    # Eval 1
                                    assert (
                                        fSumm.failureSummName == "DofW_udf")
                                    assert (frow.numRowsFailed ==
                                            tbsFromFuncMultiE1[kk][0])
                                    assert (
                                        fixedTb == tbsFromFuncMultiE1[kk][1])
                                else:
                                    # Eval 2
                                    assert (
                                        fSumm.failureSummName == "Taxiin_udf")
                                    assert (frow.numRowsFailed ==
                                            tbsFromFuncMultiE2[kk][0])
                                    assert (
                                        fixedTb == tbsFromFuncMultiE2[kk][1])
                        else:
                            break

                if "funcFails6" in evalStrs[0][0]:
                    # in 6-failure case, the row count breakdown for only
                    # first 5 are reported - following asserts remainder
                    assert (opFi.numRowsFailedTotal - totalFromBd == 21290)
                else:
                    assert (opFi.numRowsFailedTotal - totalFromBd == 1916)
            else:
                # icvMode == true; no failure breakdown
                fSumm0 = opFi.opFailureSummary[0]
                assert (fSumm0.failureSummName == "")
                assert (fSumm0.failureSummInfo[0].numRowsFailed == 0)

                fSumm1 = opFi.opFailureSummary[1]
                assert (fSumm1.failureSummName == "")
                assert (fSumm1.failureSummInfo[0].numRowsFailed == 0)

    assert (found_map is True)


# Following test evaluates a mixture of XDFs and UDFs, with some failures for
# both XDFs and UDFs, and some failures in evalStrings which occur beyond
# XcalarApisConstantsT.XcalarApiMaxFailureEvals - assuming latter == 5,
# and there are 7 eval strings, some in the first 5 fail, and some beyond
# first 5 fail - this tests that first N failures are reported, whether the
# failures occur in the first 5 submitted evals or not. Here, N == 3
#
# evalStr1: XDF (concat) passes on all rows
# evalStr2: UDF (map_udf_fail:funcPasses) passes on all rows
# evalStr3: XDF (div) fails on some rows (div by zero). Fails on some rows due to FNF.
# evalStr4: UDF (map_udf_fail:funcFails6) fails on all rows
# evalStr5: XDF (add) passes most rows. 2553 fail due to FNF.
# ---- failures in evals after this must also be reported ---
# evalStr6: UDF fails on some rows (use funcFailsMulti on DayOfWeek)
# evalStr7: UDF fails on some rows (use funcFailsMulti on TaxiIn)


@pytest.mark.parametrize(
    "evalStrs, icvMode",
    [([("concat(Origin, '->', Destination)", "Origin_concat"),
       ("map_udf_fail:funcPasses(DayOfWeek)", "DofW_plusOne"),
       ("div(int(ArrTime), int(ArrDelay))", "AirTime_div"),
       ("map_udf_fail:funcFails6(DayOfWeek)", "DofW_func6"),
       ("add(int(ArrDelay), int(DepDelay_integer))", "ArrDepDelay_add"),
       ("map_udf_fail:funcFailsMulti(DayOfWeek)", "DofW_multi"),
       ("map_udf_fail:funcFailsMulti(TaxiIn)", "Taxiin_multi")], False),
     ([("concat(Origin, '->', Destination)", "Origin_concat"),
       ("map_udf_fail:funcPasses(DayOfWeek)", "DofW_plusOne"),
       ("div(int(AirTime), int(ArrDelay))", "AirTime_div"),
       ("map_udf_fail:funcFails6(DayOfWeek)", "DofW_func6"),
       ("add(int(ArrDelay), int(DepDelay_integer))", "ArrDepDelay_add"),
       ("map_udf_fail:funcFailsMulti(DayOfWeek)", "DofW_multi"),
       ("map_udf_fail:funcFailsMulti(TaxiIn)", "Taxiin_multi")], True)])
@pytest.mark.parametrize("optimized", [True, False])
def test_multi_xdf_udf_failures(client, workbook, session, operators, xc_api,
                                airlines_dataset, evalStrs, icvMode,
                                optimized):
    assert (XcalarApisConstantsT.XcalarApiMaxFailureEvals == 5)
    udf_module_name = "map_udf_fail"
    # UDF will already exist on subsequent rounds - create it only once
    try:
        myudf = workbook.get_udf_module(udf_module_name)
    except ValueError as e:
        if e.args[0] == "No such UDF module: 'map_udf_fail'":
            myudf = workbook.create_udf_module(udf_module_name,
                                               map_udf_fail_source)
        else:
            raise
    df = Dataflow.create_dataflow_from_dataset(client, airlines_dataset)
    print("evalStrs is {}".format(evalStrs))
    dff = df.map(evalStrs, icv_mode=icvMode)

    qn = session.execute_dataflow(
        dff,
        query_name="map_xdf_udf_query",
        is_async=False,
        optimized=optimized,
        clean_job_state=False)
    qs = xc_api.queryState(qn)
    print(qs)
    found_map = False
    print("num nodes is {}".format(qs.queryGraph.numNodes))
    for jj in range(0, qs.queryGraph.numNodes):
        node = qs.queryGraph.node[jj]
        print("node {} api is {}".format(jj, node.api))
        if node.api == XcalarApisT.XcalarApiMap:
            found_map = True
            evalList = node.input.mapInput.eval
            for kk in range(0, len(evalList)):
                print("\n\nnode {} evalString[{}] is {}\n\n".format(
                    jj, kk, evalList[kk].evalString))
                # assert that the eval string has relative UDF name
                assert ('/workbook' not in evalList[kk].evalString)
                assert ('/myaddUdf' not in evalList[kk].evalString)
            opFi = node.opFailureInfo
            print("\nnode {} has {} failures:".format(jj,
                                                      opFi.numRowsFailedTotal))
            # 118793 - all fail for "DofW_func6" column (evalStr4)
            #  98792 - 5 unique failures for "DofW_multi" column
            # 106871 - 6 unique failures for "Taxiin_multi" column
            # 2553 - 1 unique failures for ArrDepDelay_add
            # 5711 - 2 unique failures for AirTime_div
            # total = 324456
            assert (opFi.numRowsFailedTotal == 332720)

            if icvMode is False:
                maxEvals = XcalarApisConstantsT.XcalarApiMaxFailureEvals
                assert (len(opFi.opFailureSummary) == maxEvals)
                maxFailures = XcalarApisConstantsT.XcalarApiMaxFailures
                totalFromBd = 0
                for ll in range(0, len(opFi.opFailureSummary)):
                    fSumm = opFi.opFailureSummary[ll]
                    for kk in range(0, maxFailures):
                        frow = fSumm.failureSummInfo[kk]
                        totalFromBd += frow.numRowsFailed
                        fdesc = frow.failureDesc
                        fixedTb = fixTb(fdesc, udf_module_name)
                        if not optimized:
                            if ll == 0:
                                assert (fSumm.failureSummName == "AirTime_div")
                            elif ll == 1:
                                assert (fSumm.failureSummName == "DofW_func6")
                                assert (
                                    frow.numRowsFailed == tbsFromFunc6[kk][0])
                                assert (fixedTb == tbsFromFunc6[kk][1])
                            elif ll == 2:
                                assert (
                                    fSumm.failureSummName == "ArrDepDelay_add")
                                if kk == 0:
                                    assert (frow.numRowsFailed == 2269)
                            elif ll == 3:
                                assert (fSumm.failureSummName == "DofW_multi")
                                assert (frow.numRowsFailed ==
                                        tbsFromFuncMultiE1[kk][0])
                                assert (fixedTb == tbsFromFuncMultiE1[kk][1])
                            elif ll == 4:
                                assert (fSumm.failureSummName == "Taxiin_multi")
                                assert (frow.numRowsFailed ==
                                        tbsFromFuncMultiE2[kk][0])
                                assert (fixedTb == tbsFromFuncMultiE2[kk][1])
                        else:
                            # we don't collected failure summary table
                            # in optimized mode
                            assert fSumm.failureSummName == ''
                            assert frow.numRowsFailed == 0
                            assert fixedTb == ''
                #
                # DofW_func6 total failures is 118,793 but only upto 101,071
                # is reported (and reflected in totalFromBd) due to the
                # XcalarApisConstantsT.XcalarApiMaxFailures limit - so gap
                # in totalFromBd would be 17722
                #
                # For DofW_multi -> all failures reported so gap is 0
                #
                # Taxiin_multi -> total failures are 106871, but reported
                # 104955 so gap in totalFromBd would be 1916
                #
                # So total gap in totalFromBd would be 17722+1916 = 19638
                assert (optimized or opFi.numRowsFailedTotal - totalFromBd == 19638)

    assert (found_map is True)


@pytest.mark.parametrize("evalStr, dstCol, icvMode", [
    ("map_udf_perf:funcPasses(DayOfWeek)", "DofW_udf", False),
    ("map_udf_perf:funcFailsAll(DayOfWeek)", "DofW_udf", False),
    ("map_udf_perf:funcFailsAll(DayOfWeek)", "DofW_udf", True),
])
def test_map_perf(client, workbook, session, operators, xc_api,
                  airlines_dataset, evalStr, dstCol, icvMode):

    udf_module_name = "map_udf_perf"
    try:
        myudf = workbook.get_udf_module(udf_module_name)
    except ValueError as e:
        if e.args[0] == "No such UDF module: 'map_udf_perf'":
            myudf = workbook.create_udf_module(udf_module_name,
                                               map_udf_perf_source)
        else:
            raise

    # execute map with parameterized eval string and icvMode
    df = Dataflow.create_dataflow_from_dataset(client, airlines_dataset)
    dff = df.map([(evalStr, dstCol)], icv_mode=icvMode)

    total_time = 0
    print("\n")
    # nRuns below should be bumped up (say to 10 or 20) for a perf run
    # It's being left at 1 in the test so it doesn't take a long time since
    # it'd be run regularly - the perf test is being checked in so it's
    # available - not as a way to catch regressions. See SYS-264
    nRuns = 1
    for iter in range(0, nRuns):
        this_query_name = "map_perf_query" + str(iter)
        # print("\nquery name {}".format(this_query_name))
        start = time.time()
        session.execute_dataflow(
            dff, query_name=this_query_name, is_async=False)
        end = time.time()
        print("time to do map {}: {}".format(iter, end - start))
        total_time = total_time + (end - start)
        session.get_table(dff.final_table_name).drop()

    print("-------------------------------------------")
    print("\naverage time to do map: {}\n".format(total_time / nRuns))
    print("-------------------------------------------")


def test_df_delete_after_timeout(client, session, airlines_dataset):
    delete_timeout_param_name = "DataflowStateDeleteTimeoutInSecs"
    delete_timeout_param_value = None
    test_time_out = 5    # secs
    for config_param in client.get_config_params():
        if config_param["param_name"] == delete_timeout_param_name:
            delete_timeout_param_value = int(config_param["param_value"])
            break
    assert delete_timeout_param_value
    client.set_config_param(
        param_name=delete_timeout_param_name, param_value=test_time_out)

    # now run a dataflow and wait for test_time_out secs to see if dataflow state deleted
    df = Dataflow.create_dataflow_from_dataset(client, airlines_dataset).map(
        [("int(DayOfWeek)", "DayOfWeek")]).map(
            [("len(Description)", "len_desc")]).filter("eq(DayOfWeek, 3)")
    result_table = "test_table"
    result_num_rows = 20130
    job_name = session.execute_dataflow(
        df,
        table_name=result_table,
        is_async=False,
        optimized=True,
        parallel_operations=random.choice([True, False]),
        clean_job_state=False)
    res_tab = session.get_table(result_table)
    assert res_tab.record_count() == result_num_rows
    time.sleep(test_time_out)
    # we have given enough time, query should be deleted by now
    with pytest.raises(XcalarApiStatusException) as e:
        while True:
            client._legacy_xcalar_api.queryState(job_name)
            time.sleep(0.1)
        assert e.status in [
            StatusT.StatusQrQueryNotExist, StatusT.StatusQrQueryAlreadyDeleted
        ]

    # set default value back
    client.set_config_param(
        param_name=delete_timeout_param_name,
        param_value=delete_timeout_param_value)


def test_table_access_from_xpus(client, workbook, airlines_dataset):
    # first create a table
    session = workbook.activate()
    df = Dataflow.create_dataflow_from_dataset(client, airlines_dataset)
    table_name = "tab_access_xpu"
    session.execute_dataflow(
        df, table_name=table_name, is_async=False, optimized=False)

    orig_tab = session.get_table(table_name)

    # add import udf
    import_udf = client.create_udf_module("test_udf", table_access_xpu_udf)

    # create dataset and table using it
    dataset_builder = workbook.build_dataset(
        "table_access_ds",
        "TableGen",
        "1",
        "udf",
        parser_name="test_udf:duplicate_table",
        parser_args={
            "session_name": session.name,
            "table_name": table_name
        })
    dataset = dataset_builder.load()
    df = Dataflow.create_dataflow_from_dataset(client, airlines_dataset)
    table_name = "new_tab_from_xpu"
    session.execute_dataflow(
        df, table_name=table_name, is_async=False, optimized=False)
    new_tab = session.get_table(table_name)

    assert orig_tab.record_count() == new_tab.record_count()
    assert orig_tab.columns == new_tab.columns

    # clean up
    orig_tab.drop(delete_completely=True)
    new_tab.drop(delete_completely=True)
    dataset.delete(delete_completely=True)
    import_udf.delete()

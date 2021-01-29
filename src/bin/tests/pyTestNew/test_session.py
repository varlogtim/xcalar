import pytest

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.LegacyApi.Operators import Operators

from xcalar.compute.util.Qa import buildNativePath, DefaultTargetName


@pytest.fixture(scope="module")
def setup(client, workbook):
    xcalarApi = XcalarApi()
    xcalarApi.setSession(workbook)
    udf = Udf(xcalarApi)
    op = Operators(xcalarApi)
    yield (client, workbook, udf, op)


def verify_download_upload(wb):
    dl = wb.download()
    uploadName = wb.name + "-upload"
    uplS = wb._client.upload_workbook(uploadName, dl)
    uplS.delete()


# this function is copied from test_udf.py and modified to
# make current session inactive, delete udf, attempt active and
# assert error string is propagated from xce to here
def test_session_replay_sanity(setup):
    (client, workbook, udf, op) = setup
    numTables = 0
    source = '''
def onLoad(fullPath, inStream):
    recs = [
        {"fruit": "apple", "count": 1},
        {"fruit": "banana", "count": 2},
        {"fruit": "orange", "count": 3},
        {"fruit": "grapefruit", "count": 4},
        {"fruit": "iran", "count": 5},
        ]
    for rec in recs:
        yield rec

def trunc(arg):
    # Truncate to len  4.
    return str(arg)[:4]

def concat(arg1, arg2):
    return str(arg1) + str(arg2)

def length(arg):
    return str(len(arg))

# Do both arguments start with "app"?
def startsWithApp(arg1, arg2):
    if arg1.startswith("app") and arg2.startswith("app"):
        return "true"
    else:
        return "" # This indicates false.
'''

    udf.addOrUpdate("pyTestUdfImm", source)

    path = buildNativePath("udfOnLoadDatasets/SandP.xlsx")
    db = workbook.build_dataset(
        "pyTestUdfImmDs",
        DefaultTargetName,
        path,
        "udf",
        parser_name="pyTestUdfImm:onLoad")
    dataset = db.load()
    op.indexDataset(".XcalarDS.pyTestUdfImmDs", "pyTestUdfImm0",
                    "xcalarRecordNum", "p")
    numTables += 1

    op.map("pyTestUdfImm0", "pyTestUdfImm1", "pyTestUdfImm:length(p::fruit)",
           "fruitLen")
    numTables += 1

    op.map("pyTestUdfImm1", "pyTestUdfImm2",
           "pyTestUdfImm:concat(p::fruit, p::count)", "concatted")
    numTables += 1

    op.map("pyTestUdfImm2", "pyTestUdfImm3", "pyTestUdfImm:trunc(p::fruit)",
           "fruitTrunc")
    numTables += 1

    # NOTE: list table not yet implemented in new SDK
    # sessTab = self.session.listTable()
    # assert(sessTab.numNodes == 4)

    # NOTE: list table not yet implemented in new SDK
    # test that an inactive session's table can be listed
    # sessTab = self.session.listTable()
    # assert(sessTab.numNodes == 4)

    verify_download_upload(workbook)

    dropResult = op.dropTable("pyTestUdfImm*")
    assert dropResult.numNodes == numTables

    udf.delete("pyTestUdfImm")

    dataset.delete()

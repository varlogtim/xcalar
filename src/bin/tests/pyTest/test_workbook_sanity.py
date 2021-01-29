import pytest

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.client import Client

from xcalar.compute.util.Qa import buildNativePath, DefaultTargetName

# Sanity tests for workbooks. Note that some state is shared between the
# pyTest test* methods (such as workbook names, datasets, etc.).

CommonDsPrefix = "testWkbookSanityOwner" + "-DS-"
CommonSessNamePrefix = "testWkbookSanityOwner" + "-Session-"
Dsname1 = CommonDsPrefix + "Data Set 1"
Dsname2 = CommonDsPrefix + "Data Set 2"
tabOneName = "table1"
tabTwoName = "table2"
tabTwoNewName = "table2NewName"
tabThreeName = "table3"
tmpTableName = "tmpTableName"
S1name = CommonSessNamePrefix + "Session Single Node Sanity Test S1"
S2name = CommonSessNamePrefix + "Session Single Node Sanity Test S2"
S2rename = CommonSessNamePrefix + "Renamed Session Single Node Sanity Test - Was S2"
S3name = CommonSessNamePrefix + "Session Single Node Sanity Test S3"
forkS1Name = CommonSessNamePrefix + "forkFromSessionTest S1"
forkS2Name = CommonSessNamePrefix + "forkFromSessionTest S2"
forkDSName = CommonDsPrefix + "forkFromSessionTest Dataset"

hasResStr = "Has resources"
hasNoResStr = "No resources"
DsPrefix = ".XcalarDS."
StatusSessionNotFoundStr = "Session does not exist"

workbooks_list = []

# XXX TODO This suite of test cases seem to not match the rest of the pyTest
# design, which is each test must be designed to run completely independently.
# Recomendation is to look at any other test suite like test_load.py.


@pytest.fixture(scope="module")
def setup():
    client = Client()
    workbook = client.create_workbook("sModName")
    workbook.activate()
    xcalarApi = XcalarApi()
    xcalarApi.setSession(workbook)
    yield (client, xcalarApi)

    for wb in client.list_workbooks():
        wb.delete()


# Calls to the download/upload routine below have to be made carefully with respect to
# deleting datasets. Basically, the upload could re-create a dataset that is supposed to
# have been deleted - so all dataset cleanout operations must be the last operations -no
# call to verifyDownloadUpload should follow dataset cleanout ops, if the 'sess' could
# re-create the dataset.
def verifyDownloadUpload(client, wb):
    dl = wb.download()
    uploadName = wb.name + "-upload"
    uplS = client.upload_workbook(uploadName, dl)
    uplS.delete()


# Create s1, s2 and s3. Validate list sanity.
#
# Actions for s1:
#
# Load dataset Dsname1 for s1.
# Index Dsname1 into tabOne and then index tabOne into tabTwo.
# Drop tabOne, index tabTwo into a new tabOne.
# Rename tabTwo to tabTwoNew.
# This should leave two tables: tabOne and tabTwoNew in s1. Validate this
# before a replay of s1 and validate that after replay, the tables are gone.
#
# s3 is a fork from s1. Load dataset Dsname2 for s3, index into a 3rd table
# in s3, and validate that s3 has 1 table (none from s1 fork and the new one)
#
# In s2, index Dsname1 into a temp table, drop it and verify num tables is 0
#
# Evaluate persist on inactive and active sessions. And validate that list
# reports hasNoResStr for inactive sessions.
#
def testSanity1(setup):
    (client, xcalarApi) = setup
    # Init the API objects for the 3 sessions this test uses
    s1XcalarApi = XcalarApi()
    s1Operators = Operators(s1XcalarApi)

    s2XcalarApi = XcalarApi()
    s2Operators = Operators(s2XcalarApi)

    s3XcalarApi = XcalarApi()
    s3Operators = Operators(s3XcalarApi)

    # Create a new session s1, activate it and validate list sanity

    s1 = client.create_workbook(S1name)
    workbooks_list.append(s1)
    listOut = client.list_workbooks(S1name)
    assert len(listOut) == 1
    assert listOut[0].info == hasResStr
    assert listOut[0].active_node == 0

    # Create s2 (will be used later)
    s2 = client.create_workbook(S2name, active=False)
    workbooks_list.append(s2)

    listOut = client.list_workbooks(CommonSessNamePrefix + "*")
    assert len(listOut) == 2
    assert listOut[0].name == S1name
    assert listOut[0].info == hasResStr
    assert listOut[0].active_node == 0
    assert listOut[1].name == S2name
    assert listOut[1].info == hasNoResStr
    assert listOut[1].active_node == -1

    # Create a dataset Dsname1, and load it into s1
    filename = "jsonRandom"
    path = buildNativePath(filename)
    # Init API object for s1
    s1XcalarApi.setSession(s1)
    dataset_builder = s1.build_dataset(Dsname1, DefaultTargetName, path,
                                       "json")
    dataset_builder.option("sampleSize", 1024 * 1024)
    dataset_builder.load()

    # Validate dataset list for s1
    dsl = s1XcalarApi.listDataset("*")
    assert dsl.numNodes == 1
    dsOneDsid = dsl.nodeInfo[0].dagNodeId
    dsOneName = dsl.nodeInfo[0].name
    assert dataset_builder.dataset_name == dsOneName

    # Index Dsname1 into a table tabOneName in s1
    tabRet = s1Operators.indexDataset(dataset_builder.dataset_name, tabOneName,
                                      "key", "p")
    assert tabRet == tabOneName

    # Index tabeOneName into a new table tabTwoName in s1
    tabRet = s1Operators.indexTable(tabOneName, tabTwoName, "p::value")
    assert tabRet == tabTwoName

    # Drop the first table tabOneName; note that second table depends on
    # this table so a replay of s1 should re-create tabOneName, so
    # tabTwoName can be created, and then it should be dropped
    s1Operators.dropTable(tabOneName)

    # Now that tabOneName is dropped, a new table with the exact same name
    # is created (replacing the dropped table). The new table is based off
    # tabTwoName
    tabRet = s1Operators.indexTable(tabTwoName, tabOneName, "p::key")
    assert tabRet == tabOneName

    # Rename tabTwoName to tabTwoNewName
    s1XcalarApi.renameNode(tabTwoName, tabTwoNewName)

    # Validate that list tables in s1 reports the two tables:
    # tabOneName and tabTwoNewName
    tabListOut = s1XcalarApi.listTable("*")
    assert tabListOut.numNodes == 2

    for ii in range(0, tabListOut.numNodes):
        assert tabListOut.nodeInfo[ii].name == tabOneName or    \
            tabListOut.nodeInfo[ii].name == tabTwoNewName

    # Now, replay of s1 should destroy tables; validate this
    s1.inactivate()
    s1.activate()

    # Ensure activating an already active session doesn't lead to an error
    s1.activate()

    verifyDownloadUpload(client, s1)

    tabListOut = s1XcalarApi.listTable("*")
    assert tabListOut.numNodes == 0

    # Now fork/copy s1 into new session s3
    s3 = s1.fork(S3name)
    workbooks_list.append(s3)

    # Init API object for s3
    s3XcalarApi.setSession(s3)
    s3.activate()

    # Create Dsname2, and load into s3 (which creates its only dataset)
    # Note that even though s3's forked from s1, it doesn't inherit the PQG or
    # SQG - it gets its own SQG which will now reflect the ds build below
    dataset_builder2 = s3.build_dataset(Dsname2, DefaultTargetName, path,
                                        "json")
    dataset_builder2.option("sampleSize", 1024 * 1024)
    dataset_builder2.load()

    # Validate dataset list for s3 - it'll have the one it built above
    dsl2 = s3XcalarApi.listDataset("*")
    assert dsl2.numNodes == 1
    dsOneDsid = dsl2.nodeInfo[0].dagNodeId
    dsOneName = dsl2.nodeInfo[0].name
    assert dataset_builder2.dataset_name == dsOneName

    # Now, index Dsname2 into tabThreeName table in s3. s3 should now have
    # the one table (table1 and table2NewName from s1-fork wouldn't have been
    # created since s3 starts afresh - so just the new table tabThreeName)
    tabRet = s3Operators.indexDataset(dsOneName, tabThreeName, "key", "p")
    assert tabRet == tabThreeName

    tabListOut = s3XcalarApi.listTable("*")
    assert tabListOut.numNodes == 1
    for ii in range(0, tabListOut.numNodes):
        print("table {} is {}".format(ii, tabListOut.nodeInfo[ii].name))

    verifyDownloadUpload(client, s3)

    # activate s2, index dataset Dsname1 into tmpTableName in S2, drop
    # this table, and verify number of tables in s2 is 0
    s2.activate()
    # init API object for s2
    s2XcalarApi.setSession(s2)

    tabRet = s2Operators.indexDataset(dataset_builder.dataset_name,
                                      tmpTableName, "key", "p")
    assert tabRet == tmpTableName
    s2Operators.dropTable(tmpTableName)
    tabListOut = s2XcalarApi.listTable("*")
    assert tabListOut.numNodes == 0

    verifyDownloadUpload(client, s2)

    # Inactivate S1 to check behavior of persist on inactive sessions
    s1.inactivate()
    # Persist the inactive session which should not lead to an error
    s1.persist()

    # Persist active sessions; these should succeed
    s2.persist()
    s3.persist()

    s3.inactivate()
    s2.inactivate()

    # All sessions now inactive and persisted. validate list all sessions
    # and that states for all reports hasNoResStr

    listAllSess = client.list_workbooks(CommonSessNamePrefix + "*")
    assert len(listAllSess) == 3
    for workbook in listAllSess:
        if workbook.name in [S1name, S2name, S3name]:
            assert workbook.info == hasNoResStr


# First validate that the session IDs for s1, s2, s3 are reported by list.
# Validate dataset list.
# Sanity check session rename and inactivate an already inactive session
def testSanity2(setup):
    (client, xcalarApi) = setup
    s1XcalarApi = XcalarApi()
    s1Operators = Operators(s1XcalarApi)

    s2XcalarApi = XcalarApi()
    s2Operators = Operators(s2XcalarApi)

    s3XcalarApi = XcalarApi()
    s3Operators = Operators(s3XcalarApi)

    listAllSess = client.list_workbooks(CommonSessNamePrefix + "*")
    assert len(listAllSess) == 3
    # Make sure the session Ids for created sessions are still valid
    # This also tests the validity of session.list output
    for ii in range(0, len(workbooks_list)):
        for workbook in listAllSess:
            if (workbook._workbook_id == workbooks_list[ii]._workbook_id):
                found = True
                break
        assert found is True

    # List all datasets from the session created by setup_class.
    # Even though datasets are global, the listDataset API used below
    # applies to the Dag nodes attached to xcalarApi (which is the session
    # created by setup_class) - since there are no datasets referenced by
    # this session, the number of ds nodes should be 0.
    dsl = xcalarApi.listDataset("*")
    assert dsl.numNodes == 0

    s1 = workbooks_list[0]
    s2 = workbooks_list[1]
    s3 = workbooks_list[2]

    # Validate that list all datasets from s1 scope returns 0 nodes, since
    # s1 was inactivated previously (in testSanity1) which must've deleted them
    s1.activate()
    s1XcalarApi.setSession(s1)
    dsl = s1XcalarApi.listDataset("*")
    assert dsl.numNodes == 0

    # Delete datasets created by this test (so prefixed by
    # testWkbookSanityOwner).  Once all workbooks which may have loaded
    # these datasets are deleted, the (global) datasets will be deleted

    unloadDS = s1Operators.dropDatasets("*" + CommonDsPrefix + "*")
    for ii in range(0, unloadDS.numDatasets):
        print("unload ds {} {} status {}".format(
            ii, unloadDS.statuses[ii].dataset.name,
            unloadDS.statuses[ii].status))

    s1.inactivate()    # so it can be deleted

    verifyDownloadUpload(client, s1)

    # Sanity check for session rename
    s2.rename(S2rename)
    s2l = client.get_workbook(S2rename)
    assert s2l.name == S2rename

    verifyDownloadUpload(client, s2)

    # Re-invoke the dropDatasets as the last operation since the calls to
    # self.verifyDownloadUpload() above could've re-created some of them
    # delDS = s1Operators.dropDatasets("*" + CommonDsPrefix + "*")
    for ii in range(0, unloadDS.numDatasets):
        print("unload ds {} {} status {}".format(
            ii, unloadDS.statuses[ii].dataset.name,
            unloadDS.statuses[ii].status))

    # Inactivate an session that is already inactive.  This should not
    # result in any error or crash
    s3.inactivate()


# Create s1f, load forkDSName dataset into s1f. Fork s2f from s1f. Delete
# the forkDSName dataset and validate the deletion. Replay both s1f, s2f.
# Validate that s2f has no tables (XXX: check why this is needed)
def testFork(setup):
    (client, xcalarApi) = setup
    s1fXcalarApi = XcalarApi()
    s1fOperators = Operators(s1fXcalarApi)

    s2fXcalarApi = XcalarApi()
    s2fOperators = Operators(s2fXcalarApi)

    s1f = client.create_workbook(forkS1Name)
    s1f.activate()

    # Init API object for s1f
    s1fXcalarApi.setSession(s1f)

    filename = "jsonRandom"
    path = buildNativePath(filename)
    dataset_builder = s1f.build_dataset(forkDSName, DefaultTargetName, path,
                                        "json")
    dataset_builder.option("sampleSize", 1024 * 1024)
    dataset_builder.load()

    # Deactivating s1f will destroy the SQG so don't inactivate s1f before
    # following operations

    verifyDownloadUpload(client, s1f)

    s2f = s1f.fork(forkS2Name)
    s2f.activate()

    # Delete dataset reference from s1f since dataset object has s1f context
    dataset1 = client.get_dataset(dataset_builder.dataset_name)
    dsdOut = dataset1.delete()

    # XXX: this fails due to "Session already inactivated" when trying to
    # destroy the newly uploaded session in self.verifyDownloadUpload(s1f).
    # This needs checking.
    # self.verifyDownloadUpload(s1f)

    s2f.inactivate()
    s1f.activate()
    s1f.inactivate()
    s2f.activate()

    verifyDownloadUpload(client, s2f)

    s2fXcalarApi.setSession(s2f)
    tabListOut = s2fXcalarApi.listTable("*")
    assert tabListOut.numNodes == 0

    verifyDownloadUpload(client, s2f)

    s2f.inactivate()

    # Now s1f, s2f are inactive and can be deleted
    s1f.delete()
    s2f.delete()


# Sanity test deletes - delete all belonging to a user and list before/after
def testDeleteSessions(setup):
    (client, xcalarApi) = setup
    listAllSess = client.list_workbooks("*")

    # XXX: Use workItem directly for delete ALL. New API is needed to bulk
    # delete all sessions.
    #
    # This will delete ALL belonging to username. Will not delete the one
    # created by setup_class which belongs to a different user
    for wb in client.list_workbooks():
        wb.delete()

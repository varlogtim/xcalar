import traceback
import getpass
import random
from multiprocessing import Pool

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
# This test continues to use the legacy session module as it is tightly
# coupled with the intricacies of sessions.
from xcalar.external.LegacyApi.Session import Session
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.LegacyApi.Dataset import UdfDataset
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.compute.util.Qa import buildNativePath, DefaultTargetName

# Brief Test Description:
# ----------------------
# Runs multiple users in parallel, each creating and operating on many workbooks
# Workbooks are active simultaneously - even those belonging to the same user.
# The idea is to simulate multiple users, all invoking ops on their workbooks
# simultaneously.
#
# Pass Criteria: no hangs, or crashes, and all workbooks ARE deleted at the end
#
# Duration: Settings of numUsers and numWorkBooks will impact test duration. So
# lower these values if the test takes too long.
#
# More details:
# -------------
#
# The test has two methods: testOneUserAndSetup and testConcUsers
#
# The test setup (via setup_class) creates a global dataset (in the backend all
# datasets are global), which uses an import UDF drawn from the UDF module in
# UdfSource. This UDF module is added to the workbook created by setup_class
# before the dataset is loaded.
#
# testOneUserAndSetup uses the initial workbook created by setup_class which
# already has the UDF, and it indexes the global dataset, uses map UDFs in the
# UDF module, and downloads / uploads the workbook several times. Each upload
# creates a new workbook, which is activated/inactivated etc., and then
# destroyed.
#
# testFixWorkbook uses the workbook created by setup_class. It invokes
# FixWorkbook module to add a new UDF to the workbook. It also tests the
# scenario in which a workbook is broken. The test will use FixWorkbook to fix
# the workbook by replacing the UDF with the correct one. The result of each
# operation will be verified.
#
# testConcUsers test launches many users, with many workbooks per user. Each
# user does the same set of workbook operations on each of its workbooks. Each
# user runs concurrently (via multiprocessing.Pool).  The workbook deletion and
# op invocation is randomized - so all workbooks aren't deleted at the same time
# at the end, nor are ops invoked on workbooks immediately on creation. The
# test randomly decides to add the UDF module or not to a workbook. For each
# workbook, the global dataset is indexed and map operations invoked on the
# table created - if the workbook doesn't have the UDF, it'll be copied from the
# global dataset at index time. This exercises the code path in which the UDF
# copy must check if the UDF module already exists, and if it does, to compare
# it with the one in the dataset - on a match, it succeeds, otherwise, the index
# should fail.
#
# Future enhancements as separate tests or extensions of this one:
#
# More randomization:
#     - Other aspects could be randomized, such as number of workbooks per user
#     - Add brief pauses b/w operations (may cause tests to slowdown though)
#     - Add different table ops / UDFs for different users
# Verification:
#     - e.g. invoke session.list() and verify the returned list is as expected
# Replay related:
#     - Leave some workbooks non-deleted, and then re-start the cluster
#
# XXX: Eventually, rename the core Session() class to Workbook() and make
# changes to all users of the class (e.g. change self.session to self.workbook
# here)
#

# Global params for test: each of numUsers creates and operates on numWorkBooks
numUsers = 2
numWorkBooks = 8
timeoutSecs = 6000    # if a user takes more than timeoutSecs, raise exception
globalDs = "pyTestUdfImmDs"
xcalarGlobalDsName = ".XcalarDS." + globalDs

UdfSource = '''
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

UdfSourceNew = '''
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

def truncNew(arg):
    # Truncate to len  4.
    return str(arg)[:4]

def concatNew(arg1, arg2):
    return str(arg1) + str(arg2)

def lengthNew(arg):
    return str(len(arg))

# Do both arguments start with "app"?
def startsWithApp(arg1, arg2):
    if arg1.startswith("app") and arg2.startswith("app"):
        return "true"
    else:
        return "" # This indicates false.
'''

# Function names are modified.
UdfSourceBroken = '''
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

def truncNewBroken(arg):
    # Truncate to len  4.
    return str(arg)[:4]

def concatNewBroken(arg1, arg2):
    return str(arg1) + str(arg2)

def lengthNewBroken(arg):
    return str(len(arg))

# Do both arguments start with "app"?
def startsWithApp(arg1, arg2):
    if arg1.startswith("app") and arg2.startswith("app"):
        return "true"
    else:
        return "" # This indicates false.
'''


# Common global method which creates a global dataset (globalDs) with
# the streaming UDF "onLoad", from the "pyTestUdfImm" UDF module. This is to
# be called only by setup_class which does this once to create the global
# dataset via the setup_class's workbook. Other workbooks and users will index
# this shared, global dataset.
def crDatasetWithUDF(cls):
    cls.udf.addOrUpdate("pyTestUdfImm", UdfSource)
    path = buildNativePath("udfOnLoadDatasets/SandP.xlsx")
    cls.dataset = UdfDataset(cls.xcalarApi, DefaultTargetName, path, globalDs,
                             "pyTestUdfImm:onLoad")
    cls.dataset.load()


# Can be invoked by any user or workbook to index the shared global dataset
def useUdfOnTable(self):
    numTables = 0
    self.op.indexDataset(xcalarGlobalDsName, "pyTestUdfImm0",
                         "xcalarRecordNum", "p")
    numTables += 1

    self.op.map("pyTestUdfImm0", "pyTestUdfImm1",
                "pyTestUdfImm:length(p::fruit)", "fruitLen")
    numTables += 1

    self.op.map("pyTestUdfImm1", "pyTestUdfImm2",
                "pyTestUdfImm:concat(p::fruit, p::count)", "concatted")
    numTables += 1

    self.op.map("pyTestUdfImm2", "pyTestUdfImm3",
                "pyTestUdfImm:trunc(p::fruit)", "fruitTrunc")
    numTables += 1
    return (numTables)


# Used to test that a new UDF, added via workbook file modification, worked
def useNewUDFonTable(op):
    numTables = 0
    op.indexDataset(xcalarGlobalDsName, "pyTestUdfImmNew0", "xcalarRecordNum",
                    "p")
    numTables += 1

    op.map("pyTestUdfImmNew0", "pyTestUdfImmNew1",
           "pyTestUdfImmNew:lengthNew(p::fruit)", "fruitLen")
    numTables += 1

    op.map("pyTestUdfImmNew1", "pyTestUdfImmNew2",
           "pyTestUdfImmNew:concatNew(p::fruit, p::count)", "concatted")
    numTables += 1

    op.map("pyTestUdfImmNew2", "pyTestUdfImmNew3",
           "pyTestUdfImmNew:truncNew(p::fruit)", "fruitTrunc")
    numTables += 1
    return (numTables)


def replayAndDestroy(session):
    session.inactivate()
    session.activate()
    session.destroy()


# Common global method which drops the session's tables.
# Expected to be used as a pair, after invoking useUdfOnTable() defined above
def clearTables(self, numTables):
    dropResult = self.op.dropTable("pyTestUdfImm*")
    assert dropResult.numNodes == numTables


class TestWorkbooks(object):
    def setup_class(cls):
        cls.xcalarApi = XcalarApi()
        cls.session = Session(cls.xcalarApi, "TestWorkbooks")
        cls.xcalarApi.setSession(cls.session)
        cls.operators = Operators(cls.xcalarApi)
        cls.udf = Udf(cls.xcalarApi)
        cls.op = Operators(cls.xcalarApi)
        cls.session.activate()
        crDatasetWithUDF(cls)

    def teardown_class(cls):
        cls.udf.delete("pyTestUdfImm")
        cls.dataset.delete()
        cls.session.destroy()
        pass

    # Installs UDF, and uses it on one worbook.
    def testOneUserAndSetup(self):
        numTables = 0
        downloadList = []
        uploadNames = []
        numTables = useUdfOnTable(self)

        downloadedWb = self.session.download(self.session.name, None)
        uploadName = self.session.name + "upload"
        # A new session is being uploaded, to create two active sessions (one
        # created by setup_class and this one) so that the test below is valid
        # (since it depends on there being two active sessions).
        uploadWb = self.session.upload(uploadName, downloadedWb, None)

        newXcalarApi = XcalarApi()
        uploadedSess = Session(
            newXcalarApi,
            "TestWorkbooks",
            reuseExistingSession=True,
            sessionName=uploadName)
        newXcalarApi.setSession(uploadedSess)

        # Setting current session name to NULL; but there are 2 active
        # sessions. This should fail (we don't allow empty session name if more
        # than one workbook is active). This is to validate the failure. Note
        # that the two flavors of APIs which need to be tested are bulk (delete,
        # and inactivate), and non-bulk (all the rest). This is because they
        # follow different code paths for session selection and both paths need
        # to be tested
        savedName = self.session.name
        self.session.name = ""
        try:
            self.session.persist()    # example of non-bulk API
        except Exception as e:
            print("session persist failed with '{}'".format(e.message))
            assert e.message == "Session does not exist"

        try:
            self.session.delete()    # example of bulk API
        except Exception as e:
            print("session delete failed with '{}'".format(e.message))
            assert e.message == "Session does not exist"

        self.session.name = savedName    # restore session name

        # Now resume normal operations...

        # Now the newly uploaded workbook is put through one replay cycle, and
        # then destroyed
        replayAndDestroy(uploadedSess)

        iterations = 20
        for ii in range(0, iterations):
            downloadList.append(self.session.download(self.session.name, None))
            uploadNames.append(self.session.name + "-upload-" + str(ii))

        for ii in range(0, iterations):
            print("uploading {}".format(uploadNames[ii]))
            self.session.upload(uploadNames[ii], downloadList[ii], None)
            uploadedSess = Session(
                self.xcalarApi,
                "TestWorkbooks",
                reuseExistingSession=True,
                sessionName=uploadNames[ii])
            uploadedSess.inactivate()
            uploadedSess.activate()
            print("destroying {}".format(uploadNames[ii]))
            uploadedSess.destroy()

        sessTab = self.session.listTable()
        assert sessTab.numNodes == numTables

        self.session.persist()
        self.session.inactivate()
        self.session.activate()

        # Note that inactivate() above would've removed the SQG and
        # re-activating it gives you back an empty SQG - so any results (e.g.
        # tables or dag nodes) of previous operations such as useUdfOnTable on
        # the session are now gone

    def testConcUsers(self):
        pool = Pool(numUsers)
        userThreads = []
        for ii in range(0, numUsers):
            # each thread represents a different synthetic user
            username = "{}-{}".format(getpass.getuser(), ii)
            testname = "wBTest"
            print("user name {} is {}".format(ii, username))
            t = pool.apply_async(
                launchUser,
                (testname, username, numWorkBooks, "invokeWorkBookOps"))
            print("user thread {} is {}".format(ii, str(t)))
            userThreads.append(t)
        print("user threads are all running")
        for t in userThreads:
            t.get(timeoutSecs)
        print("user threads have all finished")
        pool.close()
        pool.join()


def launchUser(testname, username, numWorkBooks, fn):
    try:
        user = WorkBookUser(testname, username, numWorkBooks)
        fn = getattr(user, fn)
        return fn()    # each user runs fn() to operate on numWorkBooks
    except Exception:
        traceback.print_exc()
        return None


class UserTestClass():
    def __init__(self, session, xcalarApi, op, udf):
        self.session = session
        self.xcalarApi = xcalarApi
        self.op = op
        self.udf = udf

    # This method carries out several workbook ops which are idempotent
    def idemOps(self):
        iterations = 2
        for ii in range(0, iterations):
            self.listResult = self.session.list()
            self.numSess = self.listResult.numSessions
            self.sessList = self.listResult.sessions
            for ii in range(0, self.numSess):
                print("session {} has sessID {}".format(
                    ii, self.sessList[ii].sessionId))
            self.session.persist()

    # This method carries out several workbook ops which are NOT idempotent
    def nonIdemOps(self):
        newName = self.session.name + "newName"
        self.session.rename(newName)

        # Load a dataset, index it, and use installed UDF
        numTables = useUdfOnTable(self)

        self.session.persist()

        # Download this wb, and then upload it with a new name
        # Note this wb will have a UDF copied from current wb. Inactivate,
        # and activate it, then destroy it

        download = self.session.download(newName, None)
        uploadName = self.session.name + "upload"
        uploadWB = self.session.upload(uploadName, download, None)

        print("uploaded WB {} has sessID {}".format(uploadName,
                                                    uploadWB.sessionId))

        # Activate the newly uploaded workbook
        uploadedSess = Session(
            self.xcalarApi,
            "TestWorkbooks",
            reuseExistingSession=True,
            sessionName=uploadName)

        # Repeat the inactivate, activate cycle and destroy it
        uploadedSess.inactivate()
        uploadedSess.activate()
        uploadedSess.destroy()

        sessTab = self.session.listTable()
        assert sessTab.numNodes == numTables

        self.session.inactivate()    # destroys SQG
        self.session.activate()    # creates empty SQG
        self.session.persist()


def genWorkBookName(testName, userName, number):
    return testName + userName + str(number)


class WorkBookUser():
    def __init__(self, testName, userName, numWorkBooks):
        self.testName = testName
        self.userName = userName
        self.numWorkBooks = numWorkBooks
        self.session = []
        self.xcalarApi = []
        self.op = []
        self.udf = []
        self.sessDesList = []
        self.sessDefer = []
        print("User {} starting test {} with {} workBooks".format(
            userName, testName, numWorkBooks))

    def userTestInvoke(self, thisWorkBook, xcalarApi, op, udf, fn):
        try:
            userTest = UserTestClass(thisWorkBook, xcalarApi, op, udf)
            fn = getattr(userTest, fn)
            return fn()
        except Exception:
            traceback.print_exc()
            return None

    def invokeWorkBookOps(self):
        for ii in range(0, self.numWorkBooks):
            self.xcalarApi.append(XcalarApi())
            wName = genWorkBookName(self.testName, self.userName, ii)
            print("workBook name is {}".format(wName))
            self.session.append(
                Session(
                    self.xcalarApi[ii],
                    self.testName,
                    username=self.userName,
                    sessionName=wName))
            self.xcalarApi[ii].setSession(self.session[ii])
            self.op.append(Operators(self.xcalarApi[ii]))
            self.udf.append(Udf(self.xcalarApi[ii]))
            self.session[ii].activate()
            # Flip a coin: either add the UDF or not; since the UDF is in the
            # global dataset which this workbook will index, the UDF module will
            # be copied into this workbook, if it wasn't added below. If it's
            # added below, the UDF module copy during ds index, will detect that
            # it's the exact same module and treat this as success. So the idea
            # is to test both code paths by adding (or not) the UDF module here
            if random.randint(0, 1) == 0:
                self.udf[ii].addOrUpdate("pyTestUdfImm", UdfSource)

            # flip a coin: either invoke ops immediately or defer them
            if random.randint(0, 1) == 0:
                print(
                    "nonIdem op invocation immediately on workbook {}".format(
                        self.session[ii].name))
                self.userTestInvoke(self.session[ii], self.xcalarApi[ii],
                                    self.op[ii], self.udf[ii], "nonIdemOps")
            else:
                print("nonIdem op invocation deferred on workbook {}".format(
                    self.session[ii].name))
                self.sessDefer.append(ii)

        # Now invoke ops on workbooks which were deferred
        for ix in self.sessDefer:
            print("deferred nonIdem ops on {}".format(self.session[ix].name))
            self.userTestInvoke(self.session[ix], self.xcalarApi[ix],
                                self.op[ix], self.udf[ix], "nonIdemOps")

        # Now invoke idempotent ops and destroy some workbooks after that
        for sess in self.session:
            print("idem ops on {}".format(sess.name))
            self.userTestInvoke(sess, None, None, None, "idemOps")
            # flip a coin:
            # tails you lose - sess is destroyed
            # heads you win - sess killing deferred to class destruction
            if random.randint(0, 1) == 0:
                print("destroying session {}".format(sess.name))
                sess.destroy()
            else:
                print("defer destroy of session {}".format(sess.name))
                self.sessDesList.append(sess)

        # Now delete any workbooks left
        for sess in self.sessDesList:
            print("invoking deferred destroy on sess {}".format(sess.name))
            sess.destroy()

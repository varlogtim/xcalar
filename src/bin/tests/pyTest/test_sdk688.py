import random
import string
import traceback

from multiprocessing import Pool

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
# This test continues to use the legacy session module as it is tightly
# coupled with the intricacies of sessions.
from xcalar.external.LegacyApi.Session import Session
from xcalar.external.client import Client
from xcalar.external.kvstore import KvStore as SDKKvStore

from xcalar.external.LegacyApi.Udf import Udf

from xcalar.external.LegacyApi.Operators import Operators

# this test is designed for run multi thread to heavily
# execute kvstore opreatons on servaral shared workbooks
numThread = 100
numWorkBooks = 3
timeoutSecs = 6000    # if a user takes more than timeoutSecs, raise exception
testName = "wBTest"
userName = "haveFun"


def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


rand_val = randomString(1000000)


class TestWorkbooks(object):
    def setup_class(cls):
        cls.xcalarApi = XcalarApi(bypass_proxy=True)
        cls.session = Session(cls.xcalarApi, "TestWorkbooks")
        cls.xcalarApi.setSession(cls.session)
        cls.operators = Operators(cls.xcalarApi)
        cls.op = Operators(cls.xcalarApi)
        cls.testName = testName
        cls.userName = userName
        cls.session.activate()
        cls.testWbs = []
        cls.xcalarApis = []

    def teardown_class(cls):
        cls.session.destroy()
        pass

    def testConcUsers(self):
        pool = Pool(numThread)
        userThreads = []
        # first create all workbooks
        for ii in range(0, numWorkBooks):
            wName = "sdk-688session" + str(ii)
            print("workBook name being created is {}".format(wName))
            self.xcalarApis.append(XcalarApi(bypass_proxy=True))
            self.testWbs.append(
                Session(
                    self.xcalarApis[ii],
                    self.testName,
                    username=self.userName,
                    reuseExistingSession=False,
                    sessionName=wName))

        for ii in range(0, numThread):
            # each thread represents a different synthetic user
            print("user name {} is {}".format(ii, self.userName))
            t = pool.apply_async(launchUser,
                                 (self.testName, self.userName, numWorkBooks,
                                  "invokeWorkBookOps"))
            print("user thread {} is {}".format(ii, str(t)))
            userThreads.append(t)
        print("user threads are all running")
        for t in userThreads:
            t.get(timeoutSecs)
        print("user threads have all finished")
        pool.close()
        pool.join()

        for wb in self.testWbs:
            print("workBook name being destroyed is {}".format(wb.name))
            wb.destroy()


def launchUser(testname, username, numWorkBooks, fn):
    try:
        user = WorkBookUser(testname, username, numWorkBooks)
        fn = getattr(user, fn)
        return fn()    # each user runs fn() to operate on numWorkBooks
    except Exception:
        traceback.print_exc()
        return None


class UserTestClass():
    def __init__(self, session, xcalarApi, op):
        self.session = session
        self.xcalarApi = xcalarApi
        self.op = op

    def test_kvstore(self):
        client = Client(bypass_proxy=True)
        session_kvstore = SDKKvStore.workbook_by_name(
            client, self.session.username, self.session.name)
        key = "KVSimpleTestKey"
        kvstore = client.global_kvstore()
        kvstore.add_or_replace(key, "value1", True)

        assert key in kvstore.list()
        assert key in kvstore.list(key)
        assert key in kvstore.list(f"{key}|other")
        assert key not in kvstore.list("falsepattern")

        assert kvstore.lookup(key) == "value1"
        key1, key2 = "foo", "bar"
        value1, value2 = "foo_value", "bar_value"
        session_kvstore.add_or_replace(key1, value1, True)
        session_kvstore.add_or_replace(key2, value2, True)
        session_kvstore.set_if_equal(True, key1, value1, value1)
        session_kvstore.set_if_equal(True, key2, value2, value2)
        try:
            session_kvstore.set_if_equal(True, key1, "notTrue", value1)
        except Exception:
            pass
        try:
            session_kvstore.set_if_equal(True, key2, "notTrue", value2)
        except Exception:
            pass
        keyR = randomString()
        kvstore.add_or_replace(keyR, rand_val, True)
        kvstore.delete(keyR)
        try:
            kvstore.delete("nonexitingkey")
        except Exception:
            pass
        try:
            kvstore.lookup("nonexitingkey")
        except Exception:
            pass
        assert key1 in session_kvstore.list()
        assert key2 in session_kvstore.list()
        assert session_kvstore.lookup(key1) == value1
        assert session_kvstore.lookup(key2) == value2
        keyR = randomString()
        session_kvstore.add_or_replace(keyR, rand_val, True)
        session_kvstore.delete(keyR)
        try:
            session_kvstore.delete("nonexitingkey")
        except Exception:
            pass
        try:
            session_kvstore.lookup("nonexitingkey")
        except Exception:
            pass
        if random.randint(0, 1) == 0:
            for ii in range(0, random.randint(3, 10)):
                keyR = randomString()
                kvstore.add_or_replace(keyR, rand_val, True)
                kvstore.delete(keyR)
            assert key1 in session_kvstore.list()
            assert key2 in session_kvstore.list()
        key1 = "adam"
        key2 = "jack"
        value1, value2 = "foo_value_updated", "bar_value_updated"
        kv_dict = {key1: value1, key2: value2}
        session_kvstore.multi_add_or_replace(kv_dict, True)
        assert key1 in session_kvstore.list()
        assert key2 in session_kvstore.list()
        assert session_kvstore.lookup(key1) == value1
        assert session_kvstore.lookup(key2) == value2
        for ii in range(0, random.randint(3, 10)):
            keyR = randomString()
            session_kvstore.add_or_replace(keyR, rand_val, True)
            session_kvstore.delete(keyR)
        print("kvstore_test_done")


# This method carries out several workbook ops which are idempotent


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
            userTest = UserTestClass(thisWorkBook, xcalarApi, op)
            fn = getattr(userTest, fn)
            return fn()
        except Exception:
            traceback.print_exc()
            return None

    def invokeWorkBookOps(self):
        for ii in range(0, self.numWorkBooks):
            self.xcalarApi.append(XcalarApi(bypass_proxy=True))
            wName = "sdk-688session" + str(ii)
            print("workBook name is {}".format(wName))
            self.session.append(
                Session(
                    self.xcalarApi[ii],
                    self.testName,
                    username=self.userName,
                    reuseExistingSession=True,
                    sessionName=wName))
            self.xcalarApi[ii].setSession(self.session[ii])
            self.op.append(Operators(self.xcalarApi[ii]))
            self.udf.append(Udf(self.xcalarApi[ii]))
            self.session[ii].activate()
        random.shuffle(self.session)
        for ii in range(0, self.numWorkBooks):
            self.userTestInvoke(self.session[ii], self.xcalarApi[ii],
                                self.op[ii], self.udf[ii], "test_kvstore")

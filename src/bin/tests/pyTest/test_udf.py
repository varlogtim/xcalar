import pytest
import os
import traceback
import threading
import tarfile
import json
from multiprocessing import Pool

from xcalar.compute.util.Qa import (XcalarQaDatasetPath, buildNativePath,
                                    DefaultTargetName, restartCluster)
from UdfTestUtils import UdfTestUtils

from xcalar.external.client import Client
from xcalar.external.exceptions import XDPException

from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.LegacyApi.Dataset import CsvDataset, JsonDataset, UdfDataset
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.LegacyApi.Count import TableCount
from xcalar.external.Retina import Retina
from xcalar.external.LegacyApi.Target2 import Target2

from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiWorkbookScopeT

memoryTargetName = "QA memory"
targets = [
    (memoryTargetName, "memory", {}),
]

source1 = "def foo(arg):\n return 'foo'\n"
source2 = "def bar(arg):\n return 'bar'\n"
source3 = """
def foo(arg):
    return 'fooOverride'
def bar(arg):
    return 'barOverride'
    """

sourceSyntaxError = "def foo:\n return 6\n"

sourceExecOnLoad = """
def outputSmaller(fullPath, inStream):
    rec = {"column0":"a", "column1":"b", "column2":"c"}
    yield rec
    yield rec

def outputLarger(fullPath, inStream):
    recs = [
        {"Country": "United States",
            "Average Per Capita Elbows": 2},
        {"Country": "Venezuela",
            "Average Per Capita Elbows": 2},
        {"Country": "Democratic Socialist Republic of North Dakota",
            "Average Per Capita Elbows": 2.00000000001},
        ]
    for rec in recs:
        yield rec

def outputAugmentsInput(fullPath, inStream):
    for line in inStream.read().decode("utf-8").split(';'):
        cols = []
        for col in line.split(','):
            cols.append(col * 3)
        record = {}
        for ii,col in enumerate(cols):
            record['column{}'.format(ii)] = col
        yield record

def outputManyPages(fullPath, inStream):
    row = {'column0': 'apple',
          'column1': 'banana',
          'column2': 'Jeremy Clarkson'}
    for ii in range(4096):
        yield row
        """


# Utility class for executing UDF operations in different process.
class UdfThreadUtility(object):
    def __init__(self, wkbookName):
        self.client = Client()
        self.xcalarApi = XcalarApi()
        self.session = self.client.get_session(wkbookName)
        self.session._reuse()
        self.xcalarApi.setSession(self.session)
        self.udf = Udf(self.xcalarApi)
        self.utils = UdfTestUtils(self.client, self.session, self.xcalarApi, self.udf)

    def __del__(self):
        self.xcalarApi.setSession(None)
        self.xcalarApi = None

    def get(self, moduleName):
        return self.udf.get(moduleName).source

    def add(self, moduleName, source):
        try:
            self.udf.add(moduleName, source)
        except XcalarApiStatusException as e:
            assert (e.status == StatusT.StatusUdfModuleAlreadyExists
                    or e.status == StatusT.StatusUdfModuleInUse)
            return 0
        return 1

    def delete(self, moduleName):
        try:
            self.udf.delete(moduleName)
        except XcalarApiStatusException as e:
            assert (e.status == StatusT.StatusUdfModuleNotFound
                    or e.status == StatusT.StatusUdfModuleInUse)
            return 0
        return 1

    def update(self, moduleName, source):
        try:
            self.udf.update(moduleName, source)
        except XcalarApiStatusException as e:
            assert (e.status == StatusT.StatusUdfModuleNotFound
                    or e.status == StatusT.StatusUdfModuleInUse)
            return 0
        return 1

    def execOnLoad(self, numIters):
        for i in range(numIters):
            # Verification function followed by invocation of load.
            def outputSmallerVerify(rows):
                assert (rows == [{
                    'column0': 'a',
                    'column1': 'b',
                    'column2': 'c'
                }, {
                    'column0': 'a',
                    'column1': 'b',
                    'column2': 'c'
                }])

            self.utils.onLoad(
                ["apple,banana,orange", "grapefruit,donkey,blueberry"],
                "pyTestUdfExecOnLoad:outputSmaller", outputSmallerVerify)

            def outputLargerVerify(rows):
                assert (rows[0]['Country'] == 'United States')

            self.utils.onLoad(["a,b,c", "d,e,f"],
                              "pyTestUdfExecOnLoad:outputLarger",
                              outputLargerVerify, True)

            def outputAugmentVerify(rows):
                assert (rows[1]['column2'] == "eee")

            self.utils.onLoad(["a,b,c", "d,e,f"],
                              "pyTestUdfExecOnLoad:outputAugmentsInput",
                              outputAugmentVerify)

            # This one intentionally takes a while.
            def outputManyPagesVerify(rows):
                assert (len(rows) == 4096)

            self.utils.onLoad(["a"], "pyTestUdfExecOnLoad:outputManyPages",
                              outputManyPagesVerify)

        return True


# Invokes given function (fn) in new instance of UdfThreadUtility class. args gives array
# of arguments (in order).
def invoke(wkbookName, fn, args):
    try:
        udfThread = UdfThreadUtility(wkbookName)
        fn = getattr(udfThread, fn)
        return fn(*args)
    except Exception:
        traceback.print_exc()
        return None


class TestUdf(object):
    def setup_class(cls):
        cls.client = Client()
        cls.xcalarApi = XcalarApi()
        cls.session = cls.client.create_session("TestUdf")
        cls.xcalarApi.setSession(cls.session)
        cls.udf = Udf(cls.xcalarApi)
        cls.utils = UdfTestUtils(cls.client, cls.session, cls.xcalarApi, cls.udf)
        cls.op = Operators(cls.xcalarApi)
        cls.retina = Retina(cls.xcalarApi)
        cls.target = Target2(cls.xcalarApi)
        for targetName, targetTypeId, targetArgs in targets:
            cls.target.add(targetTypeId, targetName, targetArgs)

    def teardown_class(cls):
        cls.session.destroy()
        cls.udf = None
        cls.xcalarApi = None
        cls.session = None

    def testRetinaUpdate(self):
        retinaPath = XcalarQaDatasetPath + "/retinaTests/basic_ops_df.tar.gz"
        retinaName = "retina_update_test"
        with open(retinaPath, "rb") as fp:
            retinaContents = fp.read()
            self.retina.add(retinaName, retinaContents)
        # add udfs to session
        with tarfile.open(retinaPath, "r:gz") as tFile:
            tflist = tFile.getmembers()
            for tmemb in tflist:
                if tmemb.isfile() and 'udfs/' in tmemb.name:
                    udfsource = tFile.extractfile(tmemb).read()
                    udfsourcestr = udfsource.decode("utf-8")
                    moduleName = os.path.basename(tmemb.name).split('.', 1)[0]
                    if moduleName != "default":
                        self.udf.addOrUpdate(moduleName, udfsourcestr)
        retObj = self.retina.getDict(retinaName)
        self.retina.update(retinaName,
                           retObj)    # Invoke update with no change
        testPrefix = "testUdf-retinaUpdate"
        datasetName = "%s-dataset" % testPrefix
        retinaParameters = [{
            "paramName": "pathToQaDatasets",
            "paramValue": XcalarQaDatasetPath
        }, {
            "paramName": "datasetName",
            "paramValue": datasetName
        }, {
            "paramName": "tablePrefix",
            "paramValue": testPrefix
        }]
        dstTable = retObj["tables"][0]["name"]
        retinaDstTable = "%s-%s" % (testPrefix, dstTable)
        self.retina.execute(retinaName, retinaParameters, retinaDstTable)

    def testQueryUdfPath(self):
        # Open a retina and add its udfs into the current session
        retinaPath = XcalarQaDatasetPath + "/retinaTests/basic_ops_df.tar.gz"
        with tarfile.open(retinaPath, "r:gz") as tFile:
            infoFile = tFile.extractfile("dataflowInfo.json")
            retinaInfo = json.loads(infoFile.read())
            tflist = tFile.getmembers()
            for tmemb in tflist:
                if tmemb.isfile() and 'udfs/' in tmemb.name:
                    udfsource = tFile.extractfile(tmemb).read()
                    udfsourcestr = udfsource.decode("utf-8")
                    moduleName = os.path.basename(tmemb.name).split('.', 1)[0]
                    if moduleName != "default":
                        self.udf.addOrUpdate(moduleName, udfsourcestr)

        # Run the query in a new session, but use the previous session's udfs
        newSess = self.client.create_session("NewSession")
        self.xcalarApi.setSession(newSess)

        self.xcalarApi.submitQuery(
            json.dumps(retinaInfo["query"]),
            "NewSession",
            "q1",
            udfUserName=self.session.username,
            udfSessionName=self.session.name)

        # Upload the query as a new retina and run
        ret = Retina(self.xcalarApi)
        ret.add(
            "ret1",
            retinaJsonStr=json.dumps(retinaInfo),
            udfUserName=self.session.username,
            udfSessionName=self.session.name)

        # Note that the imported retina has no UDF container so one must be
        # explicitly specified when invoking the execute method on the retina

        # Manually specified udf container
        ret.execute(
            "ret1", [],
            "tmpTable2",
            udfUserName=self.session.username,
            udfSessionName=self.session.name)

        newSess.destroy()
        self.xcalarApi.setSession(self.session)

    def testSanity(self):
        # Delete UDFs from previous test.
        try:
            self.udf.delete("pyTestUdf*")
        except XcalarApiStatusException as e:
            assert (e.status == StatusT.StatusUdfModuleNotFound)

        # Test add, get, delete.
        self.udf.add("pyTestUdfAddGetDelete", source1)
        assert (self.udf.get("pyTestUdfAddGetDelete").source == source1)

        self.udf.delete("pyTestUdfAddGetDelete")
        with pytest.raises(XcalarApiStatusException) as exc:
            self.udf.get("pyTestUdfAaddGetDelete").output.hdr.status
        assert (exc.value.status == StatusT.StatusUdfModuleNotFound)

        # Test update.
        self.udf.add("pyTestUdfAddUpdate", source1)
        self.udf.update("pyTestUdfAddUpdate", source2)
        assert (self.udf.get("pyTestUdfAddUpdate").source == source2)

        self.utils.executeOnArrayAndVerify("pyTestUdfAddUpdate", "bar",
                                           ["a", "b", "c"],
                                           ["bar", "bar", "bar"])

        self.udf.add("pyTestUdfBadUpdate", source1)
        with pytest.raises(XcalarApiStatusException) as exc:
            self.udf.update("pyTestUdfBadUpdate", sourceSyntaxError)
        assert (exc.value.status == StatusT.StatusUdfModuleLoadFailed)
        assert (self.udf.get("pyTestUdfBadUpdate").source == source1)
        self.utils.executeOnArrayAndVerify("pyTestUdfBadUpdate", "foo",
                                           ["a", "b", "c"],
                                           ["foo", "foo", "foo"])

    def testEdge(self):
        sourceManyArgs = """
def myfunc(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p):
    return p
"""
        self.udf.addOrUpdate("pyTestUdfEdgeArgs", sourceManyArgs)

    def assertAddFails(self, moduleName, source, status, message=None):
        with pytest.raises(XcalarApiStatusException) as exc:
            self.udf.add(moduleName, source)
        assert (exc.value.status == status)
        if message:
            assert message in exc.value.output.udfAddUpdateOutput.error.message

        with pytest.raises(XcalarApiStatusException) as exc:
            self.udf.get(moduleName)
        assert (exc.value.status == StatusT.StatusUdfModuleNotFound)

    def testValidation(self):
        self.assertAddFails("moduleName&InvalidChar", source1,
                            StatusT.StatusUdfModuleInvalidName)
        self.assertAddFails("moduleName:withColon", source1,
                            StatusT.StatusUdfModuleInvalidName)
        self.assertAddFails("emptySource", "", StatusT.StatusUdfModuleEmpty)
        self.assertAddFails("", source1, StatusT.StatusUdfModuleInvalidName)

    def testErrorMessage(self):
        self.assertAddFails("syntaxError", sourceSyntaxError,
                            StatusT.StatusUdfModuleLoadFailed,
                            "invalid syntax")

    def testRollback(self):
        # The below source will fail to be added/updated 1/4 of the time.
        unstableSource = """
import random
if random.randint(0, 3) == 0:
    x = 1 // 0
def foo():
    return 'foo'
"""
        for i in range(200):
            try:
                self.udf.add("pyTestUdfunstable" + str(i), unstableSource)
                self.udf.update("pyTestUdfunstable" + str(i), unstableSource)
            except XcalarApiStatusException as e:
                assert (e.status == StatusT.StatusUdfModuleLoadFailed)

    def testThreads(self):
        # XXX Kill childnodes sporadically during this test.
        numThreads = 2
        timeoutSecs = 300

        # Interactions between py.test, multiprocessing, and thrift are less than ideal.
        # The timeouts below exist because failures here tend to manifest in hung worker
        # processes that are difficult to terminate.

        pool = Pool(processes=numThreads)

        getThreads = []
        for i in range(numThreads):
            getThreads.append(
                pool.apply_async(
                    invoke,
                    (self.session.name, "get", ["pyTestUdfAddUpdate"])))
        for t in getThreads:
            assert (t.get(timeoutSecs) == source2)

        addThreads = []
        for i in range(numThreads):
            addThreads.append(
                pool.apply_async(invoke, (self.session.name, "add",
                                          ["pyTestUdfThreadedAdd", source1])))
        totalAdded = 0
        for t in addThreads:
            totalAdded += t.get(timeoutSecs)
        assert (totalAdded == 1)

        deleteThreads = []
        for i in range(numThreads):
            deleteThreads.append(
                pool.apply_async(
                    invoke,
                    (self.session.name, "delete", ["pyTestUdfThreadedAdd"])))
        totalDeleted = 0
        for t in deleteThreads:
            totalDeleted += t.get(timeoutSecs)
        assert (totalDeleted == 1)

        self.udf.add("pyTestUdfThreadedUpdate", source1)
        updateThreads = []
        for i in range(numThreads):
            updateThreads.append(
                pool.apply_async(invoke,
                                 (self.session.name, "update",
                                  ["pyTestUdfThreadedUpdate", source2])))
        totalUpdates = 0
        for t in updateThreads:
            totalUpdates += t.get(timeoutSecs)
        assert (totalUpdates <= numThreads)
        assert (self.udf.get("pyTestUdfThreadedUpdate").source == source2)
        pool.close()
        pool.join()

    def testExecOnLoad(self):
        self.udf.addOrUpdate("pyTestUdfExecOnLoad", sourceExecOnLoad)
        assert invoke(self.session.name, "execOnLoad", [1])

    # Executes updates of an in-use UDF.
    def annoyingUpdateThread(self):
        while self.beAnnoying:
            self.udf.update("pyTestUdfExecOnLoad", sourceExecOnLoad)

    @pytest.mark.skip("Broke by Xc-4597")
    def testExecOnLoadThreads(self):
        numThreads = 9
        timeoutSecs = 75

        pool = Pool(processes=numThreads)

        # Only one thread per process may be interacting with thrift at the same time.
        self.beAnnoying = True
        updateThread = threading.Thread(target=self.annoyingUpdateThread)
        updateThread.start()

        execThreads = []
        for i in range(numThreads):
            # Invoke execOnLoad test in new process.
            execThreads.append(
                pool.apply_async(invoke,
                                 (self.session.name, "execOnLoad", [2])))

        for t in execThreads:
            assert (t.get(timeoutSecs))

        self.beAnnoying = False
        updateThread.join(2)
        pool.close()
        pool.join()

    @pytest.mark.skip("Xc-5662")
    def testExecOnLoadError(self):
        source = '''
def throwsError(fullPath, inStream):
    yield inStream + 4

def throwsBigError(fullPath, inStream):
    # Just some gibberish.
    raise Exception("workItem->output->outputResult.getVersionOutput.apiVersionSignatureFullworkItem->output->outputResult.getVersionOutput.apiVersionSignatureFull" * 16)
    yield 5 # needs to be a generator function
        '''
        self.udf.addOrUpdate("pyTestUdfExecOnLoadError", source)

        # Actual input ignored. Just need a small file.
        path = buildNativePath("csvSanity/columnNameSpace.csv")

        dataset = UdfDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            "pyTestUdfExecOnLoadError:throwsError",
            name="littleError")
        loadResult = dataset.load()
        assert "concatenate" in loadResult.errorString
        dataset.delete()

        dataset = UdfDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            "pyTestUdfExecOnLoadError:throwsBigError",
            name="bigError")
        loadResult = dataset.load()
        assert "getVersionOutput" in loadResult.errorString
        dataset.delete()

    def testExcel(self):
        path = buildNativePath("udfOnLoadDatasets/customer6.xlsx")

        dataset = UdfDataset(self.xcalarApi, DefaultTargetName, path,
                             "excel sandp", "default:openExcel")
        dataset.load()
        assert (dataset.record_count() == 125)
        dataset.delete()

    def testImmediates(self):
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
        self.udf.addOrUpdate("pyTestUdfImm", source)

        path = buildNativePath("udfOnLoadDatasets/customer6.xlsx")
        dataset = UdfDataset(self.xcalarApi, DefaultTargetName, path,
                             "pyTestUdfImmDs", "pyTestUdfImm:onLoad")
        dataset.load()
        self.op.indexDataset(".XcalarDS.pyTestUdfImmDs", "pyTestUdfImm0",
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

        # First map dealing with only immediates.
        self.op.map("pyTestUdfImm3", "pyTestUdfImm4",
                    "pyTestUdfImm:trunc(p::concatted)", "concatTrunc")
        numTables += 1

        # Filter dealing with only (2) immediates.
        self.op.filter(
            "pyTestUdfImm4", "pyTestUdfImmDiscard0",
            "eq(pyTestUdfImm:trunc(concatted), pyTestUdfImm:trunc(p::fruit))")
        numTables += 1
        assert TableCount(self.xcalarApi, "pyTestUdfImmDiscard0").total() == 5

        # Lots of immediates!
        evalStr = (
            "pyTestUdfImm:concat(pyTestUdfImm:concat(fruitTrunc, fruitLen)," +
            "                    pyTestUdfImm:concat(concatted, concatTrunc))")
        self.op.map("pyTestUdfImm4", "pyTestUdfImm5", evalStr, "blob")
        numTables += 1
        assert TableCount(self.xcalarApi, "pyTestUdfImm5").total() == 5

        self.op.filter("pyTestUdfImm5", "pyTestUdfImmDiscard1",
                       "pyTestUdfImm:startsWithApp(blob, concatTrunc)")
        numTables += 1
        # XXX Xc-4060 We cannot, today, return "False" from a UDF because all strings
        #     are interpretted by XcalarEval as true. Should be 1.
        assert TableCount(self.xcalarApi, "pyTestUdfImmDiscard1").total() == 5

        # A misplaced edge case test.
        # This will produce FieldNotFound since the field 'a' and 'b' won't
        # exist for any records. This is impossible to know ahead of time, and
        # thus is not a fatal error
        self.op.map("pyTestUdfImm0", "pyTestUdfImmDiscard2",
                    "pyTestUdfImm:concat(a, b)", "hopeThisWorks")
        numTables += 1

        dropResult = self.op.dropTable("pyTestUdfImm*")
        assert dropResult.numNodes == numTables

        dataset.delete()

    def testFNF(self):
        numTables = 0
        source = '''
def convertFNF(arg):
    if arg is None:
        return "true"
    else:
        return "false"
'''
        self.udf.addOrUpdate("testFNFUdf", source)

        path = buildNativePath("jsonSanity/hasFNF.json")

        dataset = JsonDataset(
            self.xcalarApi, DefaultTargetName, path, name="hasFNF")
        dataset.load()
        assert (dataset.record_count() == 2)

        self.op.indexDataset(".XcalarDS.hasFNF", "pyTestHasFNF", "xcalar", "p")
        numTables += 1

        # Run our UDF on the table.  It will map FNF -> "true" or "false"
        self.op.map("pyTestHasFNF", "pyTestHasFNF2",
                    "testFNFUdf:convertFNF(p::a)", "a_udf")
        numTables += 1

        # Filter out the rows that were "false"
        self.op.filter("pyTestHasFNF2", "pyTestHasFNF3", "eq(a_udf,\"true\")")
        numTables += 1

        # Original dataset has two rows, one row has a FNF in field "a".
        # The FNF was converted to "true" so there should be only a single
        # row with "true".
        assert TableCount(self.xcalarApi, "pyTestHasFNF3").total() == 1

        dropResult = self.op.dropTable("pyTestHasFNF*")
        assert (dropResult.numNodes == numTables)

        dataset.delete()

    def testArgs(self):
        source = '''
def normalfn(arg):
    return arg
def argsSum(*varargs):
    s = 0
    for i in varargs:
        s += int(i)
    return s

def argsSumWithInit(initValue, *args):
    s = int(initValue)
    for i in args:
        s += int(i)
    return s

def defaultArgs(a, b="b", c=None):
    return b

def generateDataset(inPath, inStream):
    recs = [
        {"number": 9,       "word": "nine",   "other": 9},
        {"number": 8792384, "word": "turkey", "other": "(the country)"},
        {"number": 1223,    "word": "toiletspiders"},
    ]
    for r in recs:
        yield r
'''
        self.udf.addOrUpdate("pyTestUdfArgs", source)

        numTables = 0
        path = buildNativePath("udfOnLoadDatasets/customer6.xlsx")
        dataset = UdfDataset(self.xcalarApi, DefaultTargetName, path,
                             "pyTestUdfArgs", "pyTestUdfArgs:generateDataset")
        dataset.load()
        self.op.indexDataset(".XcalarDS.pyTestUdfArgs", "pyTestUdfArgs0",
                             "xcalarRecordNum", "p")
        numTables += 1

        self.utils.executeAndVerify("pyTestUdfArgs0",
                                    "pyTestUdfArgs:argsSum(p::number)",
                                    [9, 8792384, 1223])
        self.utils.executeAndVerify("pyTestUdfArgs0",
                                    "pyTestUdfArgs:argsSum(p::number)",
                                    [9, 8792384, 1223])
        self.utils.executeAndVerify(
            "pyTestUdfArgs0", "pyTestUdfArgs:argsSum(p::number, p::number, 4)",
            [9 * 2 + 4, 8792384 * 2 + 4, 1223 * 2 + 4])
        self.utils.executeAndVerify(
            "pyTestUdfArgs0",
            "pyTestUdfArgs:defaultArgs(pyTestUdfArgs:argsSum(p::number))",
            ["b", "b", "b"])
        self.utils.executeAndVerify("pyTestUdfArgs0",
                                    "pyTestUdfArgs:argsSum()", [0, 0, 0])

        self.utils.executeAndVerify(
            "pyTestUdfArgs0", "pyTestUdfArgs:argsSumWithInit(9)", [9, 9, 9])
        self.utils.executeAndVerify(
            "pyTestUdfArgs0",
            "pyTestUdfArgs:argsSumWithInit(-2, p::number, 3)",
            [10, 8792385, 1224])

        self.utils.executeAndVerify(
            "pyTestUdfArgs0", "pyTestUdfArgs:argsSumWithInit()", [0, 0, 0])

        self.utils.executeAndVerify("pyTestUdfArgs0",
                                    "pyTestUdfArgs:defaultArgs(p::number)",
                                    ["b", "b", "b"])
        self.utils.executeAndVerify(
            "pyTestUdfArgs0", "pyTestUdfArgs:defaultArgs()", ["b", "b", "b"])

        self.utils.executeAndVerify("pyTestUdfArgs0",
                                    "pyTestUdfArgs:defaultArgs(1, p::number)",
                                    [9, 8792384, 1223])

        dropResult = self.op.dropTable("pyTestUdfArgs*")
        assert dropResult.numNodes == numTables

        dataset.delete()

    def testMurderOnLoad(self):
        source = """
import os, signal
def murder(contents, inpath):
    os.kill(os.getpid(), signal.SIGSEGV)
"""
        self.udf.addOrUpdate("pyTestUdfMurder", source)
        path = buildNativePath("udfOnLoadDatasets/customer6.xlsx")
        dataset = UdfDataset(self.xcalarApi, DefaultTargetName, path,
                             "pytest udf murder", "pyTestUdfMurder:murder")
        try:
            dataset.load()
        except XcalarApiStatusException as e:
            pass
        else:
            assert False

    @pytest.mark.nolocal
    def testBillionTimestamps(self):
        source = """
import datetime
def convertFromUnixTS(colName, outputFormat):
        return datetime.datetime.fromtimestamp(float(colName)).strftime(outputFormat)
"""
        self.udf.add("pyTestUdfBillion", source)
        path = buildNativePath("timestamps")
        dataset = CsvDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            name="pytest udf billion",
            fileNamePattern="file*")
        dataset.load()
        self.op.indexDataset(".XcalarDS.pyTestUdfBillionDs",
                             "pyTestUdfBillion", "xcalarRecordNum", "p")

        self.op.map(
            "pyTestUdfBillion", "pyTestUdfBillionMap",
            "pyTestUdfBillion:convertFromUnixTS(p::column0, \"%m%d\")", "day")
        assert TableCount(self.xcalarApi,
                          "pyTestUdfBillionMap").total() == 1000000000
        self.op.dropTable("pyTestUdfBillion*")
        dataset.delete()

    @pytest.mark.skip("Need script to restart GCE cluster.")
    def testRestart(self):
        self.udf.add("pyTestUdfAfterRestart", source1)

        self.session.destroy()
        self.session = None
        self.xcalarApi.setSession(None)

        # Start usrnodes in a manner specific to this environment.
        restartCluster()

        self.session = self.client.create_session("TestUdfTestRestart")
        self.xcalarApi.setSession(self.session)

        self.udf.get("pyTestUdfAfterRestart")
        self.udf.update("pyTestUdfAfterRestart", source2)
        assert (self.udf.get("pyTestUdfAfterRestart").source == source2)

        self.session.destroy()
        self.xcalarApi.setSession(None)
        self.session = None

    # This is a very targeted test
    # If we have a retina with UDFs in it, in general it's not obvious
    # whether to run the system's version of the UDF, or the retina's version
    # of the UDF. Regardless of which of these we run, the system UDFs should
    # be unaltered.
    def testRetinaWithDefault(self):
        retinaName = "udfRetinaDefaultTest"
        retinaPath = os.path.join(XcalarQaDatasetPath,
                                  "retinaWithDefUdf.tar.gz")

        oldDefault = self.udf.get("default")

        retinaContents = None
        with open(retinaPath, "rb") as f:
            retinaContents = f.read()
        self.retina.add(retinaName, retinaContents)

        newDefault = self.udf.get("default")
        assert oldDefault == newDefault

    # Witness to Xc-7204
    def testBadUdfXdfNesting(self):
        source = '''
def badUdf(reviewCount):
    if int(reviewCount) % 2 == 0:
        return "5"
    else:
        raise Exception("Hello")
'''
        prefix = "pyTestBadUdfXdfTesting"
        moduleName = prefix + "udf"
        dsName = prefix + "ds"
        self.udf.add(moduleName, source)

        path = buildNativePath("yelp/user")
        dataset = JsonDataset(self.xcalarApi, DefaultTargetName, path, dsName)
        dataset.load()

        numTables = 0
        self.op.indexDataset(".XcalarDS." + dsName, prefix + "0",
                             "xcalarRecordNum", "p")
        numTables += 1

        self.op.map(prefix + "0", prefix + "1",
                    moduleName + ":badUdf(p::review_count)", "bogus")
        numTables += 1

        dropResult = self.op.dropTable(prefix + "*")
        assert dropResult.numNodes == numTables

        dataset.delete()

    # Shared UDF Test cases:
    # 0. Add shared UDF, verify add using get/getRes, and then execute it
    # 1. Update shared UDF, verify update using get, and its execution
    # 2. Add new UDF to current session with same name (to override shared UDF)
    #     - verify override by executing q-graph which references the UDF
    # 3. Execute shared UDF from new workbook to vet inter-workbook sharing
    #     - verify that new workbook doesn't have UDF (i.e. resolves to shared)
    # 4. Update current session's UDF, and re-execute
    def testSharedUdfSanity(self):
        # Add a UDF to shared space, verify it was added and can be executed
        self.udf.add("/sharedUDFs/pyTestUdfShared", source1)
        assert (self.udf.get("/sharedUDFs/pyTestUdfShared").source == source1)
        self.utils.executeOnArrayAndVerify(
            "pyTestUdfShared", "foo", ["a", "b", "c"], ["foo", "foo", "foo"])

        # Verify UDF resolution is to shared dir despite session scope
        assert (self.udf.getRes("pyTestUdfShared").udfResPath ==
                "/sharedUDFs/pyTestUdfShared")

        # Verify UDF resolution is to shared dir in global scope too
        scope = XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal
        assert (self.udf.getRes(
            "pyTestUdfShared",
            scope).udfResPath == "/sharedUDFs/pyTestUdfShared")

        # Then update the shared UDF, verify the update and its execution
        self.udf.addOrUpdate("/sharedUDFs/pyTestUdfShared", source2)
        assert (self.udf.get("/sharedUDFs/pyTestUdfShared").source == source2)
        self.utils.executeOnArrayAndVerify(
            "pyTestUdfShared", "bar", ["a", "b", "c"], ["bar", "bar", "bar"])

        # Revert shared UDF back to original version
        self.udf.addOrUpdate("/sharedUDFs/pyTestUdfShared", source1)
        assert (self.udf.get("/sharedUDFs/pyTestUdfShared").source == source1)

        # beat up on map operation invoking the same exact UDF module name each
        # time, but with changes to the UDF in-between invocations, by direct
        # change to the UDF and via interpositioning (workbook overriding
        # shared) and verifying that each invocation uses the latest change
        # to the UDF. The loop count can be changed to increase stress if needed
        # but keeping it low so this test doesn't consume too much time
        for ii in range(0, 10):
            self.utils.executeOnArrayAndVerify("pyTestUdfShared", "foo",
                                               ["a", "b", "c"],
                                               ["foo", "foo", "foo"])
            self.udf.add("pyTestUdfShared", source2)    # override shared UDF
            assert (self.udf.get("pyTestUdfShared").source == source2)
            self.utils.executeOnArrayAndVerify("pyTestUdfShared", "bar",
                                               ["a", "b", "c"],
                                               ["bar", "bar", "bar"])
            self.udf.delete("pyTestUdfShared")    # delete from workbook

            # the UDF should now resolve to shared space since workbook copy is
            # deleted
            self.utils.executeOnArrayAndVerify("pyTestUdfShared", "foo",
                                               ["a", "b", "c"],
                                               ["foo", "foo", "foo"])
            self.udf.addOrUpdate("/sharedUDFs/pyTestUdfShared", source2)
            assert (
                self.udf.get("/sharedUDFs/pyTestUdfShared").source == source2)
            self.utils.executeOnArrayAndVerify("pyTestUdfShared", "bar",
                                               ["a", "b", "c"],
                                               ["bar", "bar", "bar"])
            self.udf.addOrUpdate("/sharedUDFs/pyTestUdfShared", source1)
            assert (
                self.udf.get("/sharedUDFs/pyTestUdfShared").source == source1)

        # Now override the shared UDF module, with a new definition for UDF in
        # the current session (source2) and verify that execution gets the
        # current session's version, not the shared version

        self.udf.add("pyTestUdfShared", source2)    # override shared UDF
        assert (self.udf.get("pyTestUdfShared").source == source2)
        self.utils.executeOnArrayAndVerify(
            "pyTestUdfShared", "bar", ["a", "b", "c"], ["bar", "bar", "bar"])

        # Verify UDF resolves to current session which has a module with the
        # same name, overriding the shared UDF module
        assert (self.udf.getRes("pyTestUdfShared").udfResPath !=
                "/sharedUDFs/pyTestUdfShared")

        # Verify UDF resolves to shared dir despite the session-scoped override
        # if the scope is set to global
        scope = XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal
        assert (self.udf.getRes(
            "pyTestUdfShared",
            scope).udfResPath == "/sharedUDFs/pyTestUdfShared")

        # Create a new session so shared UDF can be tested for access from a
        # different session (other than current session)

        # Check that from the other session, the UDF reference continues to
        # resolve to the shared UDF since it has no override ! Destroy new
        # session afterwards since it's not needed any more

        newSess = self.client.create_session("NewSessionForSharedUDFs")
        self.xcalarApi.setSession(newSess)
        self.newUdf = Udf(self.xcalarApi)
        self.newUtils = UdfTestUtils(self.client, newSess, self.xcalarApi, self.newUdf)

        # Access shared UDF from new session using absolute path
        assert (
            self.newUdf.get("/sharedUDFs/pyTestUdfShared").source == source1)
        # Verify that accessing UDF using relative name in new Session fails
        with pytest.raises(XcalarApiStatusException) as exc:
            self.newUdf.get("pyTestUdfShared").output.hdr.status
        assert (exc.value.status == StatusT.StatusUdfModuleNotFound)

        # Verify that shared UDF is accessed from new session without override
        assert (self.udf.getRes("pyTestUdfShared").udfResPath ==
                "/sharedUDFs/pyTestUdfShared")

        # Access shared UDF from new session using relative path / execution
        # The combination of failure above and success below shows that the
        # UDF was obtained from shared space (since it's absent in session)
        self.newUtils.executeOnArrayAndVerify(
            "pyTestUdfShared", "foo", ["a", "b", "c"], ["foo", "foo", "foo"])
        newSess.destroy()

        # Switch back to current session and verify override still works
        self.xcalarApi.setSession(self.session)
        assert (self.udf.get("pyTestUdfShared").source == source2)

        # Verify UDF still resolves to session which overrides the shared UDF
        assert (self.udf.getRes("pyTestUdfShared").udfResPath !=
                "/sharedUDFs/pyTestUdfShared")

        self.utils.executeOnArrayAndVerify(
            "pyTestUdfShared", "bar", ["a", "b", "c"], ["bar", "bar", "bar"])

        # Yet another override - update the current session's UDF w/ source3
        # and verify its execution
        self.udf.addOrUpdate("pyTestUdfShared",
                             source3)    # override shared UDF
        assert (self.udf.get("pyTestUdfShared").source == source3)

        # Verify UDF continues to resolve to session despite update
        assert (self.udf.getRes("pyTestUdfShared").udfResPath !=
                "/sharedUDFs/pyTestUdfShared")

        self.utils.executeOnArrayAndVerify(
            "pyTestUdfShared", "foo", ["a", "b", "c"],
            ["fooOverride", "fooOverride", "fooOverride"])

        # Cleanup - delete the shared UDF
        self.udf.delete("/sharedUDFs/pyTestUdfShared")

        # Validate that delete worked - by attempting to get it and list it
        with pytest.raises(XcalarApiStatusException) as exc:
            self.udf.get("/sharedUDFs/pyTestUdfShared").output.hdr.status
        assert (exc.value.status == StatusT.StatusUdfModuleNotFound)

        listOut = self.udf.list("/sharedUDFs/pyTestUdfShared*", "*")
        assert (listOut.numXdfs == 0)

        # Verify UDF global scoped resolution request fails since shared UDF
        # is now deleted
        scope = XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal
        with pytest.raises(XDPException) as exc:
            self.udf.getRes("pyTestUdfShared", scope).output.hdr.status
        assert (exc.value.statusCode == StatusT.StatusUdfModuleNotFound)

        # ...but session scoped resolution continues to work!
        assert (self.udf.getRes("pyTestUdfShared").udfResPath !=
                "/sharedUDFs/pyTestUdfShared")

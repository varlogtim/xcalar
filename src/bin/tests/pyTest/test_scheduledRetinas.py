import random
import json
import time
import os
import sys
import subprocess

from xcalar.external.app import App
from xcalar.external.client import Client

# TODO: This is very hacky.  Remove.
xlrDir = os.environ["XLRDIR"]
pathToScheduleApps = os.path.join(xlrDir, "scripts", "scheduleRetinas", "apps")
pathToScheduleSupport = os.path.join(xlrDir, "scripts", "scheduleRetinas",
                                     "supportModules")
pathToScheduleCronExecute = os.path.join(xlrDir, "scripts", "scheduleRetinas",
                                         "executeFromCron")
if pathToScheduleApps not in sys.path:
    sys.path.append(pathToScheduleApps)
if pathToScheduleSupport not in sys.path:
    sys.path.append(pathToScheduleSupport)
if pathToScheduleCronExecute not in sys.path:
    sys.path.append(pathToScheduleCronExecute)

# This does some wacky stuff with python path; let's just ignore the static
# analyzer for all of these, but don't copy this pattern!
from kvMutex import KVMutex    # noqa: E402
from cronHandler import ScheduleCronHandler    # noqa: E402
from kvHandler import ScheduleKVHandler    # noqa: E402
from executeScheduledRetina import RetinaScheduler    # noqa: E402
from listSchedule import ScheduleLister    # noqa: E402
from createSchedule import ScheduleCreator    # noqa: E402
from deleteSchedule import ScheduleDeleter    # noqa: E402
from updateSchedule import ScheduleUpdater    # noqa: E402

RetinaCronTag = "_XcalarScheduledRetina"


class TestCronHandler(object):
    def setup_class(cls):
        cls.uniqueKey = "randomGarbageCronStr"
        cls.cronHandler = None

    def teardown_class(cls):
        if cls.cronHandler:
            cls.cronHandler.deleteSchedule(cls.uniqueKeyFuture)
        cls.cronHandler = None
        os.system("crontab -r")

    def testCronHandler(self):
        cronHandler = ScheduleCronHandler(RetinaCronTag)
        self.cronHandler = cronHandler
        cronTimeInfo = "1 1 1 1 1"
        cronLine = (cronTimeInfo + " python /blah/bloo.py " +
                    cronHandler.scheduleKeyToCronKey(str(
                        self.uniqueKey)) + " --biweekly")

        cronLineInfo = cronHandler.getScheduleInfoFromLine(cronLine)
        assert (cronLineInfo)
        assert (cronLineInfo["cronTimingParams"] == cronTimeInfo)
        assert (cronLineInfo["scheduleKey"] == self.uniqueKey)
        assert (cronLineInfo["isBiweekly"] is True)

        cronHandler.deleteSchedule(self.uniqueKey)
        outBytes = subprocess.check_output(["crontab", "-l"])
        assert (outBytes.decode("utf-8").find(self.uniqueKey) == -1)
        assert (not cronHandler.findScheduleInCron(self.uniqueKey))

        cronLineInfo = cronHandler.findScheduleInCron(self.uniqueKey)

        cronHandler.deleteSchedule(self.uniqueKey)
        outBytes = subprocess.check_output(["crontab", "-l"])
        assert (outBytes.decode("utf-8").find(self.uniqueKey) == -1)


class TestKVHandler(object):
    def setup_class(cls):
        cls.client = Client()
        cls.session = cls.client.create_session(cls.__class__.__name__)
        cls.kvStore = cls.client.global_kvstore()
        cls.uniqueKey = "randomGarbageKVString"
        cls.kvHandler = None

    def teardown_class(cls):
        # Hard teardown:
        if cls.kvHandler:
            try:
                cls.kvStore.delete(cls.kvHandler.ScheduleListKey)
            except Exception as e:
                pass
            try:
                cls.kvStore.delete(cls.kvHandler.ScheduleListLock)
            except Exception as e:
                pass

        cls.client.destroy_session(cls.session.name)
        cls.session = None
        cls.kvStore = None
        cls.kvHandler = None
        os.system("crontab -r")

    def testKVHandler(self):
        kvHandler = ScheduleKVHandler(
            scheduleTag=RetinaCronTag,
            client=self.client,
            session=self.session,
            kvStore=self.kvStore)
        self.kvHandler = kvHandler

        kvHandler.deleteSchedule(self.uniqueKey)
        scheduleListArr = kvHandler.getScheduleList()
        assert (kvHandler.getSchedule(self.uniqueKey, scheduleListArr) is None)
        assert (kvHandler.getScheduleResults(self.uniqueKey) is None)

        kvHandler.createSchedule({
            "testItem": 1,
            "scheduleKey": self.uniqueKey
        })
        scheduleListArr = kvHandler.getScheduleList()
        kvSched = kvHandler.getSchedule(self.uniqueKey, scheduleListArr)
        kvRes = kvHandler.getScheduleResults(self.uniqueKey)
        assert (len(kvSched) == 2)
        assert (kvSched.get("testItem", None) == 1)
        assert (kvRes == [])
        kvHandler.updateScheduleResults(self.uniqueKey, {"testResult": 0})
        kvRes = kvHandler.getScheduleResults(self.uniqueKey)
        assert (len(kvRes) == 1)
        assert (kvRes[0].get("testResult", None) == 0)
        kvHandler.deleteSchedule(self.uniqueKey)
        scheduleListArr = kvHandler.getScheduleList()
        assert (kvHandler.getSchedule(self.uniqueKey, scheduleListArr) is None)
        assert (kvHandler.getScheduleResults(self.uniqueKey) is None)


class TestExecuteScheduledRetina(object):
    def setup_class(cls):
        cls.uniqueKeyPast = "past" + str(random.randint(0, 999999)).zfill(6)
        cls.uniqueKeyFuture = "future" + str(random.randint(0,
                                                            999999)).zfill(6)

        cls.futureSchedule = '{"retName":"joinretina2","scheduleKey":"' + cls.uniqueKeyFuture + '","substitutions":{},"scheduleType":"Retina","options":{"activeSession":false,"newTableName":"","isPaused":false},"timingInfo":{"startTime":2007735469000,"dateText":"2/21/2019","timeText":"07 : 51 PM","repeat":"minute","modified":1486587921119,"recur":4}}'
        cls.pastSchedule = '{"retName":"joinretina2","scheduleKey":"' + cls.uniqueKeyPast + '","substitutions":{},"scheduleType":"Retina","options":{"activeSession":false,"newTableName":"","isPaused":false},"timingInfo":{"startTime":1447735469000,"dateText":"2/21/2016","timeText":"07 : 51 PM","repeat":"minute","modified":1486587921119,"recur":4}}'
        cls.client = Client()
        cls.session = cls.client.create_session("TestExecuteScheduledRetina")
        cls.kvStore = cls.client.global_kvstore()
        cls.kvHandler = ScheduleKVHandler(
            scheduleTag=RetinaCronTag,
            client=cls.client,
            session=cls.session,
            kvStore=cls.kvStore)
        cls.kvHandler.deleteSchedule(cls.uniqueKeyPast)
        cls.kvHandler.deleteSchedule(cls.uniqueKeyFuture)
        cls.kvHandler.createSchedule(json.loads(cls.futureSchedule))
        cls.kvHandler.createSchedule(json.loads(cls.pastSchedule))

    def teardown_class(cls):
        cls.kvHandler.deleteSchedule(cls.uniqueKeyPast)
        cls.kvHandler.deleteSchedule(cls.uniqueKeyFuture)
        cls.session.destroy()
        cls.session = None
        cls.kvStore = None
        cls.kvHandler = None
        os.system("crontab -r")

    def testScheduledRetina(self):
        pastScheduler = RetinaScheduler(
            scheduleName=self.uniqueKeyPast,
            scheduleTag=RetinaCronTag,
            biweekly=False)
        assert (pastScheduler.main() is not None)
        assert (self.kvHandler.areResultsEmpty(self.uniqueKeyPast) is not True)

        futureScheduler = RetinaScheduler(
            scheduleName=self.uniqueKeyFuture,
            scheduleTag=RetinaCronTag,
            biweekly=False)
        assert (futureScheduler.main() is None)
        assert (self.kvHandler.areResultsEmpty(self.uniqueKeyFuture) is True)


class TestConsistencyChecker(object):
    def setup_class(cls):
        cls.client = Client()
        cls.session = cls.client.create_session("TestConsistencyChecker")
        cls.kvStore = cls.client.global_kvstore()
        cls.kvHandler = ScheduleKVHandler(
            scheduleTag=RetinaCronTag,
            client=cls.client,
            session=cls.session,
            kvStore=cls.kvStore)
        cls.cronHandler = ScheduleCronHandler(RetinaCronTag)
        cls.uniqueKeyFuture = "futureSchedTestTest"

        cls.futureScheduleStr = '{"retName":"joinretina2","scheduleKey":"' + cls.uniqueKeyFuture + '","substitutions":{},"options":{"activeSession":false,"newTableName":"","isPaused":false},"timingInfo":{"startTime":2007735469000,"dateText":"2/21/2019","timeText":"07 : 51 PM","repeat":"minute","modified":1486587921119,"recur":4}}'

        cls.futureSchedule = json.loads(cls.futureScheduleStr)

        cls.futureSchedCronStr = cls.cronHandler.makeFullCronString(
            cls.futureSchedule["timingInfo"],
            cls.futureSchedule["scheduleKey"])

        # TODO: real deep copy
        futureScheduleNewerMod = {
            "startTime": cls.futureSchedule["timingInfo"]["startTime"],
            "repeat": cls.futureSchedule["timingInfo"]["repeat"],
            "modified": cls.futureSchedule["timingInfo"]["modified"] + 100
        }

        cls.futureScheduleNewer = {
            "retName": cls.futureSchedule["retName"],
            "scheduleKey": cls.futureSchedule["scheduleKey"],
            "substitutions": cls.futureSchedule["substitutions"],
            "options": cls.futureSchedule["options"],
            "timingInfo": futureScheduleNewerMod
        }

    def teardown_class(cls):
        try:
            cls.kvHandler.deleteSchedule(cls.uniqueKeyFuture, True)
        except Exception:
            pass
        try:
            cls.cronHandler.deleteSchedule(cls.uniqueKeyFuture)
        except Exception:
            pass

        cls.session.destroy()
        cls.session = None
        cls.kvStore = None
        cls.kvHandler = None
        cls.cronhandler = None
        os.system("crontab -r")


class TestScheduleAppsAsLocal(object):
    def setup_class(cls):
        cls.uniqueKeyPast = "pastSchedTestTest"
        cls.uniqueKeyFuture = "futureSchedTestTest"

        cls.futureSchedule = '{"retName":"joinretina2","scheduleKey":"' + cls.uniqueKeyFuture + '","substitutions":{},"options":{"activeSession":false,"newTableName":"","isPaused":false},"timingInfo":{"startTime":2007735469000,"dateText":"2/21/2019","timeText":"07 : 51 PM","repeat":"minute","modified":1486587921119,"recur":4}}'
        cls.pastSchedule = '{"retName":"joinretina2","scheduleKey":"' + cls.uniqueKeyPast + '","substitutions":{},"options":{"activeSession":false,"newTableName":"","isPaused":false},"timingInfo":{"startTime":1447735469000,"dateText":"2/21/2016","timeText":"07 : 51 PM","repeat":"minute","modified":1486587921119,"recur":4}}'
        cls.updateSchedule = '{"retName":"joinretina2","scheduleKey":"' + cls.uniqueKeyFuture + '","substitutions":{"N":10,"year":2017},"options":{"activeSession":false,"newTableName":"","isPaused":false},"timingInfo":{"startTime":1447735469000,"dateText":"2/21/2016","timeText":"07 : 51 PM","repeat":"minute","modified":1486587921119,"recur":4}}'
        cls.client = Client()
        cls.session = cls.client.create_session("TestScheduleAppsAsLocal")
        cls.kvStore = cls.client.global_kvstore()
        cls.kvHandler = ScheduleKVHandler(
            scheduleTag=RetinaCronTag,
            client=cls.client,
            session=cls.session,
            kvStore=cls.kvStore)
        cls.cronHandler = ScheduleCronHandler(RetinaCronTag)
        cls.kvMutex = KVMutex()

    def teardown_class(cls):
        cls.kvMutex.forceReleaseLock()
        try:
            cls.scheduleDeleter.deleteSchedule(cls.uniqueKeyPast)
        except Exception as e:
            pass
        try:
            cls.scheduleDeleter.deleteSchedule(cls.uniqueKeyFuture)
        except Exception as e:
            pass

        cls.session.destroy()
        cls.session = None
        cls.kvStore = None
        cls.kvHandler = None
        cls.cronhandler = None
        os.system("crontab -r")

    def testScheduleAppsAsLocal(self):
        scheduleCreator = ScheduleCreator(scheduleTag=RetinaCronTag)
        scheduleLister = ScheduleLister(scheduleTag=RetinaCronTag)
        scheduleDeleter = ScheduleDeleter(scheduleTag=RetinaCronTag)
        scheduleUpdater = ScheduleUpdater(scheduleTag=RetinaCronTag)
        self.scheduleDeleter = scheduleDeleter

        scheduleDeleter.execute(
            json.dumps({
                "scheduleKey": self.uniqueKeyPast
            }))
        scheduleDeleter.execute(
            json.dumps({
                "scheduleKey": self.uniqueKeyFuture
            }))
        assert (scheduleLister.execute(
            json.dumps({
                "scheduleKey": self.uniqueKeyPast
            })) == [])
        assert (scheduleLister.execute(
            json.dumps({
                "scheduleKey": self.uniqueKeyFuture
            })) == [])
        scheduleCreator.execute(self.futureSchedule)
        scheduleCreator = ScheduleCreator(scheduleTag=RetinaCronTag)
        scheduleCreator.execute(self.pastSchedule)
        # TODO: this is the shortest interval possible to wait for job
        # to execute.  Is this too long for test?  Can take up to over
        # one minute
        # Uncomment for debugging local problems
        # otherwise, same feature is largely tested in TestScheduleAppsAsApps
        # now = datetime.now()
        # now1 = now.replace(minute=now.minute + 1, second=10)
        # timeDelt = now1 - now
        # time.sleep(timeDelt.total_seconds())
        # futureList = scheduleLister.listSchedule(self.uniqueKeyFuture)
        # pastList = scheduleLister.listSchedule(self.uniqueKeyPast)
        # assert(not futureList[0]["scheduleResults"])
        # assert(pastList[0]["scheduleResults"])
        scheduleListArr = self.kvHandler.getScheduleList()
        assert (self.kvHandler.getSchedule(
            self.uniqueKeyFuture, scheduleListArr)["substitutions"] == {})
        scheduleUpdater.execute(self.updateSchedule)
        scheduleListArr = self.kvHandler.getScheduleList()
        assert (self.kvHandler.getSchedule(
            self.uniqueKeyFuture,
            scheduleListArr)["substitutions"]["year"] == 2017)
        scheduleDeleter.execute(
            json.dumps({
                "scheduleKey": self.uniqueKeyPast
            }))
        scheduleDeleter.execute(
            json.dumps({
                "scheduleKey": self.uniqueKeyFuture
            }))
        assert (scheduleLister.execute(
            json.dumps({
                "scheduleKey": self.uniqueKeyPast
            })) == [])
        assert (scheduleLister.execute(
            json.dumps({
                "scheduleKey": self.uniqueKeyFuture
            })) == [])


class TestScheduleAppsAsApps(object):
    def setup_class(cls):
        cls.uniqueKeyPast = "pastSchedTestTest"
        cls.uniqueKeyFuture = "futureSchedTestTest"

        cls.uniqueKeyPastDict = json.dumps({"scheduleKey": cls.uniqueKeyPast})

        cls.uniqueKeyFutureDict = json.dumps({
            "scheduleKey": cls.uniqueKeyFuture
        })

        cls.futureSchedule = '{"retName":"joinretina2","scheduleKey":"' + cls.uniqueKeyFuture + '","substitutions":{},"options":{"activeSession":false,"newTableName":"","isPaused":false},"timingInfo":{"startTime":2007735469000,"dateText":"2/21/2019","timeText":"07 : 51 PM","repeat":"minute","modified":1486587921119,"recur":4}}'
        cls.pastSchedule = '{"retName":"joinretina2","scheduleKey":"' + cls.uniqueKeyPast + '","substitutions":{},"options":{"activeSession":false,"newTableName":"","isPaused":false},"timingInfo":{"startTime":1447735469000,"dateText":"2/21/2016","timeText":"07 : 51 PM","repeat":"minute","modified":1486587921119,"recur":4}}'
        cls.updateSchedule = '{"retName":"joinretina2","scheduleKey":"' + cls.uniqueKeyFuture + '","substitutions":{"N":10,"year":2017},"options":{"activeSession":false,"newTableName":"","isPaused":false},"timingInfo":{"startTime":1447735469000,"dateText":"2/21/2016","timeText":"07 : 51 PM","repeat":"minute","modified":1486587921119,"recur":4}}'
        cls.client = Client()
        cls.session = cls.client.create_session("TestScheduleAppsAsApps")
        cls.app = App(cls.client)

    def teardown_class(cls):
        pastDel = cls.app.run_py_app("ScheduleDelete", True,
                                     cls.uniqueKeyPastDict)
        futureDel = cls.app.run_py_app("ScheduleDelete", True,
                                       cls.uniqueKeyFutureDict)
        cls.session.destroy()
        cls.session = None
        cls.app = None
        os.system("crontab -r")

    def testScheduleAppsAsApps(self):
        def resStr(outStr):
            return json.loads(outStr[0])[0][0]

        pastDel = self.app.run_py_app("ScheduleDelete", True,
                                      self.uniqueKeyPastDict)
        futureDel = self.app.run_py_app("ScheduleDelete", True,
                                        self.uniqueKeyFutureDict)
        assert (resStr(pastDel) == "0")
        assert (resStr(futureDel) == "0")

        pastList = self.app.run_py_app("ScheduleList", True,
                                       self.uniqueKeyPastDict)
        futureList = self.app.run_py_app("ScheduleList", True,
                                         self.uniqueKeyFutureDict)
        assert (resStr(pastList) == "[]")
        assert (resStr(futureList) == "[]")

        pastCre = self.app.run_py_app("ScheduleCreate", True,
                                      self.pastSchedule)
        futureCre = self.app.run_py_app("ScheduleCreate", True,
                                        self.futureSchedule)
        assert (resStr(pastCre) == "0")
        assert (resStr(futureDel) == "0")

        update = self.app.run_py_app("ScheduleUpdate", True,
                                     self.updateSchedule)
        assert (resStr(update) == "0")

        pastList = self.app.run_py_app("ScheduleList", True,
                                       self.uniqueKeyPastDict)
        futureList = self.app.run_py_app("ScheduleList", True,
                                         self.uniqueKeyFutureDict)
        assert (resStr(pastList) != "[]")
        assert (resStr(futureList) != "[]")
        # TODO: this is the shortest interval possible to wait for job
        # to execute.  Is this too long for test?  Can take up to over
        # one minute
        time.sleep(120)    # Seconds

        pastList = self.app.run_py_app("ScheduleList", True,
                                       self.uniqueKeyPastDict)

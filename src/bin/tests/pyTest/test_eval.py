from xcalar.compute.util.Qa import buildNativePath, DefaultTargetName
from xcalar.external.client import Client

from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.LegacyApi.Dataset import JsonDataset

from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.Status.constants import StatusTStr

testEvalSessionName = "TestEval"


class TestEval(object):
    def setup_class(cls):
        cls.client = Client()
        cls.xcalarApi = XcalarApi()
        cls.session = cls.client.create_session(testEvalSessionName)
        cls.xcalarApi.setSession(cls.session)
        cls.operators = Operators(cls.xcalarApi)

        cls.datasets = []

    def teardown_class(cls):
        try:
            cls.operators.dropTable("yelpUsers*")
        except XcalarApiStatusException:
            pass
        try:
            cls.operators.dropConstants("*")
        except XcalarApiStatusException:
            pass

        for dataset in cls.datasets:
            try:
                dataset.delete()
            except XcalarApiStatusException:
                pass
        cls.client.destroy_session(testEvalSessionName)

    def testXc9062(self):
        path = buildNativePath("yelp/user")
        dataset = JsonDataset(self.xcalarApi, DefaultTargetName, path,
                              "yelpUsers")
        dataset.load()
        assert dataset.record_count() > 0

        userId = "user_id"
        for i in range(200):
            userId += "user_id"

        self.operators.indexDataset(".XcalarDS.yelpUsers", "yelpUsersIndex",
                                    "yelping_since", "p")
        try:
            self.operators.groupBy("yelpUsersIndex", "yelpUsersGroupBy",
                                   ["count(p::" + userId + ")"],
                                   ["countYelpingSince"])
        except XcalarApiStatusException as e:
            assert (e.status == StatusT.StatusXcalarEvalTokenNameTooLong)
            assert ((StatusTStr[StatusT.StatusXcalarEvalTokenNameTooLong] in e.
                     result.output.hdr.log) is True)

        self.operators.dropTable("yelpUsers*")
        self.operators.dropConstants("*")
        dataset.delete()

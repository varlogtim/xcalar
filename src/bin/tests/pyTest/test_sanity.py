import os
import pytest

from xcalar.compute.util.Qa import XcalarQaDatasetPath
from xcalar.external.client import Client

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Dataset import JsonDataset
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.result_set import ResultSet

from xcalar.compute.coretypes.OrderingEnums.constants import XcalarOrderingT

testSanitySessionName = "TestSanity"


@pytest.mark.first
class TestSanity(object):
    def setup_class(cls):
        cls.client = Client()
        cls.xcalarApi = XcalarApi()
        cls.session = cls.client.create_session(testSanitySessionName)
        cls.xcalarApi.setSession(cls.session)
        cls.operators = Operators(cls.xcalarApi)

    def teardown_class(cls):
        assert cls.session.name == testSanitySessionName
        cls.client.destroy_session(testSanitySessionName)
        cls.xcalarApi.setSession(None)
        cls.session = None
        cls.xcalarApi = None
        pass

    def testVersion(self):
        output = self.client.get_version()
        assert output.version

    def testFunWithYelp(self):
        targetName = "Default Shared Root"
        path = os.path.join(XcalarQaDatasetPath, "yelp/user")
        dataset = JsonDataset(self.xcalarApi, targetName, path, "yelpUsers")
        dataset.load()
        assert dataset.record_count() > 0

        self.operators.indexDataset(".XcalarDS.yelpUsers", "yelpUsersIndex",
                                    "yelping_since", "p")
        self.operators.groupBy("yelpUsersIndex", "yelpUsersGroupBy",
                               ["count(p::user_id)"], ["countYelpingSince"])
        self.operators.indexTable(
            "yelpUsersGroupBy",
            "yelpUsersGroupBySortDesc",
            "countYelpingSince",
            ordering=XcalarOrderingT.XcalarOrderingDescending)
        first = next(
            ResultSet(self.client, table_name="yelpUsersGroupBySortDesc", session_name=testSanitySessionName).record_iterator())
        print(first)
        assert first["p-yelping_since"] == "2011-07"
        assert first["countYelpingSince"] == 1600

        self.operators.aggregate("yelpUsersIndex", "yelpUsersIndexAgg",
                                 "count(p::user_id)")

        self.operators.dropTable("yelpUsers*")
        self.operators.dropConstants("*")
        dataset.delete()

import os
import json
import pytest

from xcalar.compute.util.cluster import DevCluster
from xcalar.compute.util.Qa import XcalarQaDatasetPath

from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.external.LegacyApi.Dataset import JsonDataset

from xcalar.compute.coretypes.Status.ttypes import StatusT

from xcalar.external.client import Client

testExpiredLicenseSession = "TestLoadWithExpiredLicense"

pytestmark = pytest.mark.last(
    "Execute this test as late as possible since it manages its own cluster")


@pytest.fixture(scope="module")
def getTestContext():
    xce_config = os.environ["XCE_CONFIG"]
    os.environ[
        "XCE_CONFIG"] = "%s/badLicenses/expiredLicense.cfg" % XcalarQaDatasetPath

    with DevCluster(num_nodes=1) as cluster:
        client = Client()
        testContext = {}
        testContext["cluster"] = cluster
        testContext["client"] = client
        testContext["xcApi"] = XcalarApi()
        lic = testContext["client"].get_license()
        jsdata = json.loads(lic)
        oldLicense = jsdata['compressedLicense']
        testContext["session"] = client.create_session(
            testExpiredLicenseSession)
        testContext["xcApi"].setSession(testContext["session"])
        yield testContext
        testContext["client"].update_license(oldLicense)
        testContext["session"].destroy()

    os.environ["XCE_CONFIG"] = xce_config


@pytest.mark.parametrize(
    "licenseFilePath,shouldSucceed",
    [("%s/badLicenses/XcalarLic.key.update.expired.warn" % XcalarQaDatasetPath,
      True),
     ("%s/badLicenses/XcalarLic.key.update.expired.disable" %
      XcalarQaDatasetPath, False)])
def testExpiredLicense(getTestContext, licenseFilePath, shouldSucceed):
    with open(licenseFilePath, "r") as fp:
        licKey = fp.read()
    xcApi = getTestContext["xcApi"]
    client = getTestContext["client"]
    client.update_license(licKey)

    dataset = JsonDataset(
        xcApi,
        "Default Shared Root",
        "%s/indexJoin/teachers" % XcalarQaDatasetPath,
        "temporaryDs",
        fileNamePattern="*",
        isRecursive=False)
    try:
        dataset.load()
        assert dataset.record_count() == 4
        dataset.delete()
        assert (shouldSucceed is True)
    except XcalarApiStatusException as e:
        if not shouldSucceed:
            assert (e.status == StatusT.StatusLicOpDisabledUnlicensed)
        else:
            raise

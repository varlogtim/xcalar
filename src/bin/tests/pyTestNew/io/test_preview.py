import base64
import pytest

from xcalar.compute.util.Qa import buildNativePath, DefaultTargetName

from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException

from xcalar.compute.coretypes.Status.ttypes import StatusT

expectedSimple = b"""column One,column Two
1,2
3,4
"""

expectedSimple2 = b"""col1,col2
1,2
3,4
"""

# There are several files here, but the order is not well defined
expectedMultiFiles = [
    ("test.txt", b"""col1,col2
1,2
3,4
"""),
    ("test.txt", b"""col1,col2
5,6
7,8
"""),
    ("test1.txt", b"""col1,col2
9,10
11,12
"""),
]

# Only the first 1000 bytes of yelp user
expectedYelpUser = b'''[
{"yelping_since": "2011-08", "votes": {"funny": 0, "useful": 1, "cool": 1}, "review_count": 5, "name": "Glen", "user_id": "HzLh-2WyMjf6TYATFwg6NA", "friends": [], "fans": 0, "average_stars": 3.6000000000000001, "type": "user", "compliments": {}, "elite": []},
{"yelping_since": "2011-06", "votes": {"funny": 78, "useful": 7, "cool": 1}, "review_count": 5, "name": "Paul", "user_id": "gYV6bmTSgbZMGkvXHVCowg", "friends": [], "fans": 0, "average_stars": 2.2000000000000002, "type": "user", "compliments": {}, "elite": []},
{"yelping_since": "2010-10", "votes": {"funny": 1, "useful": 0, "cool": 1}, "review_count": 7, "name": "Nancy", "user_id": "4duCDxDMiRJJbc2CmnziAg", "friends": [], "fans": 0, "average_stars": 3.5699999999999998, "type": "user", "compliments": {}, "elite": []},
{"yelping_since": "2012-02", "votes": {"funny": 0, "useful": 0, "cool": 1}, "review_count": 4, "name": "Douglas", "user_id": "2EeuYhLuzvvrJ9v2pVrTjQ", "friends": [], "fans": 0, "average_stars": 3.0, "type": "user", "'''

# This is a declarative way of specifying our tests. This specifies our datasets
# and the expected results from previewing them
testCases = [
    (
        buildNativePath("csvSanity"),
        "csv",
        "columnNameSpace.csv",
        len(expectedSimple),    # exact size read
        expectedSimple,
        "columnNameSpace.csv"),
    #    XXX - skip shared nothing not impl
    #    ("localfile://" + XcalarQaDatasetPath + "/csvSanity?node_id=0",
    #        "csv",
    #        "columnNameSpace.csv",
    #        len(expectedSimple), # exact size read
    #        expectedSimple,
    #        "columnNameSpace.csv"),
    (
        buildNativePath("csvSanity"),
        "csv",
        "columnNameSpace.csv",
        5,    # short read
        expectedSimple,
        "columnNameSpace.csv"),
    (
        buildNativePath("csvSanity"),
        "csv",
        "columnNameSpace.csv",
        2**30,    # long read
        expectedSimple,
        "columnNameSpace.csv"),
    (
        buildNativePath("yelp/user"),
        "json",
        "",    # No pattern here
        100,
        expectedYelpUser,
        "yelp_academic_dataset_user_fixed.json"),
    (
        buildNativePath("yelp/user/yelp_academic_dataset_user_fixed.json"),
        "json",
        "",    # No pattern here
        100,
        expectedYelpUser,
        "yelp_academic_dataset_user_fixed.json"),
    (
        buildNativePath("edgeCases/recurSimple"),
        "csv",
        "",    # No pattern here
        100,
        expectedSimple2,
        "test.txt"),
]


# Without this pytest will print the entire dataset string every time (annoying)
def idfn(val):
    """Pretty print the testname"""
    if isinstance(val, bytes):
        if val == expectedYelpUser:
            return "yelpUserDataset"
        if val == expectedSimple:
            return "simpleDataset"
    return str(val)


@pytest.mark.parametrize("path,formatType,fileNamePattern,numBytesRequested,"
                         "expectedStr,expectedFilename",
                         testCases,
                         ids=idfn)
@pytest.mark.parametrize("offset", [0, 1, 5, 10, 100, 2**30])
def testPreviewCorrectness(client, path, formatType, fileNamePattern,
                           numBytesRequested, offset, expectedStr,
                           expectedFilename):

    data_target = client.get_data_target(DefaultTargetName)
    preview = data_target.preview(
        path, fileNamePattern, num_bytes=numBytesRequested, offset=offset)

    if offset >= len(expectedStr):
        assert preview['totalDataSize'] == 0
        assert preview['thisDataSize'] == 0
        assert preview['base64Data'] == ""
        assert preview['fileName'] == ""
        assert preview['fullPath'] == ""
        assert preview['relPath'] == ""
        return

    print("preview is {}".format(preview))
    actualStr = base64.b64decode(preview['base64Data'])
    print("{}({} long):\n{}".format(preview['fileName'],
                                    preview['thisDataSize'], actualStr))
    expectedNumBytes = min(len(expectedStr) - offset, numBytesRequested)
    expectedEncodedStr = base64.b64encode(
        expectedStr[offset:offset + expectedNumBytes]).decode("ascii")

    # this is >= because the yelp tests are just a sample of the total
    assert preview['totalDataSize'] >= len(expectedStr)
    assert preview['thisDataSize'] == expectedNumBytes
    assert actualStr == expectedStr[offset:offset + expectedNumBytes]
    assert preview['base64Data'] == expectedEncodedStr
    assert preview['fileName'] == expectedFilename


def testPreviewMultiFileCorrectness(client):
    path = buildNativePath("edgeCases/recurSimple")
    fileNamePattern = "*"

    data_target = client.get_data_target(DefaultTargetName)
    expectedTotalSize = sum([len(f[1]) for f in expectedMultiFiles])
    # request a number bigger than any individual file
    numRequest = 10 * max([len(f[1]) for f in expectedMultiFiles])

    actualFiles = []
    offset = 0
    numFound = 0
    # Iterate over all files until we have previewed everything, then
    # make sure we've seen everything we were supposed to.
    # Note that there is a lack of ordering guarantee
    while True:
        preview = data_target.preview(
            path, fileNamePattern, True, num_bytes=numRequest, offset=offset)
        # We should eventually run over the end of the dataset and fail
        if offset >= expectedTotalSize:
            assert offset == expectedTotalSize
            assert preview['totalDataSize'] == 0
            assert preview['thisDataSize'] == 0
            assert preview['base64Data'] == ""
            assert preview['fileName'] == ""
            assert preview['fullPath'] == ""
            assert preview['relPath'] == ""
            break
        assert numFound < len(expectedMultiFiles)

        # Add this found data so we can compare later
        actualFiles.append((preview['fileName'],
                            base64.b64decode(preview['base64Data'])))
        offset += preview['thisDataSize']
        assert preview['totalDataSize'] == expectedTotalSize
        numFound += 1

    assert numFound == len(expectedMultiFiles)
    assert set(expectedMultiFiles) == set(actualFiles)


def testPreviewNotExists(client):
    path = "/notexists"
    numBytesRequested = 5
    data_target = client.get_data_target(DefaultTargetName)

    try:
        preview = data_target.preview(path, num_bytes=numBytesRequested)
    except XcalarApiStatusException as e:
        assert e.status == StatusT.StatusUdfExecuteFailed
        return
    assert False

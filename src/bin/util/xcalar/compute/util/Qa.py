import os
import getpass

import tempfile
import multiset

from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.result_set import ResultSet

#
# This file can be used from tests to get environment-specific constants.
#

# Don't throw on failure to find env var.
XcalarQaDatasetPath = os.environ.get('XcalarQaDatasetPath', None)
XcalarQaDebugTest = bool(os.environ.get('XcalarQaDebugTest', None)) or None
XcalarQaScratchShare = (os.environ.get('XcalarQaScratchShare', None)
                        or "/tmp/qaScratch-" + getpass.getuser())
os.makedirs(XcalarQaScratchShare, exist_ok=True)

# HDFS Environment
XcalarQaHDFSHost = os.environ.get('XcalarQaHDFSHost', None)

# S3 Environment
XcalarQaS3Bucket = os.environ.get('XcalarQaS3Bucket', 'qa.xcalar')
XcalarQaGCSBucket = os.environ.get('XcalarQaGCSBucket', 'xcqa')
XcalarQaGCSPath = os.environ.get('XcalarQaGCSPath ', 'qa/')
XcalarQaS3ClientArgs = {
    'aws_access_key_id':
        os.environ.get('XcalarQaS3AccessKey', 'K3CC2YMWIGSZVR09Q3WO'),
    'aws_secret_access_key':
        os.environ.get('XcalarQaS3SecretKey',
                       'xqfitN99bb6l2nZRTcOqvsTrR66gCCSKaQssSu4Z'),
    'endpoint_url':
        os.environ.get('XcalarQaS3Endpoint', 'https://minio-1.int.xcalar.com'),
    'region_name':
        os.environ.get('XcalarQaS3Region', 'us-east-1'),
    'verify':
        os.environ.get('XcalarQaS3Verify', "false").lower() == "true",
}

XcalarQaLocalS3ClientArgs = {
    'aws_access_key_id':
        os.environ.get('XcalarQaS3AccessKey', 'minio'),
    'aws_secret_access_key':
        os.environ.get('XcalarQaS3SecretKey', 'minio123'),
    'endpoint_url':
        os.environ.get('XcalarQaS3Endpoint', "http://{}:9000".format(os.environ.get('HOSTIP', "localhost"))),
    'region_name':
        os.environ.get('XcalarQaS3Region', 'us-east-1'),
    'verify':
        os.environ.get('XcalarQaS3Verify', "false").lower() == "true",
}

# GCS
XcalarQaGCSBucket = os.environ.get('XcalarQaGCSBucket', 'xcqa')
XcalarQaGCSPath = os.environ.get('XcalarQaGCSPath ', 'qa/')

# Azure
XcalarQaAzBlobBucket = os.environ.get("XcalarAzBlobBucket", "datasets")
XcalarQaAzBlobPath = os.environ.get("XcalarAzBlobPath", "qa")


def restartCluster():
    os.system(os.environ['XcalarQaRestartCmd'])


def _freezeValue(value):
    if isinstance(value, dict):
        return frozenset([(k, _freezeValue(v)) for k, v in value.items()])
    elif isinstance(value, list):
        return frozenset([_freezeValue(elem) for elem in value])
    else:
        return value


def _freezeRow(row):
    return frozenset([(k, _freezeValue(v)) for k, v in row.items()])


def freezeRows(rows, useMulti=False):
    if useMulti:
        return multiset.FrozenMultiset([_freezeRow(row) for row in rows])
    else:
        return frozenset([_freezeRow(row) for row in rows])


def datasetCheck(expectedRows, resultSet, matchOrdering=False):
    """expectedRows is an array of dicts where each dict is a record"""
    # Early detect different numbers of rows, since the output is hard to read
    # filter out the 'recordNum' pseudo-field
    pureData = []
    for row in resultSet:
        pureData.append(row)
    assert len(expectedRows) == len(pureData)

    if (matchOrdering):
        assert expectedRows == pureData
    else:
        exp = freezeRows(expectedRows)
        got = freezeRows(pureData)
        if exp != got:
            with tempfile.NamedTemporaryFile(
                    "w+", delete=False, prefix="datasetCheck-",
                    dir="/tmp/") as fp:
                fp.write("Expected '{}': Got '{}'".format(exp, got))
            print("Expected '{}': Got '{}'".format(exp, got))
            assert False


# Selects cols from a dict and renames them to rcols
# Returns new dict containing only cols renamed to rcols
def getFilteredDicts(d, cols, rcols=None):
    if not rcols:
        rcols = cols
    rcolsDict = dict(zip(cols, rcols))
    try:
        return map(
            lambda x: {rcolsDict[k]: v
                       for (k, v) in x.items() if k in cols}, d)
    except Exception:    # XXX What is this trying to catch?
        return []


def buildNativePath(relPath):
    """Applies environment to the relPath, which is relative to the QA topdir"""
    return os.path.join(XcalarQaDatasetPath, relPath)


def buildS3Path(relPath):
    # the first component of the S3 path is the name of the bucket
    return os.path.join(XcalarQaS3Bucket, relPath)


def buildGcsPath(relPath):
    # the first component of the GCS path is the name of the bucket
    return os.path.join(XcalarQaGCSBucket, XcalarQaGCSPath, relPath)


def buildAzBlobPath(relPath):
    # the first component of the GCS path is the name of the bucket
    return os.path.join(XcalarQaAzBlobBucket, XcalarQaAzBlobPath, relPath)


def verifyRowCount(client, xcalarApi, srcTable, answerRows, printRows=False):
    operators = Operators(xcalarApi)
    output = operators.tableMeta(srcTable)
    srcRows = sum([output.metas[ii].numRows for ii in range(output.numMetas)])
    if printRows or srcRows != answerRows:
        print("verifyRowCount: output={}".format(output))
        print("verifyRowCount: srcTable={} srcRows={} answerRows={}".format( \
            srcTable, srcRows, answerRows))
        print(ResultSet(client, table_name=srcTable).record_iterator())
    assert (srcRows == answerRows)


DefaultTargetName = "Default Shared Root"

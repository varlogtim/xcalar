import os

import pytest

from google.protobuf.json_format import MessageToDict

from xcalar.compute.util.Qa import (buildNativePath, XcalarQaDatasetPath,
                                    buildS3Path, buildGcsPath, buildAzBlobPath)
from xcalar.compute.util.Qa import XcalarQaHDFSHost, DefaultTargetName
from xcalar.compute.util.Qa import XcalarQaS3ClientArgs

from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException
from xcalar.compute.localtypes.Connectors_pb2 import ListFilesRequest

from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.external.exceptions import XDPException

hdfsHost = "hdfs-sanity"
if XcalarQaHDFSHost:
    hdfsHost = XcalarQaHDFSHost

hdfsCrimeUrl = "hdfs://{}:8020/datasets/qa/nycCrimeSmall".format(hdfsHost)
hdfsCrimeUrlNoPort = "hdfs://{}/datasets/qa/nycCrimeSmall".format(hdfsHost)

expectedFiles = [
    " ", "tab\tsep", "<", ",", "?", "'", "~", "duplicate", "#", "testFile",
    "|", "!", "`", "@", "^", "]", "&", "_", "[", "\"", "DUPLICATE", "}", ")",
    "+", "(", ";", ".dotPre", "$", ">", "\\", "{", "=", "*", ":", "%", "\t"
]
subFiles = ["subdir/subfile", "subdir/subdir2/subfile2"]
subDirs = ["subdir/subdir2"]
expectedDirs = ["subdir"]
expectedContents = expectedFiles + expectedDirs

expRecurFiles = expectedFiles + subFiles
expRecurDirs = expectedDirs + subDirs
expRecurContents = expRecurFiles + expRecurDirs

# HDFS test expected results
hdfsTopFiles = ["2003.csv", "2014.csv"]
hdfsSubFiles = ["nycCrimeSmall/2003.csv", "nycCrimeSmall/2014.csv"]

expHdfsDirs = ["nycCrimeSmall"]
expHdfsFiles = hdfsTopFiles
expHdfsContents = expHdfsFiles + expHdfsDirs

expHdfsRecurDirs = expHdfsDirs
expHdfsRecurFiles = hdfsTopFiles + hdfsSubFiles
expHdfsRecurContents = expHdfsRecurDirs + expHdfsRecurFiles

expHugeDir = ["hugeDirectoryPart{:07}".format(ii) for ii in range(0, 150000)]

# Target tests
shared_qa_target_name = "QA shared target"
s3_minio_target_name = "QA S3 minio"
gcs_target_name = "QA GCS"
azblob_target_name = "QA azblob"
unsharedSymmTargetName = "QA unshared symmetric"
unsharedSingleNodeTargetName = "QA unshared single node"
targets = [
    (shared_qa_target_name, "shared", {
        "mountpoint": XcalarQaDatasetPath
    }),
    (unsharedSymmTargetName, "sharednothingsymm", {
        "mountpoint": XcalarQaDatasetPath
    }),
    (unsharedSingleNodeTargetName, "sharednothingsingle", {}),
    (s3_minio_target_name, "s3fullaccount", {
        "access_key": XcalarQaS3ClientArgs["aws_access_key_id"],
        "secret_access_key": XcalarQaS3ClientArgs["aws_secret_access_key"],
        "endpoint_url": XcalarQaS3ClientArgs["endpoint_url"],
        "region_name": XcalarQaS3ClientArgs["region_name"],
        "verify": "true" if XcalarQaS3ClientArgs["verify"] else "false",
    }),
    (gcs_target_name, "gcsenviron", {}),
    (
        azblob_target_name,
        "azblobfullaccount",
        {
            "account_name": "xcdatasetswestus2",
            "sas_token": ""    # Fill this in with a sas_token to test
        }),
]

ENABLE_CLOUD_TESTS = False

# We're going to run the edge case tests on these targets
standard_targets = [
    (shared_qa_target_name, (lambda x: x)),
    (s3_minio_target_name, buildS3Path),
    pytest.param(
        gcs_target_name,
        buildGcsPath,
        marks=pytest.mark.skipif(
            not ENABLE_CLOUD_TESTS,
            reason="jenkins running cloud tests is expensive")),
    pytest.param(
        azblob_target_name,
        buildAzBlobPath,
        marks=pytest.mark.skipif(
            not ENABLE_CLOUD_TESTS,
            reason="jenkins running cloud tests is expensive")),
]


@pytest.fixture(scope="module", autouse=True)
def setup(client):
    for target_name, target_type, target_args in targets:
        client.add_data_target(target_name, target_type, target_args)

    yield client

    for target_name, target_type, target_args in targets:
        client.get_data_target(target_name).delete()


def test_root(client):
    target = client.get_data_target(DefaultTargetName)
    resp = target.list_files("/")
    assert all([item['name'] for item in resp['files']])
    assert all(["/" not in item['name'] for item in resp['files']])


def test_bucket_root_s3(client):
    target = client.get_data_target(s3_minio_target_name)
    resp = target.list_files("/qa.xcalar")
    assert "qa.xcalar" not in [item['name'] for item in resp['files']]


def test_top_list_s3(client):
    try:
        target = client.get_data_target(s3_minio_target_name)
        resp = target.list_files("/", recursive=True)
    except XDPException as e:
        assert "Recursive search only available for a given bucket" in str(e)


@pytest.mark.parametrize("target_name,path_builder", [
    (DefaultTargetName, buildNativePath),
    (shared_qa_target_name, lambda x: x),
])
def test_edge_characters(client, target_name, path_builder):
    target = client.get_data_target(target_name)
    path1 = path_builder("edgeCases/iterTests/")
    path2 = path_builder("edgeCases/iterTests")
    resp1 = target.list_files(path1)
    resp2 = target.list_files(path2)

    def flatten(items):
        return set([tuple([v for v in item.values()]) for item in items])

    assert flatten(resp1['files']) == flatten(resp2['files'])
    assert set(
        [item['name'] for item in resp1['files']]) == set(expectedContents)

    files = [item['name'] for item in resp1['files'] if not item['isDir']]
    dirs = [item['name'] for item in resp1['files'] if item['isDir']]

    assert set(files) == set(expectedFiles)
    assert set(dirs) == set(expectedDirs)


@pytest.mark.parametrize(
    "target_name,path",
    [(DefaultTargetName, buildNativePath("edgeCases/iterTests")),
     (unsharedSingleNodeTargetName,
      os.path.join("0", XcalarQaDatasetPath.strip('/'), "edgeCases",
                   "iterTests"))])
def test_recursion(client, target_name, path):
    target = client.get_data_target(target_name)
    resp = target.list_files(path, recursive=True)

    items = [item['name'] for item in resp['files']]
    files = [item['name'] for item in resp['files'] if not item['isDir']]
    dirs = [item['name'] for item in resp['files'] if item['isDir']]

    assert sorted(items) == sorted(expRecurContents)
    assert sorted(files) == sorted(expRecurFiles)
    assert sorted(dirs) == sorted(expRecurDirs)


def test_unshared_root(client):
    target = client.get_data_target(unsharedSingleNodeTargetName)
    resp = target.list_files("/", recursive=True)
    # listing / returns N for N in num_nodes
    [int(item['name']) for item in resp['files']]


def test_recursive_unshared_duped(client):
    target_name = unsharedSymmTargetName
    path = os.path.join("edgeCases", "iterTests")
    target = client.get_data_target(target_name)

    resp = target.list_files(path, recursive=True)

    items = [item['name'] for item in resp['files']]
    files = [item['name'] for item in resp['files'] if not item['isDir']]
    dirs = [item['name'] for item in resp['files'] if item['isDir']]

    num_nodes = client.get_num_nodes_in_cluster()
    assert sorted(items) == sorted(expRecurContents * num_nodes)
    assert sorted(files) == sorted(expRecurFiles * num_nodes)
    assert sorted(dirs) == sorted(expRecurDirs * num_nodes)


@pytest.mark.parametrize("target_name,path_builder", standard_targets)
def test_dir_with_suffix(client, target_name, path_builder):
    path = path_builder("sisterDirs/a")
    target = client.get_data_target(target_name)

    resp = target.list_files(path, recursive=True)

    exp_dirs = []
    exp_files = ["a.txt"]
    exp_items = exp_dirs + exp_files

    assert set([item['name'] for item in resp['files']]) == set(exp_items)
    assert set([item['name'] for item in resp['files']
                if item['isDir']]) == set(exp_dirs)
    assert set([item['name'] for item in resp['files']
                if not item['isDir']]) == set(exp_files)


#
# Paged List Files Tests
#


def test_paged_list_files_recursive(client):
    # Recursive only returns files
    target = client.get_data_target(s3_minio_target_name)
    path = buildS3Path("edgeCases/20k/")

    dir_names = [f"part{ii:02}" for ii in range(100)]
    file_names = [f"file{ii:03}" for ii in range(200)]
    all_file_names = [
        f"{dir_name}/{file_name}" for dir_name in dir_names
        for file_name in file_names
    ]

    token = ""
    resp_files = []
    for ii in range(100):    # safer than while True
        resp = target.list_files(path, recursive=True, paged=True, token=token)
        resp_files = resp_files + [item['name'] for item in resp['files']]
        token = resp['continuationToken']
        if token == "":
            break
    assert sorted(resp_files) == sorted(all_file_names)


def test_paged_list_files_with_regex_non_recursive(client):
    # Recursive only returns files
    target = client.get_data_target(s3_minio_target_name)
    path = buildS3Path("edgeCases/20k/")

    name_pattern = 're:part[56][0-9]'

    dir_names = [f"part{ii:02}" for ii in range(50, 70)]
    resp_files = []
    resp = target.list_files(
        path, pattern=name_pattern, recursive=False, paged=True)
    assert resp['continuationToken'] == ""
    assert sorted(
        [item['name'] for item in resp['files']]) == sorted(dir_names)


def test_paged_list_files_with_glob_recursive(client):
    # Recursive only returns files
    target = client.get_data_target(s3_minio_target_name)
    path = buildS3Path("edgeCases/20k/")

    name_pattern = 'file0*'

    # 10K files, by filtering
    dir_names = [f"part{ii:02}" for ii in range(100)]
    file_names = [f"file0{ii:02}" for ii in range(100)]
    all_file_names = [
        f"{dir_name}/{file_name}" for dir_name in dir_names
        for file_name in file_names
    ]

    token = ""
    resp_files = []
    for ii in range(100):    # safer than while True
        resp = target.list_files(
            path,
            pattern=name_pattern,
            recursive=True,
            paged=True,
            token=token)
        resp_files = resp_files + [item['name'] for item in resp['files']]
        token = resp['continuationToken']
        if token == "":
            break
    assert sorted(resp_files) == sorted(all_file_names)

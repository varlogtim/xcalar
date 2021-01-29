import os
import pytest
import tempfile

from xcalar.compute.util.cluster import DevCluster

from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException
from xcalar.external.exceptions import XDPException
from xcalar.external.client import Client

from xcalar.compute.localtypes.Connectors_pb2 import RemoveFileRequest
from xcalar.compute.localtypes.Workbook_pb2 import WorkbookScope

pytestmark = pytest.mark.last(
    "Execute this test as late as possible since it manages its own cluster")

# Don't throw on failure to find env var.
XcalarQaDatasetPath = os.environ.get('XcalarQaDatasetPath', None)

# Target tests
shared_qa_target_name = "QA shared target"
unsharedSymmTargetName = "QA unshared symmetric"

targets = [(shared_qa_target_name, "shared", {
    "mountpoint": XcalarQaDatasetPath
}),
           (unsharedSymmTargetName, "sharednothingsymm", {
               "mountpoint": XcalarQaDatasetPath
           })]


@pytest.fixture(scope="module", autouse=True)
def one_node_cluster():
    with DevCluster(num_nodes=1):
        yield


@pytest.fixture(scope="module")
def client(one_node_cluster):
    client = Client()
    for target_name, target_type, target_args in targets:
        client.add_data_target(target_name, target_type, target_args)

    yield client

    for target_name, target_type, target_args in targets:
        client.get_data_target(target_name).delete()


@pytest.mark.parametrize("target_name", [
    (shared_qa_target_name),
    (unsharedSymmTargetName),
])
def test_delete_file(client, target_name):
    target = client.get_data_target(target_name)
    temp_dir_prefix = os.path.join(target.params["mountpoint"], "tmp")
    with tempfile.TemporaryDirectory(prefix=temp_dir_prefix) as outdir:
        # write out a file to delete using delete api
        actual_path = os.path.join(outdir, "test_file")
        with open(actual_path, 'w') as fp:
            fp.write("contents")

        # we pass paths to list_files and remove apis the relative path from mountpoint
        relative_path = os.path.relpath(actual_path,
                                        target.params["mountpoint"])
        resp = target.list_files(relative_path)
        files = resp['files']
        assert len(files) == 1
        delete_files(client, relative_path, target_name)
        with pytest.raises(XDPException) as ex:
            target.list_files(relative_path)


@pytest.mark.parametrize("target_name", [
    (shared_qa_target_name),
    (unsharedSymmTargetName),
])
def test_delete_dir(client, target_name):
    target = client.get_data_target(target_name)
    temp_dir_prefix = os.path.join(target.params["mountpoint"], "tmp")
    with tempfile.TemporaryDirectory(prefix=temp_dir_prefix) as outdir:
        # write out a directory and two files in it, to delete using delete api
        actual_path = os.path.join(outdir, "test_dir")
        os.makedirs(actual_path)
        assert os.path.isdir(actual_path)
        file1 = os.path.join(actual_path, "file1")
        file2 = os.path.join(actual_path, "file2")
        with open(file1, 'w') as fp1, open(file2, 'w') as fp2:
            fp1.write("contents1")
            fp2.write("contents2")

        # we pass paths to list_files and remove apis the relative path from mountpoint
        relative_path = os.path.relpath(actual_path,
                                        target.params["mountpoint"])
        resp = target.list_files(relative_path)
        files = resp['files']
        assert len(files) == 2    # two files written in the directory
        assert all([not file["isDir"] for file in files])
        delete_files(client, relative_path, target_name)
        with pytest.raises(XDPException) as ex:
            target.list_files(relative_path)


def delete_files(client, path, target_name):
    scope = WorkbookScope()
    scope.workbook.name.username = client.username

    req = RemoveFileRequest()
    req.path = path
    req.target_name = target_name
    req.scope.CopyFrom(scope)

    client._connectors_service.removeFile(req)

from base64 import b64encode, b64decode

import xcalar.container.context as ctx
from xcalar.container.cluster import get_running_cluster
from xcalar.compute.localtypes.Connectors_pb2 import (ListFilesRequest,
                                                      ListFilesResponse)
from xcalar.container.target.manage import get_target_data, build_target
from xcalar.container.loader.base import _distribute_files

logger = ctx.get_logger()


class FileLister():
    def __init__(self, target_name):
        self.target_name = target_name
        self.target = None
        self.cluster = get_running_cluster()

        target_data = None
        if self.cluster.is_master():
            target_data = get_target_data(target_name)
            self.cluster.broadcast_msg(target_data)
        else:
            assert self.cluster.total_size > 1
            target_data = self.cluster.recv_msg()
        self.target = build_target(
            target_name,
            target_data,
            "/",    # Fake path
            num_nodes=ctx.get_node_count(),
            node_id=ctx.get_node_id(ctx.get_xpu_id()),
            user_name=ctx.get_user_id_name(),
            num_xpus_per_node=ctx.get_num_xpus_per_node())

    def list_files(self,
                   path,
                   name_pattern="*",
                   recursive=False,
                   as_dict=False):
        """
        List all files in a given path. Supports non-global targets.
        """
        if ".." in path:
            raise ValueError(f"'..' is not allow in path '{path}'")

        with self.target:
            source_args = {
                "targetName": self.target_name,
                "path": path,
                "fileNamePattern": name_pattern,
                "recursive": recursive
            }
            # Considates local files from each node into single node

            # XXX load_files is a poor name, change in the future.
            # List of "LoadFile" namedtuples. See:
            #  - scripts/load.py
            #  - src/bin/sdk/xpu/xcalar/container/connectors/util.py
            load_files = _distribute_files([self.target], [source_args],
                                           mode="listFiles")

            if not self.cluster.is_master():
                return None

            if as_dict:
                result = {}
                result['files'] = []
                # MessageToDict doesn't cast int64 fields correctly.
                # https://github.com/protocolbuffers/protobuf/issues/2954
                result['files'] = [{
                    'name': lf.file.relPath,
                    'size': lf.file.size,
                    'mtime': lf.file.mtime,
                    'isDir': lf.file.isDir
                } for lf in load_files]
                return result

            else:
                response = ListFilesResponse()
                if len(load_files) == 0:
                    # Think about this more later.
                    raise ValueError("No files found")

                for lf in load_files:
                    resp_file = response.files.add()
                    resp_file.name = lf.file.relPath
                    resp_file.isDir = lf.file.isDir
                    resp_file.size = lf.file.size
                    resp_file.mtime = lf.file.mtime

                return response

    def paged_list_files(self,
                         path,
                         name_pattern="*",
                         recursive=False,
                         token="",
                         as_dict=False):
        """
        Get a paged files list. Limited target support.
        """
        if ".." in path:
            raise ValueError(f"'..' is not allow in path '{path}'")

        if not self.cluster.is_master():
            return None

        if not self.target.is_global():
            raise ValueError(
                "Paged list files only supported on global targets")

        get_files_paged = getattr(self.target, "get_files_paged", None)
        if not callable(get_files_paged):
            raise ValueError("Paged list files not supported by this target")

        with self.target:
            (files, token) = get_files_paged(
                path, name_pattern, recursive, token=token)

            if as_dict:
                result = {}
                result['files']
                # MessageToDict doesn't cast int64 fields correctly.
                # https://github.com/protocolbuffers/protobuf/issues/2954
                result['files'] = [{
                    'name': file.name,
                    'size': file.size,
                    'mtime': file.mtime,
                    'isDir': file.isDir
                } for file in files]
                result['continuationToken'] = token
                return result

            else:
                response = ListFilesResponse()
                # List of "File" namedtupes. See:
                #  - src/bin/sdk/xpu/xcalar/container/connectors/util.py
                if len(files) == 0:
                    # Think about this more later.
                    raise ValueError("No files found")
                for file in files:
                    resp_file = response.files.add()
                    resp_file.name = file.relPath
                    resp_file.isDir = file.isDir
                    resp_file.size = file.size
                    resp_file.mtime = file.mtime
                response.continuationToken = token

                return response


def main(b64_serialized_req: bytes) -> bytes:
    serialized_req = b64decode(b64_serialized_req)
    request = ListFilesRequest()
    request.ParseFromString(serialized_req)

    logger.info("ListFilesRequest: "
                f"target='{request.sourceArgs.targetName}', "
                f"path='{request.sourceArgs.path}', "
                f"fileNamePattern='{request.sourceArgs.fileNamePattern}', "
                f"recursive='{request.sourceArgs.recursive}', "
                f"paged={request.paged}, "
                f"continuationToken='{request.continuationToken}'")

    file_lister = FileLister(request.sourceArgs.targetName)

    if request.paged:
        ret = file_lister.paged_list_files(
            path=request.sourceArgs.path,
            name_pattern=request.sourceArgs.fileNamePattern,
            recursive=request.sourceArgs.recursive,
            token=request.continuationToken)

    else:
        ret = file_lister.list_files(
            path=request.sourceArgs.path,
            name_pattern=request.sourceArgs.fileNamePattern,
            recursive=request.sourceArgs.recursive)

    # This App is executing as InstancePerNode, which means that one
    # XPU per node will be running this code. The AppLoader expects a
    # response from exactly one App in this group. We expect that all
    # apps will, if needed, considate their local results to one XPU
    # who will then populate a ListFilesResponse for us to return.

    if ret is None:
        return ""

    if not isinstance(ret, ListFilesResponse):
        raise RuntimeError("Issue processing List Files Response")

    serialized_resp_bytes = ret.SerializeToString()
    ret_str = b64encode(serialized_resp_bytes).decode('utf-8')
    return ret_str

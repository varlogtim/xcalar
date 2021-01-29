import os

from ..connectors.util import File
from ..connectors.native import NativeConnector
import xcalar.container.target.base as target


@target.register(name="Single Node Filesystem")
class UnsharedSingleNodeTarget(target.BaseTarget):
    """
    Individual node's file system.
    """

    def __init__(self, name, path, **kwargs):
        super(UnsharedSingleNodeTarget, self).__init__(name)
        self.connector = NativeConnector()
        self.this_node_id = kwargs.get('node_id', None)
        if self.this_node_id is None:
            raise ValueError("node_id must be provided as input")
        self.num_nodes = kwargs.get('num_nodes', None)
        if self.num_nodes is None:
            raise ValueError("num_nodes must be provided as input")

    def is_global(self):
        return False

    def _abs_to_node_prefixed(self, abs_path):
        # Prefix node_id to the path so it is flat across the cluster
        node_prefixed_path = os.path.join("/", str(self.this_node_id),
                                          abs_path.strip('/'))
        return node_prefixed_path

    def _node_prefixed_to_abs(self, node_prefixed_path):
        this_path = node_prefixed_path.strip('/')
        path_parts = this_path.split('/')
        if int(path_parts[0]) != self.this_node_id:
            error = "file '{}' ended up on self.this_node_id: {}".format(
                node_prefixed_path, self.this_node_id)
            raise RuntimeError(error)
        assert int(path_parts[0]) == self.this_node_id
        abs_path = os.path.join('/', *path_parts[1:])
        return abs_path

    def get_files(self, path, name_pattern, recursive, **user_args):
        path_parts = path.strip('/').split('/')
        if len(path_parts) == 1 and path_parts[0] == "":
            if self.this_node_id == 0:
                return [
                    File(
                        path=str(node_id),
                        relPath=str(node_id),
                        isDir=True,
                        size=0,
                        mtime=0) for node_id in range(self.num_nodes)
                ]
            else:
                return []

        # check that the first part is an integer and within the possible range
        if ((not path_parts[0].isdigit())
                or (int(path_parts[0]) >= self.num_nodes)):
            raise ValueError(
                "Path should begin with Xcalar cluster node number. "
                "Node number starts with zero and goes upto the number of cluster nodes"
            )

        if int(path_parts[0]) != self.this_node_id:
            return []
        full_path = os.path.join("/", *path_parts[1:])
        abs_files = self.connector.get_files(full_path, name_pattern,
                                             recursive)
        # Remap the file paths to be relative to the mountpoint
        node_prefixed_files = [
            File(
                path=self._abs_to_node_prefixed(f.path),
                relPath=f.relPath,
                isDir=f.isDir,
                size=f.size,
                mtime=f.mtime) for f in abs_files
        ]
        return node_prefixed_files

    def open(self, fileobj, opts):
        abs_file = self._node_prefixed_to_abs(fileobj)
        return self.connector.open(abs_file, opts)

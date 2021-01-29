import os

from ..connectors.util import File
from ..connectors.native import NativeConnector
import xcalar.container.target.base as target


@target.register(name="Shared File System")
@target.param("mountpoint", "Local path to shared file system")
class SharedNativeTarget(target.BaseTarget):
    """
    A shared file system that is available at the same path (mount point)
    on all nodes. The data target can also be any subdirectory within the
    shared file system.
    """

    def __init__(self, name, path, mountpoint, **kwargs):
        super(SharedNativeTarget, self).__init__(name)
        self.connector = NativeConnector()
        self.mountpoint = mountpoint

    def is_global(self):
        return True

    def _abs_to_relative(self, abs_path):
        # This might be confusing because there are 2 things called 'relative'
        # here. 1 is relative to the mountpoint, the other is relative to the
        # user requested directory (which itself is relative to the mountpoint)
        # We're only changing the 'path'; the rest should be the same
        relative_path = os.path.relpath(abs_path, self.mountpoint)
        # Prefix '/' to the path so it looks like a full filesystem
        relative_path = os.path.join("/", relative_path)
        return relative_path

    def _relative_to_abs(self, rel_path):
        # We're only changing the 'path'; the rest should be the same
        this_path = rel_path.strip("/")
        raw_path = os.path.join(self.mountpoint, this_path)
        return raw_path

    def get_files(self, path, name_pattern, recursive, **user_args):
        full_path = os.path.join(self.mountpoint, path.lstrip('/'))
        abs_files = self.connector.get_files(full_path, name_pattern,
                                             recursive)
        # Remap the file paths to be relative to the mountpoint rather than
        # as absolute paths
        rel_files = [
            File(
                path=self._abs_to_relative(f.path),
                relPath=f.relPath,
                isDir=f.isDir,
                size=f.size,
                mtime=f.mtime) for f in abs_files
        ]
        return rel_files

    def open(self, path, opts):
        real_path = self._relative_to_abs(path)
        return self.connector.open(real_path, opts)

    def delete(self, path):
        # input path is relative to the mount point
        real_path = self._relative_to_abs(path)
        self.connector.delete(path=real_path)

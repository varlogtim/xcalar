import os
import shutil

from .util import File, match


class NativeConnector():
    def __init__(self):
        self.last_file_opened = None

    def get_files(self, path, name_pattern, recursive):
        """
        List the visible files matching name_pattern. Should return a "File"
        namedtuple.
        Results are globally visible if '_is_global()' returns true
        """
        if name_pattern == "":
            name_pattern = "*"

        # Special case of direct file
        if os.path.isfile(path):
            stat_obj = os.stat(path)
            file_obj = File(
                path=path,
                relPath=os.path.basename(path),
                isDir=False,
                size=stat_obj.st_size,
                mtime=int(stat_obj.st_mtime))
            return [file_obj]

        if not os.path.isdir(path):
            raise ValueError("Directory '{}' does not exist".format(path))

        file_objs = []
        for root, dirs, filenames in os.walk(path, followlinks=True):
            for dn in dirs:
                full_dn = os.path.join(root, dn)
                rel_dn = os.path.relpath(full_dn, path)
                stat_obj = os.stat(full_dn)
                file_obj = File(
                    path=full_dn,
                    relPath=rel_dn,
                    isDir=True,
                    size=0,
                    mtime=int(stat_obj.st_mtime))
                if match(name_pattern, recursive, file_obj):
                    file_objs.append(file_obj)
            for fn in filenames:
                full_fn = os.path.join(root, fn)
                rel_fn = os.path.relpath(full_fn, path)
                try:
                    stat_obj = os.stat(full_fn)
                except FileNotFoundError:
                    # Broken symlinked files and races will cause 'stat' to except.
                    # We choose to ignore the broken symlinks here
                    continue
                file_obj = File(
                    path=full_fn,
                    relPath=rel_fn,
                    isDir=False,
                    size=stat_obj.st_size,
                    mtime=int(stat_obj.st_mtime))
                if match(name_pattern, recursive, file_obj):
                    file_objs.append(file_obj)
            if not recursive:
                break
        return file_objs

    def open(self, path, opts):
        self.last_file_opened = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return open(path, opts, buffering=1 * 2**20)

    def delete(self, path):
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
        else:
            raise ValueError("Path {} doesn't exists".format(path))

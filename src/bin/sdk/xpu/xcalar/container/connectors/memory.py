import io
import json

from .util import File


class MemoryConnector():
    @staticmethod
    def is_available():
        return True

    def __init__(self, path, **kwargs):
        self.num_nodes = kwargs.get('num_nodes', None)
        if self.num_nodes is None:
            raise ValueError("num_nodes must be provided as input")
        self.xpus_per_node = kwargs.get('num_xpus_per_node', None)
        if self.xpus_per_node is None:
            raise ValueError("num_xpus_per_node must be provided as input")

        # Remove the leading slash from the path
        path = path.lstrip('/')
        self.tot_num_rows = int(path)

    def get_files(self, path, name_pattern, recursive):
        """
        List the visible files matching name_pattern. Should return a "File"
        namedtuple.
        """
        file_objs = []
        seq = 0

        for node_id in range(0, self.num_nodes):
            num_xpus = self.xpus_per_node[node_id]
            for ii in range(0, num_xpus):
                file_obj = File(
                    path=str(seq),
                    relPath=str(seq),
                    isDir=False,
                    size=1,
                    mtime=int(0))
                file_objs.append(file_obj)
                seq += 1

        return file_objs

    def open(self, path, opts):
        if "w" in opts:
            raise ValueError("Write files not implemented for Memory")

        tot_num_groups = sum(self.xpus_per_node)
        min_file_rows = self.tot_num_rows // tot_num_groups
        num_bonus_groups = self.tot_num_rows % tot_num_groups

        this_group_num = int(path)
        if this_group_num < num_bonus_groups:
            this_num_rows = min_file_rows + 1
        else:
            this_num_rows = min_file_rows

        start_row = min(this_group_num,
                        num_bonus_groups) + this_group_num * min_file_rows

        return self.MemFile(start_row, this_num_rows)

    class MemFile():
        def __init__(self, start_row, num_rows):
            # The file contents is the number of rows to load for this file
            in_obj = {"startRow": start_row, "numRows": num_rows}
            file_contents = json.dumps(in_obj)
            self.f = io.BytesIO(file_contents.encode("utf-8"))

        def __enter__(self):
            return self.f

        def __exit__(self, exc_type, exc_val, traceback):
            pass

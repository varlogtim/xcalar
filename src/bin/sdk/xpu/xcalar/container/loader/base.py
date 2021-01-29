import os
import re
import json
import codecs
import traceback

from .xce_dataset import XCEDataset

import xcalar.container.parent as xce
import xcalar.container.context as ctx
import xcalar.container.xpu_host as xpu_host

from xcalar.container.cluster import get_running_cluster
from xcalar.container.target.manage import get_target_data, build_target
from collections import namedtuple

LoadFile = namedtuple('LoadFile', ['nodeId', 'sourceArgs', 'file'])

logger = ctx.get_logger()


# sources should be a list of source_args
def _distribute_files(targets, sources, **user_args):
    cluster = get_running_cluster()
    my_xpu_id = cluster.my_xpu_id
    num_nodes = cluster.num_nodes
    my_node_id = cluster.my_node_id
    dlm_xpu_id = cluster.master_xpu_id
    dlm_node_id = ctx.get_node_id(dlm_xpu_id)

    # The local dlm is either the 'local xpu 0' or the global DLM
    if my_node_id != dlm_node_id:
        is_local_dlm = cluster.is_local_master()
    else:
        is_local_dlm = cluster.is_master()

    global_files = []
    my_files = []    # list of file lists, one for each target
    all_global = True
    for target, source_args in zip(targets, sources):
        path = source_args["path"]
        name_pattern = source_args["fileNamePattern"]
        recursive = source_args["recursive"]

        # Non-global targets have node affinity
        specific_node = None
        if not target.is_global():
            specific_node = my_node_id
            all_global = False

        # We want to minimize IO; global targets are listed once globally,
        # node-affine targets are listed once per node (on the local DLM)
        if target.is_global():
            should_list = cluster.is_master()
        else:
            should_list = is_local_dlm

        if should_list:
            raw_files = target.get_files(path, name_pattern, recursive,
                                         **user_args)
            my_files.extend([
                LoadFile(nodeId=specific_node, sourceArgs=source_args, file=f)
                for f in raw_files
            ])

    # Now we have all of our files; if needed, send our files to the DLM
    if not all_global and not cluster.is_master() and is_local_dlm:
        cluster.send_msg(dlm_xpu_id, my_files)

    if cluster.is_master():
        global_files = my_files
        if not all_global:
            # We already have ours, so we are expecting 1 fewer file list
            for _ in range(num_nodes - 1):
                global_files.extend(cluster.recv_msg())
    return global_files


#
# XXX This class really isn't ready. More thought needs to go into this
# which I think will become clear when I port the old load over to this.
class BaseLoader:
    """
    This is the base loader class. I will add methods inherent to all
    loading patterns as I see them.
    """

    def __init__(self, sources, sample_size, parse_args, xlr_root):

        self.sources = sources
        self.parse_args = parse_args
        self.check_paths()

        self.xce_client = xce.Client()
        self.cluster = get_running_cluster()
        self.dataset = XCEDataset(self.xce_client)

        self.format = -1    # this is used for raw parsers (csv, parquet)
        self.encoding = "utf-8"    # this is also used for raw parsers
        self.open_opts = "rb"    # not sure about this...

        self.allow_file_errors = False
        self.allow_record_errors = True

        # Stuff for actually loading data
        self.xlr_root = xlr_root
        self.user_module = None

        self.master_file_list = []    # List of LoadFile objects
        self.xpu_file_list = []
        self.parser_func = None

    def check_paths(self):
        invalids = [s["path"] for s in self.sources if ".." in s["path"]]
        if invalids:
            raise ValueError(f"'..' is not allowed in path '{invalids[0]}'")

    def build_targets(self):
        """
        This locates and constructs instances of each target needed
        """
        if self.cluster.is_master():
            target_datas = [
                get_target_data(s["targetName"]) for s in self.sources
            ]
            self.cluster.broadcast_msg(target_datas)
        else:
            target_datas = self.cluster.recv_msg()
        self.targets = [
            build_target(
                s["targetName"],
                target_datas[ii],
                s["path"],
                num_nodes=ctx.get_node_count(),
                node_id=ctx.get_node_id(ctx.get_xpu_id()),
                user_name=ctx.get_user_id_name(),
                num_xpus_per_node=ctx.get_num_xpus_per_node())
            for ii, s in enumerate(self.sources)
        ]

    def partition_master_file_list(self):
        # This is different than the "_distribute_files()" in load.py. That
        # actually does not distribute files. It actually just lists files.
        #
        # For the time being, I am going to ignore node specific files. I
        # think the proper solution is to group the node specific files, then
        # distribute the global files over the remaining available XPUs. Need
        # to think about this one in greater detail.
        num_nodes = self.cluster.num_nodes
        xpus_per_node = self.cluster.xpus_per_node    # [8, 8, 4, ..]
        total_num_xpus = sum(xpus_per_node)
        master_xpu_id = self.cluster.master_xpu_id

        # Count from each XPU id, by the total number of XPUs, until all
        # paths have been added. This produces number of XPUs lists
        # with each list containing ((paths / xpus) +- 1) paths.
        # [[path1, path2, ...], [path5, ...], ... ]
        file_groups = [
            self.master_file_list[ii::total_num_xpus]
            for ii in range(total_num_xpus)
        ]

        # Empty groups are produced if num paths < num xpus. Remove them.
        file_groups = [pp for pp in file_groups if pp != []]

        # Build a list of nodes and how many unused XPUs they have. We
        # start with none of the XPUs used.
        unused_nodes_xpus = [{
            'node_id': node,
            'remaining_xpus': num_xpus
        } for node, num_xpus in enumerate(xpus_per_node)]

        # As we assign a groups of paths to a particular node, we decrement
        # the number of remaining XPUs available to do work on that node.
        # When the node has no remaining XPUs, we remove that node from the
        # list of nodes we can assign to. Handles unbalanced XPU counts.
        assigned_groups = [[] for _ in range(num_nodes)]
        for ii, files in enumerate(file_groups):
            num_nodes_with_unused_xpus = len(unused_nodes_xpus)
            ii_offset = master_xpu_id + ii    # assign from master_xpu_id
            next_unused_index = ii_offset % num_nodes_with_unused_xpus
            assigned_node = unused_nodes_xpus[next_unused_index]['node_id']

            # makes num nodes lists of <= num XPUs lists of paths
            assigned_groups[assigned_node].append(files)

            unused_nodes_xpus[next_unused_index]['remaining_xpus'] -= 1
            unused_nodes_xpus = [
                nn for nn in unused_nodes_xpus if nn['remaining_xpus'] > 0
            ]

        # Now we have our non-empty groups of paths distributed evenly
        # among our nodes. If a given node does not have an assigned
        # group of paths for all of its XPUs, we give the unassigned XPUs
        # a list of None. This produces a num XPU list of lists of paths.
        xpu_files_groups = []
        for nn in range(num_nodes):
            node_groups = assigned_groups[nn]
            pad_length = xpus_per_node[nn] - len(node_groups)
            node_groups.extend([None] * pad_length)
            xpu_files_groups.extend(node_groups)

        assert len(xpu_files_groups) == total_num_xpus

        self.xpu_file_list = xpu_files_groups[self.cluster.my_xpu_id]

    # XXX Rename this, it is really a 'bytes_decoder'
    def get_raw_file_stream_gen(self, files_bytes_gen, encoding=None):
        reader = None
        if encoding:
            reader = codecs.getreader(encoding)
        for (file_bytes, file_size, uncompressed_file_size, file_path,
             sourceArgs) in files_bytes_gen:
            final_file = file_bytes
            if reader:
                final_file = reader(file_bytes)
            yield (final_file, file_size, uncompressed_file_size, file_path,
                   sourceArgs)

    # # FIXME
    # def unpacker_gen(self, files_bytes_gen):
    #     """ Reads from file bytes generator and handles decomression """
    #     for (file_bytes, file_size,
    #          file_path, sourceArgs) in files_bytes_gen:
    #         file_ext = file_extension(file_path)
    #         if unpackers.get(file_ext, None):
    #             try:
    #                 for (outfile, file_size) in unpack_thing(
    #                             file_path, file_bytes, file_size):
    #                     with outfile as decompressed_file:
    #                         yield (decompressed_file, file_size,
    #                                file_path, sourceArgs)
    #             except SeekableError:
    #                 raise ValueError(f"'{file_ext}' files require a "
    #                                  "target which supports seek()") from None

    def files_bytes_gen(self, file_group):
        """ This a generator which produces open file-list things"""
        # Handle opening files here... this will also start the dataset
        # We will handle decompression/archive in another gen wrapping this
        # We also need to wrap a reader.
        #
        # for load_file in file_group.files: (this is extactly load.py-like)
        for load_file in file_group:
            target_name = load_file.sourceArgs["targetName"]
            target_gen = (t for t in self.targets if t.name() == target_name)
            try:
                target = next(target_gen)
            except StopIteration:
                target = None
            if target is None:
                raise ValueError(f"target '{target_name}' not found")

            app_context = ctx._get_app_context()
            app_context["currTarget"] = target
            ctx._set_app_context(app_context)
            fileobj = load_file.file
            file_size = fileobj.size
            file_path = fileobj.path
            logger.info(f"Target {target_name} on XPU {ctx.get_xpu_id()}, "
                        f"Loading file, path: '{fileobj.path}', "
                        f"size: '{fileobj.size}', mtime: '{fileobj.mtime}'")
            with target.open(file_path, self.open_opts) as file_bytes:
                self.dataset.start_file(file_bytes, file_size)
                yield (file_bytes, file_size, file_size, file_path,
                       load_file.sourceArgs)
                self.dataset.finish_file()

    def get_apply_udf_func(self):
        """ This wraps the call to the parser generator so that we can
        get readable error messages and controls how nulls are treated
        """
        # This is just to segregate this function for readability

        # XXX I am realizing that there is another abstraction here.
        # We may want to execute a function and not pass in a file path
        # and an open file. We may want to just execute a generator func
        # that yields records.

        # UDF parse args can be set globally?
        udf_parse_args = json.loads(self.parse_args["parserArgJson"])
        treat_null_as_fnf = udf_parse_args.pop("treatNullAsFnf", False)

        # XXX Make this generic, don't assume we are passing in a file
        # Actually, we can't right now. This happens in C++
        def apply_udf(path, raw_file):
            try:
                if treat_null_as_fnf:
                    for row in self.parser_func(path, raw_file,
                                                **udf_parse_args):
                        yield {
                            k: row[k]
                            for k in row.keys() if row[k] is not None
                        }
                else:
                    yield from self.parser_func(path, raw_file,
                                                **udf_parse_args)
            except Exception as e:
                tb = traceback.format_exc()
                tb_lines = tb.splitlines()
                if "apply_udf" in tb_lines[1]:
                    # tb_lines[0] is the trace-back header statement.
                    # If tb_lines[1] - i.e. top of stack, is apply_udf, invoked
                    # just above, the next frame should be that of the actual
                    # UDF function - fix up this line to clarify the message.
                    #
                    # Report the UDF module name and its on-disk path separately
                    # for clarity.
                    #
                    # Assume 3rd line (tb.splitlines()[2]) has the module path.
                    # This is because apply_udf (this function) is on the top
                    # of the stack, having issued self.parser_func above. A
                    # traceback has a header in the first line ("Traceback
                    # (most recent call last):"), followed by a line for each
                    # stack frame - so the next line will be for apply_udf, and
                    # the one after (index 2) will be the one for
                    # self.parser_func, containing the name of the UDF and
                    # its path.
                    user_udf_line = tb_lines[2]
                    # udf_pre is something like 'File '
                    # udf_path has the path to the failing udf
                    # udf_post has the function like "line 7, in my_func"
                    matches = re.match(r'(.*)"(.*)"(.*)', user_udf_line)
                    if matches is not None:
                        udf_pre, udf_path, udf_add_info = matches.groups()
                        udf_directory, udf_module = os.path.split(udf_path)
                        # Now fix the UDF line:
                        modified_udf_line = 'UDF module "{}"{} from "{}"'.format(
                            udf_module, udf_add_info, udf_directory)
                        tb_lines[2] = modified_udf_line
                        tbfix = "\n".join(tb_lines)
                    else:
                        tbfix = tb
                    err_str = "{} at {}".format(str(e), tbfix)
                else:
                    err_str = "{} at {}".format(str(e), tb)
                raise Exception(err_str)

        return apply_udf

    def load_local(self, raw_file_stream_gen):
        if self.parser_func:
            apply_udf = self.get_apply_udf_func()
            files_stream = raw_file_stream_gen
            try:
                xpu_host.stream_out_records(files_stream, self.parse_args,
                                            self.dataset, apply_udf)
            finally:
                self.dataset.flush_buffers()
        else:
            files_stream = raw_file_stream_gen(self.encoding)
            try:
                xpu_host.stream_raw_files(files_stream, self.parse_args,
                                          self.format, self.dataset)
            finally:
                self.dataset.flush_buffers()
        return ""

    def set_parser(self, parser_func):
        return

    def main():
        raise NotImplementedError("impl me")

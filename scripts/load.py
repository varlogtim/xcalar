# Copyright 2016-2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
"""
Usage: Executed within the Xcalar Data Platform

This is a Xcalar Application which is responsible for loading
user data. It exposes a small set of functions to allow browsing
arbitrary file systems and consuming the resident data.

The overall control flow is:
    -  User sends load parameters from XI to a node in the XDP
    -  All nodes simultaneously execute this load app in xpus
    -  A 'master' node discovers all files based on user params
    -  'master' node samples the files based off user specified sample size
    -  'master' node groups the files into workitems, which will each
        be processed in a single-threaded manner across the cluster by each xpu
    -  'master' sends the respective group list to the chosen XPUs
    -  The workers request shared memory databuffers from their local XDP node
        - The workers parse/transform the file data (streaming UDF)
    -  The workers fill the databuffers with processed data in datapage format
    -  The workers tell XDP each time they finish a batch of pages
    -  XDP handles the backend of the load, tracking the datapages as a single
       dataset
"""

import os
import json
import random
import traceback
import sys
import bisect
import base64
import imp
import inspect
import pickle
import copy
import contextlib
import codecs
import importlib
import re
from collections import namedtuple
from itertools import groupby, chain

from xcalar.compute.coretypes.LibApisCommon.constants import XcalarApiDefaultCsvParserNameT
from xcalar.compute.coretypes.DataFormatEnums.ttypes import DfFormatTypeT
from xcalar.container.target.manage import get_target_data, build_target
from xcalar.container.loader.base import _distribute_files
from xcalar.container.loader.xce_dataset import XCEDataset

import xcalar.container.parent as xce
import xcalar.container.context as ctx
from xcalar.container.cluster import get_running_cluster
import xcalar.container.xpuComm as xpuComm
import xcalar.container.xpu_host as xpu_host
from xcalar.compute.util.config import detect_config
from xcalar.compute.util.utils import FileUtil

BIG_PRIME = 103113233

# Types
LoadFile = namedtuple('LoadFile', ['nodeId', 'sourceArgs', 'file'])
FileGroup = namedtuple('FileGroup', ['nodeId', 'files'])

# Set up logging
logger = ctx.get_logger()


def get_format_type(parser_name, parserArgs):
    parquetParser = parserArgs.get("parquetParser", None)
    if parser_name == XcalarApiDefaultCsvParserNameT:
        return (DfFormatTypeT.DfFormatCsv, "utf-8")
    elif "default:parseParquet" in parser_name and (parquetParser == "native"
                                                    or parquetParser is None):
        return (DfFormatTypeT.DfFormatParquet, None)
    else:
        raise ValueError("unknown parser {}".format(parser_name))


def human_readable_bytes(num_bytes):
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
    ii = 0
    if num_bytes > 0:
        while num_bytes > (2**(10 * (ii + 1))):
            ii = ii + 1
    rel_bytes = float(num_bytes) / float(2**(10 * ii))
    return "{} {}".format(round(rel_bytes, 2), suffixes[ii])


class Previewer():
    def __init__(self, source):
        if ".." in source["path"]:
            raise ValueError("'..' is not allowed in path '{}'".format(
                source["path"]))
        xce_client = xce.Client()
        self.target = None
        self.source = source
        self.cluster = get_running_cluster()

    def preview(self, offset, bytes_requested):
        target_data = None
        if self.cluster.is_master():
            target_data = get_target_data(self.source["targetName"])
            if self.cluster.total_size > 1:
                self.cluster.broadcast_msg(target_data)
        else:
            assert self.cluster.total_size > 1
            target_data = self.cluster.recv_msg()
        self.target = build_target(
            self.source["targetName"],
            target_data,
            self.source["path"],
            num_nodes=ctx.get_node_count(),
            node_id=ctx.get_node_id(ctx.get_xpu_id()),
            user_name=ctx.get_user_id_name(),
            num_xpus_per_node=ctx.get_num_xpus_per_node())

        with self.target:
            load_files = _distribute_files([self.target], [self.source],
                                           mode="preview")

            logger.debug("preview files returned by get_load_files {}".format(
                load_files))

            my_lFile = None
            #
            # The heavy lifting is all done by the DLM XPU - both in the global
            # and local storage case.
            #
            # In the global case, although all XPUs will have identical content
            # in load_files, only the DLM processes it, and sends a None File to
            # the non-DLM XPUs, which are waiting in recv_msg() - on receiving
            # the None file, they return, check that they have nothing, and
            # return an empty string
            #
            # In the local case, get_load_files() from the DistributedFs class
            # will return load_files only to the DLM XPU, which then processes
            # it, and will send the file to the right node (if it's not the DLM
            # node), and a None file to the rest of the nodes (which are all
            # waiting in recv_msg()). If a non-DLM XPU is the chosen one, it'll
            # get the file metadata from the recv_msg()- which it then returns to
            # the caller. The others receive the None file, and hence return an
            # empty string.
            #
            if self.cluster.is_master():
                logger.debug("preview - {} is DLM node Id".format(
                    self.cluster.my_xpu_id))
                no_files_found = False
                dlm_has_file = False
                l_file = None
                # This just needs to be well ordered across preview calls
                load_files.sort()
                logger.debug("preview files sorted {}".format(load_files))
                # Find the file starting from the given offset
                running_size = []
                for ii, lf in enumerate(load_files):
                    if ii == 0:
                        running_size.append(lf.file.size)
                    else:
                        running_size.append(lf.file.size +
                                            running_size[ii - 1])
                # Get file which has the desired offset
                # note that running_size is inherently sorted not that this will
                # skip all files with a size of 0 (including dirs)
                file_num = bisect.bisect_right(running_size, offset)
                if len(load_files) == 0 or file_num >= len(load_files):
                    no_files_found = True
                    logger.debug("preview - no files found")
                else:
                    l_file = load_files[file_num]
                    if file_num == 0:
                        file_offset = offset
                    else:
                        file_offset = offset - running_size[file_num - 1]

                    total_data_size = running_size[-1]

                    logger.debug(
                        "preview - file_num {} found on node {} file_offset {} total_data_size {}"
                        .format(file_num, l_file.nodeId, file_offset,
                                total_data_size))

                    if l_file.nodeId is None or l_file.nodeId == self.cluster.my_node_id:
                        logger.debug("preview - dlm XPU's node has file")
                        dlm_has_file = True

                if no_files_found or dlm_has_file:
                    logger.debug("preview - sending no-file to non-DLM nodes")
                    self.send_load_file(None, None, None)

                    if no_files_found:
                        # Have outobj start as python obj, so it can be
                        # json-ified
                        out_obj = {
                            "fileName": "",
                            "relPath": "",
                            "fullPath": "",
                            "base64Data": "",
                            "thisDataSize": 0,
                            "totalDataSize": 0
                        }
                        return json.dumps(out_obj)
                    else:
                        my_lFile = l_file
                        my_offset = file_offset
                        my_total_data_size = total_data_size
                else:
                    # DLM found a file but some other node has it
                    # Send the file to the node's DLM XPU (send routine takes
                    # care of this)
                    my_lFile = None
                    dlm_xpu = self.cluster.local_master_for_xpu(l_file.nodeId)
                    logger.debug(
                        "preview - send file to xpu {} on node {}".format(
                            dlm_xpu, l_file.nodeId))
                    self.send_load_file(l_file, file_offset, total_data_size)
            else:
                # this is useful only in the local storage case, when the
                # non-DLM XPU is the one which has the file
                my_lFile, my_offset, my_total_data_size = self.cluster.recv_msg(
                )

            # here we can guarantee that exactly 1 node has a p_file
            if my_lFile:
                assert my_lFile.nodeId is None or my_lFile.nodeId == self.cluster.my_node_id
                logger.debug("preview - xpu {} on node {} has file {}".format(
                    self.cluster.my_xpu_id, my_lFile.nodeId, my_lFile.file))
                p_file = my_lFile.file
                file_offset = my_offset
                total_data_size = my_total_data_size
                data = []

                logger.debug(
                    "previewing file {} offset {} size requested {}".format(
                        p_file, file_offset, bytes_requested))

                with self.target.open(p_file.path, 'rb') as open_file:
                    try:
                        for f, fsize in FileUtil.unpack_file(
                                p_file.path, open_file, 0):
                            f.read(file_offset)
                            data = f.read(bytes_requested)
                            # Skip the rest of the files (e.g. this is an multi-file archive)
                            break
                    except FileUtil.SeekableError:
                        raise ValueError(
                            "'{}' files require a target which supports "
                            "seek()".format(file_extension(p_file))) from None
                # Have outobj start as python obj, so it can be json-ified
                out_obj = {
                    "fileName": os.path.basename(p_file.relPath),
                    "relPath": p_file.relPath,
                    "fullPath": p_file.path,
                    "base64Data": base64.b64encode(data).decode('ascii'),
                    "thisDataSize": len(data),
                    "totalDataSize": total_data_size
                }
                return json.dumps(out_obj)
            else:
                logger.debug("preview - xpu {}, on node {} has no file".format(
                    self.cluster.my_xpu_id, self.cluster.my_node_id))
            return ""

    # If l_file is None, broadcast it to all nodes (the DLM XPU has it).  if
    # l_file is non-empty, the target node l_file.node_id is sent the file, by
    # picking the node's DLM XPU Id, and sending the payload to it. The rest of
    # the XPUs are sent the None file.
    def send_load_file(self, l_file, file_offset, total_data_size):
        abs_list = []
        if l_file is None:
            logger.debug("preview - sending None File to all, csz {}".format(
                self.cluster.total_size))
            self.cluster.broadcast_msg((None, None, None))
        else:
            dlm_xpu = self.cluster.local_master_for_node(l_file.nodeId)
            logger.debug("sending file to xpu {}, on node {}".format(
                dlm_xpu, l_file.nodeId))
            none_str = pickle.dumps((None, None, None))
            for i in range(0, self.cluster.total_size):
                if i != self.cluster.my_xpu_id and i != dlm_xpu:
                    abs_list.append((i, none_str))
                elif i == dlm_xpu:
                    self.cluster.send_msg(
                        dlm_xpu, (l_file, file_offset, total_data_size))
        if abs_list:
            xpuComm.send_fanout(abs_list)


class Loader():
    def __init__(self, sources, sample_size, parse_args, xlr_root):
        invalids = [s for s in sources if ".." in s["path"]]
        if invalids:
            raise ValueError("'..' is not allowed in path '{}'".format(
                invalids[0]["path"]))

        # Stuff for accessing files
        self.xce_client = xce.Client()
        self.sources = sources
        self.target = None
        self.parse_args = parse_args
        self.format = -1    # this is used for raw parsers (csv, parquet)
        self.encoding = "utf-8"    # this is also used for raw parsers
        self.cluster = get_running_cluster()
        self.sampler = Sampler(self.cluster, sample_size)

        self.allow_file_errors = False
        self.allow_record_errors = True

        # Stuff for actually loading data
        self.xlr_root = xlr_root
        self.user_full_function_name = parse_args["parserFnName"]
        self.user_function = None
        self.user_module = None
        self.dataset = XCEDataset(self.xce_client)
        self.set_user_func(self.xlr_root, parse_args["parserFnPath"],
                           parse_args["parserFnSource"],
                           self.user_full_function_name,
                           json.loads(parse_args["parserArgJson"]))

    def get_full_arg_dict(self):
        args = {}
        args["sources"] = self.sources
        args.update({
            "sampleSize": self.sampler.sample_size,
        })
        # remove the sUDF source
        parse_args = copy.deepcopy(self.parse_args)
        del parse_args["parserFnSource"]
        args["parseArgs"] = parse_args
        return args

    def load_master(self):
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

        arg_dict = self.get_full_arg_dict()
        if self.cluster.is_master():
            logger.info("Beginning load of {} sources: {}".format(
                len(self.sources), arg_dict))

        schema_mode = json.loads(self.parse_args["parserArgJson"]).get(
            "schemaMode", None)

        if schema_mode == "schemaFile":
            raise ValueError("schemaFile not implemented")

        udf_parse_args = json.loads(self.parse_args["parserArgJson"])

        with contextlib.ExitStack() as stack:
            # we enter all of our targets upfront, allowing all of them to get
            # set up individually. A potential optimization would be to
            # collapse all source_args that have the same target
            for target in self.targets:
                stack.enter_context(target)

            load_files = _distribute_files(
                self.targets, self.sources, mode="load", **udf_parse_args)
            # get rid of all the directories, we don't care about them
            load_files = [f for f in load_files if not f.file.isDir]

            if self.cluster.is_master():
                if len(load_files) == 0:
                    if len(self.sources) == 1:
                        raise ValueError(
                            "No files found at '{}' from '{}'".format(
                                self.sources[0]["path"],
                                self.sources[0]["targetName"],
                            ))
                    else:
                        raise ValueError(
                            "No files found at any of {} sources, including '{}' from '{}'"
                            .format(
                                len(self.sources),
                                self.sources[0]["path"],
                                self.sources[0]["targetName"],
                            ))

                sampled_files, sampled_size = self.sampler.sample_files(
                    load_files)
                file_groups = self.sampler.group_files(sampled_files)

                logger.info(
                    "Load of {} sources sampled {} files({}) of {}; scheduled into {} parse_args {} groups across {} nodes"
                    .format(
                        len(self.sources), len(sampled_files),
                        human_readable_bytes(sampled_size), len(load_files),
                        len(file_groups), self.parse_args["parserArgJson"],
                        len(set([g.nodeId for g in file_groups]))))

                down_sampled = len(sampled_files) < len(load_files)
                total_files_bytes = sum([lf.file.size for lf in sampled_files])

                # This allows XCE to track the progress of load by informing
                # how many output buffers to expect
                self.xce_client.report_num_files(
                    len(sampled_files), total_files_bytes, down_sampled)

                # Build up a flat list of the files for each XPU, or None
                xpu_groups = []
                for node_id in range(self.cluster.num_nodes):
                    node_groups = [
                        g for g in file_groups if g.nodeId == node_id
                    ]
                    # pad array with None, so there is exactly one element for
                    # each XPU in the XPU cluster
                    pad_len = self.cluster.xpus_per_node[node_id] - len(
                        node_groups)
                    node_groups.extend([None] * pad_len)
                    xpu_groups.extend(node_groups)

                assert len(xpu_groups) == self.cluster.total_size
                self.cluster.send_msg_batch(xpu_groups)
                file_group = xpu_groups[self.cluster.my_xpu_id]
            else:
                file_group = self.cluster.recv_msg()

            # wait till all xpus get their chunk of the files to parse
            self.cluster.barrier()
            if file_group and len(file_group.files):
                self.load_local(file_group)

    def set_user_func(self, xlr_root, udf_path, udf_source,
                      user_full_function_name, parserArgs):
        user_module_name = user_full_function_name.split(":")[0].split("/")[-1]
        user_function_name = user_full_function_name.split(":")[1]

        try:
            # XXX we really want native parsers to be handled more elegantly
            (self.format, self.encoding) = get_format_type(
                user_full_function_name, parserArgs)
            return
        except ValueError as e:
            pass

        if not udf_source:
            # Insert into sys.path, the paths from which the UDF module should
            # be resolved. The order of paths should be: first the path to the
            # UDF module, and second, the back-up path to the sharedUDFs
            # directory (this is just xlr_root).  Note that the order of
            # insertion is reverse so the udf's path ends up at index 0 in
            # sys.path

            # NOTE: To disambiguate with Python built-in module names, we can't
            # just import the module name, but we must use a qualified module
            # name - e.g. if the udf_path is /a/b/c, and module name is m.py,
            # then the module should be imported as "c.m", with /a/b inserted
            # in sys.path (not as "m", with /a/b/c inserted in sys.path - since
            # 'm' may clash with a built-in Python module - whereas 'c.m'
            # wouldn't clash).
            #
            # We call "c" the package name, and so the path /a/b is the path to
            # the package - this is the path to be inserted in sys.path - this
            # is the 'udf_pkg_path', and the path-qualified module name ('c.m')
            # is in 'user_module_pqname' below:

            udf_pkg_path = os.path.dirname(udf_path)
            sys.path.insert(0, xlr_root)
            sys.path.insert(0, udf_pkg_path)
            # Re-load the module if this module name is already loaded - a
            # previous deployment of this XPU could've loaded a different
            # module with the same name
            udf_pkg_name = os.path.basename(udf_path)
            user_module_pqname = udf_pkg_name + "." + user_module_name
            self.user_module_spec = importlib.util.find_spec(
                user_module_pqname)
            if self.user_module_spec is not None:
                logger.debug(
                    "setUserFunc reloading module '{}' udf_path '{}'".format(
                        user_module_name, udf_path))
                # need to re-load; can't use importlib.reload - see
                # https://docs.python.org/3/library\
                #    /importlib.html#importing-programmatically
                #
                # There are two scenarios regarding the already imported module:
                # 0. Existing file path is same as the desired 'udf_path'
                # 1. Existing file path is different from 'udf_path'
                #
                # If the paths match, re-exec/load the module from this path.
                # If the paths differ, update the user_module_spec with the new
                # path and then re-exec/load the module from the new path.
                self.user_module = importlib.util.module_from_spec(
                    self.user_module_spec)
                # get the current dirpath to the imported module
                self.user_module_dirpath = ("/").join(
                    self.user_module.__file__.split("/")[:-1])
                if self.user_module_dirpath != udf_path:
                    # currently imported module isn't from the right path
                    logger.debug(
                        "setUserFunc '{}' old path '{}' is different".format(
                            user_module_name, self.user_module_dirpath))
                    self.user_module_newdirpath = udf_path + "/" + user_module_name + ".py"
                    self.user_module_spec = importlib.util.spec_from_file_location(
                        user_module_name, self.user_module_newdirpath)
                    # update self.user_module with the newly imported module
                    self.user_module = importlib.util.module_from_spec(
                        self.user_module_spec)
                # if paths differ, the user_module_spec has been updated so
                # this should load/exec the desired module
                self.user_module_spec.loader.exec_module(self.user_module)
                sys.modules[user_module_name] = self.user_module
            else:
                logger.debug(
                    "setUserFunc import '{}' first time from '{}'".format(
                        user_module_name, udf_path))
                # import for the first time
                self.user_module = importlib.import_module(user_module_pqname)

            if not self.user_module:
                raise ValueError(
                    "Module {} can't be imported".format(user_module_name))
        else:
            # udf_source is non-empty - so use this to compile/load the module
            logger.debug(
                "setUserFunc compile '{}' from source, path '{}'".format(
                    user_module_name, udf_path))
            self.user_module = imp.new_module("userModule")
            exec(udf_source, self.user_module.__dict__)

        self.user_function = getattr(self.user_module, user_function_name,
                                     None)

        if not udf_source:
            # Remove the paths from sys.path (inserted above) so as to leave
            # the XPU's sys.path back in its original state. All modules
            # necessary to be imported from these paths (including the module
            # itself and any modules it imports via import statements in the
            # module) should have been imported via the import_module() call
            # above. So the sys.path change is no longer needed even though the
            # function in the module (user_function_name) hasn't yet been
            # executed sys.path.remove(udf_path)
            sys.path.remove(udf_pkg_path)
            sys.path.remove(xlr_root)

        if not self.user_function:
            raise ValueError(
                "Function {} does not exist".format(user_function_name))
        if not inspect.isgeneratorfunction(self.user_function):
            raise ValueError(
                "User function {} cannot be a import UDF as it is not a generator function"
                .format(user_function_name))
        argspec = inspect.getargspec(self.user_function)

        if len(argspec.args) < 2:
            raise ValueError(
                "Function '{}' has {} args, but must have at least 2".format(
                    user_function_name, len(argspec.args)))

    def load_local(self, file_group):
        def raw_file_stream_gen(encoding=None):
            reader = None
            if encoding:
                reader = codecs.getreader(encoding)

            for load_file in file_group.files:
                target_name = load_file.sourceArgs["targetName"]
                target_gen = (t for t in self.targets
                              if t.name() == target_name)
                try:
                    target = next(target_gen)
                except StopIteration:
                    target = None
                if target is None:
                    raise ValueError(
                        "target '{}' not found".format(target_name))

                app_context = ctx._get_app_context()
                app_context["currTarget"] = target
                ctx._set_app_context(app_context)
                fileobj = load_file.file
                file_size = fileobj.size
                if FileUtil.isFiledPacked(fileobj.path):
                    with target.open(fileobj.path, "rb") as file_to_open:
                        self.dataset.start_file(file_to_open, file_size)
                        # Unpack archive
                        try:
                            for outfile, uncompressed_file_size in FileUtil.unpack_file(
                                    fileobj.path, file_to_open, file_size):
                                with outfile as f:
                                    final_file = f
                                    if reader:
                                        final_file = reader(f)
                                    yield (final_file, file_size,
                                           uncompressed_file_size,
                                           fileobj.path, load_file.sourceArgs)
                        except FileUtil.SeekableError:
                            raise ValueError(
                                "'{}' files require a target which supports "
                                "seek()".format(file_extension(
                                    fileobj.path))) from None
                else:
                    with target.open(fileobj.path, "rb") as f:
                        final_file = f
                        if reader:
                            final_file = reader(f)
                        self.dataset.start_file(final_file, file_size)
                        yield (final_file, file_size, file_size, fileobj.path,
                               load_file.sourceArgs)
                self.dataset.finish_file()

        if self.user_function:
            file_stream = raw_file_stream_gen()
            self.process_UDF_files(file_stream)
        else:
            file_stream = raw_file_stream_gen(self.encoding)
            self.process_raw_files(file_stream)
        return ""

    def process_UDF_files(self, files):
        udf_parse_args = json.loads(self.parse_args["parserArgJson"])
        treatNullAsFnf = udf_parse_args.pop("treatNullAsFnf", False)

        def apply_udf(path, raw_file):
            try:
                if treatNullAsFnf:
                    for row in self.user_function(path, raw_file,
                                                  **udf_parse_args):
                        yield {
                            k: row[k]
                            for k in row.keys() if row[k] is not None
                        }
                else:
                    yield from self.user_function(path, raw_file,
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
                    # of the stack, having issued self.user_function above. A
                    # traceback has a header in the first line ("Traceback
                    # (most recent call last):"), followed by a line for each
                    # stack frame - so the next line will be for apply_udf, and
                    # the one after (index 2) will be the one for
                    # self.user_function, containing the name of the UDF and
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

        try:
            xpu_host.stream_out_records(files, self.parse_args, self.dataset,
                                        apply_udf)
        finally:
            self.dataset.flush_buffers()

    def process_raw_files(self, files):
        try:
            xpu_host.stream_raw_files(files, self.parse_args, self.format,
                                      self.dataset)
        finally:
            self.dataset.flush_buffers()


class Sampler():
    def __init__(self, cluster, sample_size):
        self.sample_size = sample_size
        self.cluster = cluster

    def group_files(self, load_files):
        """Groups load_files into some number of processing groups
        Input load_files is a list of load_files
        Output is a list of FileGroups, where file is a normal File"""

        # None is not sortable, we we're going to specially extract the
        # None group and append it
        none_group = [lf for lf in load_files if lf.nodeId is None]
        load_files = [lf for lf in load_files if lf.nodeId is not None]
        load_files.sort(key=lambda lf: lf.nodeId)

        node_groups = groupby(load_files, lambda lf: lf.nodeId)
        if none_group:
            node_groups = chain(node_groups, [(None, none_group)])
        groups = []

        # Seed the RNG to be a function of our filenames
        random.seed(frozenset([lf.file.path for lf in load_files]))

        # For the globally visible filesystem case, node_id will be None
        for node_id, l_files in node_groups:
            # l_files is the list of files on this node. For the global case,
            # this is just all files and node_id is None.
            l_files = list(l_files)

            # Account for the global case here
            num_groups = None
            if node_id is None:
                num_groups = self.cluster.total_size
            else:
                num_groups = (xpu_host.get_xpu_end_id_in_node(node_id) -
                              xpu_host.get_xpu_start_id_in_node(node_id)) + 1

            # The array might be sorted in some biased way. Shuffle the array
            random.shuffle(l_files)

            # This is partitioning our list such that each 'group' has a roughly
            # equal number of files. Each of these groups is going to be processed
            # sequentially within a single invocation of an 'app load'
            # This returns an array of arrays of LoadFiles
            load_file_groups = [
                l_files[ii::num_groups] for ii in range(0, num_groups)
            ]
            load_file_groups = [n for n in load_file_groups if n != []]

            # Find how many XPUs we have in each node [[nodeId, xpuCount], .. ]
            node_xpus = [[
                ii,
                (xpu_host.get_xpu_end_id_in_node(ii) -
                 xpu_host.get_xpu_start_id_in_node(ii)) + 1
            ] for ii in range(0, self.cluster.num_nodes)]

            master_xpu_id = self.cluster.master_xpu_id

            # We need to flatten the LoadFiles into Files, then put into FileGroups
            for group_idx, node_group in enumerate(load_file_groups):
                assigned_node = node_id
                if assigned_node is None:
                    # Our groups have no bias, due to the files being
                    # shuffled previously, so this doesn't need to be
                    # intelligent
                    ind = (group_idx + master_xpu_id) % len(node_xpus)
                    assigned_node = node_xpus[ind][0]
                    node_xpus[ind][1] -= 1
                    node_xpus = [nn for nn in node_xpus if nn[1] != 0]
                fg = FileGroup(nodeId=assigned_node, files=node_group)
                groups.append(fg)
        return groups

    def sample_files(self, files):
        total_loaded = 0
        sampled_files = []

        # Perform sampling

        # sort files in increasing size
        files = sorted(files, key=lambda f: f.file.size)
        for f in files:
            new_loaded = total_loaded + f.file.size
            if new_loaded > self.sample_size:
                break
            sampled_files.append(f)
            total_loaded = new_loaded
        if len(sampled_files) == 0:
            err_str = "The smallest file specified ({} bytes) is larger than the configured modeling memory limit ({} bytes).".format(
                files[0].file.size, self.sample_size)
            raise ValueError(err_str)
        return sampled_files, total_loaded


class FileDeleter(object):
    '''
    File Deleter app is used to delete file/directory given a path.
    '''

    def __init__(self, source):
        if ".." in source["path"]:
            raise ValueError("'..' is not allowed in path '{}'".format(
                source["path"]))
        self.source = source
        self.cluster = get_running_cluster()

    def delete_files(self):

        # if a target is global, then only a master xpu of cluster should handle delete
        # else target is not global, then only local master of each node should handle delete
        if not (self.cluster.is_master() or self.cluster.is_local_master()):
            return
        target_data = None
        if self.cluster.is_master():
            logger.info("Deleting file at {}".format(self.source["path"]))
            target_data = get_target_data(self.source["targetName"])
            self.cluster.broadcast_msg(target_data)
        else:
            assert self.cluster.total_size > 1
            target_data = self.cluster.recv_msg()
        target = build_target(
            self.source["targetName"],
            target_data,
            self.source["path"],
            num_nodes=ctx.get_node_count(),
            node_id=ctx.get_node_id(ctx.get_xpu_id()),
            user_name=ctx.get_user_id_name(),
            num_xpus_per_node=ctx.get_num_xpus_per_node())

        with target as tt:
            if (tt.is_global() and self.cluster.is_master()) or (
                    not tt.is_global() and self.cluster.is_local_master()):
                tt.delete(self.source["path"])


def main(in_blob):
    ret = None
    this_xpu_id = ctx.get_xpu_id()
    this_node_id = ctx.get_node_id(this_xpu_id)
    # set-up xlr_root; it's ok to fail from config (xlr_root default's used)
    configDict = ''
    xlr_root = "/var/opt/xcalar"
    try:
        configDict = detect_config().all_options
    except Exception as e:
        tb = traceback.format_exc()
        err_str = "{} at {}".format(str(e), tb)
        logger.info("Node {} build config failed:{}".format(
            this_node_id, err_str))
        pass

    if configDict:
        xlr_root = configDict["Constants.XcalarRootCompletePath"]

    try:
        in_obj = json.loads(in_blob)
        if in_obj["func"] == "load":
            parser_args_obj = json.loads(in_obj["parseArgs"]["parserArgJson"])
            loader_name = parser_args_obj.get('loader_name', False)
            if loader_name == 'LoadWizardLoader':
                logger.debug(f"Using LoadWizardLoader()")
                loader_start_func = parser_args_obj.get('loader_start_func')
                if not loader_start_func:
                    raise ValueError("Must provide 'loader_start_func'")
                from xcalar.container.loader.load_wizard import LoadWizardLoader
                ldr = LoadWizardLoader(in_obj["sourceArgsList"],
                                       in_obj["sampleSize"],
                                       in_obj["parseArgs"], xlr_root)
                start_func = getattr(ldr, loader_start_func, None)
                if not start_func:
                    raise ValueError(
                        f"Could not find '{loader_start_func}' in '{ldr}'")
            else:
                logger.debug("Using default legacy loader")
                ldr = Loader(in_obj["sourceArgsList"], in_obj["sampleSize"],
                             in_obj["parseArgs"], xlr_root)
                start_func = ldr.load_master
            ctx._set_app_context({"loadContext": ldr})
            ret = start_func()

        elif in_obj["func"] == "preview":
            p = Previewer(in_obj["sourceArgs"])
            ret = p.preview(in_obj["offset"], in_obj["bytesRequested"])

        elif in_obj["func"] == "deleteFiles":
            f = FileDeleter(in_obj["sourceArgs"])
            ret = f.delete_files()

        else:
            raise ValueError("Function {} not implemented".format(
                in_obj["func"]))
    except Exception:
        logger.exception("Node {} caught load exception".format(this_node_id))
        raise
    finally:
        ctx._set_app_context(None)

    return ret

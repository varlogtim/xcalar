import os
import sys
import json
import importlib
import contextlib

import xcalar.container.context as ctx
# XXX used by schema discover for getting a table belonging to a user.
# We might be able to re-use the XCE client already available. Ask Brent

from .base import BaseLoader
from .load_wizard_load_plan import LoadWizardLoadPlan
from collections import namedtuple

LoadFile = namedtuple('LoadFile', ['nodeId', 'sourceArgs', 'file'])

# logger = logging.getLogger('xcalar')
logger = ctx.get_logger()


class LoadWizardLoader(BaseLoader):
    def __init__(self, sources, sample_size, parse_args, xlr_root):
        super(LoadWizardLoader, self).__init__(sources, sample_size,
                                               parse_args, xlr_root)

        self.load_plan = None
        self.open_opts = 's3select'

    def set_dynamic_master_file_list(self):
        # XXX FIX ME ... This doesn't need to be just one anymore.
        if len(self.targets) != 1:
            raise ValueError("Must have only one target")
        if len(self.sources) != 1:
            raise ValueError("Must have only one source")

        if self.cluster.is_master():
            target = self.targets[0]
            source_args = self.sources[0]

            path = source_args['path']
            file_name_pattern = source_args['fileNamePattern']
            recursive = source_args['recursive']

            raw_files = target.get_files(path, file_name_pattern, recursive)
            self.master_file_list = [
                LoadFile(nodeId=None, sourceArgs=source_args, file=f)
                for f in raw_files if not f.isDir
            ]
            self.cluster.broadcast_msg(self.master_file_list)
        else:
            self.master_file_list = self.cluster.recv_msg()
        return

    def files_bytes_gen(self, file_group):
        """ This a generator which produces open file-list things"""
        # Handle opening files here... this will also start the dataset
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
                        f"size: '{fileobj.size}', mtime: '{fileobj.mtime}', "
                        f"load_id: '{self.load_plan.load_id}', "
                        f"file_type: '{self.load_plan.file_type}', "
                        f"num_cols: '{len(self.load_plan.cols.names)}', "
                        f"schema: '{self.load_plan.cols}'")
            with target.open(file_path, self.open_opts) as file_bytes:
                self.dataset.start_file(file_bytes, file_size)
                yield (file_bytes, file_size, file_size, file_path,
                       load_file.sourceArgs)
                self.dataset.finish_file()

    def load_with_load_plan(self):
        self.build_targets()
        self.set_load_plan(self.xlr_root, self.parse_args["parserFnPath"],
                           self.parse_args["parserFnName"],
                           json.loads(self.parse_args["parserArgJson"]))
        self.parse_args['parserArgJson'] = '{}'
        self.parser_func = self.load_plan.parser

        self.load_plan.setup()

        with contextlib.ExitStack() as stack:
            for target in self.targets:
                stack.enter_context(target)

            # self.set_master_file_list()  # No longer loading static file list
            self.set_dynamic_master_file_list()
            self.partition_master_file_list()
            self.cluster.barrier()
            if self.xpu_file_list and len(self.xpu_file_list) > 0:
                file_bytes_gen = self.files_bytes_gen(self.xpu_file_list)
                self.load_local(file_bytes_gen)

    ##
    # Loading the load plan
    #
    def set_load_plan(
            self,
            xlr_root,
            udf_path,    # parse_args["parserFnPath"]
            user_full_function_name,    # parse_args["parserFnName"]
            parserArgs):
        # XXX There is significantly more abstraction that can happen here.
        # udf_path = '/var/opt/xcalar/sharedUDFs/python'
        # user_full_func_name = '/sharedUDFs/load_plan_test:get_load_wizard_plan'
        user_module_name = user_full_function_name.split(":")[0].split("/")[-1]
        user_function_name = user_full_function_name.split(":")[1]

        udf_pkg_path = os.path.dirname(udf_path)
        udf_pkg_name = os.path.basename(udf_path)

        sys.path.insert(0, xlr_root)
        sys.path.insert(0, udf_pkg_path)

        user_module_pqname = udf_pkg_name + "." + user_module_name

        self.user_module_spec = importlib.util.find_spec(user_module_pqname)
        if self.user_module_spec is not None:
            logger.info(f"get_load_plan: reloading module: "
                        f"'{user_module_name}', udf_path: '{udf_path}'")

            self.user_module = importlib.util.module_from_spec(
                self.user_module_spec)
            # get the current dirpath to the imported module
            self.user_module_dirpath = ("/").join(
                self.user_module.__file__.split("/")[:-1])
            if self.user_module_dirpath != udf_path:
                # currently imported module isn't from the right path
                logger.info(
                    f"get_load_plan: '{user_module_name}', "
                    f"old path '{self.user_module_dirpath}' is different")
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
            logger.info(f"get_load_plan: import '{user_module_name}' "
                        f"first time from '{udf_path}'")
            # import for the first time
            self.user_module = importlib.import_module(user_module_pqname)

        if not self.user_module:
            raise ValueError(f"Module {user_module_name} can't be imported")

        load_plan_args_func = getattr(self.user_module, user_function_name,
                                      None)

        sys.path.remove(udf_pkg_path)
        sys.path.remove(xlr_root)

        if not load_plan_args_func:
            raise ValueError(f"Function {user_function_name} does not exist")

        # Get Load Plan Args
        load_plan_args = load_plan_args_func()

        # ENG-10027, load_uuid -> load_id, del(file_type)
        load_uuid = load_plan_args.pop("load_uuid", None)
        if load_uuid is not None:
            load_plan_args["load_id"] = load_uuid
        load_plan_args.pop("file_type", None)

        # Get instance of Load Plan
        self.load_plan = LoadWizardLoadPlan(**load_plan_args)

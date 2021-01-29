import os
import sys
import logging
import json
import time

import psutil
import getpass
import stat
import re

from socket import gethostname
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiWorkbookScopeT
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.container.parent import Client
from xcalar.external.exceptions import XDPException

import xcalar.container.xce_config as xce_config
import xcalar.container.cgroups.config as cgroups_config_mgr
import xcalar.container.cgroups.parse as parser
import xcalar.container.context as ctx
from xcalar.container.cluster import get_running_cluster

#
# Set up logging
#
logger = logging.getLogger("Cgroups")
logger.setLevel(logging.INFO)
if not logger.handlers:
    log_handler = logging.StreamHandler(sys.stderr)
    this_xpu_id = ctx.get_xpu_id()
    this_node_id = ctx.get_node_id(this_xpu_id)
    num_nodes = ctx.get_node_count()
    formatter = logging.Formatter(
        '%(asctime)s - Node {} - Cgroup App - %(levelname)s - %(message)s'.
        format(this_node_id))
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    logger.debug("Cgroups; nodeId:{}, num_nodes:{}, hostname:{}".format(
        this_node_id, num_nodes, gethostname()))

KB = 1024
MB = 1024 * KB
GB = 1024 * MB

# Cgroup owner XPU ID
CGROUPS_NODE_XPU_OWNER_ID = 0
CGROUPS_CLUSTER_XPU_OWNER_ID = 0
CGROUPS_TESTENV_OWNER_NODE_ID = 0


#
# Facilitate cgroups management
#
class CgroupMgr(object):
    def __init__(self):
        self.this_xpu_id = ctx.get_xpu_id()
        self.this_node_id = ctx.get_node_id(self.this_xpu_id)
        self.num_nodes = ctx.get_node_count()
        self.node_xpu_id = ctx.get_local_xpu_id(self.this_xpu_id)
        self.client = Client()
        self.xcalarApi = XcalarApi(bypass_proxy=True)
        getConfigOutput = self.xcalarApi.getConfigParams()
        for ii in range(0, getConfigOutput.numParams):
            if getConfigOutput.parameter[
                    ii].paramName == "BufferCachePercentOfTotalMem":
                self.bufCachePctOfTotalMem = int(
                    getConfigOutput.parameter[ii].paramValue)
            elif getConfigOutput.parameter[
                    ii].paramName == "CgroupsOsDefaultOnBoot":
                if getConfigOutput.parameter[ii].paramValue == "true":
                    self.cgroupsOsDefaultOnBoot = True
                else:
                    self.cgroupsOsDefaultOnBoot = False

        self.RUNTIME_KVSTORE_KEY = "/sys/runtime/schedParams"
        self.kv_store = self.client.global_kvstore()

        #
        # Cgroup names
        #

        self.CGROUP_XCALAR_XPU = []
        self.CGROUP_XCALAR_SYS_XPUS_NAME_LIST = []
        self.CGROUP_XCALAR_USR_XPUS_NAME_LIST = []
        self.CGROUP_XCALAR_MW = []
        self.CGROUP_XCALAR_XCE = []
        self.CGROUP_XCALAR_SLICE = []

        try:
            self.scope_list = os.environ['XCE_CHILDNODE_SCOPES'].split(' ')
        except KeyError:
            self.scope_list = []

        try:
            self.controller_map_list = os.environ[
                'XCE_CGROUP_CONTROLLER_MAP'].split(':')
        except KeyError:
            self.controller_map_list = []

        try:
            xce_scope = os.environ['XCE_USRNODE_SCOPE']
            self.CGROUP_XCALAR_XCE.append(xce_scope)
        except KeyError:
            pass

        unit_var_names = [
            'XCE_CADDY_UNIT', 'XCE_SQLDF_UNIT', 'XCE_XCMGMTD_UNIT'
        ]

        template_unit_var_names = {'XCE_EXPSERVER_UNIT': 'XCE_EXPSERVER_COUNT'}

        for unit_var in unit_var_names:
            try:
                unit_name = os.environ[unit_var]
                self.CGROUP_XCALAR_MW.append(unit_name)

            except KeyError:
                pass

        for env_name, env_count in template_unit_var_names.items():
            try:
                name = os.environ[env_name]
                count = int(os.environ[env_count])

                for i in range(0, count):
                    if env_name == "XCE_EXPSERVER_UNIT":
                        if os.environ["XCE_EXPSERVER_SYSD_TEMPLATE"] == "1":
                            unit_name = name.replace("@", "@" + str(i))
                        else:
                            unit_name = name.replace("@", str(i))
                    else:
                        unit_name = name.replace("@", "@" + str(i))
                    self.CGROUP_XCALAR_MW.append(unit_name)
            except KeyError:
                pass

        try:
            slice_unit = os.environ['XCE_XCALAR_SLICE']
            self.CGROUP_XCALAR_SLICE.append(slice_unit)
            self.XCALAR_SLICE = slice_unit
        except KeyError:
            pass

        for sched in self.scope_list:
            self.CGROUP_XCALAR_XPU.append(sched + '.scope')
            if 'sys_' in sched:
                self.CGROUP_XCALAR_SYS_XPUS_NAME_LIST.append(sched + '.scope')
            elif 'usr_' in sched:
                self.CGROUP_XCALAR_USR_XPUS_NAME_LIST.append(sched + '.scope')

        self.SYS_CGROUP_PATH_MAP = {}
        for pair in self.controller_map_list:
            controller, path = pair.split('%')
            self.SYS_CGROUP_PATH_MAP[controller] = path

        try:
            self.CGROUP_UNIT_PATH = os.environ['XCE_CGROUP_UNIT_PATH']
        except KeyError:
            self.CGROUP_UNIT_PATH = ""

        try:
            self.CGROUPS_CTRL_LIST = os.environ[
                'XCE_CGROUP_CONTROLLERS'].split(' ')
        except KeyError:
            self.CGROUPS_CTRL_LIST = []

        self.CGROUP_XCALAR_XPUS_SCHED0 = "sched0"
        self.CGROUP_XCALAR_XPUS_SCHED1 = "sched1"
        self.CGROUP_XCALAR_XPUS_SCHED2 = "sched2"

        #
        # Cgroup controllers
        #
        self.CGROUP_CTRL_MEMORY = "memory"
        self.CGROUP_CTRL_CPU = "cpu"
        self.CGROUP_CTRL_CPUSET = "cpuset"
        self.CGROUP_CTRL_CPUACCT = "cpuacct"

        # Cgroup path
        self.CUR_USER = getpass.getuser()

        self.cgroup_path_dict = {}
        all_cgroups = self.CGROUP_XCALAR_XPU + self.CGROUP_XCALAR_MW +\
            self.CGROUP_XCALAR_XCE + self.CGROUP_XCALAR_SLICE

        for cg_name in all_cgroups:
            self.cgroup_path_dict[cg_name] = {}
            for cg_ctrl in self.CGROUPS_CTRL_LIST:
                self.cgroup_path_dict[cg_name][cg_ctrl] = None

        #
        # memory controllers
        #
        if self.CGROUP_CTRL_MEMORY in self.CGROUPS_CTRL_LIST:
            self.MEMORY_CTRL_PARAMS_RO_LIST, self.MEMORY_CTRL_PARAMS_WO_LIST, self.MEMORY_CTRL_PARAMS_RW_LIST, self.MEMORY_CTRL_PARAMS_LIST = self.setup_controller_paths(
                self.CGROUP_CTRL_MEMORY)

        #
        # cpuset controllers
        #
        if self.CGROUP_CTRL_CPUSET in self.CGROUPS_CTRL_LIST:
            self.CPUSET_CTRL_PARAMS_RO_LIST, self.CPUSET_CTRL_PARAMS_WO_LIST, self.CPUSET_CTRL_PARAMS_RW_LIST, self.CPUSET_CTRL_PARAMS_LIST = self.setup_controller_paths(
                self.CGROUP_CTRL_CPUSET)

        #
        # cpu controllers
        #
        if self.CGROUP_CTRL_CPU in self.CGROUPS_CTRL_LIST:
            self.CPU_CTRL_PARAMS_RO_LIST, self.CPU_CTRL_PARAMS_WO_LIST, self.CPU_CTRL_PARAMS_RW_LIST, self.CPU_CTRL_PARAMS_LIST = self.setup_controller_paths(
                self.CGROUP_CTRL_CPU)

        #
        # cpuacct controllers
        #
        if self.CGROUP_CTRL_CPUACCT in self.CGROUPS_CTRL_LIST:
            self.CPUACCT_CTRL_PARAMS_RO_LIST, self.CPUACCT_CTRL_PARAMS_WO_LIST, self.CPUACCT_CTRL_PARAMS_RW_LIST, self.CPUACCT_CTRL_PARAMS_LIST = self.setup_controller_paths(
                self.CGROUP_CTRL_CPUACCT)

    def setup_controller_paths(self, controller_name):
        first_sys_controller_scope = self.CGROUP_XCALAR_XPU[0]

        # usrnode sits in a scope as a peer to the other xpus, so let's
        # do them together
        for sched in self.CGROUP_XCALAR_XPU + self.CGROUP_XCALAR_XCE:
            self.cgroup_path_dict[sched][controller_name] =\
                os.path.join(self.SYS_CGROUP_PATH_MAP[controller_name],
                             self.CGROUP_UNIT_PATH,
                             sched)

        for mw in self.CGROUP_XCALAR_MW:
            self.cgroup_path_dict[mw][controller_name] =\
                os.path.join(self.SYS_CGROUP_PATH_MAP[controller_name],
                             self.XCALAR_SLICE,
                             mw)

        for slice in self.CGROUP_XCALAR_SLICE:
            self.cgroup_path_dict[slice][controller_name] =\
                os.path.join(self.SYS_CGROUP_PATH_MAP[controller_name],
                             self.XCALAR_SLICE)

        first_controller_path = self.cgroup_path_dict[
            first_sys_controller_scope][controller_name]

        ro_ctrl_params, wo_ctrl_params, rw_ctrl_params = self.get_controller_param_names(
            first_controller_path)
        all_ctrl_params = ro_ctrl_params + wo_ctrl_params + rw_ctrl_params

        return ro_ctrl_params, wo_ctrl_params, rw_ctrl_params, all_ctrl_params

    def get_controller_param_names(self, cgroups_path):
        param_names_ro = []
        param_names_wo = []
        param_names_rw = []
        for f in os.listdir(cgroups_path):
            pathname = os.path.join(cgroups_path, f)
            fstat = os.stat(pathname)
            mode = fstat.st_mode
            if stat.S_ISDIR(mode):
                continue
            elif not stat.S_ISREG(mode):
                continue
            elif (mode & stat.S_IRUSR) and (mode & stat.S_IWUSR):
                param_names_rw.append(f)
            elif (mode & stat.S_IRUSR):
                param_names_ro.append(f)
            elif (mode & stat.S_IWUSR):
                param_names_wo.append(f)
        return param_names_ro, param_names_wo, param_names_rw

    # Return cgroup param_names for a cgroup_controller
    def get_cgroup_param_names(self, cgroup_controller, flag="ALL"):
        VALID_FLAG = ["ALL", "RONLY", "WONLY", "RW"]
        if flag not in VALID_FLAG:
            raise ValueError(
                "get_cgroup_param_names: flag: {} not valid".format(flag))
        if cgroup_controller == self.CGROUP_CTRL_MEMORY:
            if flag == "ALL":
                return self.MEMORY_CTRL_PARAMS_LIST
            elif flag == "RONLY":
                return self.MEMORY_CTRL_PARAMS_RO_LIST
            elif flag == "WONLY":
                return self.MEMORY_CTRL_PARAMS_WO_LIST
            elif flag == "RW":
                return self.MEMORY_CTRL_PARAMS_RW_LIST
        elif cgroup_controller == self.CGROUP_CTRL_CPU:
            if flag == "ALL":
                return self.CPU_CTRL_PARAMS_LIST
            elif flag == "RONLY":
                return self.CPU_CTRL_PARAMS_RO_LIST
            elif flag == "WONLY":
                return self.CPU_CTRL_PARAMS_WO_LIST
            elif flag == "RW":
                return self.CPU_CTRL_PARAMS_RW_LIST
        elif cgroup_controller == self.CGROUP_CTRL_CPUSET:
            if flag == "ALL":
                return self.CPUSET_CTRL_PARAMS_LIST
            elif flag == "RONLY":
                return self.CPUSET_CTRL_PARAMS_RO_LIST
            elif flag == "WONLY":
                return self.CPUSET_CTRL_PARAMS_WO_LIST
            elif flag == "RW":
                return self.CPUSET_CTRL_PARAMS_RW_LIST
        elif cgroup_controller == self.CGROUP_CTRL_CPUACCT:
            if flag == "ALL":
                return self.CPUACCT_CTRL_PARAMS_LIST
            elif flag == "RONLY":
                return self.CPUACCT_CTRL_PARAMS_RO_LIST
            elif flag == "WONLY":
                return self.CPUACCT_CTRL_PARAMS_WO_LIST
            elif flag == "RW":
                return self.CPUACCT_CTRL_PARAMS_RW_LIST
        else:
            raise ValueError(
                "get_cgroup_param_names: cgroup_controller: {} not supported".
                format(cgroup_controller))

    # Validate cgroup name
    def cgroup_name_valid(self, cgroup_name):
        all_cgroups = self.CGROUP_XCALAR_XPU + self.CGROUP_XCALAR_MW +\
            self.CGROUP_XCALAR_XCE + self.CGROUP_XCALAR_SLICE

        for cg in all_cgroups:
            if cg == cgroup_name:
                return True

        return False

    # Validate cgroup controller
    def cgroup_controller_valid(self, cgroup_controller):
        for cg in self.CGROUPS_CTRL_LIST:
            if cg == cgroup_controller:
                return True
        return False

    # Update the Cgroup params live for the respective cgroup
    def write_to_file(self, param_name, param_value, cgroups_path):
        fname = os.path.join(cgroups_path, param_name)
        if not os.path.exists(fname):
            raise ValueError(
                "write_to_file: file : {} does not exist".format(fname))
        with open(fname, 'w') as f:
            f.write(str(param_value))

    # Look up the Cgroup params live for the respective cgroup
    def read_from_file(self, param_name, cgroups_path):
        fname = os.path.join(cgroups_path, param_name)
        if not os.path.exists(fname):
            raise ValueError(
                "read_from_file: file: {} does not exist".format(fname))
        param_value = None
        with open(fname, 'r') as f:
            try:
                param_value = f.read()
            except OSError as e:
                logger.error("Error {} reading {}. Continuing.".format(
                    str(e), fname))
        return param_value

    # Update cgroup_params
    def write_cgroup_params(self, cgroup_params, cgroups_path):
        memsw_limit_in_bytes = None
        limit_in_bytes = None
        soft_limit_in_bytes = None

        for param_name, param_value in cgroup_params.items():
            if param_name == "memory.memsw.limit_in_bytes":
                memsw_limit_in_bytes = param_value
            elif param_name == "memory.limit_in_bytes":
                limit_in_bytes = param_value
            elif param_name == "memory.soft_limit_in_bytes":
                soft_limit_in_bytes = param_value
            else:
                self.write_to_file(param_name, param_value, cgroups_path)

        # If any of the memory settings were written to
        # Note that below equation needs to hold true.
        # memory.soft_limit_in_bytes <= memory.limit_in_bytes <= memory.memsw.limit_in_bytes
        if memsw_limit_in_bytes or limit_in_bytes or soft_limit_in_bytes:
            # In some cgroup memory controllers, memory.memsw.limit_in_bytes
            # may not be present.
            try:
                self.write_to_file("memory.memsw.limit_in_bytes", "-1",
                                   cgroups_path)
            except ValueError as e:
                if "memory.memsw.limit_in_bytes does not exist" not in str(e):
                    raise
            self.write_to_file("memory.limit_in_bytes", "-1", cgroups_path)
            self.write_to_file("memory.soft_limit_in_bytes", "-1",
                               cgroups_path)

        # Below conditions capture the order in which below memory related
        # params need to be set.
        if soft_limit_in_bytes:
            self.write_to_file("memory.soft_limit_in_bytes",
                               soft_limit_in_bytes, cgroups_path)
        if limit_in_bytes:
            self.write_to_file("memory.limit_in_bytes", limit_in_bytes,
                               cgroups_path)
        if memsw_limit_in_bytes:
            self.write_to_file("memory.memsw.limit_in_bytes",
                               memsw_limit_in_bytes, cgroups_path)

    # Get cgroup_params
    def read_cgroup_params(self, cgroup_param_names_list, cgroups_path):
        cgroup_params = {}
        for param_name in cgroup_param_names_list:
            if param_name.endswith("memory.kmem.slabinfo"):
                continue
            try:
                cgroup_params[param_name] = self.read_from_file(
                    param_name, cgroups_path)
            except ValueError as e:
                logger.error("Error reading file: {}. Continuing.".format(
                    str(e)))

        return cgroup_params

    # Set the params and then update Cgroup params to KvStore
    def update_cgroup_params(self, cgroup_name, cgroup_controller,
                             cgroup_params):
        sched = cgroup_name
        if sched.endswith('.scope'):
            sched = sched[:-6]

        cg_path = self.cgroup_path_dict[cgroup_name][cgroup_controller]

        # Note that the cgroup_params updates will only succeed if the param
        # has write permissions in cgroups FS.
        if self.node_xpu_id == CGROUPS_NODE_XPU_OWNER_ID:
            logger.info(
                "update_cgroup_params: cgroup_name: {}, cgroup_controller: {}, cgroup_params: {}, cg_path: {}"
                .format(cgroup_name, cgroup_controller, cgroup_params,
                        cg_path))
            self.write_cgroup_params(cgroup_params, cg_path)

        # Allow only one XPU to update the global KvStore
        if self.this_xpu_id == CGROUPS_CLUSTER_XPU_OWNER_ID:
            cgroups_config_mgr.add_or_update_cgroup_params(
                re.sub(r"\/", "_", cgroup_name), cgroup_controller,
                json.dumps(cgroup_params))

    # Get the cgroup params persisted
    def get_cgroup_params_persisted(self, cgroup_name, cgroup_controller):
        cgroup_params_persisted = {}
        cg_name_persisted = re.sub(r"\/", "_", cgroup_name)
        try:
            cgroup_params_persisted = cgroups_config_mgr.get_cgroup_params(
                cg_name_persisted, cgroup_controller)
        except ValueError as e:
            if str(e) == "No such cgroup '{}' controller '{}'".format(
                    cg_name_persisted, cgroup_controller):
                return cgroup_params_persisted
        return cgroup_params_persisted

    # Get the cgroup params
    def get_cgroup_params(self,
                          cgroup_name,
                          cgroup_controller,
                          flag="RW",
                          kvstore_lookup=True,
                          excluded=[]):
        if not self.cgroup_name_valid(cgroup_name):
            raise ValueError("Invalid cgroup name: {}".format(cgroup_name))
        if not self.cgroup_controller_valid(cgroup_controller):
            raise ValueError(
                "Invalid cgroup controller: {}".format(cgroup_controller))

        cgroup_param_names_list = self.get_cgroup_param_names(
            cgroup_controller, flag=flag)
        cgroup_param_names_list = [
            item for item in cgroup_param_names_list if item not in excluded
        ]

        cg_path = self.cgroup_path_dict[cgroup_name][cgroup_controller]

        # Reading cgroup params will exclude params that do not have read
        # permissions
        cgroup_params = self.read_cgroup_params(cgroup_param_names_list,
                                                cg_path)

        if kvstore_lookup:
            # Include the cgroup param update values that may be persisted in
            # KvStore.
            cgroup_params_persisted = self.get_cgroup_params_persisted(
                cgroup_name, cgroup_controller)
            for k, v in cgroup_params_persisted.items():
                cgroup_params[k] = cgroup_params_persisted[k]

        if "cgroup_path" not in excluded:
            cgroup_params["cgroup_path"] = cg_path

        return cgroup_params

    def get_def_sys_xpus_cgparams_memory(self, cgroup_name):
        cgroup_params = {}
        sys_mem = psutil.virtual_memory()

        # Pick a good soft limit. For now 90% sounds like a good number?
        soft_mem_limit_pct = 90

        # XPU cgroups can use the remaining of memory setting aside
        # buffer cache memory.
        if self.bufCachePctOfTotalMem < 100:
            xpu_pct = 100 - self.bufCachePctOfTotalMem
        else:
            # Pick a resonable default. For now 30% sounds like a good number?
            xpu_pct = 30

        if self.cgroupsOsDefaultOnBoot:
            cgroup_params["memory.limit_in_bytes"] = int(-1)
            cgroup_params["memory.soft_limit_in_bytes"] = int(-1)
        else:
            cgroup_params["memory.limit_in_bytes"] = int(
                (xpu_pct * sys_mem.total) / 100)

            # Limit xpu_pct
            cgroup_params["memory.soft_limit_in_bytes"] = int(
                (soft_mem_limit_pct * xpu_pct * sys_mem.total) / (100 * 100))

        # Disallow swap for system XPUs
        cgroup_params["memory.swappiness"] = 0

        # Enable OOM killer. XCE will cleanout any remnamts of this TXN.
        cgroup_params["memory.oom_control"] = 0

        return cgroup_params

    def get_def_usr_xpus_cgparams_memory(self, cgroup_name):
        cgroup_params = {}
        sys_mem = psutil.virtual_memory()
        swap_mem = psutil.swap_memory()

        cgroup_param_names_list = self.get_cgroup_param_names(
            self.CGROUP_CTRL_MEMORY, flag="RW")
        set_swap_limit = next((x for x in cgroup_param_names_list
                               if x == "memory.memsw.limit_in_bytes"), None)

        # Pick a good soft limit. For now 90% sounds like a good number?
        soft_mem_limit_pct = 90

        # XPU cgroups can use the remaining of memory setting aside
        # buffer cache memory.
        if self.bufCachePctOfTotalMem < 100:
            xpu_pct = 100 - self.bufCachePctOfTotalMem
        else:
            # Pick a resonable default. For now 30% sounds like a good number?
            xpu_pct = 30

        if self.cgroupsOsDefaultOnBoot:
            cgroup_params["memory.limit_in_bytes"] = int(-1)
            cgroup_params["memory.soft_limit_in_bytes"] = int(-1)
            if set_swap_limit is not None:
                cgroup_params["memory.memsw.limit_in_bytes"] = int(-1)
        else:
            cgroup_params["memory.limit_in_bytes"] = int(
                (xpu_pct * sys_mem.total) / 100)

            # Limit xpu_pct
            cgroup_params["memory.soft_limit_in_bytes"] = int(
                (soft_mem_limit_pct * xpu_pct * sys_mem.total) / (100 * 100))

            # Allow atmost 8GB of swap
            if set_swap_limit is not None:
                cgroup_params["memory.memsw.limit_in_bytes"] = min(
                    cgroup_params["memory.limit_in_bytes"] + 8 * GB,
                    cgroup_params["memory.limit_in_bytes"] + swap_mem.total)

        # Allow maximum swap for user XPUs
        cgroup_params["memory.swappiness"] = 100

        # Enable OOM killer
        cgroup_params["memory.oom_control"] = 0

        return cgroup_params

    def get_def_mw_cgparams_memory(self, cgroup_name):
        cgroup_params = {}
        sys_mem = psutil.virtual_memory()

        # Pick a good soft limit. For now 90% sounds like a good number?
        soft_mem_limit_pct = 90

        # Let's pick 2GB memory for the middleware cgroup
        min_mw_mem = 2 * GB
        mw_pct = 10
        mem = max(min_mw_mem, int((mw_pct * sys_mem.total) / 100))

        if self.cgroupsOsDefaultOnBoot:
            cgroup_params["memory.limit_in_bytes"] = int(-1)
            cgroup_params["memory.soft_limit_in_bytes"] = int(-1)
        else:
            cgroup_params["memory.limit_in_bytes"] = mem

            # Pick soft limit as soft_mem_limit_pct of hard limit.
            cgroup_params["memory.soft_limit_in_bytes"] = int(
                (soft_mem_limit_pct * mem) / 100)

        # Disallow swap for middleware XPUs
        cgroup_params["memory.swappiness"] = 0

        # Enable OOM killer. Since these middleware processes are stateless,
        # we can let supervisord restart these.
        cgroup_params["memory.oom_control"] = 0

        return cgroup_params

    def get_def_xce_cgparams_memory(self, cgroup_name):
        cgroup_params = {}

        # Disallow swap XCE cgroups
        cgroup_params["memory.swappiness"] = 0

        # Disable OOM killer
        cgroup_params["memory.oom_control"] = 1

        return cgroup_params

    # Get cgroup default settings for memory controller
    def get_default_cgroup_params_memory(self, cgroup_name):
        cgroup_params = {}
        if cgroup_name in self.CGROUP_XCALAR_USR_XPUS_NAME_LIST:
            cgroup_params = self.get_def_usr_xpus_cgparams_memory(cgroup_name)
        elif cgroup_name in self.CGROUP_XCALAR_SYS_XPUS_NAME_LIST:
            cgroup_params = self.get_def_sys_xpus_cgparams_memory(cgroup_name)
        elif cgroup_name in self.CGROUP_XCALAR_MW:
            cgroup_params = self.get_def_mw_cgparams_memory(cgroup_name)
        elif cgroup_name in self.CGROUP_XCALAR_XCE:
            cgroup_params = self.get_def_xce_cgparams_memory(cgroup_name)
        else:
            raise ValueError("Unknown cgroup name: {}".format(cgroup_name))
        return cgroup_params

    # Get the default cgroup params
    def get_default_cgroup_params(self, cgroup_name, cgroup_controller):
        # XXX TODO Only memory controller is supported now.
        if cgroup_controller != self.CGROUP_CTRL_MEMORY:
            raise ValueError(
                "Unsupported cgroup controller: {}, only memory is supported".
                format(cgroup_controller))

        return self.get_default_cgroup_params_memory(cgroup_name)

    # Update the default cgroup params
    def update_default_cgroup_params(self, cgroup_name, cgroup_controller):
        # XXX TODO Only memory controller is supported now.
        if cgroup_controller != self.CGROUP_CTRL_MEMORY:
            raise ValueError(
                "Unsupported cgroup controller: {}, only memory is supported".
                format(cgroup_controller))

        cg_param_names_list = self.get_cgroup_param_names(
            self.CGROUP_CTRL_MEMORY, flag="RW")

        # Always override persisted config params with defaults for now.
        cg_params = {}
        cg_params_default = self.get_default_cgroup_params(
            cgroup_name, cgroup_controller)
        for k, v in cg_params_default.items():
            if k in cg_param_names_list:
                cg_params[k] = v
        self.update_cgroup_params(cgroup_name, cgroup_controller, cg_params)

    def get_runtime_params(self):
        value = None
        try:
            value = self.kv_store.lookup(self.RUNTIME_KVSTORE_KEY)
        except XDPException as e:
            if e.statusCode == StatusT.StatusKvEntryNotFound:
                raise ValueError(
                    "No such key {} in kvstore for managing runtime params".
                    format(self.RUNTIME_KVSTORE_KEY))
        return json.loads(value)

    def get_cgroups_per_controller(self, cgroup_controller):
        result_cgroups = [
            k for (k, v) in self.cgroup_path_dict.items()
            if cgroup_controller in v and os.path.isdir(v[cgroup_controller])
        ]
        return result_cgroups

    def pid_info_helper(self, pid):
        result = {}
        # https://psutil.readthedocs.io/en/latest/#psutil.Process.as_dict
        attrs = [
            "cmdline", "cpu_times", "io_counters", "memory_full_info",
            "memory_percent", "num_ctx_switches", "num_fds", "num_threads",
            "status"
        ]
        try:
            proc = psutil.Process(pid=int(pid))
            result = proc.as_dict(attrs=attrs)
        except Exception:
            result = None
        return result

    def get_cgroups_info(self,
                         cgroups_list,
                         controllers_list,
                         kvstore_lookup=True,
                         excluded=[],
                         include_per_proc_details=True):
        cluster = get_running_cluster()
        my_node = cluster.my_node_id
        res = {}
        for cgroup_controller in controllers_list:
            cur_controller = []
            for cgroup_name in cgroups_list:
                cgroup_params = self.get_cgroup_params(
                    cgroup_name,
                    cgroup_controller,
                    "RW",
                    kvstore_lookup=kvstore_lookup,
                    excluded=excluded)
                cgroup_params.update(
                    self.get_cgroup_params(
                        cgroup_name,
                        cgroup_controller,
                        "RONLY",
                        kvstore_lookup=kvstore_lookup,
                        excluded=excluded))

                # XXX Ideally, we should be getting pid info from the systemd cgroup hierarchy,
                # but we don’t have env vars exported to expose that to an xpu right now.
                # Until then, get it from the memory controller.  That is universally used.
                if include_per_proc_details and 'cgroup.procs' in cgroup_params and cgroup_controller == self.CGROUP_CTRL_MEMORY:
                    pid_list = cgroup_params['cgroup.procs'].split('\n')
                    rawProcInfo = {
                        pid: self.pid_info_helper(pid)
                        for pid in pid_list if pid != ''
                    }
                    cgroup_params['cgroup_proc_info'] = {
                        k: v
                        for k, v in rawProcInfo.items() if v is not None
                    }

                cgroup_params["cgroup_name"] = cgroup_name
                cgroup_params["cgroup_controller"] = cgroup_controller
                cgroup_params["timestamp"] = int(time.time() * 1000)
                cgroup_params["cluster_node"] = my_node
                cur_controller.append(
                    parser.cgroupParser(cgroup_controller, cgroup_params))
            res[cgroup_controller] = cur_controller
        return res

    def list_all_cgroups(self,
                         kvstore_lookup=True,
                         excluded=[],
                         include_per_proc_details=False):
        cgroup_stats = {}
        cgroup_stats["xpu_cgroups"] = self.get_cgroups_info(
            self.CGROUP_XCALAR_XPU,
            self.CGROUPS_CTRL_LIST,
            kvstore_lookup,
            excluded,
            include_per_proc_details)
        cgroup_stats["usrnode_cgroups"] = self.get_cgroups_info(
            self.CGROUP_XCALAR_XCE, self.CGROUPS_CTRL_LIST, kvstore_lookup,
            excluded, include_per_proc_details)
        cgroup_stats["middleware_cgroups"] = self.get_cgroups_info(
            self.CGROUP_XCALAR_MW, self.CGROUPS_CTRL_LIST, kvstore_lookup,
            excluded, include_per_proc_details)
        return cgroup_stats


def refresh_mixed_mode_cgroups_internal(cg_mgr):
    runtime_params = cg_mgr.get_runtime_params()
    CPU_SHARES_DEFAULT = 1024

    logger.info(
        "refresh_mixed_mode_cgroups_internal {}".format(runtime_params))

    sched_cpu_reserved = {}
    for schedInfo in runtime_params["schedInfo"]:
        if schedInfo["schedName"] == "Scheduler-0":
            sched_cpu_reserved[cg_mgr.CGROUP_XCALAR_XPUS_SCHED0] = int(
                schedInfo["cpuReservedPct"])
        elif schedInfo["schedName"] == "Scheduler-1":
            sched_cpu_reserved[cg_mgr.CGROUP_XCALAR_XPUS_SCHED1] = int(
                schedInfo["cpuReservedPct"])
        elif schedInfo["schedName"] == "Scheduler-2":
            sched_cpu_reserved[cg_mgr.CGROUP_XCALAR_XPUS_SCHED2] = int(
                schedInfo["cpuReservedPct"])
        elif schedInfo["schedName"] == "Immediate":
            pass
        else:
            raise ValueError("Unknown schedName {}".format(
                schedInfo["schedName"]))

    cg_params = {}
    cg_params["cpu.shares"] = CPU_SHARES_DEFAULT

    # Get SYS XPU Cgroup params
    for cg_name in cg_mgr.CGROUP_XCALAR_SYS_XPUS_NAME_LIST:
        if cg_mgr.CGROUP_XCALAR_XPUS_SCHED0 in cg_name:
            cg_params["cpu.shares"] = \
                    int((CPU_SHARES_DEFAULT * sched_cpu_reserved[cg_mgr.CGROUP_XCALAR_XPUS_SCHED0]) / 100)
        elif cg_mgr.CGROUP_XCALAR_XPUS_SCHED1 in cg_name:
            cg_params["cpu.shares"] = \
                    int((CPU_SHARES_DEFAULT * sched_cpu_reserved[cg_mgr.CGROUP_XCALAR_XPUS_SCHED1]) / 100)
        elif cg_mgr.CGROUP_XCALAR_XPUS_SCHED2 in cg_name:
            cg_params["cpu.shares"] = \
                    int((CPU_SHARES_DEFAULT * sched_cpu_reserved[cg_mgr.CGROUP_XCALAR_XPUS_SCHED2]) / 100)
        else:
            raise ValueError("Unknown cgroup name {}".format(cg_name))

        cg_mgr.update_cgroup_params(cg_name, cg_mgr.CGROUP_CTRL_CPU, cg_params)

    # Get USR XPU Cgroup params
    for cg_name in cg_mgr.CGROUP_XCALAR_USR_XPUS_NAME_LIST:
        if cg_mgr.CGROUP_XCALAR_XPUS_SCHED0 in cg_name:
            cg_params["cpu.shares"] = \
                    int((CPU_SHARES_DEFAULT * sched_cpu_reserved[cg_mgr.CGROUP_XCALAR_XPUS_SCHED0]) / 100)
        elif cg_mgr.CGROUP_XCALAR_XPUS_SCHED1 in cg_name:
            cg_params["cpu.shares"] = \
                    int((CPU_SHARES_DEFAULT * sched_cpu_reserved[cg_mgr.CGROUP_XCALAR_XPUS_SCHED1]) / 100)
        elif cg_mgr.CGROUP_XCALAR_XPUS_SCHED2 in cg_name:
            cg_params["cpu.shares"] = \
                    int((CPU_SHARES_DEFAULT * sched_cpu_reserved[cg_mgr.CGROUP_XCALAR_XPUS_SCHED2]) / 100)
        else:
            raise ValueError("Unknown cgroup name {}".format(cg_name))

        cg_mgr.update_cgroup_params(cg_name, cg_mgr.CGROUP_CTRL_CPU, cg_params)


# Called during cluster boot to init cgroup params from Kvstore or to
# compute defaults and then store in Kvstore and then init cgroup params.
def init_cgroups():
    cg_mgr = CgroupMgr()

    if cg_mgr.node_xpu_id != CGROUPS_NODE_XPU_OWNER_ID:
        logger.info(
            "init_cgroups: node_xpu_id {} is not owner node_xpu_id {}".format(
                cg_mgr.node_xpu_id, CGROUPS_NODE_XPU_OWNER_ID))
        return

    if not xce_config.check_node_host_unique(
            cg_mgr.this_node_id
    ) and cg_mgr.this_node_id != CGROUPS_TESTENV_OWNER_NODE_ID:
        logger.info(
            "init_cgroups: node host is not unique and node Id {} is not test owner node Id {}"
            .format(cg_mgr.this_node_id, CGROUPS_TESTENV_OWNER_NODE_ID))
        return

    # Update default SYS XPU Cgroup params
    for cg_name in cg_mgr.CGROUP_XCALAR_SYS_XPUS_NAME_LIST:
        cg_mgr.update_default_cgroup_params(cg_name, cg_mgr.CGROUP_CTRL_MEMORY)

    # Update default USR XPU Cgroup params
    for cg_name in cg_mgr.CGROUP_XCALAR_USR_XPUS_NAME_LIST:
        cg_mgr.update_default_cgroup_params(cg_name, cg_mgr.CGROUP_CTRL_MEMORY)

    refresh_mixed_mode_cgroups_internal(cg_mgr)


# Set the cgroup params for the respective cgroup
def set_cgroup(cgroup_name, cgroup_controller, cgroup_params_str):
    cg_mgr = CgroupMgr()

    if cg_mgr.node_xpu_id != CGROUPS_NODE_XPU_OWNER_ID:
        logger.info(
            "set_cgroups: node_xpu_id {} is not owner node_xpu_id {}".format(
                cg_mgr.node_xpu_id, CGROUPS_NODE_XPU_OWNER_ID))
        return

    if not xce_config.check_node_host_unique(
            cg_mgr.this_node_id
    ) and cg_mgr.this_node_id != CGROUPS_TESTENV_OWNER_NODE_ID:
        logger.info(
            "set_cgroups: node host is not unique and node Id {} is not test owner node Id {}"
            .format(cg_mgr.this_node_id, CGROUPS_TESTENV_OWNER_NODE_ID))
        return

    if not cg_mgr.cgroup_name_valid(cgroup_name):
        raise ValueError("Invalid cgroup name: {}".format(cgroup_name))

    if not cg_mgr.cgroup_controller_valid(cgroup_controller):
        raise ValueError(
            "Invalid cgroup controller: {}".format(cgroup_controller))

    cgroup_params = json.loads(cgroup_params_str)
    cg_mgr.update_cgroup_params(cgroup_name, cgroup_controller, cgroup_params)


# List the active controllers
def get_controllers():
    cg_mgr = CgroupMgr()
    return cg_mgr.CGROUPS_CTRL_LIST


def get_all_cgroups_for_controller(cgroup_controller):
    cg_mgr = CgroupMgr()
    if not cg_mgr.cgroup_controller_valid(cgroup_controller):
        raise ValueError(
            "Invalid cgroup controller: {}".format(cgroup_controller))

    cgroups_per_controller = cg_mgr.get_cgroups_per_controller(
        cgroup_controller)

    return cg_mgr.get_cgroups_info(cgroups_per_controller, [cgroup_controller])


# Called during mixed mode runtime changes
def refresh_mixed_mode_cgroups():
    cg_mgr = CgroupMgr()

    if cg_mgr.node_xpu_id != CGROUPS_NODE_XPU_OWNER_ID:
        logger.info(
            "refresh_mixed_mode_cgroups: node_xpu_id {} is not owner node_xpu_id {}"
            .format(cg_mgr.node_xpu_id, CGROUPS_NODE_XPU_OWNER_ID))
        return

    if not xce_config.check_node_host_unique(
            cg_mgr.this_node_id
    ) and cg_mgr.this_node_id != CGROUPS_TESTENV_OWNER_NODE_ID:
        logger.info(
            "refresh_mixed_mode_cgroups: node host is not unique and node Id {} is not test owner node Id {}"
            .format(cg_mgr.this_node_id, CGROUPS_TESTENV_OWNER_NODE_ID))
        return

    refresh_mixed_mode_cgroups_internal(cg_mgr)

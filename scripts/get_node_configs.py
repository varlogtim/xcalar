import psutil
import multiprocessing
import json
import os
from datetime import datetime
import shutil
import time
import sys
from google.protobuf.json_format import MessageToDict

from xcalar.compute.services.Cgroup_xcrpc import Cgroup, CgRequest
from xcalar.compute.util.utils import thrift_to_json
from xcalar.compute.util.config import get_cluster_node_list
import xcalar.compute.localtypes.Version_pb2 as Version_pb2

from xcalar.external.LegacyApi.XcalarApi import XcalarApi

from xcalar.external.client import Client

node_id = sys.argv[1]
node_list = get_cluster_node_list()
port = os.getenv("XCE_HTTPS_PORT", '8443')
xcalar_api = XcalarApi("https://{}:{}".format(node_list[str(node_id)], port), bypass_proxy=True)
client = Client("https://{}:{}".format(node_list[str(node_id)], port), bypass_proxy=True)

def list_cgroup_params():
    cgserv = Cgroup(client)
    inputObj = {}
    inputObj["func"] = "listAllCgroups"
    cgreq = CgRequest()
    cgreq.jsonInput = json.dumps(inputObj)
    cgres = cgserv.process(cgreq)
    cgstats = json.loads(json.loads(cgres.jsonOutput)[0][0])
    res_dict = {}
    for group in cgstats:
        res_dict[group] = {}
        for controller in cgstats[group]:
            res_dict[group][controller] = {}
            for cg_group in cgstats[group][controller]:
                try:
                    cgroupName = cg_group["cgroup_name"]
                except:
                    cgroupName = cg_group["cgroupName"]
                res_dict[group][controller][cgroupName] = {}
                for stat, val in cg_group.items():
                    if "limit" in stat.lower() or "quota" in stat.lower() or \
                        "shares" in stat.lower():
                        res_dict[group][controller][cgroupName][stat] = val
    return res_dict


def add_cluster_configs():
    cluster_info = {}
    cluster_info["node_id"] = node_id
    cluster_version = client.get_version()
    if isinstance(cluster_version, Version_pb2.GetVersionResponse):
        cluster_version = MessageToDict(cluster_version)
    cluster_info["xcalar_version"] = cluster_version
    cluster_info["num_cluster_nodes"] = client.get_num_nodes_in_cluster()
    cluster_info["num_cores"] = multiprocessing.cpu_count()
    cluster_info["memory"] = psutil.virtual_memory().total
    cluster_info["swap"] = psutil.swap_memory().total
    configs = xcalar_api.getConfigParams()
    configs = json.loads(thrift_to_json(configs))
    configs_dict = {}
    for config in configs["parameter"]:
        configs_dict[config["paramName"]] = config["paramValue"]
        if config["paramName"] == "XdbLocalSerDesPath":
            if os.path.isdir(config["paramValue"]):
                xcalar_paging_size = shutil.disk_usage(config["paramValue"]).total
    cluster_info["xcalar_paging"] = xcalar_paging_size
    cluster_info["config_params"] = configs_dict
    try:
        cluster_info["cgroup_configs"] = list_cgroup_params()
    except Exception as e:
        sys.stderr.write("Failed to get Cgroup configs with error: {}\n".format(str(e)))
        sys.stderr.flush()
    return json.dumps(cluster_info, indent=4)

print(add_cluster_configs())

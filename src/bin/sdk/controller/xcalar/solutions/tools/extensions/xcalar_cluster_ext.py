import os
import subprocess
from pathlib import Path
from colorama import Style
import json

from xcalar.solutions.tools.connector import XShell_Connector
from google.protobuf.json_format import MessageToDict
import xcalar.compute.localtypes.Version_pb2 as Version_pb2
from xcalar.compute.util.utils import thrift_to_json

from xcalar.compute.services.Cgroup_xcrpc import Cgroup, CgRequest

cg_config_keywords = ["shares", "limit", "quota", "swappiness", "oom_control"]

def list_cluster_configs(client, xcalar_api):
    cluster_info = {}
    cluster_version = client.get_version()
    if isinstance(cluster_version, Version_pb2.GetVersionResponse):
        cluster_version = MessageToDict(cluster_version)
    cluster_info["xcalar_version"] = cluster_version
    cluster_info["num_cluster_nodes"] = client.get_num_nodes_in_cluster()
    configs = xcalar_api.getConfigParams()
    configs = json.loads(thrift_to_json(configs))
    configs_dict = {}
    for config in configs["parameter"]:
        configs_dict[config["paramName"]] = config["paramValue"]
    cluster_info["config_params"] = configs_dict
    return json.dumps(cluster_info, indent=4)

def list_cgroup_params(client):
    cgserv = Cgroup(client)
    inputObj = {}
    inputObj["func"] = "listAllCgroups"
    cgreq = CgRequest()
    cgreq.jsonInput = json.dumps(inputObj)
    cgres = json.loads(cgserv.process(cgreq).jsonOutput)
    res_dict = {}
    for cluster_node in range(len(cgres)):
        res_dict[cluster_node] = {}
        cgstats = json.loads(cgres[cluster_node][0])
        for group in cgstats:
            res_dict[cluster_node][group] = {}
            for controller in cgstats[group]:
                res_dict[cluster_node][group][controller] = {}
                for cg_group in cgstats[group][controller]:
                    try:
                        cgroupName = cg_group["cgroup_name"]
                    except:
                        cgroupName = cg_group["cgroupName"]
                    res_dict[cluster_node][group][controller][cgroupName] = {}
                    for stat, val in cg_group.items():
                        for keyword in cg_config_keywords:
                            if keyword in stat.lower():
                                res_dict[cluster_node][group][controller][cgroupName][stat] = val
    return json.dumps(res_dict, indent=4)


def dispatcher(line):

    usage = f"""
Usage:

Cluster Adminstration
    {Style.BRIGHT}xcalar_cluster
                {Style.DIM}-list_configs{Style.RESET_ALL}                                     {Style.BRIGHT}- List xcalar cluster configs.{Style.RESET_ALL}
                {Style.DIM}-list_cgroup_configs{Style.RESET_ALL}                              {Style.BRIGHT}- List cgroup configs on each cluster node

    """
    # clean up injection characters
    line = line.replace('&', ' ')
    line = line.replace(';', ' ')

    tokens = line.split()

    try:
        # ---------------------
        # show menu
        # ---------------------
        if len(tokens) == 0:
            print(usage)
            return

        # connectivity check
        if not (XShell_Connector.self_check_client()):
            return None

        client = XShell_Connector.get_client()
        xcalar_api = XShell_Connector.get_xcalarapi()
        if tokens[0] == '-list_configs':
            configs = list_cluster_configs(client, xcalar_api)
            print(configs)
        elif tokens[0] == '-list_cgroup_configs':
            cg_configs = list_cgroup_params(client)
            print(cg_configs)
        else:
            print(usage)

    except Exception as e:
        print(e)


def load_ipython_extension(ipython, *args):
    def auto_completers(self, event):
        return [
            '-list_configs', '-list_cgroup_configs'
        ]

    ipython.register_magic_function(
        dispatcher, 'line', magic_name='xcalar_cluster')
    ipython.set_hook(
        'complete_command', auto_completers, str_key='xcalar_cluster')
    ipython.set_hook(
        'complete_command', auto_completers, str_key='%xcalar_cluster')


def unload_ipython_extension(ipython):
    pass

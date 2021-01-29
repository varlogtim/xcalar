import os
import json
import imp
import sys
import shutil
from collections import namedtuple

from xcalar.container.cluster import get_running_cluster
import xcalar.container.xpu_host as xpu_host    # Injected by XPU process.

from xcalar.container.target.manage import get_target_data, build_target

xlr_dir = os.environ.get("XLRDIR", "/opt/xcalar")

# Types should be redefined for picking purposes
LoadFile = namedtuple('LoadFile', ['nodeId', 'sourceArgs', 'file'])
FileGroup = namedtuple('FileGroup', ['nodeId', 'files'])


def load_load_module():
    module = imp.new_module("load")
    module_source = None
    with open("{}/scripts/load.py".format(xlr_dir)) as fp:
        module_source = fp.read()
        exec(module_source, module.__dict__)
        return module


load_mod = load_load_module()
load_mod_dict = load_mod.__dict__
logger = load_mod_dict.get("logger")
Sampler = load_mod_dict.get("Sampler")
ctx = load_mod_dict.get("ctx")
distribute_files = load_mod_dict.get("_distribute_files")
human_readable_bytes = load_mod_dict.get("human_readable_bytes")


def shardFilesAcrossClusterNodes(source, output_path):
    proceed = True
    cluster = get_running_cluster()

    # source should be seen by all nodes
    if cluster.is_local_master():
        if not os.path.exists(source["path"]):
            send_msg_local_group(cluster, False)
            raise ValueError("Path {} cannot be seen from node {}".format(
                source["path"], cluster.my_node_id))
        send_msg_local_group(cluster, True)
    else:
        proceed = cluster.recv_msg()
    if not proceed:
        return
    cluster.barrier()

    sampler = Sampler(cluster, sys.maxsize)

    # build target
    if cluster.is_master():
        target_data = get_target_data(source["targetName"])
        cluster.broadcast_msg(target_data)
    else:
        target_data = cluster.recv_msg()
    target = build_target(
        source["targetName"],
        target_data,
        source["path"],
        num_nodes=ctx.get_node_count(),
        node_id=ctx.get_node_id(ctx.get_xpu_id()),
        user_name=ctx.get_user_id_name(),
        num_xpus_per_node=ctx.get_num_xpus_per_node())

    with target as tt:
        load_files = distribute_files([tt], [source], mode="load")
        # get rid of all the directories, we don't care about them
        load_files = [
            LoadFile(f.nodeId, f.sourceArgs, f.file) for f in load_files
            if not f.file.isDir
        ]

        if cluster.is_master():
            if len(load_files) == 0:
                raise ValueError("No files found at '{}' from '{}'".format(
                    source["path"],
                    source["targetName"],
                ))

            sampled_files, sampled_size = sampler.sample_files(load_files)
            file_groups = sampler.group_files(sampled_files)

            logger.info(
                "Load of source sampled {} files({}) of {}; scheduled into {} groups across {} nodes"
                .format(
                    len(sampled_files), human_readable_bytes(sampled_size),
                    len(load_files), len(file_groups),
                    len(set([g.nodeId for g in file_groups]))))

            # Build up a flat list of the files for each XPU, or None
            xpu_groups = []
            for node_id in range(cluster.num_nodes):
                node_groups = [
                    FileGroup(nodeId=g.nodeId, files=g.files)
                    for g in file_groups if g.nodeId == node_id
                ]
                # pad array with None, so there is exactly one element for
                # each XPU in the XPU cluster
                pad_len = cluster.xpus_per_node[node_id] - len(node_groups)
                node_groups.extend([None] * pad_len)
                xpu_groups.extend(node_groups)

            assert len(xpu_groups) == cluster.total_size
            cluster.send_msg_batch(xpu_groups)
            file_group = xpu_groups[cluster.my_xpu_id]
        else:
            file_group = cluster.recv_msg()
        logger.debug("\nFile groups {} for xpu {}\n".format(
            file_group, cluster.my_xpu_id))
        cluster.barrier()
        copy_files(cluster, file_group, output_path)


# local xpu master sends message to local xpus
def send_msg_local_group(cluster, msg):
    start_xpu_id = xpu_host.get_xpu_start_id_in_node(cluster.my_node_id)
    end_xpu_id = xpu_host.get_xpu_end_id_in_node(cluster.my_node_id)
    for dest_xpu_id in range(start_xpu_id, end_xpu_id + 1):
        if dest_xpu_id != cluster.my_xpu_id:
            assert cluster.local_master_for_xpu(
                dest_xpu_id) == cluster.local_master_for_xpu(cluster.my_xpu_id)
            cluster.send_msg(dest_xpu_id, msg)


def copy_files(cluster, file_group, output_dir):
    # make directories first
    proceed = True
    if cluster.is_local_master():
        try:
            if os.path.exists(output_dir) and os.path.isfile(output_dir):
                raise ValueError(
                    "Path {} cannot be a file path".format(output_dir))
            else:
                os.makedirs(output_dir, exist_ok=True)
            send_msg_local_group(cluster, True)
        except Exception as ex:
            send_msg_local_group(cluster, False)
            raise
    else:
        proceed = cluster.recv_msg()
    if not proceed:
        return

    for fileObj in file_group.files:
        file = fileObj.file
        assert file.isDir is False
        # dest_path = os.path.join(output_dir, os.path.basename(file.path))
        logger.info("Copying file {} to {}".format(file.path, output_dir))
        shutil.copy(file.path, output_dir)


def main(in_blob):
    this_xpu_id = ctx.get_xpu_id()
    this_node_id = ctx.get_node_id(this_xpu_id)
    try:
        in_obj = json.loads(in_blob)
        shardFilesAcrossClusterNodes(in_obj["sourceArgs"], in_obj["destPath"])
    except:
        logger.exception("Node {} caught shard exception".format(this_node_id))

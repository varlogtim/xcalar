import json

import xcalar.container.cgroups.config as cgroups_config_mgr
import xcalar.container.cgroups.base as cgroups_base_mgr
import xcalar.container.context as ctx

# Set up logging
logger = ctx.get_logger()


def main(in_blob):
    logger.info("Received input: {}".format(in_blob))
    ret = None
    try:
        in_obj = json.loads(in_blob)
        if in_obj["func"] == "initCgroups":
            # Called during cluster bootstrap
            cgroups_base_mgr.init_cgroups()
            ret = json.dumps({"status": "success"})

        elif in_obj["func"] == "refreshMixedModeCgroups":
            # Called during mixed mode runtime change
            cgroups_base_mgr.refresh_mixed_mode_cgroups()
            ret = json.dumps({"status": "success"})

        elif in_obj["func"] == "setCgroup":
            # Set cgroup params for a given cgroup
            cgroups_base_mgr.set_cgroup(
                in_obj["cgroupName"],
                in_obj["cgroupController"],
                json.dumps(in_obj["cgroupParams"]))
            ret = json.dumps({"status": "success"})

        elif in_obj["func"] == "getCgroup":
            # Get cgroup params given a cgroup
            base_mgr = cgroups_base_mgr.CgroupMgr()
            cgroup_params = base_mgr.get_cgroups_info(
                [in_obj["cgroupName"]],
                [in_obj["cgroupController"]])
            ret = json.dumps(cgroup_params)

        elif in_obj["func"] == "listCgroupsConfigs":
            # List all cgroup configs
            cgroup_params_list = list(cgroups_config_mgr.list_cgroup_params())
            ret = json.dumps(cgroup_params_list)

        elif in_obj["func"] == "listControllers":
            # List active cgroup controllers
            controllers = cgroups_base_mgr.get_controllers()
            ret = json.dumps(controllers)

        elif in_obj["func"] == "listAllCgroups":
            # List all cgroups for all controllers
            base_mgr = cgroups_base_mgr.CgroupMgr()
            cgroups = base_mgr.list_all_cgroups()
            ret = json.dumps(cgroups)

        elif in_obj["func"] == "listCgroupsForController":
            # List all cgroup data per controller
            cgroups_params_dict = cgroups_base_mgr.get_all_cgroups_for_controller(
                in_obj["cgroupController"])
            ret = json.dumps(cgroups_params_dict)

        else:
            raise ValueError("Not implemented")
    except Exception:
        logger.exception("Caught Cgroups manager exception")
        raise
    logger.info("Received input: {}, ret: {}".format(in_blob, ret))
    return ret

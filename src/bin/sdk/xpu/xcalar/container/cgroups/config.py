import re
import json
import logging

from xcalar.container.parent import Client

from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiWorkbookScopeT

from xcalar.external.exceptions import XDPException

logger = logging.getLogger("xcalar")
logger.setLevel(logging.INFO)

ALL_KEYS_REGEX_STR = r"\/sys\/cgroup\/(.*)\/(.*)\/params"
KEY_FORMAT_STR = "/sys/cgroup/{cgroup_name}/{cgroup_controller}/params"


class CgroupsStore:
    def __init__(self):
        # XXX We need to bypass the proxy here until we figure out how to
        # get the callers token at this point
        self.client = Client()
        self.kv_store = self.client.global_kvstore()
        self.all_keys_regex = re.compile(ALL_KEYS_REGEX_STR)

    def _gen_key(self, cgroup_name, cgroup_controller):
        return KEY_FORMAT_STR.format(
            cgroup_name=cgroup_name, cgroup_controller=cgroup_controller)

    def _parse_key(self, key):
        match = self.all_keys_regex.match(key)
        if len(match.groups()) != 2:
            raise ValueError(
                "Cgroup name and controller not found in {}".format(key))
        cgroup_name = match.groups()[0]
        cgroup_controller = match.groups()[1]
        return cgroup_name, cgroup_controller

    def add_cgroup_params(self, cgroup_name, cgroup_controller, cgroup_params):
        key = self._gen_key(cgroup_name, cgroup_controller)
        value = json.dumps(cgroup_params)
        self.kv_store.add_or_replace(key, value, True)

    def add_or_update_cgroup_params(self, cgroup_name, cgroup_controller,
                                    cgroup_params):
        key = self._gen_key(cgroup_name, cgroup_controller)
        value_update = json.dumps(cgroup_params)
        self.kv_store.add_or_replace(key, value_update, True)

    def update_cgroup_params(self, cgroup_name, cgroup_controller,
                             cgroup_params):
        key = self._gen_key(cgroup_name, cgroup_controller)
        value_old = self.kv_store.lookup(key)
        value_update = json.dumps(cgroup_params)
        self.kv_store.add_or_replace(key, value_update, True)

    def get_cgroup_params(self, cgroup_name, cgroup_controller):
        key = self._gen_key(cgroup_name, cgroup_controller)
        try:
            value = self.kv_store.lookup(key)
        except XDPException as e:
            if e.statusCode == StatusT.StatusKvEntryNotFound:
                raise ValueError(
                    "No such cgroup name '{}', controller '{}'".format(
                        cgroup_name, cgroup_controller)) from None
            raise
        cgroup_params = json.loads(value)
        return cgroup_params

    def dict_cgroup_keys(self):
        cgroup_dict = {}
        cgroup_params_keys = self.kv_store.list(ALL_KEYS_REGEX_STR)
        for key in cgroup_params_keys:
            cgroup_name, cgroup_controller = self._parse_key(key)
            try:
                cgroup_dict[cgroup_name].append(cgroup_controller)
            except KeyError:
                cgroup_dict[cgroup_name] = []
                cgroup_dict[cgroup_name].append(cgroup_controller)
        return cgroup_dict

    def delete_cgroup_params(self, cgroup_name, cgroup_controller):
        key = self._gen_key(cgroup_name, cgroup_controller)
        self.kv_store.delete(key)


def add_cgroup_params(cgroup_name, cgroup_controller, cgroup_params_str):
    cgroup_params = json.loads(cgroup_params_str)
    store = CgroupsStore()
    store.add_cgroup_params(cgroup_name, cgroup_controller, cgroup_params)


def update_cgroup_params(cgroup_name, cgroup_controller, config_param_str):
    cgroup_params = json.loads(config_param_str)
    store = CgroupsStore()
    store.update_cgroup_params(cgroup_name, cgroup_controller, cgroup_params)


def add_or_update_cgroup_params(cgroup_name, cgroup_controller,
                                config_param_str):
    cgroup_params = json.loads(config_param_str)
    store = CgroupsStore()
    store.add_or_update_cgroup_params(cgroup_name, cgroup_controller,
                                      cgroup_params)


def get_cgroup_params(cgroup_name, cgroup_controller):
    store = CgroupsStore()
    cgroup_params = store.get_cgroup_params(cgroup_name, cgroup_controller)
    cgroup_params["cgroupName"] = cgroup_name
    cgroup_params["cgroupController"] = cgroup_controller
    return cgroup_params


def delete_cgroup_params(cgroup_name, cgroup_controller):
    store = CgroupsStore()
    store.delete_cgroup_params(cgroup_name, cgroup_controller)


def list_cgroup_params():
    store = CgroupsStore()
    cgroup_dict = store.dict_cgroup_keys()
    for cgroup_name, controllers in cgroup_dict.items():
        for ctrl in controllers:
            cgroup_params_dict = get_cgroup_params(cgroup_name, ctrl)
            yield cgroup_params_dict

import re
import json
import os
import importlib
import logging

from xcalar.external.client import Client

from xcalar.compute.coretypes.Status.ttypes import StatusT

from xcalar.external.exceptions import XDPException

from .base import extract_target_types

ALL_KEYS_REGEX_STR = r"xce\.target\.(.*)"
KEY_FORMAT_STR = "xce.target.{instance_name}"

identifiers = [
    "shared", "s3environ", "s3fullaccount", "memory", "maprfullaccount",
    "maprimpersonation", "azblobfullaccount", "azblobenviron", "gcsenviron",
    "sharednothingsymm", "sharednothingsingle", "webhdfskerberos",
    "httpfskerberos", "webhdfsnokerberos", "httpfsnokerberos", "custom",
    "parquetds", "dsn", "kvstore", "imd_target", "urltarget", "snowflake"
]

# 'Default Data Target' default is root, but env var can specify custom path
# (needed for XPE app)
default_data_target_path = os.environ.get(
    "XCE_CUSTOM_DEFAULT_DATA_TARGET_PATH") or "/"
default_builtin_targets = [
    {
        "name": "Default Shared Root",
        "type_id": "shared",
        "params": {
            "mountpoint": default_data_target_path
        },
    },
    {
        "name": "Xcalar S3 Connector",
        "type_id": "s3environ",
        "params": {}
    },
    {
        "name": "TableGen",
        "type_id": "memory",
        "params": {}
    },
    {
        "name": "TableStore",
        "type_id": "imd_target",
        "params": {
            "mountpoint":
                os.environ.get("XCE_IMD_BACKING_STORE_PATH",
                               default_data_target_path)
        }
    }
]

logger = logging.getLogger("xcalar")
logger.setLevel(logging.INFO)


class TargetType:
    def __init__(self, identifier):
        self.identifier = identifier
        self._module = None

    def module(self):
        if not self._module:
            mod_name = "xcalar.container.target.{}".format(self.identifier)
            self._module = importlib.import_module(mod_name)
        return self._module

    def id(self):
        return self.identifier

    def name(self):
        return getattr(self.module(), "get_name")()

    def description(self):
        return getattr(self.module(), "get_description")()

    def parameters(self):
        return getattr(self.module(), "list_parameters")()

    def construct_target(self, name, param_dict, path):
        return getattr(self.module(), "get_target")(name, param_dict, path)

    def is_available(self):
        return getattr(self.module(), "is_available")()

    def make_target_context(self, param_dict):
        build_context = getattr(self.module(), "build_context", None)
        if build_context is not None:
            return build_context(param_dict)
        else:
            return None


def _gen_target_types():
    for type_id in identifiers:
        mod_name = "xcalar.container.target.{}".format(type_id)
        module = importlib.import_module(mod_name)
        yield from extract_target_types(module)


def _get_target_type(type_id):
    for target_type in _gen_target_types():
        if target_type.identifier() == type_id:
            return target_type
    raise ValueError("No such target type {}".format(type_id))


class TargetStore:
    def __init__(self):
        # XXX We need to bypass the proxy here until we figure out how to
        # get the callers token at this point
        self.client = Client(bypass_proxy=True)
        self.kv_store = self.client.global_kvstore()
        self.all_keys_regex = re.compile(ALL_KEYS_REGEX_STR)
        self._builtin_targets = None

    def _gen_key(self, instance_name):
        return KEY_FORMAT_STR.format(instance_name=instance_name)

    def _parse_key(self, key):
        match = self.all_keys_regex.match(key)
        if len(match.groups()) != 1:
            raise ValueError("Target Instance not found in {}".format(key))
        instance_name = match.groups()[0]

        return instance_name

    def _get_builtin_targets(self):
        if self._builtin_targets is None:
            self._builtin_targets = default_builtin_targets
        return self._builtin_targets

    def add_target(self, instance_name, instance_param_dict):
        if instance_name in (t["name"] for t in self._get_builtin_targets()):
            raise ValueError(
                "'{}' is already a default target".format(instance_name))
        key = self._gen_key(instance_name)
        value = json.dumps(instance_param_dict)
        self.kv_store.add_or_replace(key, value, True)

    def delete_target(self, instance_name):
        key = self._gen_key(instance_name)
        self.kv_store.delete(key)

    def list_target_keys(self):
        targets = []
        target_keys = self.kv_store.list(ALL_KEYS_REGEX_STR)

        for key in target_keys:
            instance_name = self._parse_key(key)
            targets.append(instance_name)

        # add default targets
        targets.extend([t["name"] for t in self._get_builtin_targets()])

        return targets

    def get_target_params(self, instance_name):
        # handle default targets
        def_targ = next((t for t in self._get_builtin_targets()
                         if t["name"] == instance_name), None)
        if def_targ:
            type_id = def_targ["type_id"]
            typ = _get_target_type(type_id)
            targ_params = {
                "type_id": typ.identifier(),
                "type_name": typ.type_name()
            }
            targ_params.update(def_targ["params"])
            return targ_params

        key = self._gen_key(instance_name)
        try:
            value = self.kv_store.lookup(key)
        except XDPException as e:
            if e.statusCode == StatusT.StatusKvEntryNotFound:
                raise ValueError(
                    "No such target '{}'".format(instance_name)) from None
            raise

        instance_dict = json.loads(value)
        return instance_dict


def add_target(type_id, instance_name, param_dict):
    typ = _get_target_type(type_id)
    req_params = [p.name for p in typ.parameters() if not p.optional]

    req_set = set(req_params)
    provided_set = set(param_dict.keys())
    if not req_set.issubset(provided_set):
        missing_params = req_set - provided_set
        raise ValueError("Missing parameters for Target Type '{}': {}".format(
            type_id, ", ".join(missing_params)))
    target_params = dict(param_dict)    # deep copy
    target_params["type_id"] = typ.identifier()
    target_params["type_name"] = typ.type_name()
    target_params["type_context"] = typ.augment_instance_info(param_dict)

    TargetStore().add_target(instance_name, target_params)


def list_targets():
    store = TargetStore()
    targets = store.list_target_keys()

    for target in targets:
        target_params = store.get_target_params(target)
        type_id = target_params["type_id"]
        typ = _get_target_type(type_id)
        del target_params["type_id"]
        del target_params["type_name"]
        target_dict = {
            "name": target,
            "type_id": typ.identifier(),
            "type_name": typ.type_name(),
            "params": target_params
        }
        yield target_dict


def get_target_data(target_name):
    target_data = TargetStore().get_target_params(target_name)
    return target_data


def build_target(target_name, target_data, path, **runtime_args):
    target_type = _get_target_type(target_data["type_id"])
    target = target_type.construct_target(target_name, target_data, path,
                                          **runtime_args)
    return target


def delete_target(target_name):
    TargetStore().delete_target(target_name)


def list_target_types():
    type_descs = []
    target_type_set = set()
    for targ_type in _gen_target_types():
        if not (targ_type.is_available()):
            logger.warning("Target type '{}' is not available".format(
                targ_type.type_name()))
            continue
        if targ_type in target_type_set:
            continue
        target_type_set.add(targ_type)
        desc = {}
        desc["type_id"] = targ_type.identifier()
        desc["type_name"] = targ_type.type_name()
        desc["description"] = targ_type.description()
        desc["parameters"] = [p.to_dict() for p in targ_type.parameters()]

        type_descs.append(desc)
    del target_type_set
    return type_descs

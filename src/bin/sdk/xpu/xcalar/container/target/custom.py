# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import imp

import xcalar.container.target.base as target
from .manage import get_target_data, build_target

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Udf import Udf


def build_context(params):
    (module_name, fn_name) = params["listUdf"].split(":")
    xcalar_api = XcalarApi(bypass_proxy=True)
    udf_module = Udf(xcalar_api)
    module_src = udf_module.get(module_name).source
    backing_target_data = get_target_data(params["backingTargetName"])
    return {
        "backingTargetData": backing_target_data,
        "moduleSrc": module_src,
        "fnName": fn_name
    }


@target.register("Custom Target")
@target.param("backingTargetName", "Need an underlying target to read files")
@target.param("listUdf",
              "moduleName:fnName of UDF to use to select files to read")
@target.register_augment_instance_func(build_context)
class CustomTarget(target.BaseTarget):
    """
    Custom 2nd order Data Target
    To create this data target, contact Xcalar technical support for help.
    Use this with an existing data target to overlay your own
    custom UDF to filter/select the set of files to read.
    """

    def __init__(self, name, path, backingTargetName, listUdf, type_context,
                 **kwargs):
        super(CustomTarget, self).__init__(name)
        backing_target_name = "{} on {}".format(name, backingTargetName)
        target_data = type_context["backingTargetData"]
        module_src = type_context["moduleSrc"]
        fn_name = type_context["fnName"]
        backing_target = build_target(backing_target_name, target_data, path)

        self.backing_target = backing_target
        self.dataset_root = path
        self.connector = backing_target.connector
        self.user_module = imp.new_module("userModule")
        exec(module_src, self.user_module.__dict__)
        self.module_src = module_src
        self.fn_name = fn_name

    def is_global(self):
        return True

    def get_files(self, path, name_pattern, recursive, **user_args):
        (dataset_root, node_files) = getattr(self.user_module, self.fn_name)(
            self.backing_target, path, name_pattern, recursive, **user_args)
        self.dataset_root = dataset_root
        return node_files

    def open(self, path, opts):
        return self.backing_target.open(path, opts)

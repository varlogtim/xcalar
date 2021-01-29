# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import urllib.parse
import os

import xcalar.container.target.base as target
from .manage import get_target_data, build_target


def build_context(params):
    backing_target_data = get_target_data(params["backingTargetName"])
    return {"backingTargetData": backing_target_data}


@target.register(name="Parquet Dataset")
@target.param("backingTargetName",
              "Underlying Data Target to read Parquet files")
@target.register_augment_instance_func(build_context)
class SparkParquetTarget(target.BaseTarget):
    """
    Second order data target for Parquet datasets.
    In the backingTargetName field, select an existing data target that
    specifies the location of the Parquet files.
    When creating a Parquet dataset, select the second order data target from the Data Target list.
    Parquet datasets must use a globally accessible backing target.
    """

    def __init__(self, name, path, backingTargetName, type_context, **kwargs):
        super(SparkParquetTarget, self).__init__(name)
        backing_target_name = "{} on {}".format(name, backingTargetName)
        target_data = type_context["backingTargetData"]
        backing_target = build_target(backing_target_name, target_data, path)

        self.backing_target = backing_target
        self.connector = backing_target.connector
        self.dataset_root = path

    def is_global(self):
        return self.backing_target.is_global()

    def get_files_partitioned(self, name_pattern, partition_keys,
                              partition_keys_ordered, path):
        files_to_return = []

        if len(partition_keys_ordered) == 0:
            try:
                return self.backing_target.get_files(path, name_pattern, False)
            except Exception:
                # XXX Unfortunately I have to do a generic catch all here,
                # because different connectors throw different classes of
                # exceptions when the path is not found. E.g. NativeConnector
                # throws ValueError, and HdfsConnector throws HdfsError
                return []

        key = partition_keys_ordered.pop(0)
        if "*" in partition_keys[key]:
            node_files = [
                nf for nf in self.backing_target.get_files(
                    path, "re:[^=]+=[^=]+", False) if nf.isDir
            ]
            values = [
                file_obj.relPath.split("=")[1] for file_obj in node_files
            ]
        else:
            values = partition_keys[key]

        for value in values:
            new_path = os.path.join(path, "%s=%s" % (key, value))
            files_to_return.extend(
                self.get_files_partitioned(name_pattern, partition_keys,
                                           partition_keys_ordered[:],
                                           new_path))

        return files_to_return

    def get_files(self, path, name_pattern, recursive, **user_args):
        parsed_url = urllib.parse.urlparse(path)
        path = parsed_url.path
        mode = user_args.get('mode', None)
        if mode == "listFiles":
            return self.backing_target.get_files(path, name_pattern, recursive)

        partition_keys = user_args.get('partitionKeys', {})
        partition_keys_ordered = list(partition_keys.keys())

        if len(partition_keys) == 0:
            to_remove = ["_SUCCESS", "_metadata", "_common_metadata"]
            node_files = [
                nf for nf in filter(
                    lambda x: os.path.basename(x.path) not in to_remove,
                    self.backing_target.get_files(path, name_pattern, True))
                if not nf.isDir
            ]

            if len(list(node_files)) == 0:
                path = os.path.dirname(path)
                node_files = [
                    nf for nf in filter(
                        lambda x: os.path.basename(x.path) not in to_remove,
                        self.backing_target.get_files(path, name_pattern,
                                                      True)) if not nf.isDir
                ]

            return node_files

        return self.get_files_partitioned(name_pattern, partition_keys,
                                          partition_keys_ordered[:], path)

    def open(self, path, opts):
        return self.backing_target.open(path, opts)

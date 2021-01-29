#!/usr/bin/env python3.6
# Copyright 2019-2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
# This program fetches performance statistics from a Xcalar cluster
#
from xcalar.compute.services.Stats_xcrpc import Stats
from xcalar.external.client import Client
import xcalar.compute.localtypes.Stats_pb2
from xcalar.compute.util.config import detect_config

import os


class GetTopStats():
    def __init__(self):
        self.stat_units = {}
        desc = xcalar.compute.localtypes.Stats_pb2.StatUnits.DESCRIPTOR
        for (k, v) in desc.values_by_name.items():
            self.stat_units[v.number] = k

        self._csv_content = ""

    def _append_stats_node(self, node_id, timestamp, path, stats_node,
                           is_json_format):
        if (len(path) != 0):
            path += "."
        if (len(stats_node.children) == 0):
            value = self._get_stat_value(stats_node.values[0])

            if not is_json_format:
                units = self.stat_units[stats_node.meta.units]
                if (len(stats_node.meta.maxName.value) > 0):
                    if (stats_node.meta.maxName.value == "Constant"):
                        max_name = "Constant"
                    else:
                        max_name = path + stats_node.meta.maxName.value
                else:
                    max_name = ''
                csv = str(node_id) + ',' + str(
                    timestamp
                ) + ',\"' + path + stats_node.meta.name.value + '\",' + '\"' + units + '\",' + '\"' + max_name + '\",' + str(
                    value)
                self._csv_content += csv + '\n'
            else:
                if self.prev_timestamp is None:
                    self.prev_timestamp = timestamp

                if (timestamp != self.prev_timestamp):
                    self.prev_timestamp = timestamp
                    self.final_stats.append(self.current_timestamp_stats)
                    self.current_timestamp_stats = {}

                name = path + stats_node.meta.name.value
                name = name.replace(".", "").replace("-", "_").replace(
                    '\"', '')
                if "timestamp" not in self.current_timestamp_stats:
                    self.current_timestamp_stats["timestamp"] = int(
                        timestamp) // 1000
                if "cluster_node" not in self.current_timestamp_stats:
                    self.current_timestamp_stats["cluster_node"] = node_id

                self.current_timestamp_stats[name] = value

        else:
            for i in range(0, len(stats_node.children)):
                self._append_stats_node(node_id, timestamp,
                                        path + stats_node.meta.name.value,
                                        stats_node.children[i], is_json_format)

    def _get_stat_value(self, stats_value):
        which = stats_value.WhichOneof('StatValue')
        try:
            return getattr(stats_value, which)
        except Exception:
            print("Invalid statValue", which)
            return 0

    def _get_top_stats_from_backend(self, client, get_from_all_nodes,
                                    is_json_format):
        serv = Stats(client)
        get_stats_request = xcalar.compute.localtypes.Stats_pb2.GetStatsRequest(
        )
        get_stats_request.getMeta = True
        get_stats_request.getFromAllNodes = get_from_all_nodes
        stats_response = serv.getStats(get_stats_request)

        # initialize everything
        self.final_stats = []
        self.prev_timestamp = None
        self.current_timestamp_stats = {}
        self._csv_content = "node,timestamp,name,units,maxName,value\n"

        for per_host_stats_index in range(0, len(stats_response.perHostStats)):
            per_host_stats = stats_response.perHostStats[per_host_stats_index]
            if (not per_host_stats.status):
                raise Exception("Failed to get stats from node ",
                                per_host_stats.nodeId.value, "with error",
                                per_host_stats.errMsg)
            for stat_nodes_index in range(0, len(per_host_stats.statNodes)):
                ts = int((per_host_stats.ts.seconds * 1000000) +
                         (per_host_stats.ts.nanos / 1000))
                self._append_stats_node(
                    per_host_stats.nodeId.value, ts, "",
                    per_host_stats.statNodes[stat_nodes_index], is_json_format)

        if is_json_format:
            self.final_stats.append(self.current_timestamp_stats)
            return self.final_stats
        else:
            return self._csv_content

    def _get_node_list(self):
        config = detect_config()
        config_dict = {}

        parsed_config = config.all_options
        for name in parsed_config:
            value = parsed_config[name]
            if (name.endswith('IpAddr')):
                host_split = name.split('.')
                config_dict[host_split[1]] = value
        return config_dict

    # connect to nodes via proxy
    def _connect(self, host):
        port = os.getenv("XCE_HTTPS_PORT", '8443')
        return Client("https://{}:{}".format(host, port), bypass_proxy=True)

    def get_top_stats(self, node_id, get_from_all_nodes, is_json_format=True):
        node_list = self._get_node_list()
        client = self._connect(node_list[str(node_id)])
        return self._get_top_stats_from_backend(client, get_from_all_nodes,
                                                is_json_format)

# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import time
import os
import json
from datetime import datetime, timedelta

from xcalar.external.dataflow import Dataflow
from xcalar.external.LegacyApi.Operators import Operators

job_stats_schema = {
    "job_id": "string",
    "session_name": "string",
    "job_start_timestamp_microsecs": "int",
    "job_end_timestamp_microsecs": "int",
    "job_elapsed_time_millisecs": "int",
    "job_status": "string",
    "job_start_timestamp": "timestamp",
    "job_end_timestamp": "timestamp"
}

operator_stats_schema = {
    "graph_node_name": "string",
    "graph_node_id": "string",
    "operator_name": "string",
    "operator_state": "string",
    "operator_status": "string",
    "graph_node_start_timestamp_microsecs": "int",
    "graph_node_start_timestamp": "timestamp",
    "graph_node_time_elapsed_millisecs": "int",
    "graph_node_end_timestamp_microsecs": "int",
    "graph_node_end_timestamp": "timestamp",
    "input_parameters": "string",
    "input_size": "int",
    "size_total": "int",
    "size_per_cluster_node": "string",
    "num_trans_pages_received_per_cluster_node": "string",
    "true_size_total": "int",
    "xdb_bytes_required": "int",
    "xdb_bytes_consumed": "int",
    "input_graph_nodes": "string",
    "output_graph_nodes": "string",
    "total_row_count_output_table": "int",
    "rows_per_cluster_node": "string",
    "skew": "int",
    "hash_slot_skew_per_cluster_node": "string",
    "tag": "string",
    "comment": "string",
    "job_id": "string",
    "job_total_time_elapsed_millisecs": "int"
}

cg_memory_schema = {
    "timestamp": "timestamp",
    "cluster_node": "integer",
    "memory_use_hierarchy": "integer",
    "memory_soft_limit_in_bytes": "string",
    "memory_move_charge_at_immigrate": "integer",
    "memory_kmem_tcp_max_usage_in_bytes": "integer",
    "memory_max_usage_in_bytes": "integer",
    "memory_oom_control": "String",
    "memory_limit_in_bytes": "String",
    "memory_swappiness": "String",
    "memory_kmem_failcnt": "integer",
    "memory_kmem_max_usage_in_bytes": "integer",
    "memory_failcnt": "integer",
    "memory_kmem_tcp_failcnt": "integer",
    "memory_kmem_limit_in_bytes": "integer",
    "notify_on_release": "integer",
    "memory_kmem_tcp_limit_in_bytes": "integer",
    "cgroup_clone_children": "integer",
    "cgroup_path": "string",
    "memory_kmem_tcp_usage_in_bytes": "integer",
    "memory_usage_in_bytes": "integer",
    "memory_kmem_usage_in_bytes": "integer",
    "memory_memsw_limit_in_bytes": "integer",
    "memory_memsw_max_usage_in_bytes": "integer",
    "memory_memsw_usage_in_bytes": "integer",
    "cgroup_name": "string",
    "cgroup_controller": "string"
}

cg_detailed_memory_schema = {
    "timestamp":"timestamp",
    "node_id":"integer",
    "cgroup_name":"string",
    "cgroup_controller":"string",
    "total_swap":"integer",
    "rss":"integer",
    "total_pgpgin":"integer",
    "total_unevictable":"integer",
    "swap":"integer",
    "pgmajfault":"integer",
    "total_pgmajfault":"integer",
    "active_file":"integer",
    "pgpgin":"integer",
    "total_pgfault":"integer",
    "pgfault":"integer",
    "cache":"integer",
    "total_inactive_anon":"integer",
    "mapped_file":"integer",
    "total_rss_huge":"integer",
    "total_active_file":"integer",
    "rss_huge":"integer",
    "hierarchical_memsw_limit":"integer",
    "inactive_anon":"integer",
    "total_mapped_file":"integer",
    "total_cache":"integer",
    "total_rss":"integer",
    "active_anon":"integer",
    "total_inactive_file":"integer",
    "total_swap":"integer",
    "hierarchical_memory_limit":"integer",
    "unevictable":"integer",
    "inactive_file":"integer",
    "total_pgpgout":"integer",
    "pgpgout":"integer",
    "total_active_anon":"integer"
}

cg_cpu_schema = {
    "timestamp": "timestamp",
    "cluster_node": "integer",
    "cpu_cfs_period_us": "integer",
    "cpu_shares": "integer",
    "cpuacct_usage": "integer",
    "cpu_cfs_quota_us": "integer",
    "notify_on_release": "integer",
    "cgroup_clone_children": "integer",
    "cgroup_path": "string",
    "cpuacct_usage_percpu": "string",
    "cpuacct_usage_percpu_sys": "string",
    "cpuacct_usage_sys": "string",
    "cpuacct_usage_all": "string",
    "cpuacct_usage_percpu_user": "string",
    "cpuacct_usage_user": "string",
    "cgroup_name": "string",
    "cgroup_controller": "string",
}

os_cpu_stats_schema = {
    "timestamp": "timestamp",
    "cluster_node": "integer",
    "CPU": "string",
    "usr": "float",
    "sys": "float",
    "iowait": "float",
    "steal": "float",
    "idle": "float"
}

os_io_stats_schema = {
    "timestamp": "timestamp",
    "cluster_node": "int",
    "Device": "string",
    "tps": "float",
    "kB_read_per_s": "float",
    "kB_wrtn_per_s": "float",
    "kB_read": "int",
    "kB_wrtn": "int"
}

os_misc_stats_schema = {
    "timestamp": "timestamp",
    "cluster_node": "int",
    "SystemNumCores": "int",
    "SystemCpuMaximumUsage": "int",
    "SystemCpuTotalUsage": "int",
    "SystemCpuXCEUsage": "int",
    "SystemCpuXPUUsage": "int",
    "SystemNetworkRecvBytes": "int",
    "SystemNetworkSendBytes": "int",
    "SystemMemoryUsed": "int",
    "SystemMemoryTotalAvailable": "int",
    "SystemSwapUsed": "int",
    "SystemSwapTotal": "int",
    "SystemDatasetUsed": "int",
    "SystemMemoryCgXPUUsed": "int",
    "SystemMemoryCgXCEUsed": "int",
    "SystemCgXPUPgpgin": "int",
    "SystemCgXPUPgpgout": "int",
    "SystemCgXCEPgpgin": "int",
    "SystemCgXCEPgpgout": "int",
    "XdbUsedBytes": "int",
    "XdbTotalBytes": "int"
}

xcalar_internal_stats_schema = {
    "timestamp": "timestamp",
    "cluster_node": "int",
    "MemoryPile_12_numPilePagesAlloced": "int",
    "MemoryPile_12_numPilePagesFreed": "int",
    "Scheduler-0_12_numRunnableSchedObjs": "int",
    "Scheduler-0_10_timeSuspendedSchedObjs": "int",
    "Scheduler-0_12_numSuspendedSchedObjs": "int",
    "Scheduler-1_12_numRunnableSchedObjs": "int",
    "Scheduler-1_10_timeSuspendedSchedObjs": "int",
    "Scheduler-1_12_numSuspendedSchedObjs": "int",
    "Scheduler-2_12_numRunnableSchedObjs": "int",
    "Scheduler-2_10_timeSuspendedSchedObjs": "int",
    "Scheduler-2_12_numSuspendedSchedObjs": "int",
    "Immediate_12_numRunnableSchedObjs": "int",
    "Immediate_10_timeSuspendedSchedObjs": "int",
    "Immediate_12_numSuspendedSchedObjs": "int",
    "merge_10_init": "int",
    "merge_10_initFailure": "int",
    "merge_10_initLocal": "int",
    "merge_10_initLocalFailure": "int",
    "merge_10_prepare": "int",
    "merge_10_prepareFailure": "int",
    "merge_10_prepareLocal": "int",
    "merge_10_prepareLocalFailure": "int",
    "merge_10_commit": "int",
    "merge_10_commitFailure": "int",
    "merge_10_postCommit": "int",
    "merge_10_postCommitFailure": "int",
    "merge_10_postCommitLocal": "int",
    "merge_10_postCommitLocalFailure": "int",
    "merge_10_abort": "int",
    "merge_10_abortFailure": "int",
    "merge_10_abortLocal": "int",
    "merge_10_abortLocalFailure": "int",
    "XdbMgr_12_statNumSerializationFailures": "int",
    "XdbMgr_12_statNumDeserializationFailures": "int",
    "XdbMgr_12_statNumSerDesDropFailures": "int",
    "XdbMgr_12_numSerializedPages": "int",
    "XdbMgr_13_numSerializedPagesHWM": "int",
    "XdbMgr_12_numDeserializedPages": "int",
    "XdbMgr_12_numSerDesDroppedPages": "int",
    "XdbMgr_12_numSerDesDroppedBytes": "int",
    "XdbMgr_12_numSerializedBatches": "int",
    "XdbMgr_12_numDeserializedBatches": "int",
    "XdbMgr_12_numSerializedBytes": "int",
    "XdbMgr_13_numSerializedBytesHWM": "int",
    "XdbMgr_12_numDeserializedBytes": "int",
    "XdbMgr_12_numSerDesDroppedBatches": "int",
    "XdbMgr_12_numKvBufDropFailures": "int",
    "XdbMgr_12_numSerializationLockRetries": "int",
    "XdbMgr_12_numXdbPageableSlabOom": "int",
    "XdbMgr_12_numOsPageableSlabOom": "int",
    "Dag_12_numDagNodesAlloced": "int",
    "Dag_12_numDagNodesFreed": "int",
    "xcrpcApis_12_Operator_OpMerge": "int",
    "xdb_pagekvbuf_bc_12_elemSizeBytes": "int",
    "xdb_pagekvbuf_bc_12_fastAllocs": "int",
    "xdb_pagekvbuf_bc_12_fastFrees": "int",
    "xdb_pagekvbuf_bc_13_fastHWM": "int",
    "xdb_pagekvbuf_bc_12_slowAllocs": "int",
    "xdb_pagekvbuf_bc_12_slowFrees": "int",
    "xdb_pagekvbuf_bc_13_slowHWM": "int",
    "xdb_pagekvbuf_bc_12_totMemBytes": "int",
    "xdb_pagekvbuf_bc_12_mlockChunk": "int",
    "xdb_pagekvbuf_bc_12_mlockChunkFailure": "int"
}

table_col_types_map = {
    "STRING": "DfString",
    "INTEGER": "DfInt64",
    "INT": "DfInt64",
    "BOOLEAN": "DfBoolean",
    "FLOAT": "DfFloat64",
    "TIMESTAMP": "DfTimespec",
    "MONEY": "DfMoney"
}


def table_load_dataflow(schema,
                        target,
                        paths,
                        parser_name,
                        parser_args,
                        evals=[]):
    schema_json_array = []
    export_schema_array = []
    for col_name, col_type in schema.items():
        col_info = {
            'sourceColumn': col_name,
            'destColumn': col_name.upper(),
            'columnType': table_col_types_map[col_type.upper()]
        }
        schema_json_array.append(col_info)
        export_schema_array.append({
            'columnName': col_name.upper(),
            'headerAlias': col_name.upper()
        })

    export_schema_array.append({
        'columnName': "XcalarRankOver",
        'headerAlias': "XcalarRankOver"
    })
    export_schema_array.append({
        'columnName': "XcalarOpCode",
        'headerAlias': "XcalarOpCode"
    })

    source_args_list = []
    for path in paths:
        source_args = {
            'targetName': target,
            'path': path,
            'fileNamePattern': '',
            'recursive': True
        }
        source_args_list.append(source_args)

    load_query = {
        'operation': 'XcalarApiBulkLoad',
        'comment': '',
        'tag': '',
        'args': {
            'dest': '.XcalarDS.Optimized.52065.admin.52563.genData',
            'loadArgs': {
                'sourceArgsList': source_args_list,
                'parseArgs': {
                    'parserFnName': parser_name,
                    'parserArgJson': json.dumps(parser_args),
                    'fileNameFieldName': '',
                    'recordNumFieldName': '',
                    'allowFileErrors': True,
                    'allowRecordErrors': True,
                    'schema': schema_json_array
                },
                'size': 10737418240
            }
        },
        'annotations': {}
    }

    index_query = {
        'operation': 'XcalarApiSynthesize',
        'args': {
            'source': '.XcalarDS.Optimized.52065.admin.52563.genData',
            'dest': 'genData_1',
            'columns': schema_json_array,
            'sameSession': True,
            'numColumns': 1
        },
        'tag': ''
    }

    row_num_query = {
        "operation": "XcalarApiGetRowNum",
        "args": {
            "source": "genData_1",
            "dest": "genData_2",
            "newField": "XcalarRankOver"
        }
    }
    map_query = {
        "operation": "XcalarApiMap",
        "args": {
            "source":
                "genData_2",
            "dest":
                "genData_3",
            "eval": [{
                "evalString": "int(1)",
                "newField": "XcalarOpCode"
            }] + evals,
            "icv":
                False
        }
    }

    dht_ordering_query = {
        "operation": "XcalarApiIndex",
        "args": {
            "source":
                "genData_3",
            "dest":
                "genData_4",
            "key": [{
                "name": "XcalarRankOver",
                "type": "DfUnknown",
                "keyFieldName": "XcalarRankOver",
                "ordering": "Unordered"
            }],
            "prefix":
                "",
            "dhtName":
                "",
            "delaySort":
                False,
            "broadcast":
                False
        },
    }

    dataflow_info = {}
    dataflow_info["query"] = json.dumps([
        load_query, index_query, row_num_query, map_query, dht_ordering_query
    ])
    dataflow_info["tables"] = [{
        'name': 'genData_4',
        'columns': export_schema_array
    }]
    optimized_query = {'retina': json.dumps(dataflow_info)}
    return dataflow_info["query"], json.dumps(optimized_query)


class UploadHelper:
    def __init__(self, client, xcalar_api, session, stats_root):
        self.client = client
        self.xcalar_api = xcalar_api
        self.session = session
        self.stats_root = stats_root

    def get_additional_dates(self, is_sys, date, num_days_prior):
        all_dates = [date]
        dt_object_start = datetime.strptime(date, "%Y-%m-%d")
        delta = timedelta(days=1)
        for _ in range(num_days_prior):
            dt_object_start -= delta
            date_string = dt_object_start.strftime("%Y-%m-%d")
            if is_sys:
                year, month, day = date_string.split("-")
                sys_month = str(int(month))
                sys_day = str(int(day))
                date_string = "{}-{}-{}".format(year, sys_month, sys_day)
            all_dates.append(date_string)
        return all_dates

    def get_all_paths(self, dates, stats_dir):
        paths = []
        if dates == []:
            paths = [os.path.join(self.stats_root, stats_dir)]
        else:
            for date in dates:
                path = os.path.join(self.stats_root, stats_dir, date)
                paths.append(path)
        return paths

    def load_table(self,
                   table_name,
                   schema,
                   parser_name,
                   parser_args,
                   dates,
                   stats_dir,
                   postfix,
                   evals=[]):
        print("Loading {}...".format(table_name))
        start = int(time.time())
        op = Operators(self.xcalar_api)
        published_table_name = "{}{}".format(table_name, postfix).upper()
        tmp_table_name = "tmp_" + published_table_name.lower()

        paths = self.get_all_paths(dates, stats_dir)

        qs, retina_str = table_load_dataflow(schema, "Default Shared Root",
                                             paths, parser_name, parser_args,
                                             evals)
        num_retries = 2
        for _ in range(num_retries):
            df = Dataflow.create_dataflow_from_query_string(
                self.client, qs, optimized_query_string=retina_str)
            try:
                df._dataflow_name = table_name + "_load"
                self.session.execute_dataflow(
                    df,
                    table_name=tmp_table_name,
                    optimized=True,
                    is_async=False)
                break
            except Exception as e:
                if "DAG name already exists" in str(
                        e) or "Table already exists" in str(e):
                    op.dropTable(tmp_table_name)
                    print(
                        "Temporary table {} already exists. Dropped table and trying again."
                        .format(tmp_table_name))
                else:
                    raise

        try:
            op.unpublish(published_table_name)
            print('Deleted already existing published table: {}'.format(
                published_table_name))
        except Exception as e:
            if 'Publish table name not found' in str(e):
                pass

        op.publish(tmp_table_name, published_table_name)
        # Clean up
        op.dropTable(tmp_table_name)
        time_elapsed = int(time.time()) - start
        print(
            "Sucessfully loaded new published table {} in {} seconds.".format(
                published_table_name, time_elapsed))

    def load_stats_tables(self, job_dates, sys_dates, postfix, load_cg_stats,
                          load_iostats, detailed_mem_cg_stats):
        job_stats_dir = "jobStats"
        sys_stats_dir = "systemStats"
        parser_name = "/sharedUDFs/default:getJobStats"
        evals = [{
            "evalString":
                "timestamp(div(JOB_START_TIMESTAMP_MICROSECS,1000))",
            "newField":
                "JOB_START_TIMESTAMP"
        },
                 {
                     "evalString":
                         "timestamp(div(JOB_END_TIMESTAMP_MICROSECS,1000))",
                     "newField":
                         "JOB_END_TIMESTAMP"
                 }]
        try:
            self.load_table("job_stats", job_stats_schema, parser_name, {},
                            job_dates, job_stats_dir, postfix, evals)
        except Exception as e:
            print("Failed to load job_stats table with error: {}. Continuing.".
                  format(str(e)))

        parser_name = "/sharedUDFs/default:getOperatorStats"
        evals = [{
            "evalString":
                "timestamp(div(GRAPH_NODE_START_TIMESTAMP_MICROSECS,1000))",
            "newField":
                "GRAPH_NODE_START_TIMESTAMP"
        },
                 {
                     "evalString":
                         "timestamp(div(GRAPH_NODE_END_TIMESTAMP_MICROSECS,1000))",
                     "newField":
                         "GRAPH_NODE_END_TIMESTAMP"
                 }]

        try:
            self.load_table("operator_stats", operator_stats_schema,
                            parser_name, {}, job_dates, job_stats_dir, postfix,
                            evals)
        except Exception as e:
            print(
                "Failed to load operator_stats table with error: {}. Continuing."
                .format(str(e)))

        stat_types = ["os_cpu_stats", "os_misc_stats", "xcalar_internal_stats"]
        if load_iostats:
            stat_types.append("os_io_stats")
        if load_cg_stats:
            stat_types.extend([
                "cg_usrnode_cpu", "cg_usrnode_mem", "cg_xpu_mem", "cg_xpu_cpu",
                "cg_mw_cpu", "cg_mw_mem"
            ])

        parser_name = "/sharedUDFs/default:extractJsonlineRecords"
        for stat_type in stat_types:
            if stat_type == "os_cpu_stats":
                jmes_path = "cpustats"
                schema = os_cpu_stats_schema
            elif stat_type == "os_misc_stats":
                jmes_path = "system_stats"
                schema = os_misc_stats_schema
            elif stat_type == "os_io_stats":
                jmes_path = "iostats"
                schema = os_io_stats_schema
            elif stat_type == "xcalar_internal_stats":
                jmes_path = stat_type
                schema = xcalar_internal_stats_schema
            elif stat_type == "cg_mw_cpu":
                jmes_path = "cgroup_stats.middleware_cgroups.cpu"
                schema = cg_cpu_schema
            elif stat_type == "cg_mw_mem":
                jmes_path = "cgroup_stats.middleware_cgroups.memory"
                schema = cg_memory_schema
            elif stat_type == "cg_usrnode_cpu":
                jmes_path = "cgroup_stats.usrnode_cgroups.cpu"
                schema = cg_cpu_schema
            elif stat_type == "cg_usrnode_mem":
                jmes_path = "cgroup_stats.usrnode_cgroups.memory"
                schema = cg_memory_schema
            elif stat_type == "cg_xpu_cpu":
                jmes_path = "cgroup_stats.xpu_cgroups.cpu"
                schema = cg_cpu_schema
            elif stat_type == "cg_xpu_mem":
                jmes_path = "cgroup_stats.xpu_cgroups.memory"
                schema = cg_memory_schema

            parser_args = {}
            parser_args["structsToExtract"] = jmes_path
            self.load_table(stat_type, schema, parser_name, parser_args,
                            sys_dates, sys_stats_dir, postfix)

        if detailed_mem_cg_stats:
            parser_name = "/sharedUDFs/default:getDetailedXpuMemoryCgStats"
            self.load_table("cg_xpu_mem_additional_stats", cg_detailed_memory_schema,
                                parser_name, {}, sys_dates, sys_stats_dir, postfix)
            parser_name = "/sharedUDFs/default:getDetailedUsrnodeMemoryCgStats"
            self.load_table("cg_usrnode_mem_additional_stats", cg_detailed_memory_schema,
                                parser_name, {}, sys_dates, sys_stats_dir, postfix)
            parser_name = "/sharedUDFs/default:getDetailedMiddlewareMemoryCgStats"
            self.load_table("cg_middleware_mem_additional_stats", cg_detailed_memory_schema,
                                parser_name, {}, sys_dates, sys_stats_dir, postfix)

    def upload_stats(self,
                     date,
                     num_days_prior,
                     postfix="",
                     load_cg_stats=False,
                     load_iostats=False,
                     detailed_mem_cg_stats=False):
        year, month, day = date.split("-")

        job_month = month.zfill(2)
        job_day = day.zfill(2)

        sys_month = str(int(month))
        sys_day = str(int(day))
        job_date = "{}-{}-{}".format(year, job_month, job_day)
        sys_date = "{}-{}-{}".format(year, sys_month, sys_day)

        job_dates = self.get_additional_dates(False, job_date, num_days_prior)
        sys_dates = self.get_additional_dates(True, sys_date, num_days_prior)
        self.load_stats_tables(job_dates, sys_dates, postfix, load_cg_stats,
                               load_iostats, detailed_mem_cg_stats)

    def upload_all_stats(self,
                         postfix="",
                         load_cg_stats=False,
                         load_iostats=False,
                         detailed_mem_cg_stats=False):
        self.load_stats_tables([], [], postfix, load_cg_stats, load_iostats, detailed_mem_cg_stats)

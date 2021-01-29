# Copyright 2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
# This app runs for a lifetime of a cluster. It's a simple grpc server that
# translates each service request to the associated xcrpc request.  There must
# be code for each service in this file.
#

from concurrent import futures
import logging
import grpc
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2

import xcalar.external.grpc_client as grpc_client
import xcalar.container.xpu_host as xpu_host
import xcalar.container.parent as xce
from xcalar.external.exceptions import XDPException
from xcalar.compute.util.config import detect_config

from xcalar.external.services.app_grpc import AppServicer
from xcalar.external.services.cgroup_grpc import CgroupServicer
from xcalar.external.services.dag_node_grpc import DagNodeServicer
from xcalar.external.services.dataflow_grpc import DataflowServicer
from xcalar.external.services.echo_grpc import EchoServicer
from xcalar.external.services.connectors_grpc import ConnectorsServicer
from xcalar.external.services.kv_store_grpc import KvStoreServicer
from xcalar.external.services.license_grpc import LicenseServicer
from xcalar.external.services.operator_grpc import OperatorServicer
from xcalar.external.services.published_table_grpc import PublishedTableServicer
from xcalar.external.services.query_grpc import QueryServicer
from xcalar.external.services.result_set_grpc import ResultSetServicer
from xcalar.external.services.schema_load_grpc import SchemaLoadServicer
from xcalar.external.services.sql_grpc import SqlServicer
from xcalar.external.services.stats_grpc import StatsServicer
from xcalar.external.services.table_grpc import TableServicer
from xcalar.external.services.udf_grpc import UDFServicer
from xcalar.external.services.version_grpc import VersionServicer
from xcalar.external.services.workbook_grpc import WorkbookServicer

from xcalar.external.services.log_grpc import LogServicer
from xcalar.external.services.memory_grpc import MemoryServicer

logger = logging.getLogger()


def invoke(context, stub, func_name, return_type, *args):
    method_to_call = getattr(stub, func_name)
    try:
        res = method_to_call(*args)
        return res
    except XDPException as e:
        context.set_code(grpc.StatusCode.ABORTED)
        context.set_details(str(e.statusCode) + ':' + e.message)
        return return_type()


def registerServicers(client, server):
    AppServicer(client, invoke).register(server)
    CgroupServicer(client, invoke).register(server)
    DagNodeServicer(client, invoke).register(server)
    DataflowServicer(client, invoke).register(server)
    LogServicer(client, invoke).register(server)
    EchoServicer(client, invoke).register(server)
    ConnectorsServicer(client, invoke).register(server)
    KvStoreServicer(client, invoke).register(server)
    LicenseServicer(client, invoke).register(server)
    OperatorServicer(client, invoke).register(server)
    PublishedTableServicer(client, invoke).register(server)
    QueryServicer(client, invoke).register(server)
    ResultSetServicer(client, invoke).register(server)
    SchemaLoadServicer(client, invoke).register(server)
    SqlServicer(client, invoke).register(server)
    StatsServicer(client, invoke).register(server)
    TableServicer(client, invoke).register(server)
    UDFServicer(client, invoke).register(server)
    VersionServicer(client, invoke).register(server)
    WorkbookServicer(client, invoke).register(server)
    MemoryServicer(client, invoke).register(server)


def main(in_blob):
    logging.basicConfig()
    grpc_server_port = grpc_client.get_port()
    logger.info(f'Listening on port {grpc_server_port}')

    client = xce.Client()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=256))

    registerServicers(client, server)

    server.add_insecure_port('127.0.0.1:' + str(grpc_server_port))
    server.start()
    server.wait_for_termination()

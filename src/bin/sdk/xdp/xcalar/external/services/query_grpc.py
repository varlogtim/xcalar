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

import xcalar.external.grpc_client as grpc_client

import xcalar.compute.localtypes.Query_pb2 as Query_pb2
import xcalar.compute.localtypes.Query_pb2_grpc as Query_pb2_grpc
import xcalar.compute.services.Query_xcrpc as Query_xcrpc

class QueryClient():
    def __init__(self, client=None):
        pass

    def list(self, request):
        return grpc_client.invoke(Query_pb2_grpc.QueryStub, "List", request)


class QueryServicer(Query_pb2_grpc.QueryServicer):
    def __init__(self, client, invoker):
        self.stub = Query_xcrpc.Query(client)
        self.invoker = invoker

    def register(self, grpc_server):
        Query_pb2_grpc.add_QueryServicer_to_server(self, grpc_server)

    def List(self, request, context):
        return self.invoker(context, self.stub, "list", Query_pb2.ListResponse, request)

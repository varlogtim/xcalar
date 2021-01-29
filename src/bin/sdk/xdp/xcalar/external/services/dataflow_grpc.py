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

import xcalar.compute.localtypes.Dataflow_pb2 as Dataflow_pb2
import xcalar.compute.localtypes.Dataflow_pb2_grpc as Dataflow_pb2_grpc
import xcalar.compute.services.Dataflow_xcrpc as Dataflow_xcrpc


#
# Currently we only support the execute method for grpc because all of
# the other methods are handled by expServer and the xcrpc code knows
# how to do the dispatch.  This code should eventually be changed to bypass
# the xcprc stubs and go directly to expServer from here.
#
class DataflowClient(Dataflow_xcrpc.Dataflow):
    def __init__(self, client=None):
        super().__init__(client)

    def execute(self, request):
        return grpc_client.invoke(Dataflow_pb2_grpc.DataflowStub, "Execute",
                                  request)


class DataflowServicer(Dataflow_pb2_grpc.DataflowServicer):
    def __init__(self, client, invoker):
        self.stub = Dataflow_xcrpc.Dataflow(client)
        self.invoker = invoker

    def register(self, grpc_server):
        Dataflow_pb2_grpc.add_DataflowServicer_to_server(self, grpc_server)

    def Execute(self, request, context):
        return self.invoker(context, self.stub, "execute",
                            Dataflow_pb2.ExecuteResponse, request)

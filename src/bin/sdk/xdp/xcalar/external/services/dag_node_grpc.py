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

import xcalar.compute.localtypes.DagNode_pb2 as DagNode_pb2
import xcalar.compute.localtypes.DagNode_pb2_grpc as DagNode_pb2_grpc
import xcalar.compute.services.DagNode_xcrpc as DagNode_xcrpc

class DagNodeClient():
    def __init__(self, client=None):
        pass

    def pin(self, request):
        return grpc_client.invoke(DagNode_pb2_grpc.DagNodeStub, "Pin", request)

    def unpin(self, request):
        return grpc_client.invoke(DagNode_pb2_grpc.DagNodeStub, "Unpin", request)


class DagNodeServicer(DagNode_pb2_grpc.DagNodeServicer):
    def __init__(self, client, invoker):
        self.stub = DagNode_xcrpc.DagNode(client)
        self.invoker = invoker

    def register(self, grpc_server):
        DagNode_pb2_grpc.add_DagNodeServicer_to_server(self, grpc_server)

    def Pin(self, request, context):
        return self.invoker(context, self.stub, "pin", grpc_client.empty, request)

    def Unpin(self, request, context):
        return self.invoker(context, self.stub, "unpin", grpc_client.empty, request)

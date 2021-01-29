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
import xcalar.compute.localtypes.memory_pb2 as memory_pb2
import xcalar.compute.localtypes.memory_pb2_grpc as memory_pb2_grpc
import xcalar.compute.services.memory_xcrpc as memory_xcrpc

class MemoryClient:
    #
    # Don't really need the constructor but since we are used by client.py which wants
    # to pass a client object we need a constructor.  We need to match the same type
    # signature as the xcrpc version
    #
    def __init__(self, client=None):
        pass

    def getUsage(self, request):
        return grpc_client.invoke(memory_pb2_grpc.MemoryStub, "GetUsage", request)

class MemoryServicer(memory_pb2_grpc.MemoryServicer):
    def __init__(self, client, invoker):
        self.stub = memory_xcrpc.Memory(client)
        self.invoker = invoker

    def register(self, grpc_server):
        memory_pb2_grpc.add_MemoryServicer_to_server(self, grpc_server)

    def GetUsage(self, request, context):
        return self.invoker(context, self.stub, "getUsage",
                            memory_pb2.GetUsageResponse, request)

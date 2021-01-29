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

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2

import xcalar.external.grpc_client as grpc_client
import xcalar.compute.localtypes.log_pb2 as log_pb2
import xcalar.compute.localtypes.log_pb2_grpc as log_pb2_grpc
import xcalar.compute.services.log_xcrpc as log_xcrpc

class LogClient:
    #
    # Don't really need the constructor but since we are used by client.py which wants
    # to pass a client object we need a constructor.  We need to match the same type
    # signature as the xcrpc version
    #
    def __init__(self, client=None):
        pass

    def getLevel(self):
        return grpc_client.invoke(log_pb2_grpc.LogStub, "GetLevel",
                                  google_dot_protobuf_dot_empty__pb2.Empty())

    def setLevel(self, request):
        return grpc_client.invoke(log_pb2_grpc.LogStub, "SetLevel", request)

class LogServicer(log_pb2_grpc.LogServicer):
    def __init__(self, client, invoker):
        self.stub = log_xcrpc.Log(client)
        self.invoker = invoker

    def register(self, grpc_server):
        log_pb2_grpc.add_LogServicer_to_server(self, grpc_server)

    def GetLevel(self, request, context):
        return self.invoker(context, self.stub, "getLevel",
                            log_pb2.GetLevelResponse, request)

    def SetLevel(self, request, context):
        return self.invoker(context, self.stub, "setLevel",
                            google_dot_protobuf_dot_empty__pb2.Empty(), request)

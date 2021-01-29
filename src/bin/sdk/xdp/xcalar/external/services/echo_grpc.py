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

import xcalar.compute.localtypes.Echo_pb2 as Echo_pb2
import xcalar.compute.localtypes.Echo_pb2_grpc as Echo_pb2_grpc
import xcalar.compute.services.Echo_xcrpc as Echo_xcrpc


class EchoClient:
    #
    # Don't really need the constructor but since we are used by client.py which wants
    # to pass a client object we need a constructor.  We need to match the same type
    # signature as the xcrpc version
    #
    def __init__(self, client=None):
        pass

    def echoMessage(self, echoRequest):
        return grpc_client.invoke(Echo_pb2_grpc.EchoStub, "EchoMessage",
                                  echoRequest)

    def echoErrorMessage(self, echoErrorRequest):
        return grpc_client.invoke(Echo_pb2_grpc.EchoStub, "EchoErrorMessage",
                                  echoErrorRequest)


class EchoServicer(Echo_pb2_grpc.EchoServicer):
    def __init__(self, client, invoker):
        self.stub = Echo_xcrpc.Echo(client)
        self.invoker = invoker

    def register(self, grpc_server):
        Echo_pb2_grpc.add_EchoServicer_to_server(self, grpc_server)

    def EchoMessage(self, request, context):
        return self.invoker(context, self.stub, "echoMessage",
                            Echo_pb2.EchoResponse, request)

    def EchoErrorMessage(self, request, context):
        return self.invoker(context, self.stub, "echoErrorMessage",
                            grpc_client.empty, request)

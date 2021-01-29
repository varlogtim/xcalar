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

import xcalar.compute.localtypes.UDF_pb2 as UDF_pb2
import xcalar.compute.localtypes.UDF_pb2_grpc as UDF_pb2_grpc
import xcalar.compute.services.UDF_xcrpc as UDF_xcrpc

class UDFClient():
    def __init__(self, client=None):
        pass

    def getResolution(self, request):
        return grpc_client.invoke(UDF_pb2_grpc.UserDefinedFunctionStub, "GetResolution", request)


class UDFServicer(UDF_pb2_grpc.UserDefinedFunctionServicer):
    def __init__(self, client, invoker):
        self.stub = UDF_xcrpc.UserDefinedFunction(client)
        self.invoker = invoker

    def register(self, grpc_server):
        UDF_pb2_grpc.add_UserDefinedFunctionServicer_to_server(self, grpc_server)

    def GetResolution(self, request, context):
        return self.invoker(context, self.stub, "getResolution", UDF_pb2.GetResolutionResponse, request)

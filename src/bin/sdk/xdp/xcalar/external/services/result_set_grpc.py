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

import xcalar.compute.localtypes.ResultSet_pb2 as ResultSet_pb2
import xcalar.compute.localtypes.ResultSet_pb2_grpc as ResultSet_pb2_grpc
import xcalar.compute.services.ResultSet_xcrpc as ResultSet_xcrpc

class ResultSetClient():
    def __init__(self, client=None):
        pass

    def make(self, request):
        return grpc_client.invoke(ResultSet_pb2_grpc.ResultSetStub, "Make", request)

    def release(self, request):
        return grpc_client.invoke(ResultSet_pb2_grpc.ResultSetStub, "Release", request)

    def next(self, request):
        return grpc_client.invoke(ResultSet_pb2_grpc.ResultSetStub, "Next", request)

    def seek(self, request):
        return grpc_client.invoke(ResultSet_pb2_grpc.ResultSetStub, "Seek", request)


class ResultSetServicer(ResultSet_pb2_grpc.ResultSetServicer):
    def __init__(self, client, invoker):
        self.stub = ResultSet_xcrpc.ResultSet(client)
        self.invoker = invoker

    def register(self, grpc_server):
        ResultSet_pb2_grpc.add_ResultSetServicer_to_server(self, grpc_server)

    def Make(self, request, context):
        return self.invoker(context, self.stub, "make", ResultSet_pb2.ResultSetMakeResponse, request)

    def Release(self, request, context):
        return self.invoker(context, self.stub, "release", grpc_client.empty, request)

    def Next(self, request, context):
        return self.invoker(context, self.stub, "next", ResultSet_pb2.ResultSetNextResponse, request)

    def Seek(self, request, context):
        return self.invoker(context, self.stub, "seek", grpc_client.empty, request)

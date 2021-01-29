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

import xcalar.compute.localtypes.Operator_pb2 as Operator_pb2
import xcalar.compute.localtypes.Operator_pb2_grpc as Operator_pb2_grpc
import xcalar.compute.services.Operator_xcrpc as Operator_xcrpc

class OperatorClient():
    def __init__(self, client=None):
        pass

    def opMerge(self, request):
        return grpc_client.invoke(Operator_pb2_grpc.OperatorStub, "OpMerge", request)


class OperatorServicer(Operator_pb2_grpc.OperatorServicer):
    def __init__(self, client, invoker):
        self.stub = Operator_xcrpc.Operator(client)
        self.invoker = invoker

    def register(self, grpc_server):
        Operator_pb2_grpc.add_OperatorServicer_to_server(self, grpc_server)

    def OpMerge(self, request, context):
        return self.invoker(context, self.stub, "opMerge", grpc_client.empty, request)

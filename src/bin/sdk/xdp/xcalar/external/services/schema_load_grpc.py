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

import xcalar.compute.localtypes.SchemaLoad_pb2 as SchemaLoad_pb2
import xcalar.compute.localtypes.SchemaLoad_pb2_grpc as SchemaLoad_pb2_grpc
import xcalar.compute.services.SchemaLoad_xcrpc as SchemaLoad_xcrpc

class SchemaLoadClient():
    def __init__(self, client=None):
        pass

    def appRun(self, request):
        return grpc_client.invoke(SchemaLoad_pb2_grpc.SchemaLoadStub, "AppRun", request)


class SchemaLoadServicer(SchemaLoad_pb2_grpc.SchemaLoadServicer):
    def __init__(self, client, invoker):
        self.stub = SchemaLoad_xcrpc.SchemaLoad(client)
        self.invoker = invoker

    def register(self, grpc_server):
        SchemaLoad_pb2_grpc.add_SchemaLoadServicer_to_server(self, grpc_server)

    def AppRun(self, request, context):
        return self.invoker(context, self.stub, "appRun", SchemaLoad_pb2.AppResponse, request)

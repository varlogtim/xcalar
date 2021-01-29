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

import xcalar.compute.localtypes.License_pb2 as License_pb2
import xcalar.compute.localtypes.License_pb2_grpc as License_pb2_grpc
import xcalar.compute.services.License_xcrpc as License_xcrpc

class LicenseClient():
    def __init__(self, client=None):
        pass

    def create(self, request):
        return grpc_client.invoke(License_pb2_grpc.LicenseStub, "Create", request)

    def destroy(self, request):
        return grpc_client.invoke(License_pb2_grpc.LicenseStub, "Destroy", request)

    def get(self, request):
        return grpc_client.invoke(License_pb2_grpc.LicenseStub, "Get", request)

    def validate(self, request):
        return grpc_client.invoke(License_pb2_grpc.LicenseStub, "Validate", request)

    def update(self, request):
        return grpc_client.invoke(License_pb2_grpc.LicenseStub, "Update", request)

class LicenseServicer(License_pb2_grpc.LicenseServicer):
    def __init__(self, client, invoker):
        self.stub = License_xcrpc.License(client)
        self.invoker = invoker

    def register(self, grpc_server):
        License_pb2_grpc.add_LicenseServicer_to_server(self, grpc_server)

    def Create(self, request, context):
        return self.invoker(context, self.stub, "create", grpc_client.empty, request)

    def Destroy(self, request, context):
        return self.invoker(context, self.stub, "destroy", grpc_client.empty, request)

    def Get(self, request, context):
        return self.invoker(context, self.stub, "get", License_pb2.GetResponse, request)

    def Validate(self, request, context):
        return self.invoker(context, self.stub, "validate", License_pb2.ValidateResponse, request)

    def Update(self, request, context):
        return self.invoker(context, self.stub, "update", grpc_client.empty, request)


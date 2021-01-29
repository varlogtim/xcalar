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

import xcalar.compute.localtypes.Connectors_pb2 as Connectors_pb2
import xcalar.compute.localtypes.Connectors_pb2_grpc as Connectors_pb2_grpc
import xcalar.compute.services.Connectors_xcrpc as Connectors_xcrpc


class ConnectorsClient():
    def __init__(self, client=None):
        pass

    def removeFile(self, request):
        return grpc_client.invoke(Connectors_pb2_grpc.ConnectorsStub,
                                  "RemoveFile", request)

    def listFiles(self, request):
        return grpc_client.invoke(Connectors_pb2_grpc.ConnectorsStub,
                                  "ListFiles", request)


class ConnectorsServicer(Connectors_pb2_grpc.ConnectorsServicer):
    def __init__(self, client, invoker):
        self.stub = Connectors_xcrpc.Connectors(client)
        self.invoker = invoker

    def register(self, grpc_server):
        Connectors_pb2_grpc.add_ConnectorsServicer_to_server(self, grpc_server)

    def RemoveFile(self, request, context):
        return self.invoker(context, self.stub, "removeFile",
                            grpc_client.empty, request)

    def ListFiles(self, request, context):
        return self.invoker(context, self.stub, "listFiles",
                            Connectors_pb2.ListFilesResponse, request)

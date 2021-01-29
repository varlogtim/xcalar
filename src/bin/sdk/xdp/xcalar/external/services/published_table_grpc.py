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

import xcalar.compute.localtypes.PublishedTable_pb2 as PublishedTable_pb2
import xcalar.compute.localtypes.PublishedTable_pb2_grpc as PublishedTable_pb2_grpc
import xcalar.compute.services.PublishedTable_xcrpc as PublishedTable_xcrpc

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2

class PublishedTableClient():
    def __init__(self, client=None):
        pass

    def select(self, request):
        return grpc_client.invoke(PublishedTable_pb2_grpc.PublishedTableStub, "Select", request)

    def listTables(self, request):
        return grpc_client.invoke(PublishedTable_pb2_grpc.PublishedTableStub, "ListTables", request)

    def changeOwner(self, request):
        return grpc_client.invoke(PublishedTable_pb2_grpc.PublishedTableStub,
                                  "ChangeOwner", request)


class PublishedTableServicer(PublishedTable_pb2_grpc.PublishedTableServicer):
    def __init__(self, client, invoker):
        self.stub = PublishedTable_xcrpc.PublishedTable(client)
        self.invoker = invoker

    def register(self, grpc_server):
        PublishedTable_pb2_grpc.add_PublishedTableServicer_to_server(self, grpc_server)

    def Select(self, request, context):
        return self.invoker(context, self.stub, "select", PublishedTable_pb2.SelectResponse, request)

    def ListTables(self, request, context):
        return self.invoker(context, self.stub, "listTables", PublishedTable_pb2.ListTablesResponse, request)

    def ChangeOwner(self, request, context):
        return self.invoker(context, self.stub, "changeOwner",
                            google_dot_protobuf_dot_empty__pb2.Empty(), request)

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

import xcalar.compute.localtypes.Table_pb2 as Table_pb2
import xcalar.compute.localtypes.Table_pb2_grpc as Table_pb2_grpc
import xcalar.compute.services.Table_xcrpc as Table_xcrpc

class TableClient():
    def __init__(self, client=None):
        pass

    def addIndex(self, request):
        return grpc_client.invoke(Table_pb2_grpc.TableStub, "AddIndex", request)

    def removeIndex(self, request):
        return grpc_client.invoke(Table_pb2_grpc.TableStub, "RemoveIndex", request)

    def publishTable(self, request):
        return grpc_client.invoke(Table_pb2_grpc.TableStub, "PublishTable", request)

    def unpublishTable(self, request):
        return grpc_client.invoke(Table_pb2_grpc.TableStub, "UnpublishTable", request)

    def listTables(self, request):
        return grpc_client.invoke(Table_pb2_grpc.TableStub, "ListTables", request)

    def tableMeta(self, request):
        return grpc_client.invoke(Table_pb2_grpc.TableStub, "TableMeta", request)


class TableServicer(Table_pb2_grpc.TableServicer):
    def __init__(self, client, invoker):
        self.stub = Table_xcrpc.Table(client)
        self.invoker = invoker

    def register(self, grpc_server):
        Table_pb2_grpc.add_TableServicer_to_server(self, grpc_server)

    def AddIndex(self, request, context):
        return self.invoker(context, self.stub, "addIndex", grpc_client.empty, request)

    def RemoveIndex(self, request, context):
        return self.invoker(context, self.stub, "removeIndex", grpc_client.empty, request)

    def PublishTable(self, request, context):
        return self.invoker(context, self.stub, "publishTable", Table_pb2.PublishResponse, request)

    def UnpublishTable(self, request, context):
        return self.invoker(context, self.stub, "unpublishTable", grpc_client.empty, request)

    def ListTables(self, request, context):
        return self.invoker(context, self.stub, "listTables", Table_pb2.ListTablesResponse, request)

    def TableMeta(self, request, context):
        return self.invoker(context, self.stub, "tableMeta", Table_pb2.TableMetaResponse, request)

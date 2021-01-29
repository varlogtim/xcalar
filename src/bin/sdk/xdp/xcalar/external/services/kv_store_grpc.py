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

import xcalar.compute.localtypes.KvStore_pb2 as KvStore_pb2
import xcalar.compute.localtypes.KvStore_pb2_grpc as KvStore_pb2_grpc
import xcalar.compute.services.KvStore_xcrpc as KvStore_xcrpc

class KvStoreClient():
    def __init__(self, client=None):
        pass

    def lookup(self, request):
        return grpc_client.invoke(KvStore_pb2_grpc.KvStoreStub, "Lookup", request)

    def addOrReplace(self, request):
        return grpc_client.invoke(KvStore_pb2_grpc.KvStoreStub, "AddOrReplace", request)

    def multiAddOrReplace(self, request):
        return grpc_client.invoke(KvStore_pb2_grpc.KvStoreStub, "MultiAddOrReplace", request)

    def deleteKey(self, request):
        return grpc_client.invoke(KvStore_pb2_grpc.KvStoreStub, "DeleteKey", request)

    def append(self, request):
        return grpc_client.invoke(KvStore_pb2_grpc.KvStoreStub, "Append", request)

    def setIfEqual(self, request):
        return grpc_client.invoke(KvStore_pb2_grpc.KvStoreStub, "SetIfEqual", request)

    def list(self, request):
        return grpc_client.invoke(KvStore_pb2_grpc.KvStoreStub, "List", request)


class KvStoreServicer(KvStore_pb2_grpc.KvStoreServicer):
    def __init__(self, client, invoker):
        self.stub = KvStore_xcrpc.KvStore(client)
        self.invoker = invoker

    def register(self, grpc_server):
        KvStore_pb2_grpc.add_KvStoreServicer_to_server(self, grpc_server)

    def Lookup(self, request, context):
        return self.invoker(context, self.stub, "lookup", KvStore_pb2.LookupResponse, request)

    def AddOrReplace(self, request, context):
        return self.invoker(context, self.stub, "addOrReplace", grpc_client.empty, request)

    def MultiAddOrReplace(self, request, context):
        return self.invoker(context, self.stub, "multiAddOrReplace", grpc_client.empty, request)

    def DeleteKey(self, request, context):
        return self.invoker(context, self.stub, "deleteKey", grpc_client.empty, request)

    def Append(self, request, context):
        return self.invoker(context, self.stub, "append", grpc_client.empty, request)

    def SetIfEqual(self, request, context):
        return self.invoker(context, self.stub, "setIfEqual", grpc_client.empty, request)

    def List(self, request, context):
        return self.invoker(context, self.stub, "list", KvStore_pb2.ListResponse, request)

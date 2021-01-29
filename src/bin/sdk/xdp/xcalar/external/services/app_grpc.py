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

import xcalar.compute.localtypes.App_pb2 as App_pb2
import xcalar.compute.localtypes.App_pb2_grpc as App_pb2_grpc
import xcalar.compute.services.App_xcrpc as App_xcrpc

class AppClient():
    def __init__(self, client=None):
        pass

    def appStatus(self, request):
        return grpc_client.invoke(App_pb2_grpc.AppStub, "AppStatus", request)

    def driver(self, request):
        return grpc_client.invoke(App_pb2_grpc.AppStub, "Driver", request)


class AppServicer(App_pb2_grpc.AppServicer):
    def __init__(self, client, invoker):
        self.stub = App_xcrpc.App(client)
        self.invoker = invoker

    def register(self, grpc_server):
        App_pb2_grpc.add_AppServicer_to_server(self, grpc_server)

    def AppStatus(self, request, context):
        return self.invoker(context, self.stub, "appStatus", App_pb2.AppStatusResponse, request)

    def Driver(self, request, context):
        return self.invoker(context, self.stub, "driver",
                            App_pb2.DriverResponse, request)

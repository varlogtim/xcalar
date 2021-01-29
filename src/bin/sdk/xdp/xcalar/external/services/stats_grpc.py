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

import xcalar.compute.localtypes.Stats_pb2 as Stats_pb2
import xcalar.compute.localtypes.Stats_pb2_grpc as Stats_pb2_grpc
import xcalar.compute.services.Stats_xcrpc as Stats_xcrpc

class StatsClient():
    def __init__(self, client=None):
        pass

    def getStats(self, request):
        return grpc_client.invoke(Stats_pb2_grpc.StatsStub, "GetStats", request)

    def getLibstats(self, request):
        return grpc_client.invoke(Stats_pb2_grpc.StatsStub, "GetLibstats", request)

    def resetStats(self, request):
        return grpc_client.invoke(Stats_pb2_grpc.StatsStub, "ResetStats", request)


class StatsServicer(Stats_pb2_grpc.StatsServicer):
    def __init__(self, client, invoker):
        self.stub = Stats_xcrpc.Stats(client)
        self.invoker = invoker

    def register(self, grpc_server):
        Stats_pb2_grpc.add_StatsServicer_to_server(self, grpc_server)

    def GetStats(self, request, context):
        return self.invoker(context, self.stub, "getStats", Stats_pb2.GetStatsResponse, request)

    def GetLibstats(self, request, context):
        return self.invoker(context, self.stub, "getLibstats", Stats_pb2.GetLibstatsResponse, request)

    def ResetStats(self, request, context):
        return self.invoker(context, self.stub, "resetStats", Stats_pb2.ResetStatsResponse, request)

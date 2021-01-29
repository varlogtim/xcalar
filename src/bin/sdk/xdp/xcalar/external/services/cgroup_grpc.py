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

import xcalar.compute.localtypes.Cgroup_pb2 as Cgroup_pb2
import xcalar.compute.localtypes.Cgroup_pb2_grpc as Cgroup_pb2_grpc
import xcalar.compute.services.Cgroup_xcrpc as Cgroup_xcrpc

class CgroupClient():
    def __init__(self, client=None):
        pass

    def process(self, request):
        return grpc_client.invoke(Cgroup_pb2_grpc.CgroupStub, "Process", request)


class CgroupServicer(Cgroup_pb2_grpc.CgroupServicer):
    def __init__(self, client, invoker):
        self.stub = Cgroup_xcrpc.Cgroup(client)
        self.invoker = invoker

    def register(self, grpc_server):
        Cgroup_pb2_grpc.add_CgroupServicer_to_server(self, grpc_server)

    def Process(self, request, context):
        return self.invoker(context, self.stub, "process", Cgroup_pb2.CgResponse, request)

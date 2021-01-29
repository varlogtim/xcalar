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

import xcalar.compute.localtypes.Sql_pb2 as Sql_pb2
import xcalar.compute.localtypes.Sql_pb2_grpc as Sql_pb2_grpc
import xcalar.compute.services.Sql_xcrpc as Sql_xcrpc

#
# The ExecuteSQL method goes straight to expServer which xcrpc
# stubs figure out so we use them for now.
#
class SqlClient(Sql_xcrpc.Sql):
    def __init__(self, client):
        super().__init__(client)

class SqlServicer(Sql_pb2_grpc.SqlServicer):
    def __init__(self, client, invoker):
        self.stub = Sql_xcrpc.Sql(client)
        self.invoker = invoker

    def register(self, grpc_server):
        Sql_pb2_grpc.add_SqlServicer_to_server(self, grpc_server)

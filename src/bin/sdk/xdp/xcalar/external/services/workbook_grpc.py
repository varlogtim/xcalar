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

import xcalar.compute.localtypes.Workbook_pb2 as Workbook_pb2
import xcalar.compute.localtypes.Workbook_pb2_grpc as Workbook_pb2_grpc
import xcalar.compute.services.Workbook_xcrpc as Workbook_xcrpc

#
# The ConvertKvsToQuery method goes straight to expServer which xcrpc
# stubs figure out so we use them for now.
#
class WorkbookClient(Workbook_xcrpc.Workbook):
    def __init__(self, client):
        super().__init__(client)

class WorkbookServicer(Workbook_pb2_grpc.WorkbookServicer):
    def __init__(self, client, invoker):
        self.stub = Workbook_xcrpc.Workbook(client)
        self.invoker = invoker

    def register(self, grpc_server):
        Workbook_pb2_grpc.add_WorkbookServicer_to_server(self, grpc_server)

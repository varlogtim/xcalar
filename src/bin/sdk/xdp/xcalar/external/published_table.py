# Copyright 2018-2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

from xcalar.compute.localtypes.PublishedTable_pb2 import ListTablesRequest
from xcalar.compute.localtypes.PublishedTable_pb2 import ChangeOwnerRequest

class PublishedTable:
    """
    A published xcalar table which contains data as a result of running a dataflow.

    """

    @staticmethod
    def list(client, namePattern="*", updateStartBatchId=-1,
             getUpdates=True, maxUpdateCount=128,
             getSelects=True, maxSelectCount=128):
        """
        This method returns a list of published tables.

        Example:
            >>> from xcalar.external.published_table import PublishedTable
            >>> res = PublishedTable.list(client, "*")

        :param client: *Required.* An sdk client object
        :type client: Client
        :param namePattern: pattern to match for listed published tables
        :type namePattern: str
        :param updateStartBatchId: id of next batch of matching tables to return
        :type updateStartBatchId: int
        :param getUpdates: return any updates to the published table
        :type getUpdates: bool
        :param maxUpdateCount: maximum amount of updates to return
        :type maxUpdateCount: int
        :param getSelects: return any selects on the published table
        :type getSelects: bool
        :param maxSelectCount: maximum amount of selects to return
        :type maxSelectCount: int
        """
        req = ListTablesRequest()
        req.namePattern = namePattern
        req.updateStartBatchId = updateStartBatchId
        if (getUpdates):
            req.maxUpdateCount = maxUpdateCount
        else:
            req.maxUpdateCount = 0
        if (getSelects):
            req.maxSelectCount = maxSelectCount
        else:
            req.maxSelectCount = 0
        return client._published_table_service.listTables(req)

    @staticmethod
    def changeOwner(client, publishedTableName, sessionName, userIdName):
        """
        This method changes the owner of the published table as specified

        Example:
            >>> from xcalar.external.published_table import PublishedTable
            >>> PublishedTable.change_owner(client, "table1", "sess1", "admin")

        :param client: *Required.* An sdk client object
        :type client: Client
        :param: publishedTableName: name of the published table to change owner
        :type publishTableName: str
        :param sessionName: session name for the published table
        :type sessionName: str
        :param userIdName: the new owner of the published table
        :type userIdName: str
        """
        req = ChangeOwnerRequest()
        req.publishedTableName = publishedTableName
        req.scope.workbook.name.username = userIdName
        req.scope.workbook.name.workbookName = sessionName
        return client._published_table_service.changeOwner(req)

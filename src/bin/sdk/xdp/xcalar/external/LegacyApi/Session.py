# Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import os
import time
import getpass

from .WorkItem import (WorkItemSessionNew, WorkItemSessionInact,
                       WorkItemSessionDelete, WorkItemSessionActivate,
                       WorkItemSessionList, WorkItemListDagInfo,
                       WorkItemSessionPersist, WorkItemSessionRename,
                       WorkItemSessionUpload, WorkItemSessionDownload)

from xcalar.compute.coretypes.SourceTypeEnum.ttypes import SourceTypeT

# XXX: Rename Session() class to Workbook() eventually, and fix all users of
# this class accordingly.
#
# XXX: A bigger change is to improve the interface, in general, to Workbook()
# APIs, and enforce use of context manager (the "with" statement), when using
# the Workbook() class. Specifically, this would mean greatly simplifying the
# __init__ method below, and adding the __enter__ and __exit__ magic methods,
# which would do set-up/teardown so that users (e.g. pyTests) don't leak
# workbooks.
#
# Session() returns an object instance of the Session() class. It has 3 modes:
# 1. Create a new session/workbook with supplied name
#    (reuseExistingSession=False, and sessionName is supplied).
#    The new session is forked from forkedSession if fork is True
# 2. Create a new session/workbook with generated name
#    (reuseExistingSession=False, and sessionName is empty)
#    The new session is forked from forkedSession if fork is True
# 3. Activate an existing session/workbook
#    (reuseExistingSession=True, and sessionName must of course, be supplied)


class Session(object):
    def __init__(self,
                 xcalarApi,
                 owner,
                 username=getpass.getuser(),
                 userIdUnique=None,
                 reuseExistingSession=False,
                 sessionName=None,
                 fork=False,
                 forkedSession=None):
        self.xcalarApi = xcalarApi

        if not userIdUnique:
            userIdUniqueStr = username + str(os.getpid()) + str(time.time())
            userIdUnique = hash(userIdUniqueStr) & 0xFFFFFFFF

        self.username = username
        self.userIdUnique = userIdUnique
        if (sessionName):
            self.name = sessionName
        else:
            self.name = Session.genSessionName(self.username, owner,
                                               self.userIdUnique)

        if reuseExistingSession:
            try:
                eSession = self.xcalarApi.execute(
                    WorkItemSessionList(self.name, username, userIdUnique))
                if eSession:
                    workItem = WorkItemSessionActivate(self.name, username,
                                                       userIdUnique)
                    try:
                        self.xcalarApi.execute(workItem)
                    except Exception:    # XXX Needs to catch specific exception
                        # If session is already active, good for us
                        pass
                    return
            except Exception:    # XXX needs to catch specific exception
                # If no such session exists, no worries. Just go ahead
                # and create
                pass
        workItem = WorkItemSessionNew(
            self.name,
            fork,
            forkedSession,
            userName=self.username,
            userIdUnique=self.userIdUnique)
        result = self.xcalarApi.execute(workItem)
        self.sessionId = result.output.outputResult.sessionNewOutput.sessionId

    @staticmethod
    def genSessionName(username, owner, userIdUnique):
        return username + owner + str(userIdUnique)

    def activate(self):
        activateWorkItem = WorkItemSessionActivate(self.name, self.username,
                                                   self.userIdUnique)
        self.xcalarApi.execute(activateWorkItem)

    def inactivate(self):
        inactivateWorkItem = WorkItemSessionInact(
            self.name, userName=self.username, userIdUnique=self.userIdUnique)
        self.xcalarApi.execute(inactivateWorkItem)

    def delete(self):
        deleteSessionWorkItem = WorkItemSessionDelete(
            self.name, userName=self.username, userIdUnique=self.userIdUnique)
        self.xcalarApi.execute(deleteSessionWorkItem)

    def destroy(self):
        workItem = WorkItemSessionInact(
            self.name, userName=self.username, userIdUnique=self.userIdUnique)
        self.xcalarApi.execute(workItem)
        workItem = WorkItemSessionDelete(
            self.name, userName=self.username, userIdUnique=self.userIdUnique)
        self.xcalarApi.execute(workItem)

    def list(self, pattern="*"):
        workItem = WorkItemSessionList(pattern, self.username,
                                       self.userIdUnique)
        return self.xcalarApi.execute(workItem)

    def listTable(self):
        workItem = WorkItemListDagInfo("*", SourceTypeT.SrcTable)
        return self.xcalarApi.execute(workItem)

    def rename(self, newName):
        workItem = WorkItemSessionRename(
            newName,
            self.name,
            userName=self.username,
            userIdUnique=self.userIdUnique)
        self.xcalarApi.execute(workItem)
        self.name = newName

    def persist(self):
        workItem = WorkItemSessionPersist(
            self.name, userName=self.username, userIdUnique=self.userIdUnique)
        self.xcalarApi.execute(workItem)

    def upload(self, sessionName, sessionContent, pathToAdditionalFiles):
        workItem = WorkItemSessionUpload(
            sessionName,
            sessionContent,
            pathToAdditionalFiles,
            userName=self.username,
            userIdUnique=self.userIdUnique)
        return self.xcalarApi.execute(workItem)

    def download(self, sessionName, pathToAdditionalFiles):
        workItem = WorkItemSessionDownload(
            sessionName,
            pathToAdditionalFiles,
            userName=self.username,
            userIdUnique=self.userIdUnique)
        return self.xcalarApi.execute(workItem)

// Copyright 2014 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LIBAPIS_SEND_H_
#define _LIBAPIS_SEND_H_

#include "libapis/LibApisCommon.h"
#include "libapis/WorkItem.h"

extern Status xcalarApiGetExportDir(char *exportDir,
                                    size_t bufSize,
                                    const char *exportTargetName,
                                    const char *destIp,
                                    int destPort,
                                    const char *username,
                                    const unsigned int userIdUnique);
extern Status xcalarApiDeleteTable(const char *tableName,
                                   const char *destIp,
                                   int destPort,
                                   const char *username,
                                   const unsigned int userIdUnique,
                                   const char *sessionName = NULL);
extern Status xcalarApiFreeResultSet(uint64_t resultSetId,
                                     const char *destIp,
                                     int destPort,
                                     const char *username,
                                     const unsigned int userIdUnique);
extern Status xcalarApiResultSetAbsolute(uint64_t resultSetId,
                                         uint64_t position,
                                         const char *destIp,
                                         int destPort,
                                         const char *username,
                                         const unsigned int userIdUnique);
// XXX In future, might want to add a timeout rather than block forever
extern Status xcalarApiWaitForCmd(const char *tableName,
                                  const char *destIp,
                                  int destPort,
                                  const char *username,
                                  const unsigned int userIdUnique);
extern void xcalarApiFreeWorkItem(XcalarWorkItem *workItem);
extern Status xcalarApiQueueWork(XcalarWorkItem *workItem,
                                 const char *destIp,
                                 int destPort,
                                 const char *username,
                                 const unsigned int userIdUnique);

class ServiceSocket
{
  public:
    const static std::string UnixDomainSocketDir;
    const static std::string UnixDomainSocketPath;

    MustCheck Status init();
    ServiceSocket() = default;
    ~ServiceSocket();

    Status sendRequest(const ProtoRequestMsg *request,
                       ProtoResponseMsg *response);

  private:
    int fd_ = -1;
};

#endif  // _LIBAPIS_SEND_H_

// Copyright 2015, 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// monitorListener.cpp: implements the C and C++ interfaces

#include <stdio.h>
#include <string.h>
#include <cstdlib>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <getopt.h>
#include <sys/epoll.h>

#include <unordered_map>

#include "primitives/Primitives.h"
#include "monitor/MonConfig.h"
#include "monitor/MonListener.h"
#include "monitor/MonConnection.h"
#include "sys/XLog.h"

#include "monitor/MonListenerC.h"

static constexpr const char *moduleName = "monitorListener";

void
MonitorListener::Listen(int port)
{
    int connAttempts = 0;
    int connSuccess = 0;
    int waitTime = 0;
    bool firstConnect = true;
    const int SleepInterval = 5;  // retry every 5 seconds
    // Connection error msg every 5 minutes
    const int MsgInterval = SleepInterval * SecsPerMinute;
    while (true) {
        Connection *cnx = Connection::SyncConnect("localhost", port);
        connAttempts++;

        if (cnx == NULL) {
            if (firstConnect) {
                WARNING(moduleName,
                        "Failed to connect to monitor on localhost:%d",
                        port);
                firstConnect = false;
            }
            sysSleep(SleepInterval);
            waitTime = waitTime + SleepInterval;
            if (waitTime % MsgInterval == 0) {
#ifdef XCMONITOR_ENABLED
                WARNING(moduleName,
                        "Unable to connect monitor for %d minutes on "
                        "localhost:%d",
                        waitTime / 60,
                        port);
#endif
            }
            continue;
        }

        Ref<Connection> cnxRef = cnx;
        connSuccess++;
        waitTime = 0;
        firstConnect = true;

        cnx->SendMsg(NOTIFIER_ATTACH_OP, NULL, 0);

        while (true) {
            MsgOp op;
            void *msgBody;
            uint32_t msgLength;
            int status = cnx->SyncReadMsg(op, msgBody, msgLength);
            if (status != 0) {
                WARNING(moduleName, "Failed to read msg");
                cnx->Close();
                break;
            }

            switch (op) {
            case MONITOR_ENABLED_OP:
                MonitorEnabled();
                break;
            case MONITOR_DISABLED_OP:
                MonitorDisabled();
                break;
            case NEW_CLUSTER_STATUS_PREPARE_OP: {
                ConfigInfo *ciArray = (ConfigInfo *) msgBody;
                ClusterStateChangePending(ciArray,
                                          msgLength / sizeof(ConfigInfo));
                break;
            }
            case NEW_CLUSTER_STATUS_COMMIT_OP:
                ClusterStateChangeComplete();
                break;
            default:
                INFO(moduleName, "MonitorListener::listen unhandled OP");
                break;
            }
        }
    }
}

class CListener : public MonitorListener
{
  public:
    CListener(MonitorListenFuncs *mlf) : _mlf(mlf) {}
    void MonitorEnabled() { _mlf->monitorEnabled(); }
    void MonitorDisabled() { _mlf->monitorDisabled(); }

    void ClusterStateChangePending(ConfigInfo *ciArray, uint32_t ciCount)
    {
        _mlf->clusterStateChangePending(ciArray, ciCount);
    }
    void ClusterStateChangeComplete() { _mlf->clusterStateChangeComplete(); }

  private:
    MonitorListenFuncs *_mlf;
};

void
monitorListen(int port, MonitorListenFuncs *listenFuncs)
{
    CListener listener(listenFuncs);
    listener.Listen(port);
}

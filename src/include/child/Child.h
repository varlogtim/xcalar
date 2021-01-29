// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _CHILD_H_
#define _CHILD_H_

#include "primitives/Primitives.h"
#include "localmsg/LocalConnection.h"
#include "localmsg/LocalMsg.h"
#include "localmsg/ILocalMsgHandler.h"
#include "util/AtomicTypes.h"
#include "XpuRecvQ.h"

//
// Controls the lifetime and messaging of a child process. This code runs in the
// child process and communicates with the parent process.
//

class Child final : public ILocalMsgHandler
{
  public:
    enum class XpuCommTag {
        CtagBarr = 0,
        CtagUser,
        CtagInvalid,  // last so enums can be used as indices; add new ones
                      // above
    };
    static MustCheck Status init(unsigned parentId,
                                 uint64_t childId,
                                 const char *parentUds);
    void destroy();

    // Kills child by instructing main thread to initiate teardown.
    // @SymbolCheckIgnore
    void exit();

    // Waits for teardown to be initiated.
    void waitForExit();

    MustCheck Status sendSync(ProtoParentRequest *parentRequest,
                              uint64_t timeoutUSecs,
                              LocalConnection::Response **responseOut);
    MustCheck Status sendAsync(ProtoParentRequest *parentRequest,
                               LocalConnection::Response **responseOut);

    void onRequestMsg(LocalMsgRequestHandler *reqHandler,
                      LocalConnection *connection,
                      const ProtoRequestMsg *request,
                      ProtoResponseMsg *reponse) override;
    void onConnectionClose(LocalConnection *connection, Status status) override;

    void refGet() override;
    void refPut() override;

    MustCheck unsigned getParentId() const;
    MustCheck void *getBufCacheBaseAddr();
    MustCheck void *getBufFromOffset(uint64_t offset);
    MustCheck Status setupShm();

    static MustCheck Child *get();
    MustCheck uint64_t getChildId() const { return childId_; }
    MustCheck uint64_t getXpuId() const { return xpuId_; }
    void setXpuId(uint64_t xpuId);
    MustCheck uint32_t getXpuClusterSize() const { return xpuClusterSize_; }
    void setXpuClusterSize(uint32_t xpuClusterSize);
    MustCheck const char *getParentUds() const { return parentUds_; }
    MustCheck const char *getUserIdName() const { return userIdName_; }
    MustCheck bool isApp() const { return isApp_; }
    void setAppName(const char *appName)
    {
        verify(strlcpy(appName_, appName, sizeof(appName_)) < sizeof(appName_));
    }
    MustCheck const char *getAppName() const { return appName_; }

    // Fetches runtime perf information (histograms) from the parent process.
    MustCheck RuntimeHistogramsResponse getParentPerfInfo();

    void setTxn(ProtoTxn txn);
    MustCheck uint8_t *recvBufferFromLocalXpu(XpuCommTag commTag,
                                              uint64_t *payloadLength);
    static XpuCommTag parseCommTag(const char *str);
    void setXpuDistributionInCluster(const ChildAppStartRequest &startRequest);
    MustCheck unsigned getXpuStartId(NodeId xceNodeId);
    MustCheck unsigned getXpuEndId(NodeId xceNodeId);
    void setAppContext(const ChildAppStartRequest &startArgs);
    void unsetAppContext();

  private:
    static constexpr const char *ModuleName = "libchild";
    static constexpr long int ParentDeathIntervalSecs = 10;

    static Child *instance;
    unsigned parentId_;
    uint64_t childId_;
    char *parentUds_;
    LocalConnection *parentConn_;
    // Signalled when child has exited for some reason.
    sem_t semChildExit_;
    // For detecting parent death.
    pid_t parentPid_;
    Atomic32 ref_;

    // base address of xdb page bc mapped into child's address space
    void *bufCacheBase_ = NULL;

    // A XpuRecvFifoQ exists for each type of comm Tag with which payloads can
    // be tagged. The size of the recvQ array is 1 less than sizeof(XpuCommTag)
    // since the Invalid enum doesn't need a recvQ

    XpuRecvFifoQ xpuRecvQ_[sizeof(XpuCommTag) - 1];

    // For debugging
    Status exitReason_ = StatusUnknown;

    // Run time context for child (initialized at each execution of app/UDF)
    bool isApp_ = false;  // App or UDF ?
    char appName_[XcalarApiMaxAppNameLen + 1];

    // Context common to either App or UDF
    char userIdName_[LOGIN_NAME_MAX + 1];
    uint64_t sessionId_;

    // Context for App
    uint64_t xpuId_ = 0xdeadbeefULL;
    uint32_t xpuClusterSize_;
    struct XpuIdRange {
        unsigned startXpuId;
        unsigned endXpuId;
    };
    XpuIdRange *xpuIdRangeInCluster_ = NULL;

    // Context for UDF - no UDF specifics (just common userIdName)

    void setUdfContext(const ChildUdfInitRequest &udfInitReq);

    MustCheck Status initInternal();
    MustCheck Status
    recvBufferFromSrcXpu(const XpuReceiveBufferFromSrc *recvBufferFromSrc);
    MustCheck Status recvBufferProcess(XpuCommTag commTag,
                                       uint8_t *payload,
                                       uint64_t payloadLength);
    void recvBufferAddQ(XpuRecvFifoQ *recvQ, XpuRecvQElem *recvQelemP);
    MustCheck XpuRecvQElem *recvBufferDelQ(XpuRecvFifoQ *recvQ);
    void setUserContext(const char *userIdName, uint64_t sessionId);
    void childnodeSetupMemPolicy();

    // Steals reference to parentUds
    Child(unsigned parentIdParam, uint64_t childIdParam, char *parentUds);
    ~Child();

    Child(const Child &) = delete;
    Child &operator=(const Child &) = delete;
};

#endif  // _CHILD_H_

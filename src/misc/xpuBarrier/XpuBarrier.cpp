// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <stdlib.h>
#include <pthread.h>
#include <malloc.h>
#include "XpuBarrier.h"
#include "Mbox.h"

XpuBarrier::XpuBarrier(int myXPUid, int nXPUs, int nNodes):
    myXPUid_(myXPUid),
    nXPUs_(nXPUs),
    nNodes_(nNodes),
    nXPUsPerNode_(nXPUs/nNodes)
{
}

XpuBarrier::~XpuBarrier()
{
}

int
XpuBarrier::xpuBarrierLocalAggr(int XPUid)
{
    return XPUid - (XPUid % (nXPUsPerNode_));
}

int
XpuBarrier::xpuBarrierMyGlobalAggr()
{
    // Global aggregator is the local aggregator of the last XPUid.
    return xpuBarrierLocalAggr(nXPUs_ - 1);
}

bool
XpuBarrier::xpuBarrierIamSlave()
{
    return(((myXPUid_ % (nXPUsPerNode_)) != 0));
}

bool
XpuBarrier::xpuBarrierIamLocalAggr()
{
    return myXPUid_ == xpuBarrierLocalAggr(myXPUid_);
}

bool
XpuBarrier::xpuBarrierIamGlobalAggr()
{
    return myXPUid_ == xpuBarrierMyGlobalAggr();
}

int
XpuBarrier::XpuBarrierWait()
{
    int i;
    void *sendBuf;

    if (xpuBarrierIamSlave()) {
        int recvBuf[1];
        int targetXPU;

        printf("myXPUid: %d: slave\n", myXPUid_);
        sendBuf = xpuAllocFillBuf(myXPUid_, sizeof(myXPUid_));
        targetXPU = xpuBarrierLocalAggr(myXPUid_);
        printf("slave %d sending to local aggr %d\n", myXPUid_, targetXPU);
        xpuSend(targetXPU, sendBuf, sizeof(myXPUid_), 0);
        xpuRecv(recvBuf, sizeof(recvBuf), 0);
        // assert(recvBuf[0] == xpuBarrierLocalAggr(myXPUid_));
        xpuFreeBuf(sendBuf);
        // barrier done; proceed
    } else if (xpuBarrierIamLocalAggr()) {
        if (xpuBarrierIamGlobalAggr()) {
            int *recvBuf;
            int localAggr;
            int nXPUsToAggr;

            // I am local aggr, AND a global aggr
            // so need to aggr from my local XPUs, AND from the other local
            // aggr XPUs
            printf("myXPUid: %d: globalAggr\n", myXPUid_);
            nXPUsToAggr = (nXPUsPerNode_ - 1) + (nNodes_ - 1);
            recvBuf = (int *)malloc(sizeof(myXPUid_) * nXPUsToAggr);
            for (i = 0; i < nXPUsToAggr; i++) {
                xpuRecv(&recvBuf[i], sizeof(myXPUid_), 0);
            }
            // XXX: validate recvBuf[]: distinct local XPU slaves + local aggrs
            free(recvBuf);

            // barrier is done; tell local XPUs and then the local aggrs

            sendBuf = xpuAllocFillBuf(myXPUid_, sizeof(myXPUid_));
            for (i = 0; i < (nXPUsPerNode_-1); i++) {
                printf("global aggr %d sending to local slave %d\n",
                        *((int *)sendBuf), myXPUid_+i);
                xpuSend(myXPUid_+i+1, sendBuf, sizeof(myXPUid_), 0);
            }
            localAggr = 0;
            for (i = 0; i < nNodes_-1; i++) {
                printf("global aggr %d sending to local aggr %d\n",
                        *((int *)sendBuf), localAggr);
                xpuSend(localAggr, sendBuf, sizeof(myXPUid_), 0);
                localAggr += nXPUsPerNode_;
            }
            xpuFreeBuf(sendBuf);
            // barrier is done; proceed
        } else {
            int *recvBuf;

            printf("myXPUid: %d: localAggr\n", myXPUid_);
            recvBuf = (int *)malloc(sizeof(myXPUid_) * nXPUsPerNode_-1);

            // I am local aggr, but not a global aggr
            // first, aggregate from all local xpus
            for (i = 0; i < (nXPUsPerNode_-1) ; i++) {
                xpuRecv(&recvBuf[i], sizeof(myXPUid_), 0);
            }
            // XXX: validate recvBuf[]: distinct XPU slaves received
            // and then free recvBuf
            free(recvBuf);

            sendBuf = xpuAllocFillBuf(myXPUid_, sizeof(myXPUid_));

            printf("local aggr %d sending to global aggr %d\n",
                    *((int *)sendBuf), xpuBarrierMyGlobalAggr());
            // next, send to my global aggr XPU that my XPUs are ready
            xpuSend(xpuBarrierMyGlobalAggr(), sendBuf, sizeof(myXPUid_), 0);

            recvBuf = (int *)malloc(sizeof(myXPUid_) * 1);
            // wait for global aggr XPU to notify me
            xpuRecv(recvBuf, sizeof(myXPUid_), 0);
            //assert(*recvBuf == xpuBarrierMyGlobalAggr());
            free(recvBuf);
            xpuFreeBuf(sendBuf);

            // barrier is done ; tell my local XPUs

            sendBuf = xpuAllocFillBuf(myXPUid_, sizeof(myXPUid_));
            for (i = 0; i < (nXPUsPerNode_-1); i++) {
                printf("local aggr %d sending to global aggr %d\n",
                        *((int *)sendBuf), myXPUid_+i);
                xpuSend(myXPUid_+i+1, sendBuf, sizeof(myXPUid_), 0);
            }
            xpuFreeBuf(sendBuf);
            // XXX: when to free sendBuf? since we need to know when my slaves
            // have received the payload: if xpuSend() is synchronous (i.e.
            // payload is delivered or copied, on return), then sendBuf can be
            // freed right after the for loop above...for now, assume this is
            // OK.

            // Barrier reached; proceed
        }
    }
}

// Following code exercises the above barrier using threads as XPUs, and
// values for total number of XPUs, and number of nodes

#define NXPUS     2048
#define NNODES    256
__thread int myXPUid = 0;

void *
xpu(void *arg)
{
    long myId = (long) arg;
    XpuBarrier *xpuBarr;

    printf("hello from tid: %lu, myXPUid is %lu\n",
        (unsigned long)pthread_self(), myId);
    myXPUid = myId;
    xpuBarr = new XpuBarrier(myId, NXPUS, NNODES);
    xpuBarr->XpuBarrierWait();
    printf("tid: %lu, myXPUid %lu: barrier done\n",
        (unsigned long)pthread_self(), myId);
}

main()
{
    long i;
    pthread_t tids[NXPUS];

    for (i = 0; i < NXPUS; i++) {
        pthread_create(&tids[i], NULL, xpu, (void *)i);
    }

    for (i = 0; i < NXPUS; i++) {
        pthread_join(tids[i], NULL);
    }
}

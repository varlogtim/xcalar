// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef MBOX_H
#define MBOX_H

struct MboxElem {
    int       MboxMsg;
    struct    MboxElem *mbef;    /* hashtable chain */
    struct    MboxElem *mbeb;
};

struct Mbox {
    pthread_mutex_t   MboxLock;
    pthread_cond_t    MboxCv;
    MboxElem          *MboxHead;
    MboxElem          *MboxTail;
};

void xpuSend(int xpuId, void *buf, int buflen, int flags);
void xpuRecv(void *buf, int buflen, int flags);
void *xpuAllocFillBuf(int payLoad, int payloadLen);
void xpuFreeBuf(void *buf);

#endif // MBOX_H

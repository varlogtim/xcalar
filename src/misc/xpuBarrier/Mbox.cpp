// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <pthread.h>
#include <malloc.h>
#include "Mbox.h"

#define MAX_XPUS 5000
Mbox Mboxes[MAX_XPUS];

// following are dummy simulations for use by barrier testing

void *
xpuAllocFillBuf(int payLoad, int payloadLen)
{
    void *buf;

    buf = malloc(payloadLen);
    *((int *)buf) = payLoad;
    return buf;
}

void
xpuFreeBuf(void *buf)
{
    free(buf);
}

static void
MboxAdd(Mbox *mbx, MboxElem *Mboxp)
{
    MboxElem *head = mbx->MboxHead;
    MboxElem *tail = mbx->MboxTail;

    if (head == NULL) {
        mbx->MboxHead = Mboxp;
        mbx->MboxTail = Mboxp;
        /*
         * Init the hashtable q head's f/b pointers
         */
        Mboxp->mbef = Mboxp;
        Mboxp->mbeb = Mboxp;
    } else {
        /* insert at head of doubly linked list */
        Mboxp->mbef = head;
        Mboxp->mbeb = head->mbeb;
        head->mbeb->mbef = Mboxp;
        head->mbeb = Mboxp;
        mbx->MboxHead = Mboxp;
    }
}

MboxElem *
MboxDelTail(Mbox *mbx)
{
    MboxElem *retElem = NULL;
    MboxElem *tail = mbx->MboxTail;
    MboxElem *head = mbx->MboxHead;

    if (tail) {
        retElem = tail;
        if (tail == head) {
            mbx->MboxTail = mbx->MboxHead = NULL;
        } else {
            tail->mbeb->mbef = tail->mbef;
            tail->mbef->mbeb = tail->mbeb;
            mbx->MboxTail = tail->mbeb;
        }
        retElem->mbef = retElem->mbeb = NULL;
    }
    return retElem;
}

static void
MboxDel(MboxElem **Mboxl, MboxElem *Mboxp)
{
    MboxElem *head = *Mboxl;

    Mboxp->mbeb->mbef = Mboxp->mbef;
    Mboxp->mbef->mbeb = Mboxp->mbeb;
    if (Mboxp == head) {
       if (Mboxp->mbef == Mboxp) { /* last item */
           *Mboxl = NULL;
       } else {
           *Mboxl = Mboxp->mbef;
       }
    }
    Mboxp->mbef = Mboxp->mbeb = NULL;
}

void
xpuSend(int xpuId, void *buf, int buflen, int flags)
{
    Mbox *mbx;
    MboxElem  *mbep;

    mbep = (MboxElem *)calloc(1, sizeof(MboxElem));
    mbep->MboxMsg = *((int *)buf); /* XPU id for now */

    mbx = &Mboxes[xpuId];

    pthread_mutex_lock(&mbx->MboxLock);

    MboxAdd(mbx, mbep);
    pthread_cond_signal(&mbx->MboxCv);

    pthread_mutex_unlock(&mbx->MboxLock);
}

void
xpuRecv(void *buf, int buflen, int flags)
{

    // myXPUid is in TLS
    extern __thread int myXPUid;
    Mbox *mbx;
    MboxElem *msg;

    mbx = &Mboxes[myXPUid];

    pthread_mutex_lock(&mbx->MboxLock);
    while (mbx->MboxHead == NULL) {
        pthread_cond_wait(&mbx->MboxCv, &mbx->MboxLock);
    }
    msg = MboxDelTail(mbx);
    pthread_mutex_unlock(&mbx->MboxLock);

    *((int *)buf) = msg->MboxMsg;
    free(msg);
}

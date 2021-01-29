// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <dirent.h>
#include <fnmatch.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <stdio.h>
#include <linux/limits.h>
#include <unistd.h>

#include "xdb/Xdb.h"
#include "xdb/XdbInt.h"
#include "sys/XLog.h"
#include "strings/String.h"
#include "util/FileUtils.h"
#include "util/WorkQueue.h"
#include "xccompress/XcSnappy.h"
#include "runtime/Runtime.h"
#include "util/SchedulableFsm.h"
#include "msg/Xid.h"
#include "runtime/Tls.h"

static constexpr const char *moduleName = "libxdb";
const char *subdirName = "Xdb";
const char *serFilePrefix = ".XcalarXdb";
const char *serFileExt = "xlrswp";
char XdbMgr::xdbSerPath_[XcalarApiMaxPathLen];
bool compEnabled;
uint64_t XdbMgr::defaultPagingIdent_;

static bool isInit = false;

class SerDesSlotWork : public WorkQueue::Elt
{
  public:
    SerDesSlotWork() {}
    virtual ~SerDesSlotWork() {}

    virtual void func();

    Semaphore *sem_;
    Atomic32 *outstandingSlots_;

    Xdb *xdb_;
    uint64_t slotId_;
    Status *status_;

    // true for serialize, false for deserialize
    bool serialize_;

    // This is used to pass TXN to the worker thread so that it can do work
    // under the same id. This is needed as the pool of worker threads is a
    // preexisting general pool. As a result the worker threads do not inherit
    // the TXN via the normal mechanism (TXN ID is passed to the descendant
    // thread when it is created) and so we need to explicity include it as
    // something that needs to be done.
    Txn txn_;
};

Status
XdbMgr::xdbSerDesInit()
{
    Status status;
    char *path = XcalarConfig::get()->xdbLocalSerDesPath_;
    const XcalarConfig *xconfig = XcalarConfig::get();
    const XcalarConfig::SerDesMode mode =
        (XcalarConfig::SerDesMode) xconfig->xdbSerDesMode_;

    assert(!isInit);

    if (path[0] == '\0') {
        status = StatusNoSerDesPath;
        goto CommonExit;
    }

    status = strSnprintf(xdbSerPath_,
                         sizeof(xdbSerPath_),
                         "%s/%s/%u/",
                         path,
                         subdirName,
                         Config::get()->getMyNodeId());
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Invalid SerDes path name '%s/%s/%u/': %s",
                path,
                subdirName,
                Config::get()->getMyNodeId(),
                strGetFromStatus(status));
        goto CommonExit;
    }

    // We programatically append "/Xdb/<nodeId>/" to the user's provided
    // xdbLocalSerDesPath_, so we should be protected against most bad rm -rf
    // scenarios
    status = FileUtils::rmDirRecursive(xdbSerPath_);
    if (status != StatusOk && status != StatusNoEnt) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to unlink SerDes files in '%s': %s",
                xdbSerPath_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = FileUtils::recursiveMkdir(xdbSerPath_, 0700);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to created SerDes path '%s': %s",
                xdbSerPath_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName, XlogInfo, "SerDesMode=%d", mode);

    if (mode == XcalarConfig::SerDesMode::PageToLocalFile) {
        status = xdbSerDesInitSparse();
        BailIfFailed(status);
    }

    compEnabled = XcalarConfig::get()->xdbSerdesEnableCompression_;
    defaultPagingIdent_ = rand();
    isInit = true;

CommonExit:
    return status;
}

uint64_t
XdbMgr::xdbCksumVec(const struct iovec *iov, const ssize_t count) const
{
    uint32_t crc = 0;

    for (ssize_t i = 0; i < count; i++) {
        crc = hashCrc32c(crc, iov[i].iov_base, iov[i].iov_len);
    }

    return crc;
}

Status
XdbMgr::xdbReadVec(Xid &batchId, struct iovec *iov, ssize_t iovcnt)
{
    return xdbIoVec(batchId, iov, iovcnt, false);
}

Status
XdbMgr::xdbWriteVec(Xid &batchId, struct iovec *iov, ssize_t iovcnt)
{
    return xdbIoVec(batchId, iov, iovcnt, true);
}

Status
XdbMgr::xdbGetSerDesFname(Xid batchId, char *buf, size_t sz)
{
    assert(batchId != 0);
    return strSnprintf(buf,
                       sz,
                       "%s-%016lx.%s",
                       serFilePrefix,
                       batchId,
                       serFileExt);
}

class AsyncPagingFsm : public Schedulable
{
  public:
    AsyncPagingFsm(Xid &batchId, struct iovec *iov, ssize_t iovcnt, bool isSer)
        : Schedulable("SchedFsmAsyncPaging"),
          batchId_(batchId),
          iov_(iov),
          iovcnt_(iovcnt),
          isSer_(isSer)
    {
        ioWaitSema_.init(0);
    }

    void run() override
    {
        status_ = XdbMgr::get()->xdbIoVecWork(batchId_, iov_, iovcnt_, isSer_);
    }

    void done() override { ioWaitSema_.post(); }

    void wait() { ioWaitSema_.semWait(); }

    const Status &getStatus() { return status_; }

  private:
    Xid &batchId_;
    struct iovec *iov_;
    ssize_t iovcnt_;
    bool isSer_;
    Semaphore ioWaitSema_;
    Status status_;
};

Status
XdbMgr::xdbIoVec(Xid &batchId, struct iovec *iov, ssize_t iovcnt, bool isSer)
{
    AsyncPagingFsm apFsm(batchId, iov, iovcnt, isSer);
    Status status =
        Runtime::get()->schedule(&apFsm, Runtime::SchedId::AsyncPager);
    if (status == StatusOk) {
        apFsm.wait();
        status = apFsm.getStatus();
    }

    return status;
}

Status
XdbMgr::xdbIoVecWork(Xid &batchId,
                     struct iovec *iov,
                     ssize_t iovcnt,
                     bool isSer)
{
    Status status = StatusOk;
    const XcalarConfig *xconfig = XcalarConfig::get();
    const XcalarConfig::SerDesMode mode =
        (XcalarConfig::SerDesMode) xconfig->xdbSerDesMode_;
    char fname[XcalarApiMaxPathLen];
    int fd = -1;

    assert(mode != XcalarConfig::SerDesMode::Disabled);
    assert(isInit);

    status = xdbGetSerDesFname(batchId, fname, sizeof(fname));
    BailIfFailed(status);

    switch (mode) {
    case XcalarConfig::SerDesMode::PageToLocalFile: {
        return pageToOrFromLocalFile(batchId, iov, iovcnt, isSer);
    }
    case XcalarConfig::SerDesMode::Local: {
        char fqpn[XcalarApiMaxPathLen];
        status = strSnprintf(fqpn, sizeof(fqpn), "%s/%s", xdbSerPath_, fname);
        BailIfFailed(status);

        if (isSer) {
            fd = open(fqpn,
                      O_CREAT | O_EXCL | O_CLOEXEC | O_RDWR,
                      S_IRWXU | S_IRWXG | S_IRWXO);
        } else {
            fd = open(fqpn, O_CLOEXEC | O_RDONLY, S_IRWXU | S_IRWXG | S_IRWXO);
        }

        if (fd == -1) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogWarn,
                    "%s %sserialization open error %s",
                    fqpn,
                    isSer ? "" : "de",
                    strGetFromStatus(status));
            goto CommonExit;
        }
        while (iovcnt > 0) {
            const size_t ioBatchCount = xcMin((int64_t) IOV_MAX, iovcnt);
            ssize_t ioBatchResult;
            ssize_t expectedIoBatchResult = 0;
            if (isSer) {
                ioBatchResult = writev(fd, iov, ioBatchCount);
            } else {
                ioBatchResult = readv(fd, iov, ioBatchCount);
            }
            if (ioBatchResult < 0) {
                status = sysErrnoToStatus(errno);
                xSyslog(moduleName,
                        XlogErr,
                        "%s %sserialization file IO error %s",
                        fqpn,
                        isSer ? "" : "de",
                        strGetFromStatus(status));
                goto CommonExit;
            }

            for (size_t vecNum = 0; vecNum < ioBatchCount; vecNum++) {
                expectedIoBatchResult += iov[vecNum].iov_len;
            }

            if (ioBatchResult != expectedIoBatchResult) {
                xSyslog(moduleName,
                        XlogErr,
                        "%s %sserialization file IO error expected %lu bytes "
                        "IO, got %lu bytes",
                        fqpn,
                        isSer ? "" : "de",
                        expectedIoBatchResult,
                        ioBatchResult);
                status = StatusXdbDesError;
                goto CommonExit;
            }

            iov += ioBatchCount;
            iovcnt -= ioBatchCount;
        }
        if (!isSer) {
            int ret = unlink(fqpn);
            if (ret == -1) {
                xSyslog(moduleName,
                        XlogInfo,
                        "%s failed to remove stale file %s",
                        fqpn,
                        strGetFromStatus(sysErrnoToStatus(errno)));
            }
        }
    } break;
    default:
        assert(0);
        return StatusInval;
    }

CommonExit:
    if (fd != -1) {
        close(fd);
        fd = -1;
    }

    return status;
}
Status
XdbMgr::xdbSerializeVec(XdbId xdbId,
                        Xid batchId,
                        XdbPage *xdbPages[],
                        const size_t numPages)
{
    Status status;
    Xdb *xdb = NULL;
    XdbMeta *xdbMeta;

    status = xdbGet(xdbId, &xdb, &xdbMeta);
    if (status != StatusOk && status != StatusXdbNotFound) {
        assert(false);
        goto CommonExit;
    }

    status = xdbSerializeVec(xdb, batchId, xdbPages, numPages);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
XdbMgr::xdbSerializeVec(Xdb *xdb,
                        Xid batchId,
                        XdbPage *xdbPages[],
                        const size_t numPages)
{
    Status status;
    XdbBatchSerializedHdr *batchHdr = NULL;
    size_t totBytesSer = 0;

    // Reserve the first IOV for the base header
    const ssize_t iovcnt = numPages + 1;
    struct iovec *iovBase = NULL;
    struct iovec *iov = NULL;
    char *tmpCompBuf = NULL;
    char *tupBufBak = NULL;
    bool tupBufBackedUp = false;
    const int64_t serializedBytes = numSerializedBytes_->statUint64;
    const int64_t deserializedBytes = numDeserializedBytes_->statUint64;
    const int64_t droppedBytes = numSerDesDroppedBytes_->statUint64;
    const int64_t maxDiskBytes = XcalarConfig::get()->xdbSerDesMaxDiskMB_ * MB;
    // Account for racyness between stats
    const int64_t bytesOnDisk =
        MAX(serializedBytes - deserializedBytes - droppedBytes, 0);

    assert(isInit);

    if (maxDiskBytes > 0 && bytesOnDisk > maxDiskBytes) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to write %lu page(s) batchID: %lu, bytes paged out: "
                "%ld, bytes paged in: %ld, bytes dropped: %ld, bytes ondisk: "
                "%ld",
                numPages,
                batchId,
                serializedBytes,
                deserializedBytes,
                droppedBytes,
                bytesOnDisk);
        // XXX: Add new disk full status code
        status = StatusNoXdbPageBcMem;
        goto CommonExit;
    }

    iovBase =
        (struct iovec *) memAllocExt(sizeof(*iovBase) * iovcnt, moduleName);
    BailIfNull(iovBase);

    iovBase[0].iov_len =
        sizeof(*batchHdr) + sizeof(XdbItemSerializedHdr) * numPages;
    batchHdr =
        (XdbBatchSerializedHdr *) memAllocExt(iovBase[0].iov_len, moduleName);
    BailIfNull(batchHdr);
    iovBase[0].iov_base = batchHdr;

    batchHdr->magic = serializedMagic;
    batchHdr->cksum = 0;
    batchHdr->randIdent = defaultPagingIdent_;

    batchHdr->batchId = batchId;
    batchHdr->xdbId = xdb ? xdb->xdbId : 0;

    if (compEnabled) {
        const size_t maxCompLen = XcSnappy::getMaxCompressedLength(bcSize());
        // XXX: Preallocate pool of these on startup
        tmpCompBuf = (char *) memAllocExt(maxCompLen, moduleName);
        BailIfNull(tmpCompBuf);

        // XXX: Preallocate pool of these on startup.  Note we currently only
        // use a batch size (numPages) of 1 in production (> 1 used for tests).
        tupBufBak = (char *) memAllocExt(bcSize() * numPages, moduleName);
        BailIfNull(tupBufBak);
        for (size_t page = 0; page < numPages; page++) {
            XdbPage *xdbPage = xdbPages[page];
            assert(atomicRead32(&xdbPage->hdr.pageState) ==
                   XdbPage::Serializing);
            assert(xdbPage->hdr.pageSize == bcSize());
            memcpy(&tupBufBak[page], xdbPage->tupBuf, bcSize());
        }
        tupBufBackedUp = true;
    }

    iov = &iovBase[1];
    for (size_t page = 0; page < numPages; page++) {
        XdbPage *xdbPage = xdbPages[page];
        bool doComp = false;
        size_t compSize = 0;
        XdbItemSerializedHdr *itemHdr =
            (XdbItemSerializedHdr *) ((uintptr_t) &batchHdr->itemHdrs +
                                      (sizeof(XdbItemSerializedHdr) * page));

        if (atomicRead32(&xdbPage->hdr.pageState) != XdbPage::Serializing) {
            status = StatusXdbSerError;
            goto CommonExit;
        }

        itemHdr->xdbPage = xdbPage;
        // Store the offset in the header rather than calling
        // tupleBufferSerialize to avoid modifying the in-memory state in
        // case of a serialization failure.
        itemHdr->bufferStartOffset = xdbPage->tupBuf->getBufferStartOffset();
        itemHdr->bufferEndOffset = xdbPage->tupBuf->getBufferEndOffset();

        assert(xdbPage->hdr.pageSize == bcSize());

        if (compEnabled && !xdbPage->hdr.isCompressed) {
            status = XcSnappy::compress(xdbPage->tupBuf,
                                        xdbPage->hdr.pageSize,
                                        tmpCompBuf,
                                        &compSize);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "0x%016lx page %lu failed to compress (%s)",
                        batchId,
                        page,
                        strGetFromStatus(status));
                goto CommonExit;
            }

            doComp = compSize < xdbPage->hdr.pageSize;
            xSyslog(moduleName,
                    XlogVerbose,
                    "0x%016lx page %lu %scompress %lu-->%lu",
                    batchId,
                    page,
                    doComp ? "" : "NOT ",
                    bcSize(),
                    compSize);
        }

        if (doComp) {
            assert(compSize > 0);
            memcpy(xdbPage->tupBuf, tmpCompBuf, compSize);
            iov[page].iov_len = compSize;
            xdbPage->hdr.pageSize = compSize;
            xdbPage->hdr.isCompressed = true;
        } else {
            iov[page].iov_len = xdbPage->hdr.pageSize;
        }

        totBytesSer += xdbPage->hdr.pageSize;

        iov[page].iov_base = xdbPage->tupBuf;
    }

    batchHdr->cksum = xdbCksumVec(iovBase, iovcnt);

    status = xdbWriteVec(batchId, iovBase, iovcnt);
    BailIfFailed(status);

    // XDB pages all successfully written out so free the tupBufs now
    for (size_t page = 0; page < numPages; page++) {
        XdbPage *xdbPage = xdbPages[page];
        // We can't even change state to Dropped for even an instant.
        // Because this can race with bcScanCleanout
        xdbFreeTupBuf(xdbPage, false);
        xdbPage->serializedXid = batchId;
        assert(batchId != 0);
        atomicWrite32(&xdbPage->hdr.pageState, XdbPage::Serialized);
    }

    StatsLib::statAtomicAdd64(numSerializedPages_, numPages);
    StatsLib::statAtomicAdd64(numSerializedBytes_, totBytesSer);
    StatsLib::statAtomicIncr64(numSerializedBatches_);

    {
        const int64_t serializedPages = numSerializedPages_->statUint64;
        const int64_t deserializedPages = numDeserializedPages_->statUint64;
        const int64_t droppedPages = numSerDesDroppedPages_->statUint64;
        // Account for racyness between stats
        const int64_t pagesOnDisk =
            MAX(serializedPages - deserializedPages - droppedPages, 0);
        numSerializedPagesHWM_->statUint64 =
            MAX(numSerializedPagesHWM_->statUint64, (uint64_t) pagesOnDisk);
        numSerializedBytesHWM_->statUint64 =
            MAX(numSerializedBytesHWM_->statUint64, (uint64_t) bytesOnDisk);
    }
CommonExit:
    if (status != StatusOk) {
        for (size_t page = 0; page < numPages; page++) {
            XdbPage *xdbPage = xdbPages[page];
            assert(atomicRead32(&xdbPage->hdr.pageState) ==
                   XdbPage::Serializing);
            if (xdbPage->tupBuf != NULL && xdbPage->hdr.isCompressed &&
                tupBufBackedUp) {
                // deCompress can fail and we can't handle failure here, so
                // restore the raw tuple buffer from backup
                memcpy(xdbPage->tupBuf, &tupBufBak[page], bcSize());
                xdbPage->hdr.pageSize = bcSize();
                xdbPage->hdr.isCompressed = false;
            }
        }
        StatsLib::statAtomicIncr64(numSerializationFailures_);
#ifdef PAGING_ERRORS_DEBUG
        static constexpr const uint32_t BtBuffer = 4096;
        static constexpr const uint32_t TraceSize = 8;
        char btBuffer[BtBuffer];
        void *trace[TraceSize];
        int traceSize = getBacktrace(trace, TraceSize);
        printBackTrace(btBuffer, sizeof(btBuffer), traceSize, trace);
        xSyslog(moduleName,
                XlogErr,
                "Failed xdbSerializeVec: XdbId %lu batchId %lu numPages %lu: "
                "%s\n%s",
                xdb ? xdb->xdbId : 0,
                batchId,
                numPages,
                strGetFromStatus(status),
                btBuffer);
#else   // PAGING_ERRORS_DEBUG
        xSyslog(moduleName,
                XlogErr,
                "Failed xdbSerializeVec: XdbId %lu batchId %lu numPages %lu: "
                "%s",
                xdb ? xdb->xdbId : 0,
                batchId,
                numPages,
                strGetFromStatus(status));
#endif  // PAGING_ERRORS_DEBUG
    }

    if (tmpCompBuf != NULL) {
        memFree(tmpCompBuf);
        tmpCompBuf = NULL;
    }

    if (tupBufBak != NULL) {
        memFree(tupBufBak);
        tupBufBak = NULL;
    }

    if (iovBase != NULL) {
        memFree(iovBase);
        iovBase = NULL;
    }

    if (batchHdr != NULL) {
        memFree(batchHdr);
        batchHdr = NULL;
    }

    return status;
}

Status
XdbMgr::xdbDeserializeVec(XdbId xdbId,
                          Xid batchId,
                          XdbPage *xdbPages[],
                          const size_t numPages)
{
    Status status;
    Xdb *xdb;
    XdbMeta *xdbMeta;

    status = xdbGet(xdbId, &xdb, &xdbMeta);
    if (status != StatusOk && status != StatusXdbNotFound) {
        assert(false);
        goto CommonExit;
    }

    status = xdbDeserializeVec(xdb, batchId, xdbPages, numPages);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
XdbMgr::xdbDeserializeVec(Xdb *xdb,
                          Xid batchId,
                          XdbPage *xdbPages[],
                          const size_t numPages)
{
    Status status;
    XdbBatchSerializedHdr *batchHdr = NULL;
    uint64_t calcCksum;
    uint64_t ondiskCksum;
    char fname[XcalarApiMaxPathLen];
    size_t totBytesDes = 0;

    // Reserve the first IOV for the base header
    const ssize_t iovcnt = numPages + 1;
    struct iovec *iovBase = NULL;
    struct iovec *iov = NULL;
    char *tmpDecompBuf = NULL;

    assert(isInit);

    // Move all allocations up-front. This is so that transient allocation
    // failures can be recovered by retrying.
    status = xdbGetSerDesFname(batchId, fname, sizeof(fname));
    BailIfFailed(status);

    if (compEnabled) {
        tmpDecompBuf = (char *) memAllocExt(bcSize(), moduleName);
        BailIfNull(tmpDecompBuf);
    }

    iovBase =
        (struct iovec *) memAllocExt(sizeof(*iovBase) * iovcnt, moduleName);
    BailIfNull(iovBase);

    iovBase[0].iov_len =
        sizeof(*batchHdr) + sizeof(XdbItemSerializedHdr) * numPages;
    batchHdr =
        (XdbBatchSerializedHdr *) memAllocExt(iovBase[0].iov_len, moduleName);
    BailIfNull(batchHdr);
    iovBase[0].iov_base = batchHdr;

    iov = &iovBase[1];
    for (size_t page = 0; page < numPages; page++) {
        XdbPage *xdbPage = xdbPages[page];

        assert(xdbPage->serializedXid == batchId);

        status = xdbAllocTupBuf(xdbPage,
                                xdbPage->txnId,
                                xdbPage->cleanoutMode,
                                XdbPage::Deserializing);
        BailIfFailed(status);
        iov[page].iov_base = xdbPage->tupBuf;
        iov[page].iov_len = xdbPage->hdr.pageSize;
    }

    status = xdbReadVec(batchId, iovBase, iovcnt);
    BailIfFailed(status);

    // From this point on, all failures are permanent. This is because we've
    // deleted the backing file.

    if (batchHdr->magic != serializedMagic) {
        xSyslog(moduleName, XlogWarn, "%s is not a serialized XDB page", fname);
        status = StatusXdbDesError;
        goto CommonExit;
    }

    ondiskCksum = batchHdr->cksum;
    batchHdr->cksum = 0;
    calcCksum = xdbCksumVec(iovBase, iovcnt);
    if (ondiskCksum != calcCksum) {
        xSyslog(moduleName,
                XlogWarn,
                "%s checksum failure (0x%016lx != 0x%016lx)",
                fname,
                ondiskCksum,
                calcCksum);
        status = StatusXdbDesError;
        goto CommonExit;
    }

    if (batchHdr->randIdent != defaultPagingIdent_) {
        xSyslog(moduleName,
                XlogWarn,
                "%s instance mismatch (0x%016lx != 0x%016lx)",
                fname,
                batchHdr->randIdent,
                defaultPagingIdent_);
        status = StatusXdbDesError;
        goto CommonExit;
    }

    // Verify all items before modifying in-memory state
    for (size_t page = 0; page < numPages; page++) {
        XdbPage *xdbPage = xdbPages[page];
        XdbItemSerializedHdr *itemHdr =
            (XdbItemSerializedHdr *) ((uintptr_t) &batchHdr->itemHdrs +
                                      (sizeof(XdbItemSerializedHdr) * page));

        // We should be deserializing back to the same page we serialized from
        const bool pageMatch = itemHdr->xdbPage == xdbPage;
        assert(pageMatch);
        if (!pageMatch) {
            xSyslog(moduleName,
                    XlogWarn,
                    "%s serialized page %lu address %p differs from xdbPage %p",
                    fname,
                    page,
                    itemHdr->xdbPage,
                    xdbPage);
            status = StatusXdbDesError;
            goto CommonExit;
        }
    }

    for (size_t page = 0; page < numPages; page++) {
        XdbPage *xdbPage = xdbPages[page];
        NewTuplesBuffer *tupleBuf;
        XdbItemSerializedHdr *itemHdr =
            (XdbItemSerializedHdr *) ((uintptr_t) &batchHdr->itemHdrs +
                                      (sizeof(XdbItemSerializedHdr) * page));

        assert(itemHdr->xdbPage == xdbPage);

        totBytesDes += xdbPage->hdr.pageSize;
        if (compEnabled && xdbPage->hdr.isCompressed) {
            size_t decompSize;
            status = XcSnappy::getUncompressedLength(xdbPage->tupBuf,
                                                     xdbPage->hdr.pageSize,
                                                     &decompSize);
            BailIfFailed(status);

            if (decompSize != bcSize()) {
                xSyslog(moduleName,
                        XlogErr,
                        "%s serialized page %lu invalid uncompressed size: %lu",
                        fname,
                        page,
                        decompSize);
                assert(false);
                status = StatusXdbDesError;
                goto CommonExit;
            }

            status = XcSnappy::deCompress(tmpDecompBuf,
                                          xdbPage->tupBuf,
                                          xdbPage->hdr.pageSize);
            BailIfFailed(status);
            xSyslog(moduleName,
                    XlogVerbose,
                    "%s page %lu decompress %lu-->%lu",
                    fname,
                    page,
                    xdbPage->hdr.pageSize,
                    decompSize);
            memcpy(xdbPage->tupBuf, tmpDecompBuf, bcSize());
            xdbPage->hdr.isCompressed = false;
            xdbPage->hdr.pageSize = bcSize();
        } else if (xdbPage->hdr.isCompressed) {
            xSyslog(moduleName,
                    XlogErr,
                    "%s page %lu unexpectedly marked compressed",
                    fname,
                    page);
            status = StatusXdbDesError;
            assert(false);
            goto CommonExit;
        }

        tupleBuf = xdbPage->tupBuf;
        tupleBuf->setBufferStartOffset(itemHdr->bufferStartOffset);
        tupleBuf->setBufferEndOffset(itemHdr->bufferEndOffset);
        tupleBuf->deserialize();
    }

    for (size_t page = 0; page < numPages; page++) {
        assert(atomicRead32(&xdbPages[page]->hdr.pageState) ==
               XdbPage::Deserializing);
        atomicWrite32(&xdbPages[page]->hdr.pageState, XdbPage::Resident);
    }

    StatsLib::statAtomicAdd64(numDeserializedPages_, numPages);
    StatsLib::statAtomicAdd64(numDeserializedBytes_, totBytesDes);
    StatsLib::statAtomicIncr64(numDeserializedBatches_);

CommonExit:
    if (tmpDecompBuf != NULL) {
        memFree(tmpDecompBuf);
        tmpDecompBuf = NULL;
    }

    if (iovBase != NULL) {
        memFree(iovBase);
        iovBase = NULL;
    }

    if (batchHdr != NULL) {
        memFree(batchHdr);
        batchHdr = NULL;
    }

    if (status != StatusOk) {
        for (size_t page = 0; page < numPages; page++) {
            xSyslog(moduleName,
                    XlogDebug,
                    "Failed to deserialize page %p: %s",
                    xdbPages[page],
                    strGetFromStatus(status));
            if (atomicRead32(&xdbPages[page]->hdr.pageState) ==
                    XdbPage::Resident ||
                atomicRead32(&xdbPages[page]->hdr.pageState) ==
                    XdbPage::Deserializing) {
                xdbFreeTupBuf(xdbPages[page]);
            }
        }
        StatsLib::statAtomicIncr64(numDeserializationFailures_);
#ifdef PAGING_ERRORS_DEBUG
        static constexpr const uint32_t BtBuffer = 4096;
        static constexpr const uint32_t TraceSize = 8;
        char btBuffer[BtBuffer];
        void *trace[TraceSize];
        int traceSize = getBacktrace(trace, TraceSize);
        printBackTrace(btBuffer, sizeof(btBuffer), traceSize, trace);
        xSyslog(moduleName,
                XlogErr,
                "Failed xdbDeserializeVec: XdbId %lu batchId %lu numPages %lu: "
                "%s\n%s",
                xdb ? xdb->getXdbId() : 0,
                batchId,
                numPages,
                strGetFromStatus(status),
                btBuffer);
#else   // PAGING_ERRORS_DEBUG
        xSyslog(moduleName,
                XlogErr,
                "Failed xdbDeserializeVec: XdbId %lu batchId %lu numPages %lu: "
                "%s",
                xdb ? xdb->getXdbId() : 0,
                batchId,
                numPages,
                strGetFromStatus(status));
#endif  // PAGING_ERRORS_DEBUG
    }

    return status;
}

void
XdbMgr::xdbSerDesDropVec(Xid batchId,
                         XdbPage *xdbPages[],
                         const size_t numPages)
{
    Status status = StatusOk;
    char fname[XcalarApiMaxPathLen];
    size_t totBytesDropped = 0;
    const XcalarConfig *xconfig = XcalarConfig::get();
    const XcalarConfig::SerDesMode mode =
        (XcalarConfig::SerDesMode) xconfig->xdbSerDesMode_;

    assert(mode != XcalarConfig::SerDesMode::Disabled &&
           "Xdb pages cannot be marked for serialization with SerDesMode "
           "disabled");
    assert(isInit);

    status = xdbGetSerDesFname(batchId, fname, sizeof(fname));
    BailIfFailed(status);

    switch (mode) {
    case XcalarConfig::SerDesMode::Local: {
        char fqpn[XcalarApiMaxPathLen];
        status = strSnprintf(fqpn, sizeof(fqpn), "%s/%s", xdbSerPath_, fname);
        BailIfFailed(status);
        int ret = unlink(fqpn);

        if (ret == -1) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogInfo,
                    "%s failed to remove stale file %s",
                    fqpn,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        break;
    }
    case XcalarConfig::SerDesMode::PageToLocalFile: {
        uint64_t fileNum;
        uint64_t blockNum;
        uint32_t blockCount;
        Status status =
            batchIdToPageInfo(batchId, fileNum, blockNum, blockCount);
        BailIfFailed(status);
        uint64_t fileOffset = blockNum * pagingFileBlockSize_;
#ifdef PAGE_TO_FILE_DEBUG
        xSyslog(moduleName,
                XlogInfo,
                "dropping page %ld @ offset %ld with %d blocks in file %ld for "
                "batch 0x%lx",
                blockNum,
                fileOffset,
                blockCount,
                fileNum,
                batchId);
#endif
        status = deallocateBlock(pagingFileFds_[fileNum],
                                 fileOffset,
                                 blockCount * pagingFileBlockSize_);
        if (status != StatusOk) {
            goto CommonExit;
        }
        break;
    }
    default:
        assert(0);
        status = StatusInval;
        goto CommonExit;
    }

CommonExit:
    for (size_t page = 0; page < numPages; page++) {
        XdbPage *xdbPage = xdbPages[page];
        assert(xdbPage->serializedXid == batchId);
        atomicWrite32(&xdbPage->hdr.pageState, XdbPage::Dropped);
        totBytesDropped += xdbPage->hdr.pageSize;
        // Already freed when this was serialized
        xdbPage->tupBuf = NULL;
    }

    if (status == StatusOk) {
        StatsLib::statAtomicAdd64(numSerDesDroppedPages_, numPages);
        StatsLib::statAtomicAdd64(numSerDesDroppedBytes_, totBytesDropped);
        StatsLib::statAtomicIncr64(numSerDesDroppedBatches_);
    } else {
        StatsLib::statAtomicIncr64(numSerDesDropFailures_);
        xSyslog(moduleName,
                XlogErr,
                "Failed xdbSerDesDropVec: batchId %lu numPages %lu: %s",
                batchId,
                numPages,
                strGetFromStatus(status));
    }
}

void
SerDesSlotWork::func()
{
    // Assume the TXN ID of the thread that created the work item. This has
    // knowledge that the worker queue is comprised of blockable threads.
    __txn = txn_;

    Status status = StatusOk;
    XdbPage *xdbPage, *firstPage;
    uint64_t pageNum = 0;
    XdbMgr *xdbMgr = XdbMgr::get();
    Xid batchId;
    XdbPage **xdbPages = NULL;

    XdbAtomicHashSlot *xdbHashSlot = &xdb_->hashSlotInfo.hashBase[slotId_];
    XdbHashSlotAug *xdbHashSlotAug = &xdb_->hashSlotInfo.hashBaseAug[slotId_];

    xdbMgr->lockSlot(&xdb_->hashSlotInfo, slotId_);
    bool locked = true;

    if (*status_ != StatusOk) {
        // we've already failed
        goto CommonExit;
    }

    xdbPage = xdbMgr->getXdbHashSlotNextPage(xdbHashSlot);

    if (xdbPage == NULL) {
        goto CommonExit;
    }

    firstPage = xdbPage;

    xdbPages =
        (XdbPage **) memAllocExt(sizeof(*xdbPages) * xdbHashSlotAug->numPages,
                                 moduleName);
    BailIfNull(xdbPages);

    while (xdbPage) {
        xdbPages[pageNum] = xdbPage;

        if (serialize_) {
            atomicWrite32(&xdbPage->hdr.pageState, XdbPage::Serializing);
        }

        xdbPage = (XdbPage *) xdbPage->hdr.nextPage;
        pageNum++;
    }

    assert(pageNum == xdbHashSlotAug->numPages);

    if (serialize_) {
        if (!xdbHashSlot->serialized) {
            batchId = XidMgr::get()->xidGetNext();
            status = xdbMgr->xdbSerializeVec(xdb_->xdbId,
                                             batchId,
                                             xdbPages,
                                             pageNum);
            if (unlikely(status != StatusOk)) {
                for (unsigned ii = 0; ii < pageNum; ii++) {
                    atomicCmpXchg32(&xdbPages[ii]->hdr.pageState,
                                    XdbPage::Serializing,
                                    XdbPage::Resident);
                }
                goto CommonExit;
            }

            xdbHashSlot->serialized = true;
        }
    } else {
        if (xdbHashSlot->serialized) {
            batchId = firstPage->serializedXid;

            status = xdbMgr->xdbDeserializeVec(xdb_->xdbId,
                                               batchId,
                                               xdbPages,
                                               pageNum);
            BailIfFailed(status);

            xdbHashSlot->serialized = false;
        }
    }

CommonExit:
    if (locked) {
        xdbMgr->unlockSlot(&xdb_->hashSlotInfo, slotId_);
    }

    if (xdbPages) {
        memFree(xdbPages);
        xdbPages = NULL;
    }

    if (status != StatusOk) {
        *status_ = status;
    }

    if (atomicDec32(outstandingSlots_) == 0) {
        sem_->post();
    }
}

Status
XdbMgr::distributeSerDesWork(Xdb *xdb, bool serialize)
{
    Status status;

    status = distributeSerDesWorkInt(xdb, serialize);

    return status;
}

Status
XdbMgr::distributeSerDesWorkInt(Xdb *xdb, bool serialize)
{
    Status status = StatusOk;
    uint64_t numSlots = xdb->hashSlotInfo.hashSlots;

    Semaphore sem(0);
    Atomic32 outstandingSlots;
    atomicWrite32(&outstandingSlots, numSlots);

    SerDesSlotWork *work = new (std::nothrow) SerDesSlotWork[numSlots];
    BailIfNull(work);

    for (uint64_t slot = 0; slot < numSlots; slot++) {
        work[slot].slotId_ = slot;
        work[slot].sem_ = &sem;
        work[slot].outstandingSlots_ = &outstandingSlots;
        work[slot].xdb_ = xdb;
        work[slot].status_ = &status;
        work[slot].txn_ = Txn::currentTxn();
        work[slot].serialize_ = serialize;

        // This workItem is now ready, we can enqueue it
        workQueueForIoToDisk->enqueue(&work[slot]);
    }

    sem.semWait();

CommonExit:
    if (work) {
        delete[] work;
        work = NULL;
    }

    return status;
}

Status
XdbMgr::xdbSerializeLocal(const XdbId xdbId)
{
    Status status = StatusOk;
    Xdb *xdb;
    XdbMeta *xdbMeta;
    assert(isInit);

    status = xdbGet(xdbId, &xdb, &xdbMeta);
    BailIfFailed(status);

    status = xdbSerialize(xdb);
    BailIfFailed(status);

CommonExit:

    return status;
}

Status
XdbMgr::xdbSerialize(Xdb *xdb)
{
    Status status = StatusOk;
    XdbId xdbId = xdb->getXdbId();

    assert(isInit);

    if (!xdb->loadDone) {
        xSyslog(moduleName,
                XlogWarn,
                "Failed to serialize table %lu due to load in progress",
                xdbId);
        status = StatusBusy;
        goto CommonExit;
    }

    if (!xdb->isResident) {
        xSyslog(moduleName,
                XlogWarn,
                "Failed to serialize non-resident table %lu",
                xdbId);
        status = StatusInval;
        goto CommonExit;
    }

    // XXX: Racy check
    if (CursorManager::get()->getTotalRefs(xdbId) != 0) {
        xSyslog(moduleName,
                XlogWarn,
                "Failed to serialize table %lu due to active cursors",
                xdbId);
        status = StatusBusy;
        goto CommonExit;
    }

    status = distributeSerDesWork(xdb, true);
    BailIfFailed(status);

    xdb->isResident = false;

CommonExit:
    return status;
}

Status
XdbMgr::xdbDeserializeLocal(const XdbId xdbId)
{
    Status status = StatusOk;
    Xdb *xdb;
    XdbMeta *xdbMeta;

    assert(isInit);

    status = xdbGet(xdbId, &xdb, &xdbMeta);
    BailIfFailed(status);

    status = xdbDeserialize(xdb);
    BailIfFailed(status);

CommonExit:

    return status;
}

Status
XdbMgr::xdbDeserialize(Xdb *xdb)
{
    Status status = StatusOk;
    XdbId xdbId = xdb->getXdbId();

    assert(isInit);

    if (xdb->isResident) {
        xSyslog(moduleName,
                XlogWarn,
                "Failed to deserialize to resident table %lu",
                xdbId);
        status = StatusInval;
        goto CommonExit;
    }

    if (!xdb->loadDone) {
        xSyslog(moduleName,
                XlogWarn,
                "Failed to deserialize table 0x%016lx due to load in progress",
                xdbId);
        status = StatusBusy;
        goto CommonExit;
    }

    // XXX: Racy check
    if (CursorManager::get()->getTotalRefs(xdbId) != 0) {
        xSyslog(moduleName,
                XlogWarn,
                "Failed to deserialize table 0x%016lx due to active cursors",
                xdbId);
        status = StatusBusy;
        goto CommonExit;
    }

    status = distributeSerDesWork(xdb, false);
    BailIfFailed(status);

    xdb->isResident = true;

CommonExit:
    return status;
}

Status
XdbMgr::xdbSerDes(XdbId xdbId, bool serialize)
{
    Status status;
    MsgEphemeral eph;
    unsigned numNodes = Config::get()->getActiveNodes();
    Status statusArray[numNodes];
    TwoPcCallId twoPcFunc;

    assert(isInit);

    if (serialize) {
        twoPcFunc = TwoPcCallId::Msg2pcSerializeXdb;
    } else {
        twoPcFunc = TwoPcCallId::Msg2pcDeserializeXdb;
    }

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      &xdbId,
                                      sizeof(xdbId),
                                      0,
                                      TwoPcSlowPath,
                                      twoPcFunc,
                                      statusArray,
                                      (TwoPcBufLife)(TwoPcMemCopyInput |
                                                     TwoPcMemCopyOutput));
    TwoPcHandle twoPcHandle;
    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  MsgTypeId::Msg2pcDeserializeXdbPages,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                     MsgRecvHdrPlusPayload),
                                  TwoPcSyncCmd,
                                  TwoPcAllNodes,
                                  TwoPcIgnoreNodeId,
                                  TwoPcClassNonNested);
    BailIfFailed(status);

    assert(!twoPcHandle.twoPcHandle);
    for (unsigned jj = 0; jj < numNodes; ++jj) {
        if (statusArray[jj] != StatusOk) {
            status = statusArray[jj];
            break;
        }
    }

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "XDB deserialize pages failed for table ID %lu with %s",
                xdbId,
                strGetFromStatus(status));
    }

    return status;
}

Status
XdbMgr::xdbDeserializeAllPages(XdbId xdbId)
{
    Xdb *xdb;
    XdbMeta *xdbMeta;
    MsgEphemeral eph;
    unsigned numNodes = Config::get()->getActiveNodes();
    Status statusArray[numNodes];

    Status status = xdbGet(xdbId, &xdb, &xdbMeta);
    BailIfFailed(status);

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      &xdbId,
                                      sizeof(xdbId),
                                      0,
                                      TwoPcSlowPath,
                                      TwoPcCallId::Msg2pcDeserializeXdbPages1,
                                      statusArray,
                                      (TwoPcBufLife)(TwoPcMemCopyInput |
                                                     TwoPcMemCopyOutput));

    if (xdb->globalState & XdbGlobal) {
        TwoPcHandle twoPcHandle;
        status = MsgMgr::get()->twoPc(&twoPcHandle,
                                      MsgTypeId::Msg2pcDeserializeXdbPages,
                                      TwoPcDoNotReturnHandle,
                                      &eph,
                                      (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                         MsgRecvHdrPlusPayload),
                                      TwoPcSyncCmd,
                                      TwoPcAllNodes,
                                      TwoPcIgnoreNodeId,
                                      TwoPcClassNonNested);
        BailIfFailed(status);

        assert(!twoPcHandle.twoPcHandle);
        for (unsigned jj = 0; jj < numNodes; ++jj) {
            if (statusArray[jj] != StatusOk) {
                status = statusArray[jj];
                break;
            }
        }
    } else {
        xdbDeserializeAllPagesLocal(&eph, &xdbId);
    }

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "XDB deserialize pages failed for table ID %lu with %s",
                xdbId,
                strGetFromStatus(status));
    }

    return status;
}

void
XdbMgr::xdbDeserializeAllPagesLocal(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    Xdb *xdb;
    XdbId xdbId = *(XdbId *) payload;

    if (XcalarConfig::get()->xdbSerDesMode_ ==
        (uint32_t) XcalarConfig::SerDesMode::Disabled) {
        // Serialization is disabled
        goto CommonExit;
    }

    assert(isInit);

    status = xdbGet(xdbId, &xdb, NULL);
    BailIfFailed(status);

    status = xdbDeserializeAllPages(xdb);
    BailIfFailed(status);

CommonExit:
    eph->status = status;
}

Status
XdbMgr::xdbDeserializeAllPages(Xdb *xdb)
{
    XdbPage *xdbPage;
    XdbId xdbId = xdb->getXdbId();
    Status status = StatusOk;

    assert(xdb->loadDone);
    assert(CursorManager::get()->getTotalRefs(xdbId) == 0);

    for (uint64_t slot = 0; slot < xdb->hashSlotInfo.hashSlots; slot++) {
        XdbAtomicHashSlot *xdbHashSlot = &xdb->hashSlotInfo.hashBase[slot];

        xdbPage = (XdbPage *) getXdbHashSlotNextPage(xdbHashSlot);

        if (xdbPage == NULL) {
            continue;
        }

        while (xdbPage) {
            status = xdbPage->getRef(xdb);
            BailIfFailed(status);

            xdbPage = xdbPage->hdr.nextPage;
        }
    }

CommonExit:
    return status;
}

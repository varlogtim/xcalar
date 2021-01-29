// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <time.h>
#include "StrlFunc.h"
#include "hash/Hash.h"
#include "durable/Durable.h"
#include "DurableVersions.h"
#include "common/Version.h"
#include "constants/XcalarConfig.h"
#include "util/MemTrack.h"
#include "strings/String.h"
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/io/coded_stream.h>

static constexpr const char *moduleName = "libdurable";
LibDurable *LibDurable::instance = NULL;

using namespace xcalar::internal;
using namespace google::protobuf;

static constexpr uint64_t DurableLogV1 = 1;

static const char *const xceWhiteList[] =
    {"1.2.0-982-xcalardev-57a0d2d5-57a0d2d5",
     "1.2.1-1044-xcalardev-67cf7211-67cf7211",
     "1.2.1-1154-xcalardev-9442f201-9442f201"};

LibDurable *
LibDurable::get()
{
    return instance;
}

Status
LibDurable::init()
{
    if (instance != NULL) {
        return StatusOk;
    }

    instance = new (std::nothrow) LibDurable();
    if (instance == NULL) {
        return StatusNoMem;
    }

    return StatusOk;
}

void
LibDurable::destroy()
{
    delete instance;
    instance = NULL;
}

void
LibDurable::pbGetTs(struct timespec *val, const Timestamp &field)
{
    val->tv_sec = field.seconds();
    val->tv_nsec = field.nanos();
}

void
LibDurable::pbSetTs(Timestamp *field, const struct timespec val)
{
    field->set_seconds(val.tv_sec);
    field->set_nanos(val.tv_nsec);
}

void
LibDurable::pbSetStr(StringValue *field,
                     const char *val,
                     const size_t maxSrcLen) const
{
    assert(strlen(val) <= maxSrcLen);
    field->set_value(val);
}

size_t
LibDurable::pbGetStr(char *val,
                     const StringValue &field,
                     const size_t dstLen) const
{
    return strlcpy(val, field.value().c_str(), dstLen);
}

int32_t
LibDurable::pbShaToVer(const char *sha,
                       const char *shaArray[],
                       const int32_t shaArrayLen)
{
    for (int32_t i = 0; i < shaArrayLen; i++) {
        if (strnstr(sha, shaArray[i], gitShaLen)) {
            // Element 0 is actually version 1
            return i + 1;
        }
    }

    return 0;
}

Status
LibDurable::serialize(Handle *h)
{
    Status status;
    if (!h->isInit_) {
        status = StatusDurHandleNoInit;
        goto CommonExit;
    }

    if (!h->isCurrent_) {
        status = StatusDurVerError;
        goto CommonExit;
    }

    status = serialize(h->logHandle_, h->durable);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
LibDurable::serialize(LogLib::Handle *log,
                      durable::DurableObject *durable,
                      LogLib::WriteOptionsMask optionsMask)
{
    Status status = StatusOk;
    size_t serializedSize;
    size_t bufSize;
    uint8_t *buf = NULL;
    PbLogHdr *hdr;

    try {
        if (durable->hdr().type() == durable::DurableType::DurableInvalidType) {
            status = StatusDurVerError;
            goto CommonExit;
        }

        // protobuf GetCurrentTime broken and deprecated
        struct timespec currTime;
        clock_gettime(CLOCK_REALTIME, &currTime);
        pbSetTs(durable->mutable_hdr()->mutable_lastwritetime(), currTime);
        durable->mutable_hdr()->set_verxce(versionGetStr());
        durable->mutable_hdr()->set_shadurableobject(
            DurableVersions::pbGetCurrSha_DurableObject());
        durable->mutable_hdr()->set_isdirtyworkspace(
            DurableVersions::isDirtyWorkspace);
        durable->mutable_hdr()->set_haslocalcommits(
            DurableVersions::hasLocalCommits);
        durable->set_magic(magicDurableObject);

        if (durable->hdr().shadurableobject().length() + 1 != gitShaLen) {
            xSyslog(moduleName,
                    XlogErr,
                    "Cannot write durable object due to missing IDL SHA");
            assert(false);
            status = StatusProtobufEncodeError;
            goto CommonExit;
        }
    } catch (std::exception &e) {
        status = StatusProtobufEncodeError;
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogDebug,
            "Serializing to object IDL: %s",
            DurableVersions::pbGetCurrSha_DurableObject());

    // Calculate and allocate space needed to store serialized durable.
    serializedSize = durable->ByteSizeLong();
    assert(serializedSize > 0 && "Verify protobuf isn't giving invalid size");

    bufSize = roundUp(sizeof(*hdr) + serializedSize + reservedLogEditBytes,
                      logLib->minLogicalBlockAlignment);

    buf = (uint8_t *) (memAlloc(bufSize));
    BailIfNull(buf);
    memZero(buf, bufSize);

    hdr = (PbLogHdr *) buf;
    hdr->magic = magicDurableObjectLog;
    hdr->ver = DurableLogV1;
    hdr->pbAllocatedBytes = serializedSize + reservedLogEditBytes;
    hdr->pbUsedBytes = serializedSize;

    // Serialize actual
    if (!durable->SerializeToArray(buf + sizeof(PbLogHdr), serializedSize)) {
        status = StatusProtobufEncodeError;
        goto CommonExit;
    }

    assert((bufSize % sizeof(uint64_t)) == 0);
    // Checksum of header (with hdr->cksum zero) and data
    hdr->cksum = hashCrc32c(0, buf, bufSize);

    status = logLib->writeRecord(log, buf, bufSize, optionsMask);
    BailIfFailed(status);

CommonExit:

    if (buf != NULL) {
        memFree(buf);
        buf = NULL;
    }

    return status;
}

MustCheck bool
LibDurable::checkXCEWhitelist(const char *const verStr) const
{
    const size_t numVerStrs = sizeof(xceWhiteList) / sizeof(*xceWhiteList);
    size_t i;

    for (i = 0; i < numVerStrs; i++) {
        if (strMatch(verStr, xceWhiteList[i])) {
            return true;
        }
    }

    return false;
}

Status
LibDurable::deserialize(LogLib::Handle *log,
                        durable::DurableObject *durable,
                        LogLib::ReadMode logReadMode)
{
    void *buf = NULL;
    size_t bufSize = 0;
    PbLogHdr *hdr;
    uint64_t onDiskCksum;
    uint64_t calcCksum;

    // Read record from log.
    Status status =
        logLib->readRecordInternal(log, &buf, &bufSize, logReadMode);
    if (status != StatusOk) {
        return status;
    }

    assert(bufSize >= sizeof(PbLogHdr));
    assert((bufSize % sizeof(uint64_t)) == 0);
    hdr = (PbLogHdr *) buf;

    if (hdr->magic != magicDurableObjectLog) {
        xSyslog(moduleName,
                XlogErr,
                "Log record with header 0x%lx is not a durable object type"
                "(log %s)",
                hdr->magic,
                log->filePrefix);
        status = StatusLogCorrupt;
        goto CommonExit;
    }

    onDiskCksum = hdr->cksum;
    hdr->cksum = 0;
    calcCksum = hashCrc32c(0, buf, bufSize);
    if (calcCksum != onDiskCksum) {
        xSyslog(moduleName,
                XlogErr,
                "Checksum error reading durable object: 0x%lx != 0x%lx"
                "(log %s)",
                onDiskCksum,
                calcCksum,
                log->filePrefix);
        status = StatusLogCorrupt;
        goto CommonExit;
    }

    if (hdr->ver != DurableLogV1) {
        xSyslog(moduleName,
                XlogErr,
                "Durable object log version %lu not supported"
                "(log %s)",
                hdr->magic,
                log->filePrefix);
        status = StatusDurVerError;
        goto CommonExit;
    }

    if (hdr->pbUsedBytes + sizeof(*hdr) > bufSize) {
        xSyslog(moduleName,
                XlogErr,
                "Serialized size of %lu bytes is invalid for durable object"
                "(log %s)",
                hdr->pbUsedBytes,
                log->filePrefix);
        status = StatusLogCorrupt;
        goto CommonExit;
    }

    try {
        google::protobuf::io::CodedInputStream coded_fs((uint8_t *) buf +
                                                            sizeof(*hdr),
                                                        hdr->pbUsedBytes);
        coded_fs.SetTotalBytesLimit(protobufTotalBytesLimit);
        if (!durable->ParseFromCodedStream(&coded_fs)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Unable to parse durable object (log %s)",
                    log->filePrefix);
            status = StatusProtobufDecodeError;
            goto CommonExit;
        }

        if (durable->magic() != magicDurableObject) {
            xSyslog(moduleName,
                    XlogErr,
                    "Incorrect type 0x%lx for durable object (log %s)",
                    durable->magic(),
                    log->filePrefix);
            status = StatusProtobufDecodeError;
            goto CommonExit;
        }

        const bool reqClean = XcalarConfig::get()->durableEnforceClean_;
        const bool isDirty = durable->hdr().isdirtyworkspace() ||
                             durable->hdr().haslocalcommits();

        // We will likely default production builds to only loading durable
        // data generated by a "clean" build
        if (reqClean && isDirty) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to load durable object %s due to generation by a "
                    "%s workspace with%s local commits",
                    log->filePrefix,
                    durable->hdr().isdirtyworkspace() ? "dirty" : "clean",
                    durable->hdr().haslocalcommits() ? "" : "out");
            status = StatusDurDirtyWriter;
            goto CommonExit;
        }

        const int32_t intVer =
            pbShaToVer(durable->hdr().shadurableobject().c_str(),
                       DurableVersions::pbIntToSha_DurableObject,
                       DurableVersions::pbGetNumShas_DurableObject());

        if (intVer == 0) {
            const bool reqKnownVer = XcalarConfig::get()->durableEnforceKnown_;
            // Handle missing IDL SHAs (see bug 9604)
            const bool validXceVer =
                checkXCEWhitelist(durable->hdr().verxce().c_str());
            const bool noLoad = reqKnownVer && !validXceVer;
            xSyslog(moduleName,
                    XlogInfo,
                    "%soad durable object %s written by %s at %lu.%u "
                    "with unknown IDL SHA %s",
                    noLoad ? "Cannot l" : "L",
                    log->filePrefix,
                    durable->hdr().verxce().c_str(),
                    durable->hdr().lastwritetime().seconds(),
                    durable->hdr().lastwritetime().nanos(),
                    durable->hdr().shadurableobject().c_str());
            if (noLoad) {
                status = StatusDurVerError;
                goto CommonExit;
            }
        } else {
            xSyslog(moduleName,
                    XlogInfo,
                    "Loaded durable object %s written by %s at %lu.%u "
                    "IDL SHA %s; writer had %s workspace with%s local commits",
                    log->filePrefix,
                    durable->hdr().verxce().c_str(),
                    durable->hdr().lastwritetime().seconds(),
                    durable->hdr().lastwritetime().nanos(),
                    durable->hdr().shadurableobject().c_str(),
                    durable->hdr().isdirtyworkspace() ? "dirty" : "clean",
                    durable->hdr().haslocalcommits() ? "" : "out");
        }

    } catch (std::bad_alloc) {
        status = StatusNoMem;
        goto CommonExit;
    } catch (std::exception &e) {
        status = StatusProtobufDecodeError;
        goto CommonExit;
    }

CommonExit:
    if (buf != NULL) {
        memFree(buf);
    }
    return status;
}

Status
LibDurable::deserialize(Handle *h)
{
    if (h->isInit_) {
        return StatusDurHandleNoInit;
    }

    Status status = deserializeLast(h->logHandle_, h->durable);
    if (status == StatusOk) {
        h->isInit_ = true;
    }

    return status;
}

Status
LibDurable::deserializeLast(LogLib::Handle *log,
                            durable::DurableObject *durable)
{
    Status status;

    status = logLib->readLastLogicalRecordInt(log);
    if (status != StatusOk) {
        return status;
    }

    status = deserialize(log, durable, LogLib::ReadNext);
    if (status != StatusOk) {
        return status;
    }
    log->positionedAtEnd = true;

    return status;
}

void
LibDurable::printPbException(std::exception &e)
{
    xSyslog(moduleName,
            XlogErr,
            "Caught protocol buffer exception: %s",
            e.what());
}

// Use protobuf reflection to get fields by name
Message *
LibDurable::Handle::getMutableMsgByName(Message *msg, const char *name) const
{
    const Descriptor *msgDesc = msg->GetDescriptor();
    if (msgDesc == NULL) {
        return NULL;
    }
    const FieldDescriptor *fieldDesc = msgDesc->FindFieldByName(name);
    if (fieldDesc == NULL) {
        return NULL;
    }
    const Reflection *reflection = msg->GetReflection();
    if (reflection == NULL) {
        return NULL;
    }
    const OneofDescriptor *oneofDesc = fieldDesc->containing_oneof();
    if (oneofDesc != NULL) {
        const bool hasField = reflection->HasField(*msg, fieldDesc);
        if (!hasField) {
            // Only require the field to have been set if this object is
            // initialized (ie after deserialization).
            if (isInit_) {
                // Probably ended up here attempting to deserialize durable
                // data of the wrong type (eg we expected to load a dag but got
                // a session)
                xSyslog(moduleName,
                        XlogWarn,
                        "%s is not a %s",
                        oneofDesc->full_name().c_str(),
                        name);
                return NULL;
            }
        }
    }

    return reflection->MutableMessage(msg, fieldDesc);
}

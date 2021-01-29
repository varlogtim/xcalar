// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DURABLE_H_
#define _DURABLE_H_

#include "log/Log.h"
#include "sys/XLog.h"
#include "constants/XcalarConfig.h"
#include "StrlFunc.h"
#include "common/Version.h"

#include "DurableObject.pb.h"
#include "DurableVersions.h"
#include "subsys/DurableSession.pb.h"
#include "util/ProtoWrap.h"

#include <google/protobuf/util/time_util.h>
#include <google/protobuf/arena.h>

static constexpr size_t gitShaLen = 41;  // git sha is 40 characters

// See Xc-12422. Protobuf log records can get very large so the default limit of
// 64MB on the protobuf message size in Xcalar's current version of protobuf
// (3.0.0), is insufficient - as seen in this bug.  See
// https://github.com/google/protobuf/blob/v3.0.0/src/google/protobuf/io/coded_stream.h
// and look for kDefaultTotalBytesLimit (set to 64MB by default).
//
// Raising this limit to 1 GB is probably sufficient but the max (from protobuf
// source) could be INT_MAX so let's set it to that (a little less than 2 * GB).
// See version 3.6.0.1:
// https://github.com/google/protobuf/blob/v3.6.0.1/src/google/protobuf/io/coded_stream.h
// When the protobuf version if upgraded to this version, that's the limit we'd
// get by default.

static constexpr size_t protobufTotalBytesLimit = INT_MAX;

// Clients of LibDurable MUST catch exceptions.
// TODO: Consider wrapping all accessors in templated methods which catch
// excpetions and convert to Status
class LibDurable final
{
  public:
    class Handle final
    {
        // Avoid methods returning pointers to private durable
        friend class LibDurable;

      public:
        Handle(LogLib::Handle *logHandle, const char *submodName)
            : logHandle_(logHandle),
              libDur_(LibDurable::get()),
              submodName_(submodName),
              isInit_(false),
              isCurrent_(false),
              arena_(xceArenaOpt)
        {
            durable = google::protobuf::Arena::CreateMessage<
                xcalar::internal::durable::DurableObject>(&arena_);
        };

        // Returns the type contained in this durable object
        xcalar::internal::durable::DurableType getType() const
        {
            assert(isInit_);
            return durable->hdr().type();
        }

        MustCheck Status getVerXce(char *dst, size_t dstLen) const
        {
            assert(isInit_);
            assert(dstLen > 0);
            size_t retval =
                strlcpy(dst, durable->hdr().verxce().c_str(), dstLen);
            return retval < dstLen ? StatusOk : StatusOverflow;
        }

        // Generic interfaces to underlying serialized types

        // Returns the IDL version of the type contained in this durable
        // object
        template <class PAYLOAD, class VERNUM>
        VERNUM getVer()
        {
            assert(isInit_);
            PAYLOAD *payload = getMutablePayload<PAYLOAD>();
            if (payload == NULL) {
                return (VERNUM) 0;  // Invalid version
            }
            return payload->hdr().vermajor();
        }

        template <class PAYLOAD>
        MustCheck Status getSha(char *dst, size_t dstLen)
        {
            assert(isInit_);
            assert(dstLen >= gitShaLen);
            PAYLOAD *payload = getMutablePayload<PAYLOAD>();
            if (payload == NULL) {
                if (dstLen > 0) {
                    dst[0] = '\0';
                }
                return StatusDurVerError;
            } else {
                size_t retval =
                    strlcpy(dst, payload->hdr().shaidl().c_str(), dstLen);
                return retval < dstLen ? StatusOk : StatusOverflow;
            }
        }

        // TODO: eliminate arguments and use RTTI on the types to dispatch
        // the appropriate messages...
        template <class PAYLOAD, class VERSION, class VERNUM>
        MustCheck Status
        getMutable(xcalar::internal::durable::DurableType durType,
                   VERNUM majorVer,
                   VERSION **retVer,
                   const char *idlSha = "")
        {
            Status status;
            VERSION *ver = NULL;

            if (retVer != NULL) {
                *retVer = NULL;  // init return to NULL
            }
            PAYLOAD *payload = getMutablePayload<PAYLOAD>();
            BailIfNullWith(payload, StatusProtobufDecodeError);

            ver = getMutableVer<VERSION, VERNUM>(majorVer);
            BailIfNullWith(ver, StatusProtobufDecodeError);

            if (isInit_) {
                int ver = getVer<PAYLOAD, VERNUM>();
                if (ver != majorVer) {
                    // Wrong IDL major version to deserialize the on-disk
                    // data
                    xSyslog("libdurable", XlogErr, "version %d bad", ver);
                    status = StatusDurBadIdlVer;
                    goto CommonExit;
                }

                const int32_t intVer =
                    libDur_
                        ->pbShaToVer(payload->hdr().shaidl().c_str(),
                                     DurableVersions::pbIntToSha_AllShas,
                                     DurableVersions::pbGetNumShas_AllShas());
                if (intVer == 0) {
                    const bool reqKnownVer =
                        XcalarConfig::get()->durableEnforceKnown_;
                    // Handle missing IDL SHAs (see bug 9604)
                    const bool validXceVer = libDur_->checkXCEWhitelist(
                        durable->hdr().verxce().c_str());
                    const bool noLoad = reqKnownVer && !validXceVer;
                    xSyslog("libdurable",
                            XlogInfo,
                            "%soad durable %s %s written by %s at %lu.%u "
                            "with unknown IDL SHA %s (Xcalar version %s)",
                            noLoad ? "Cannot l" : "L",
                            submodName_,
                            logHandle_->filePrefix,
                            durable->hdr().verxce().c_str(),
                            durable->hdr().lastwritetime().seconds(),
                            durable->hdr().lastwritetime().nanos(),
                            payload->hdr().shaidl().c_str(),
                            versionGetStr());
                    if (noLoad) {
                        status = StatusDurBadSha;
                        goto CommonExit;
                    }
                }
            } else {
                // Populate the header for this version
                payload->mutable_hdr()->set_shaidl(idlSha);

                payload->mutable_hdr()->set_vermajor(majorVer);
                durable->mutable_hdr()->set_type(durType);
                isInit_ = true;

                if (getMutableVerMsg(majorVer + 1) == NULL) {
                    // IDL doesn't support the next version, so this must
                    // be the current version.
                    isCurrent_ = true;
                } else {
                    isCurrent_ = false;
                }
            }

            if (retVer != NULL) {
                *retVer = ver;
            }
            status = StatusOk;
        CommonExit:
            return status;
        }

      private:
        LogLib::Handle *const logHandle_;
        LibDurable *const libDur_;
        const char *const submodName_;

        google::protobuf::Message *getMutableMsgByName(
            google::protobuf::Message *msg, const char *name) const;

        template <class PAYLOAD>
        PAYLOAD *getMutablePayload()
        {
            google::protobuf::Message *msg =
                getMutableMsgByName(durable, submodName_);
            if (msg == NULL) {
                return NULL;
            }

            return dynamic_cast<PAYLOAD *>(msg);
        }

        // TODO: eliminate arguments and use RTTI on the types to dispatch
        // the appropriate messages...
        template <class VERNUM>
        google::protobuf::Message *getMutableVerMsg(VERNUM majorVer)
        {
            google::protobuf::Message *msg =
                getMutableMsgByName(durable, submodName_);
            if (msg == NULL) {
                return NULL;
            }
            return getMutableMsgByName(msg,
                                       DurableVersions::pbVerMap(majorVer));
        }

        // TODO: eliminate arguments and use RTTI on the types to dispatch
        // the appropriate messages...
        template <class VERSION, class VERNUM>
        VERSION *getMutableVer(VERNUM majorVer)
        {
            google::protobuf::Message *msg = getMutableVerMsg<VERNUM>(majorVer);
            if (msg == NULL) {
                return NULL;
            }

            return dynamic_cast<VERSION *>(msg);
        }

        bool isInit_;
        bool isCurrent_;
        google::protobuf::Arena arena_;
        xcalar::internal::durable::DurableObject *durable;

        Handle(const Handle &) = delete;
        Handle &operator=(const Handle &) = delete;
    };

    static LibDurable *get();
    static MustCheck Status init();
    void destroy();

    void pbGetTs(struct timespec *val,
                 const google::protobuf::Timestamp &field);

    void pbSetTs(google::protobuf::Timestamp *field, const struct timespec val);

    void pbSetStr(google::protobuf::StringValue *field,
                  const char *val,
                  const size_t maxSrcLen) const;

    size_t pbGetStr(char *val,
                    const google::protobuf::StringValue &field,
                    const size_t dstLen) const;

    // Brute force to map a SHA to a sequential integer version number.  Maybe
    // convert to a hash map one day.
    int32_t pbShaToVer(const char *sha,
                       const char *shaArray[],
                       const int32_t shaArrayLen);

    // Returns log cursor to beginning of record
    MustCheck Status serialize(Handle *h);

    MustCheck Status deserialize(Handle *h);

    static void printPbException(std::exception &e);

  private:
    // XXX: revisit binary layout when considering Log metadata format
    typedef struct PbLogHdr {
        uint64_t magic;
        uint64_t cksum;
        uint64_t ver;
        // Total including actual protobuf and edit reserved bytes
        uint64_t pbAllocatedBytes;
        // Bytes used by serialized protobuf
        uint64_t pbUsedBytes;
    } PbLogHdr;

    LibDurable() : logLib(LogLib::get()){};

    // Magic prefix for durable object protobuf bytestream in Log
    // Generated by "od -Ax -N8 -t x8 /dev/random" and prefixed
    static constexpr uint64_t magicDurableObjectLog = 0xdead3c5deb96c17eULL;
    static constexpr uint64_t magicDurableObject = 0xbeefeed82be80db6ULL;

    // Reserve some space so that we can offline edit the protobufs (which use
    // variable length encoding) and easily reserialize them in place in the
    // log.
    static constexpr uint64_t reservedLogEditBytes = 512;

    ~LibDurable() = default;

    MustCheck Status serialize(
        LogLib::Handle *log,
        xcalar::internal::durable::DurableObject *durable,
        LogLib::WriteOptionsMask optionsMask = LogLib::WriteOptionsMaskDefault);

    MustCheck Status
    deserialize(LogLib::Handle *log,
                xcalar::internal::durable::DurableObject *durable,
                LogLib::ReadMode logReadMode);

    MustCheck Status deserializeLast(
        LogLib::Handle *log, xcalar::internal::durable::DurableObject *durable);

    MustCheck bool checkXCEWhitelist(const char *const verStr) const;

    static LibDurable *instance;
    LogLib *const logLib;

    LibDurable(const LibDurable &) = delete;
    LibDurable &operator=(const LibDurable &) = delete;
};

#endif

// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _UDFOPERATION_H_
#define _UDFOPERATION_H_

#include "libapis/LibApisCommon.h"
#include "msg/MessageTypes.h"
#include "udf/UdfTypes.h"
#include "runtime/Mutex.h"
#include "gvm/GvmTarget.h"
#include "ns/LibNs.h"

class UdfLocal;
class UdfPersist;

class UdfOperation final : public GvmTarget
{
  public:
    struct UdfModuleSrcVersion {
        uint64_t consistentVersion;
        uint64_t nextVersion;
        char modulePrefix[LibNsTypes::MaxPathNameLen + 1];
        UdfModuleSrc addUpdateInput;
        // DO NOT ADD ANY FIELD HERE! The UDF source is added to UdfModuleSrc
    };

    struct XcalarApiUdfDeleteInputVersion {
        uint64_t consistentVersion;
        uint64_t nextVersion;
        char modulePrefix[LibNsTypes::MaxPathNameLen + 1];
        XcalarApiUdfDeleteInput deleteInput;
    };

    union UdfOperationInput {
        UdfModuleSrcVersion addUpdateInputVersion;
        XcalarApiUdfDeleteInputVersion deleteInputVersion;
        UdfNwFlushPath addUpdateFlush;
    };

    enum class Action : uint32_t {
        Add,
        Update,
        Delete,
        FlushNwCache,
    };

    struct UdfUpdateDlmMsg {
        Action action;
        Status status = StatusOk;
        LibNsTypes::NsHandle udfLibNsHandle;
        XcalarApiUdfContainer udfContainer;
        UdfModuleSrcVersion udfInfo;
        // DO NOT ADD ANY FIELD HERE! The UDF source is in UdfModuleSrcVersion
    };

    virtual MustCheck Status localHandler(uint32_t action,
                                          void *payload,
                                          size_t *outputSizeOut) override;
    virtual MustCheck GvmTarget::Index getGvmIndex() const override;

    // udfUpdateCommit atomically updates libNs and on-disk UDF code. It uses
    // dispatchUdfUpdateToDlmNode() to do a one-node 2pc to the libNs DLM node
    // for the UDF's libNs object so the DLM node can do the update atomically.
    // See the implementation .cpp for the details
    MustCheck Status udfUpdateCommit(UdfModuleSrcVersion *udfInfo,
                                     Action action,
                                     const char *udfFullyQualName,
                                     XcalarApiUdfContainer *udfContainer,
                                     LibNsTypes::NsHandle libNsHandle);

    MustCheck Status dispatchUdfUpdateToDlmNode(NodeId dlmNode,
                                                void *payload,
                                                size_t payloadSize,
                                                void *ephemeral);

    UdfOperation(UdfLocal *udfLocal, UdfPersist *udfPersist);
    ~UdfOperation();

  private:
    static constexpr const char *ModuleName = "libudf";

    // Disallow.
    UdfOperation(const UdfOperation &) = delete;
    UdfOperation &operator=(const UdfOperation &) = delete;

    UdfLocal *udfLocal_;
    UdfPersist *udfPersist_;
};

#endif  // _UDFOPERATION_H_

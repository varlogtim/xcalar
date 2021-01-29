// Copyright 2015 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#ifndef _SUPPORTBUNDLE_H_
#define _SUPPORTBUNDLE_H_

#include "primitives/Primitives.h"
#include "constants/XcalarConfig.h"

// Max possible size of support bundle path.
#define SupportBundlePathSize                                \
    (XcalarApiUuidStrLen +                                   \
     XcalarApiSizeOfOutput(XcalarApiSupportGenerateOutput) + \
     strlen(XcalarConfig::get()->xcalarRootCompletePath_) +  \
     XcalarApiMaxFileNameLen)

struct XcalarApiOutput;

class SupportBundle final
{
    friend class LibSupportGvm;

  public:
    static MustCheck Status init();
    void destroy();
    static MustCheck SupportBundle *get();
    MustCheck Status supportDispatch(bool generateMiniBundle,
                                     uint64_t supportCaseId,
                                     XcalarApiOutput **output,
                                     size_t *outputSize);
    // Used by mgmtd to generate a support bundle when one cannot be initiated
    // from usrnode.
    MustCheck Status supportGenerate(char *supportId,
                                     size_t supportIdSize,
                                     char *bundlePath,
                                     size_t bundlePathSize,
                                     NodeId nodeId,
                                     uint64_t supportCaseId);

  private:
    bool inited_ = false;

    static SupportBundle *instance;

    void supportNewSupportId(char *supportId, size_t supportIdSize);
    void supportBundlePath(const char *supportId,
                           char *bundlePath,
                           size_t bundlePathSize);
    MustCheck Status supportGenerateLocal(void *payload);
    MustCheck Status supportGenerateInternal(const char *supportId,
                                             bool generateMiniBundle,
                                             uint64_t supportCaseId,
                                             NodeId nodeId);

    SupportBundle();
    ~SupportBundle();
    SupportBundle(const SupportBundle &) = delete;
    SupportBundle &operator=(const SupportBundle &) = delete;
};

#endif  // _SUPPORTBUNDLE_H_

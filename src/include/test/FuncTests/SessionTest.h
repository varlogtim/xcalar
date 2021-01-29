// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SESSION_TEST_H_
#define _SESSION_TEST_H_

#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"

extern Status sessionStress();

Status libsessionFuncTestParseConfig(Config::Configuration *config,
                                     char *key,
                                     char *value,
                                     bool stringentRules);

class UserMgr;

class TestSession
{
  public:
    bool init(int tid, int seqNum);
    void testOperations();

    bool isValid() { return isValid_; };

    void inactivate(bool checkDag);
    void activate();
    void downloadUpload();
    void rename();
    void doCleanup();

  private:
    UserMgr *userMgr_;
    XcalarApiUserId sessionUser_;
    uint64_t sessionId_;
    char sessionName_[256];
    char uploadedSessionName_[256];
    char datasetName_[256];
    char prefixDatasetName_[256];
    // Represents whether this session's operations have thus far succeeded
    bool isValid_ = false;
    // Set true if successful upload has completed.  The flag is used to
    // destroy such session at cleanup time.
    bool successfulUpload_ = false;
};

#endif  // _SESSION_TEST_H

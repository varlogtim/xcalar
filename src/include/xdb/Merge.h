// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MERGE_H_
#define _MERGE_H_

#include "operators/GenericTypes.h"
#include "xdb/Merge.h"
#include "xdb/Xdb.h"

class MergeMgr
{
  private:
    static constexpr const uint64_t StatsCount = 18;
    StatGroupId statsGrpId_;

  public:
    static MustCheck MergeMgr *get();
    static MustCheck Status init();
    void destroy();

    enum ImdXdbs {
        ImdPendingXdb,
        ImdCommittedXdb,
        ImdXdbCount,
    };

    struct Stats {
        StatHandle init;
        StatHandle initFailure;
        StatHandle initLocal;
        StatHandle initLocalFailure;
        StatHandle prepare;
        StatHandle prepareFailure;
        StatHandle prepareLocal;
        StatHandle prepareLocalFailure;
        StatHandle commit;
        StatHandle commitFailure;
        StatHandle postCommit;
        StatHandle postCommitFailure;
        StatHandle postCommitLocal;
        StatHandle postCommitLocalFailure;
        StatHandle abort;
        StatHandle abortFailure;
        StatHandle abortLocal;
        StatHandle abortLocalFailure;
    };

    class MergeInfo
    {
      public:
        MergeInfo() {}

        ~MergeInfo();

        MustCheck Status init(Xdb *deltaXdb, Xdb *targetXdb);
        MustCheck Xdb *getImdPendingXdb();
        MustCheck Xdb *getImdCommittedXdb();
        MustCheck Xdb *getTargetXdb();
        MustCheck XdbMeta *getImdPendingXdbMeta();
        MustCheck XdbMeta *getImdCommittedXdbMeta();
        MustCheck XdbMeta *getTargetXdbMeta();
        MustCheck VersionId getImdPendingVersionNum();
        void setImdPendingVersionNum(uint64_t versionNum);
        void resetImdXdbForLoadDone(Xdb *imdXdb);
        void setupPostMerge();
        void teardownPostMerge(Status mergeStatus);
        void incImdSlotsProcessedCounter();
        int64_t getImdSlotsProcessedCounter();
        void updateImdPendingMergeRowCount(int64_t rowCount);
        int64_t getImdPendingMergeRowCount();

      private:
        NewTupleMeta tupMeta_;
        XdbMeta *xdbMeta_ = NULL;
        Xdb *imdPendingXdb_ = NULL;
        Xdb *imdCommittedXdb_ = NULL;
        Xdb *targetXdb_ = NULL;
        VersionId imdPendingVersionNum_ = InvalidVersion;
        Mutex imdPostMergeLock_;
        Atomic64 imdSlotsProcessedCounter_;
        Atomic64 imdPendingMergeRowCount_;

        void freeXdb(Xdb *fXdb);
        MustCheck XdbMeta *setupXdbMeta(XdbMeta *deltaXdbMeta,
                                        XdbMeta *targetXdbMeta);
        MustCheck Status initXdb(Xdb *xdb,
                                 XdbMeta *deltaXdbMeta,
                                 XdbMeta *targetXdbMeta);
        void fixupTargetXdbSlotAugs();
    };

    MustCheck Stats *getStats() { return &stats_; }

  private:
    static MergeMgr *instance;
    Stats stats_;

    MustCheck Status initInternal();

    // Keep this private, use init instead
    MergeMgr() {}

    // Keep this private, use destroy instead
    ~MergeMgr() {}

    MergeMgr(const MergeMgr &) = delete;
    MergeMgr(const MergeMgr &&) = delete;
    MergeMgr &operator=(const MergeMgr &) = delete;
    MergeMgr &operator=(const MergeMgr &&) = delete;
};

#endif  // _MERGE_H_

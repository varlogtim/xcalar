// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _STATISTICS_H_
#define _STATISTICS_H_

#include <stdio.h>
#include <thread>
#include <unistd.h>
#include "util/Atomics.h"
#include "operators/GenericTypes.h"
#include "stat/StatisticsTypes.h"
#include "msg/Message.h"
#include "hash/Hash.h"
#include "xcalar/compute/localtypes/Stats.pb.h"
#include "msg/TwoPcFuncDefsClient.h"

struct XcalarApiOutput;
struct MsgEphemeral;
struct XcalarApiTopInput;
struct XcalarApiTopOutputPerNode;

class StatsLib
{
  public:
    static constexpr uint64_t cGroupMaxPathLen = 255;
    char xceCgroupPath_[cGroupMaxPathLen + 1];
    char usrXpusCgroupPath_[cGroupMaxPathLen + 1];
    char sysXpusCgroupPath_[cGroupMaxPathLen + 1];
    char cgroupsMemoryPath_[cGroupMaxPathLen + 1];
    char cgroupsCpuPath_[cGroupMaxPathLen + 1];

    // All stats can be viewed as diff stats if this bool is set to true. For
    // now, this is a global control for all stats and not for each individual
    // stat.
    static MustCheck Status createSingleton();
    static void deleteSingleton();
    static MustCheck bool isInit();
    static MustCheck StatsLib *get();

    MustCheck Status initNewStatGroup(const char *groupName,
                                      StatGroupId *groupId,
                                      uint64_t size);

    MustCheck Status initAndMakeGlobal(StatGroupId groupId,
                                       const char *statName,
                                       StatHandle handle,
                                       StatValType statValType,
                                       StatType statType,
                                       uint32_t statMaxRefVal);

    Status getStatsReq(NodeId nodeId,
                       XcalarApiOutput **outputOut,
                       size_t *outputSizeOut);
    void getStatsLocal(MsgEphemeral *eph);
    void getStatsCompletion(MsgEphemeral *eph, void *payload);

    Status getStatsGroupIdMapReq(NodeId nodeId,
                                 XcalarApiOutput **outputOut,
                                 size_t *outputSizeOut);
    void getStatsGroupIdMapLocal(MsgEphemeral *eph);

    MustCheck Status initStatHandle(StatHandle *handle);

    // Only called by twoPc
    void top(MsgEphemeral *eph, void *payload);

    MustCheck Status getTopStats(XcalarApiTopInput *input,
                                 XcalarApiTopOutputPerNode *topOutputPerNode,
                                 bool detailedCpuStats=true);

    struct GetStatsOutput {
        Status status;
        xcalar::compute::localtypes::Stats::GetStatsResponse *response;
        Mutex statsMutex;
    };

    MustCheck Status getPerfStats(
        bool getMeta,
        xcalar::compute::localtypes::Stats::GetStatsPerHostResponse *response);

    MustCheck Status getStatsCommonProto(
        xcalar::compute::localtypes::Stats::GetLibstatsResponse *response,
        StatGroupId *inputStatsGroupId,
        uint64_t inputSize,
        bool allStatsGroups);

    static void resetStats(bool resetHwmStats, bool resetCumulativeStats);

    // inline functions
    static void statAtomicIncr32(StatHandle handle)
    {
        atomicInc32(&handle->statUint32);
    }

    static void statAtomicDecr32(StatHandle handle)
    {
        atomicDec32(&handle->statUint32);
    }

    static void statAtomicIncr64(StatHandle handle)
    {
        atomicInc64(&handle->statUint64Atomic);
    }

    static void statAtomicDecr64(StatHandle handle)
    {
        atomicDec64(&handle->statUint64Atomic);
    }

    // Certain stats like dirty values can use a non-atomic
    // increment. This works for both 32 and 64 bit ints because the ordering of
    // the fields in union Stat.
    static void statNonAtomicIncr(StatHandle handle) { ++(handle->statUint64); }

    static void statNonAtomicDecr(StatHandle handle) { --(handle->statUint64); }

    static void statNonAtomicAdd(StatHandle handle, uint32_t num)
    {
        handle->statUint64 += num;
    }

    static void statNonAtomicSubtr(StatHandle handle, uint32_t num)
    {
        handle->statUint64 -= num;
    }

    static void statNonAtomicSubtr64(StatHandle handle, uint64_t num)
    {
        handle->statUint64 -= num;
    }

    static void statNonAtomicAdd64(StatHandle handle, uint64_t num)
    {
        handle->statUint64 += num;
    }

    static void statAtomicAdd32(StatHandle handle, uint32_t num)
    {
        (void) atomicAdd32(&handle->statUint32, num);
    }

    static void statNonAtomicSetDouble(StatHandle handle, double num)
    {
        handle->statDouble = num;
    };

    static void statNonAtomicAddDouble(StatHandle handle, double num)
    {
        handle->statDouble += num;
    }

    static void statNonAtomicSet64(StatHandle handle, uint64_t num)
    {
        handle->statUint64 = num;
    }

    static MustCheck uint64_t statReadUint64(StatHandle handle)
    {
        return handle->statUint64;
    }

    static void statAtomicAdd64(StatHandle handle, uint64_t num)
    {
        (void) atomicAdd64(&handle->statUint64Atomic, num);
    }

    static void statAtomicSubtr64(StatHandle handle, uint64_t num)
    {
        (void) atomicSub64(&handle->statUint64Atomic, num);
    }

    MustCheck Status getGroupIdFromName(const char *groupName,
                                        uint64_t *groupId);

    // for tests to remove their stats group before re-initing
    // we need a clean way to tear down and all tests should
    // invoke that instead
    void removeGroupIdFromHashTable(const char *groupName);

    void addPerfStats(MsgEphemeral *ephemeral, void *payload);

  private:
    static constexpr uint64_t SlotsPerGroupNameToIdTable = 256;

    struct GroupNameToIdMap {
        StringHashTableHook hook;
        const char *getGroupName() const { return groupName; };
        uint64_t groupId;
        // this will be pointing to groupName on stats buffer
        // we tear down stats buffer only when usrnode is going down
        // hence it is OK to dereference this unless stats design changes
        const char *groupName;
        void del();
    };

    Mutex groupNameToIdTableLock_;

    StringHashTable<GroupNameToIdMap,
                    &GroupNameToIdMap::hook,
                    &GroupNameToIdMap::getGroupName,
                    SlotsPerGroupNameToIdTable,
                    hashStringFast>
        groupNameToIdTable_;

    typedef struct StatsGroupInfo {
        uint64_t startIdx;
        unsigned idx;
        unsigned size;
        StatGroupName statsGroupName;
    } StatsGroupInfo;

    // Info per stat
    // Note: There can be much optimization to reduce the memory footprint of
    // stats. For now keep things simple until we get a better sense of what
    // functionalty is needed.
    typedef struct SingleStat {
        char statName[StatStringLen];
        Stat *statAddr;  // Memory allocated by caller of statMakeGlobal()
        StatValType statValType;
        StatType statType;
        uint32_t expiryCount;
        uint64_t groupId;
    } SingleStat;

    // Minimum info per stat for diff stats
    typedef struct SingleStatMin {
        Stat statVal;  // actual stat value
    } SingleStatMin;

    enum { MaxStatsGroupNum = 16 * KB, MaxStatsNum = 128 * KB };

    static bool statsInited;
    static StatsLib *statsLib;
    static bool loggedUptimeError;

    // number of values we read from proc/net
    static const unsigned headerIdx = 2;
    static const unsigned recvIdx = 1;

    static const unsigned sendIdx = 9;

    static constexpr const char *loopBackInterface = "lo:";

    StatsGroupInfo *statsGroupInfoArray;
    SingleStat *statsArray;
    Mutex statLock;
    StatGroupId groupIdx;
    unsigned statIndex;

    struct timespec bootTime_;

    StatGroupId statsMetaGroupId;
    StatHandle statNumStats;
    StatHandle statHwmResetTimestamp;
    StatHandle statCumResetTimestamp;

    MustCheck Status getMyResidentSetSize(uint64_t *rssInBytes);

    MustCheck Status getMemoryUsage(double *myMemUsageInPercent,
                                    uint64_t *totalUsedSysMemoryInBytes,
                                    uint64_t *totalSysMemoryInBytes,
                                    uint64_t *sysSwapUsedInBytes,
                                    uint64_t *sysSwapTotalInBytes);

    MustCheck Status getCpuUsageAbsoluteInUSeconds(double *parentCpuUsage,
                                                   double *childrenCpuUsage,
                                                   double *totalCpuUsage,
                                                   uint64_t *numCores);

    MustCheck Status getTotalCpuUsageAbsoluteInUSeconds(double *totalCpuUsage,
                                                    uint64_t *numCores);

    MustCheck Status getUptimeInSeconds(uint64_t *uptimeInSeconds);

    void calculateCpuUsage(struct rusage *rUsage, double *cpuUsage);

    MustCheck Status getCpuUsageOfThisProcess(pid_t processId,
                                              uint64_t *totalJiffies);

    MustCheck Status getCpuFromAllChildren(pid_t *listOfChildren,
                                           size_t numChildren,
                                           double *childrenCpuUsage,
                                           long numJiffiesPerSecond);

    MustCheck Status getStatsCommon(XcalarApiOutput *output,
                                    size_t *inoutSize,
                                    StatGroupId *inputStatsGroupId,
                                    uint64_t inputSize,
                                    bool allStatsGroups);

    MustCheck Status createInternal();

    MustCheck Status readNetworkTraffic(uint64_t *recvBytes,
                                        uint64_t *sentBytes);

    MustCheck Status groupIdHashInsert(GroupNameToIdMap *groupNameToIdMap);

    MustCheck GroupNameToIdMap *groupIdHashFind(const char *groupName);

    MustCheck GroupNameToIdMap *groupIdHashRemove(const char *groupName);

    MustCheck Status retrieveStatsFromXdbGroups(uint64_t *usedBytes,
                                                uint64_t *totalBytes);

    MustCheck Status numFastAllocUsedAndTotalBytesByBufCacheObj(
        StatGroupId groupId, uint64_t *usedBytes, uint64_t *totalBytes);

    MustCheck SingleStat *getSingleStatObjectByStatName(
        StatsGroupInfo *statsGroupInfo, const char *singleStatName);

    MustCheck Status fastAllocBytesUsedByBufCacheObj(
        StatsGroupInfo *statsGroupInfo, uint64_t *usedBytes);

    MustCheck Status getDatasetUsage(uint64_t *bytesUsed);

    MustCheck Status getStats(XcalarApiOutput *output, size_t *inoutSize);

    MustCheck Status getStatsByGroupId(XcalarApiOutput *statOutput,
                                       size_t *outputSize,
                                       StatGroupId *inputStatsGroupId,
                                       uint64_t inputSize);

    MustCheck uint64_t getTotalStatsSize();

    MustCheck Status allocAndGetStats(void **output, size_t *outputSize);

    MustCheck Status allocAndGetGroupIdMap(void **outputBuffer,
                                           size_t *allocatedSize);

    MustCheck StatGroupName *getGroupIdNameFromGroupId(StatGroupId groupId);

    struct MsgStatsResponse {
        Status status = StatusOk;
        void *output = NULL;
        size_t outputSize = 0;
    };

    StatsLib(){};
    ~StatsLib(){};

    StatsLib(const StatsLib &) = delete;
    StatsLib &operator=(const StatsLib &) = delete;
};

class TwoPcMsg2pcGetPerfStats1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcGetPerfStats1() {}
    virtual ~TwoPcMsg2pcGetPerfStats1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcGetPerfStats1(const TwoPcMsg2pcGetPerfStats1 &) = delete;
    TwoPcMsg2pcGetPerfStats1(const TwoPcMsg2pcGetPerfStats1 &&) = delete;
    TwoPcMsg2pcGetPerfStats1 &operator=(const TwoPcMsg2pcGetPerfStats1 &) =
        delete;
    TwoPcMsg2pcGetPerfStats1 &operator=(const TwoPcMsg2pcGetPerfStats1 &&) =
        delete;
};

#endif  // _STATISTICS_H_

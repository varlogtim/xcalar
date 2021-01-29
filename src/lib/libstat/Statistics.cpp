// Copyright 2013 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <stdio.h>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <stdio.h>
#include <stdlib.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "primitives/Subsys.h"
#include "stat/Statistics.h"
#include "config/Config.h"
#include "util/System.h"
#include "libapis/LibApisCommon.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "dataset/Dataset.h"
#include "service/StatsService.h"

#include "msgstream/MsgStream.h"
#include "libapis/ApiHandlerGetStat.h"
#include "parent/Parent.h"

#include "util/XcalarSysHelper.h"
#include "util/CgroupMgr.h"
#include "xdb/HashTree.h"

#define CGROUP_BUFFER_SIZE 100

static constexpr const char *moduleName = "libstat";

bool StatsLib::statsInited = false;
StatsLib *StatsLib::statsLib = NULL;
bool StatsLib::loggedUptimeError = false;

bool
StatsLib::isInit()
{
    return statsInited;
}

Status
StatsLib::getGroupIdFromName(const char *groupName, uint64_t *groupId)
{
    GroupNameToIdMap *groupNameToIdMap = NULL;

    assert(groupName != NULL);

    groupNameToIdMap = groupIdHashFind(groupName);
    if (groupNameToIdMap == NULL) {
        return StatusInval;
    }

    *groupId = groupNameToIdMap->groupId;

    return StatusOk;
}

Status
StatsLib::groupIdHashInsert(GroupNameToIdMap *groupNameToIdMap)
{
    Status status = StatusOk;

    assert(groupNameToIdMap != NULL);
    assert(groupNameToIdMap->groupName != NULL);

    groupNameToIdTableLock_.lock();
    status = groupNameToIdTable_.insert(groupNameToIdMap);
    groupNameToIdTableLock_.unlock();

    return status;
}

StatsLib::GroupNameToIdMap *
StatsLib::groupIdHashFind(const char *groupName)
{
    GroupNameToIdMap *groupNameToIdMap = NULL;

    assert(groupName != NULL);

    groupNameToIdTableLock_.lock();
    groupNameToIdMap = groupNameToIdTable_.find(groupName);
    groupNameToIdTableLock_.unlock();

    return groupNameToIdMap;
}

StatsLib::GroupNameToIdMap *
StatsLib::groupIdHashRemove(const char *groupName)
{
    GroupNameToIdMap *groupNameToIdMap = NULL;
    assert(groupName != NULL);

    groupNameToIdTableLock_.lock();
    groupNameToIdMap = groupNameToIdTable_.remove(groupName);
    groupNameToIdTableLock_.unlock();

    return groupNameToIdMap;
}

void
StatsLib::removeGroupIdFromHashTable(const char *groupName)
{
    GroupNameToIdMap *groupNameToIdMap = NULL;

    assert(groupName != NULL);

    groupNameToIdMap = groupIdHashRemove(groupName);

    if (groupNameToIdMap != NULL) {
        memFree(groupNameToIdMap);
    }
}

void
StatsLib::GroupNameToIdMap::del()
{
    memFree(this);
}

Status
StatsLib::createSingleton()
{
    Status status;
    void *ptr = NULL;

    ptr = memAllocExt(sizeof(StatsLib), __PRETTY_FUNCTION__);
    if (ptr == NULL) {
        return StatusNoMem;
    }

    StatsLib::statsLib = new (ptr) StatsLib();

    status = StatsLib::statsLib->createInternal();

    if (status != StatusOk) {
        if (ptr != NULL) {
            StatsLib::statsLib->~StatsLib();
            memFree(ptr);
        }
    }

    return status;
}

// There is only one statistics object and it has global scope.
// This module uses coarse grained locking as this is all slow path.
Status
StatsLib::createInternal()
{
    StatsLib *statsLib = StatsLib::get();
    Status status = StatusOk;
    char *sliceUnitEnv = NULL;
    char *usrnodeUnitEnv = NULL;
    char *controllerMap = NULL;
    char controllerMapCopy[statsLib->cGroupMaxPathLen];
    char *controllerMapsSplit = NULL;
    // default values
    const char *usrnodeUnitName = "xcalar-usrnode.service";
    const char *sliceUnitName = "xcalar.slice";
    const char *memoryPathDefault = "/sys/fs/cgroup/memory";
    const char *cpuPathDefault = "/sys/fs/cgroup/cpu,cpuacct";

    bool memoryPathFound = false;
    bool cpuPathFound = false;

    assertStatic(MaxStatsNum > MaxStatsGroupNum);

    this->groupIdx = 0;
    this->statIndex = 0;
    this->statsArray = NULL;
    this->statsGroupInfoArray = NULL;

    this->statsGroupInfoArray =
        (StatsGroupInfo *) memAllocExt(sizeof(StatsGroupInfo) *
                                           MaxStatsGroupNum,
                                       __PRETTY_FUNCTION__);

    if (this->statsGroupInfoArray == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    this->statsArray =
        (SingleStat *) memAllocExt(sizeof(SingleStat) * MaxStatsNum,
                                   __PRETTY_FUNCTION__);

    if (this->statsArray == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    StatsLib::statsInited = true;

    status = this->initNewStatGroup("stats", &this->statsMetaGroupId, 3);
    BailIfFailed(status);

    status = this->initStatHandle(&this->statNumStats);
    BailIfFailed(status);
    status = this->initAndMakeGlobal(this->statsMetaGroupId,
                                     "numStats",
                                     this->statNumStats,
                                     StatUint64,
                                     StatAbsoluteWithNoRefVal,
                                     StatRefValueNotApplicable);
    status = this->initStatHandle(&this->statHwmResetTimestamp);
    BailIfFailed(status);
    status = this->initAndMakeGlobal(this->statsMetaGroupId,
                                     "lastHwmResetTimestamp",
                                     this->statHwmResetTimestamp,
                                     StatUint64,
                                     StatAbsoluteWithNoRefVal,
                                     StatRefValueNotApplicable);
    status = this->initStatHandle(&this->statCumResetTimestamp);
    BailIfFailed(status);
    status = this->initAndMakeGlobal(this->statsMetaGroupId,
                                     "lastCumResetTimestamp",
                                     this->statCumResetTimestamp,
                                     StatUint64,
                                     StatAbsoluteWithNoRefVal,
                                     StatRefValueNotApplicable);
    BailIfFailed(status);

    if (clock_gettime(CLOCK_REALTIME, &bootTime_)) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
    ;

    sliceUnitEnv = getenv("XCE_XCALAR_SLICE");
    if (sliceUnitEnv != NULL) {
        sliceUnitName = sliceUnitEnv;
    }

    usrnodeUnitEnv = getenv("XCE_USRNODE_UNIT");
    if (usrnodeUnitEnv != NULL) {
        usrnodeUnitName = usrnodeUnitEnv;
    }

    status = strSnprintf(statsLib->xceCgroupPath_,
                        sizeof(statsLib->xceCgroupPath_),
                        "%s/%s/xcalar-usrnode.scope",
                        sliceUnitName,
                        usrnodeUnitName);
    BailIfFailed(status);

    status = strSnprintf(statsLib->sysXpusCgroupPath_,
                        sizeof(statsLib->sysXpusCgroupPath_),
                        "%s/%s/sys_xpus-",
                        sliceUnitName,
                        usrnodeUnitName);
    BailIfFailed(status);

    status = strSnprintf(statsLib->usrXpusCgroupPath_,
                        sizeof(statsLib->usrXpusCgroupPath_),
                        "%s/%s/usr_xpus-",
                        sliceUnitName,
                        usrnodeUnitName);
    BailIfFailed(status);
    xSyslog(moduleName,
            XlogInfo,
            "xceCgroupPath: \"%s\", sysXpusCgroupPath: \"%s\", usrXpusCgroupPath: \"%s\"",
            statsLib->xceCgroupPath_,
            statsLib->sysXpusCgroupPath_,
            statsLib->usrXpusCgroupPath_);

    controllerMap = getenv("XCE_CGROUP_CONTROLLER_MAP");
    if (controllerMap != NULL) {
        // Must take care not to modify pointer to string returned by getenv() call, since
        // that would change the environment of the process. So creating a copy.
        status = strSnprintf(controllerMapCopy,
                            sizeof(controllerMapCopy),
                           "%s",
                            controllerMap);
        BailIfFailed(status);
        // Format of controllerMap is <controller>%<path>:<controller>%<path>..
        controllerMapsSplit = strtok(controllerMapCopy, "%:");
        while(controllerMapsSplit != NULL && (!memoryPathFound || !cpuPathFound)) {
            if (!memoryPathFound && (strcmp(controllerMapsSplit, "memory") == 0)) {
                // the next value is the memory path
                controllerMapsSplit = strtok(NULL, "%:");
                if (controllerMapsSplit == NULL) {
                    break;
                }
                status = strSnprintf(statsLib->cgroupsMemoryPath_,
                                    sizeof(statsLib->cgroupsMemoryPath_),
                                    "%s",
                                    controllerMapsSplit);
                BailIfFailed(status);
                memoryPathFound = true;
            } else if (!cpuPathFound && (strcmp(controllerMapsSplit, "cpuacct") == 0)) {
                // the next value is the cpuacct path
                controllerMapsSplit = strtok(NULL, "%:");
                if (controllerMapsSplit == NULL) {
                    break;
                }
                status = strSnprintf(statsLib->cgroupsCpuPath_,
                                    sizeof(statsLib->cgroupsCpuPath_),
                                    "%s",
                                    controllerMapsSplit);
                BailIfFailed(status);
                cpuPathFound = true;
            }
            controllerMapsSplit = strtok(NULL, "%:");
        }
    }
    // if cgropus memory path not found, use default
    if (!memoryPathFound) {
        status = strSnprintf(statsLib->cgroupsMemoryPath_,
                            sizeof(statsLib->cgroupsMemoryPath_),
                            "%s",
                            memoryPathDefault);
        xSyslog(moduleName,
            XlogErr,
            "cgroupsMemoryPath not found. Using default: \"%s\"",
            memoryPathDefault);
    }
    // if cgroups cpu path not found, use default
    if (!cpuPathFound) {
        status = strSnprintf(statsLib->cgroupsCpuPath_,
                            sizeof(statsLib->cgroupsCpuPath_),
                            "%s",
                            cpuPathDefault);
        xSyslog(moduleName,
            XlogErr,
            "cgroupsCpuPath not found. Using default: \"%s\"",
            cpuPathDefault);
    }

    xSyslog(moduleName,
            XlogInfo,
            "cgroupsMemoryPath: \"%s\", cgroupsCpuPath: \"%s\"",
            statsLib->cgroupsMemoryPath_,
            statsLib->cgroupsCpuPath_);
CommonExit:
    if (status != StatusOk) {
        StatsLib::statsInited = false;
        if (this->statsGroupInfoArray != NULL) {
            memFree(this->statsGroupInfoArray);
        }

        if (this->statsArray != NULL) {
            memFree(this->statsArray);
        }
    }

    return status;
}

void
StatsLib::deleteSingleton()
{
    unsigned ii, jj;
    StatsGroupInfo *statsGroupInfo = NULL;

    if (!statsInited) {
        return;
    }
    StatsLib *statsLib = StatsLib::get();

    statsInited = false;
    for (ii = 0; ii < statsLib->groupIdx; ++ii) {
        statsGroupInfo = &statsLib->statsGroupInfoArray[ii];
        for (jj = 0; jj < statsGroupInfo->idx; ++jj) {
            memFree(
                statsLib->statsArray[statsGroupInfo->startIdx + jj].statAddr);
        }
    }

    statsLib->groupNameToIdTable_.removeAll(&GroupNameToIdMap::del);

    memFree(statsLib->statsGroupInfoArray);
    statsLib->statsGroupInfoArray = NULL;

    memFree(statsLib->statsArray);
    statsLib->statsArray = NULL;

    statsLib->~StatsLib();
    memFree(statsLib);
}

StatsLib *
StatsLib::get()
{
    assert(statsLib);
    return statsLib;
}

// The caller must decide the number of stats will be in the group beforehand,
// and this function will reserve the spaces for the newly created group
Status
StatsLib::initNewStatGroup(const char *groupName,
                           StatGroupId *groupId,
                           uint64_t size)
{
    GroupNameToIdMap *groupNameToIdMap = NULL;

    assert(statsInited);

    Status status = StatusOk;
    StatsGroupInfo *statsGroupInfo = NULL;
    int ret;
    bool insertedToHash = false;
    bool lockAcquired = false;

    groupNameToIdMap =
        (GroupNameToIdMap *) memAllocExt(sizeof(GroupNameToIdMap), moduleName);
    BailIfNull(groupNameToIdMap);

    this->statLock.lock();
    lockAcquired = true;

    *groupId = this->groupIdx;

    if (*groupId >= MaxStatsGroupNum) {
        status = StatusMaxStatsGroupExceeded;
        goto CommonExit;
    }

    if (this->statIndex + size >= MaxStatsNum) {
        status = StatusMaxStatsExceeded;
        goto CommonExit;
    }

    statsGroupInfo = &this->statsGroupInfoArray[*groupId];
    memZero(statsGroupInfo, sizeof(*statsGroupInfo));

    ret = strlcpy(statsGroupInfo->statsGroupName,
                  groupName,
                  sizeof(statsGroupInfo->statsGroupName));

    if ((unsigned) ret >= sizeof(statsGroupInfo->statsGroupName)) {
        status = StatusStatsGroupNameTooLong;
        goto CommonExit;
    }

    groupNameToIdMap->groupId = *groupId;
    groupNameToIdMap->groupName = statsGroupInfo->statsGroupName;
    status = groupIdHashInsert(groupNameToIdMap);
    if (status != StatusOk) {
        // we reach here if someone is trying to init a new stats
        // group with a duplicate groupName
        // There are a few occurrences in func tests
        // which have been fixed; can not be here in production
        assert(0);
        goto CommonExit;
    }
    insertedToHash = true;

    statsGroupInfo->startIdx = this->statIndex;
    statsGroupInfo->size = size;
    statsGroupInfo->idx = 0;

    // reserved spaces for the newly created group
    this->statIndex += size;
    this->groupIdx++;

CommonExit:
    if (lockAcquired == true) {
        this->statLock.unlock();
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "initNewStatGroup(groupName=\"%s\", size=%lX) failed: %s",
                groupName,
                size,
                strGetFromStatus(status));
        if (groupNameToIdMap != NULL) {
            if (insertedToHash == true) {
                GroupNameToIdMap *tmp = NULL;
                tmp = groupIdHashRemove(groupNameToIdMap->groupName);
                assert(tmp == groupNameToIdMap);
            }
            memFree(groupNameToIdMap);
        }

        *groupId = 0;
    }

    return status;
}

Status
StatsLib::allocAndGetGroupIdMap(void **outputBuffer, size_t *allocatedSize)
{
    Status status = StatusOk;
    void *output = NULL;
    XcalarApiOutput *apiOut = NULL;
    XcalarApiGetStatGroupIdMapOutput *statGroupIdMapOutput = NULL;

    StatsGroupInfo *cursorForGroupInfo = NULL;
    XcalarStatGroupInfo *cursorForOutput = NULL;

    size_t outputSize = 0;
    size_t numGroups = 0;

    assert(statsInited == true);
    assert(outputBuffer != NULL);
    assert(*outputBuffer == NULL);
    assert(allocatedSize != NULL);
    *allocatedSize = 0;

    // dirty read and we ship back so many groupIds' back
    numGroups = this->groupIdx;

    outputSize = XcalarApiSizeOfOutput(XcalarApiGetStatGroupIdMapOutput) +
                 (numGroups * sizeof(XcalarStatGroupInfo)) + sizeof(size_t);

    output = memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    apiOut = (XcalarApiOutput *) ((uintptr_t) output + sizeof(size_t));
    statGroupIdMapOutput = &apiOut->outputResult.statGroupIdMapOutput;

    for (unsigned ii = 0; ii < numGroups; ++ii) {
        cursorForGroupInfo = &(this->statsGroupInfoArray[ii]);
        cursorForOutput = &(statGroupIdMapOutput->groupNameInfo[ii]);

        cursorForOutput->groupIdNum = ii;

        // note that we are copying this without holding the lock
        // so if there is func tests running, cursorForGroupInfo->size
        // need not be equal to cursorForGroupInfo->idx, since group
        // may be registered but actual stat handles are being registered

        cursorForOutput->totalSingleStats = cursorForGroupInfo->size;
        verifyOk(strStrlcpy(cursorForOutput->statsGroupName,
                            cursorForGroupInfo->statsGroupName,
                            sizeof(StatGroupName)));
    }

    if (numGroups != this->groupIdx) {
        assert(numGroups < this->groupIdx);
        statGroupIdMapOutput->truncated = true;
    } else {
        statGroupIdMapOutput->truncated = false;
    }
    statGroupIdMapOutput->numGroupNames = numGroups;

    *((size_t *) output) = outputSize - sizeof(size_t);
    *outputBuffer = output;
    *allocatedSize = outputSize;
    apiOut->hdr.status = StatusOk.code();

CommonExit:
    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
        }
    }

    return status;
}

// this is a helper routine only for stats sender
// there wont be out of boundary requests hence
// just asserts for boundary checks
StatGroupName *
StatsLib::getGroupIdNameFromGroupId(StatGroupId groupId)
{
    assert(groupId < this->groupIdx);
    assert(this->statsGroupInfoArray[groupId].size > 0);

    return &(this->statsGroupInfoArray[groupId].statsGroupName);
}

void
StatsLib::getStatsGroupIdMapLocal(MsgEphemeral *eph)
{
    Status status = StatusOk;
    size_t outputSize = 0;

    // XXX Needed because of hack in Message.cpp to free
    // msg->payload and assign msg->payload to eph->payloadToDistribute.
    eph->payloadToDistribute = NULL;

    status = allocAndGetGroupIdMap(&eph->payloadToDistribute, &outputSize);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    eph->setAckInfo(status, outputSize);
}

// Publish stat into global addressing range.
// Stats can be cumulative (uint64_t) or just absolute values(uint32_t).
// Absolute values may be accompanied by a non-zero max reference value
// for that stat so that we can calculate percentages.
// Absolute stats should use the prefix statAbs for scripts to work.
// For example, statAbsAllocedMessages.
Status
StatsLib::initAndMakeGlobal(StatGroupId groupId,
                            const char *statName,
                            StatHandle handle,
                            StatValType statValType,
                            StatType statType,
                            uint32_t statMaxRefVal)
{
    SingleStat *singleStat = NULL;
    Status status = StatusOk;
    StatsGroupInfo *statsGroupInfo;
    Stat *statAddr = handle;
    bool statLockAck = false;
    int ret;
    bool statInserted = false;

    assert(statsInited);
    assert(statAddr != NULL);

#ifdef DEBUG
    // delimiter for XEM stats shipment
    const char *found = strchrnul(statName, '|');
    if (*found != '\0') {
        assert(0);
    }

    // graphite drops stats if there are spaces in them
    found = strchrnul(statName, ' ');
    if (*found != '\0') {
        assert(0);
    }
#endif  // DEBUG

    this->statLock.lock();
    statLockAck = true;

    if (groupId >= this->groupIdx) {
        assert(0);
        status = StatusStatsInvalidGroupId;
        goto CommonExit;
    }

    statsGroupInfo = &this->statsGroupInfoArray[groupId];

    assert(statsGroupInfo->startIdx + statsGroupInfo->size <= this->statIndex);

    if (statsGroupInfo->idx == statsGroupInfo->size) {
        assert(0);
        status = StatusStatsGroupIsFull;
        goto CommonExit;
    }

    singleStat =
        &this->statsArray[statsGroupInfo->startIdx + (statsGroupInfo->idx)];

    // initialize stat
    ret = strlcpy(singleStat->statName, statName, sizeof(singleStat->statName));

    if ((unsigned) ret >= sizeof(singleStat->statName)) {
        status = StatusStatsNameTooLong;
        goto CommonExit;
    }

    memZero(statAddr, sizeof(*statAddr));

    singleStat->statAddr = statAddr;
    statInserted = true;

    singleStat->statValType = statValType;
    singleStat->statType = statType;
    singleStat->expiryCount = 0;
    singleStat->groupId = groupId;

    switch (statType) {
    case StatAbsoluteWithRefVal:
        // StatAbsoluteWithRef must have a non zero value to be able to
        // compute percentage.
        singleStat->statAddr->statMaxRefValUint32 = statMaxRefVal;
        assert(statMaxRefVal);
        break;

    case StatCumulative:
    case StatHWM:
    case StatAbsoluteWithNoRefVal:
        assert(statMaxRefVal == StatRefValueNotApplicable);
        break;

    default:
        assert(0);
        status = StatusUnimpl;
    }

    StatsLib::statAtomicIncr32(this->statNumStats);

    statsGroupInfo->idx++;

CommonExit:

    if (statLockAck) {
        this->statLock.unlock();
        statLockAck = false;
    }
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "initAndMakeGlobal failed for statName=\"%s\": %s",
                statName,
                strGetFromStatus(status));
        if (!statInserted) {
            memFree(statAddr);
        }
    }

    return status;
}

// This is the static function called by getLibstats
Status
StatsLib::getStatsCommonProto(
    xcalar::compute::localtypes::Stats::GetLibstatsResponse *response,
    StatGroupId *inputStatsGroupId,
    uint64_t inputSize,
    bool allStatsGroups)
{
    assert(statsInited);

    Status status = StatusOk;
    StatType statType;
    uint64_t ii, jj;
    uint64_t idx = 0;
    StatGroupId groupId;
    StatsGroupInfo *statsGroupInfo = NULL;
    SingleStat *singleStat = NULL;

    this->statLock.lock();
    if (allStatsGroups) {
        inputSize = this->groupIdx;
    }

    assert(inputSize <= this->groupIdx);

    for (ii = 0; ii < inputSize; ii++) {
        if (allStatsGroups) {
            assert(inputStatsGroupId == NULL);
            groupId = ii;
        } else {
            groupId = inputStatsGroupId[ii];
        }

        statsGroupInfo = &this->statsGroupInfoArray[groupId];

        if (statsGroupInfo == NULL) {
            assert(0);
            continue;
        }

        for (jj = 0; jj < statsGroupInfo->idx; ++jj) {
            assert(statsGroupInfo->startIdx + jj < this->statIndex);
            singleStat = &this->statsArray[statsGroupInfo->startIdx + jj];
            xcalar::compute::localtypes::Stats::LibstatNode *node =
                response->add_libstatnodes();
            node->set_statname(
                std::string(singleStat->statName,
                            strnlen(singleStat->statName,
                                    sizeof(singleStat->statName))));

            statType = singleStat->statType;
            node->set_stattype(
                static_cast<xcalar::compute::localtypes::Stats::StatType>(
                    statType));

            node->set_groupid(singleStat->groupId);

            switch (singleStat->statValType) {
            case StatUint64:
                node->mutable_statvalue()->set_vint(
                    singleStat->statAddr->statUint64);
                break;
            case StatDouble:
                node->mutable_statvalue()->set_vdouble(
                    singleStat->statAddr->statDouble);
                break;
            default:
                assert(0);
                break;
            }
            idx++;
        }
    }

    this->statLock.unlock();
    return (status);
}

// This is the static function called by statsLib->getStats and
// statGetStatsForStatsGroups
// This function is just a wrapper for the getStatsCommonProto function, which
// returns stats in protobuf format. This function then copies over the output
// to the XcalarApiOutput struct.
//
// XXX In this future, this function needs to be removed, but we are leaving
// this here for now since the get_libstats() thrift call is being used in
// various areas in the codebase (including xccli stats). When all the libstats
// calls are converted to use proto, this function can be deleted.
Status
StatsLib::getStatsCommon(XcalarApiOutput *output,
                         size_t *inoutSize,
                         StatGroupId *inputStatsGroupId,
                         uint64_t inputSize,
                         bool allStatsGroups)
{
    xcalar::compute::localtypes::Stats::GetLibstatsResponse response;
    XcalarApiGetStatOutput *statsOutput = NULL;
    unsigned maxStatsCount;
    Status status = StatusOk;
    StatType statType;
    uint64_t idx = 0;
    size_t outSize;

    assert(output != NULL);

    outSize = XcalarApiSizeOfOutput(*statsOutput);

    maxStatsCount =
        (unsigned) ((*inoutSize - outSize) / sizeof(statsOutput->stats[0]));

    statsOutput = &output->outputResult.statOutput;
    statsOutput->truncated = false;

    // getStats
    try {
        status = StatsLib::get()->getStatsCommonProto(&response,
                                                      inputStatsGroupId,
                                                      inputSize,
                                                      allStatsGroups);
    } catch (std::exception &e) {
        status = StatusNoMem;
    }

    BailIfFailed(status);

    statsOutput = &output->outputResult.statOutput;

    for (xcalar::compute::localtypes::Stats::LibstatNode node :
         *response.mutable_libstatnodes()) {
        if (maxStatsCount == idx) {
            statsOutput->truncated = true;
            goto CommonExit;
        }
        strlcpy(statsOutput->stats[idx].statName,
                node.statname().c_str(),
                node.statname().length() + 1);

        statType = static_cast<StatType>(node.stattype());
        statsOutput->stats[idx].statType = statType;
        statsOutput->stats[idx].groupId = node.groupid();

        switch (node.statvalue().StatValue_case()) {
        case xcalar::compute::localtypes::Stats::StatValue::kVInt:
            statsOutput->stats[idx].statValue.statUint64 =
                node.statvalue().vint();
            break;
        case xcalar::compute::localtypes::Stats::StatValue::kVDouble:
            statsOutput->stats[idx].statValue.statDouble =
                node.statvalue().vdouble();
            break;
        default:
            assert(0);
            break;
        }
        idx++;
    }

CommonExit:
    outSize += sizeof(*statsOutput->stats) * (idx);

    assert(outSize <= *inoutSize);
    *inoutSize = outSize;

    statsOutput->numStats = idx;
    output->hdr.status = status.code();
    return status;
}

Status
StatsLib::allocAndGetStats(void **output, size_t *outputSize)
{
    Status status = StatusOk;
    *output = NULL;
    *outputSize = 0;
    XcalarApiOutput *apiOut = NULL;
    size_t allocatedSize = XcalarApiSizeOfOutput(XcalarApiGetStatOutput) +
                           getTotalStatsSize() + sizeof(size_t);
    size_t numBytesCopied = allocatedSize;
    void *buffer = memAllocExt(allocatedSize, moduleName);
    if (buffer == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    apiOut = (XcalarApiOutput *) ((uintptr_t) buffer + sizeof(size_t));
    status = getStats(apiOut, &numBytesCopied);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(numBytesCopied + sizeof(size_t) == allocatedSize);
    *(size_t *) buffer = numBytesCopied;
    *output = buffer;
    *outputSize = allocatedSize;
    buffer = NULL;

CommonExit:
    if (buffer != NULL) {
        memFree(buffer);
        buffer = NULL;
    }
    return status;
}

uint64_t
StatsLib::getTotalStatsSize()
{
#ifdef DEBUG
    StatsGroupInfo *statsGroupInfo = NULL;
    unsigned totalStatsObjects = 0;
    uint64_t ii = 0;

    this->statLock.lock();
    for (ii = 0; ii < this->groupIdx; ii++) {
        statsGroupInfo = &this->statsGroupInfoArray[ii];
        assert(statsGroupInfo != NULL);
        assert(statsGroupInfo->idx > 0);
        assert(statsGroupInfo->idx == statsGroupInfo->size);
        totalStatsObjects += statsGroupInfo->size;
    }

    assert(totalStatsObjects == this->statIndex);

    this->statLock.unlock();
#endif  // DEBUG

    assert(statsInited);
    // we never decrement this; dirty read is ok
    assert(this->statIndex > 0);

    return (uint64_t)(sizeof(XcalarApiStatOutput) * this->statIndex);
}

// Get all the stats
Status
StatsLib::getStats(XcalarApiOutput *output, size_t *inoutSize)
{
    assert(statsInited);
    Status status = getStatsCommon(output, inoutSize, NULL, 0, true);
    assert((*inoutSize) > 0);
    return status;
}

void
StatsLib::getStatsCompletion(MsgEphemeral *eph, void *payload)
{
    MsgStatsResponse *response = (MsgStatsResponse *) eph->ephemeral;

    response->status = eph->status;
    if (response->status != StatusOk) {
        return;
    }

    response->outputSize = *((size_t *) payload);
    response->output = memAlloc(response->outputSize);
    if (response->output == NULL) {
        response->status = StatusNoMem;
        return;
    }

    memcpy(response->output,
           (void *) ((uintptr_t) payload + sizeof(size_t)),
           response->outputSize);
}

void
StatsLib::getStatsLocal(MsgEphemeral *eph)
{
    Status status = StatusOk;
    size_t outputSize = 0;

    // XXX Needed because of hack in Message.cpp to free
    // msg->payload and assign msg->payload to eph->payloadToDistribute.
    eph->payloadToDistribute = NULL;

    status = this->allocAndGetStats(&eph->payloadToDistribute, &outputSize);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    eph->setAckInfo(status, outputSize);
}

// The caller need to prvoide a array of the module ID and the size of the array
Status
StatsLib::getStatsByGroupId(XcalarApiOutput *output,
                            size_t *outputSize,
                            StatGroupId *inputStatsGroupId,
                            uint64_t inputSize)
{
    assert(statsInited);
    return this->getStatsCommon(output,
                                outputSize,
                                inputStatsGroupId,
                                inputSize,
                                false);
}

// Zero out all cumulative stats and/or HWMstats
void
StatsLib::resetStats(bool resetHwmStats, bool resetCumulativeStats)
{
    assert(statsInited);

    StatType statType;
    SingleStat *singleStat = NULL;
    StatsGroupInfo *statsGroupInfo = NULL;
    uint64_t ii, jj;

    statsLib->statLock.lock();

    for (ii = 0; ii < statsLib->groupIdx; ++ii) {
        statsGroupInfo = &statsLib->statsGroupInfoArray[ii];
        for (jj = 0; jj < statsGroupInfo->idx; ++jj) {
            singleStat = &statsLib->statsArray[statsGroupInfo->startIdx + jj];
            statType = singleStat->statType;

            switch (statType) {
            case StatCumulative:
                if (resetCumulativeStats) {
                    singleStat->statAddr->statUint64 = 0;
                }
                break;
            case StatHWM:
                if (resetHwmStats) {
                    singleStat->statAddr->statUint64 = 0;
                }
                break;
            case StatAbsoluteWithRefVal:
            case StatAbsoluteWithNoRefVal:
                // Do not reset absolute stats as it can cause problems
                // for atomically managed stats like the number used
                // messages.
                break;

            default:
                assert(0);
            }
        }
    }
    time_t ts = time(NULL);
    if (resetHwmStats) {
        StatsLib::statNonAtomicSet64(statsLib->statHwmResetTimestamp, ts);
        xSyslog(moduleName, XlogInfo, "HWM stats have been reset");
    }
    if (resetCumulativeStats) {
        StatsLib::statNonAtomicSet64(statsLib->statCumResetTimestamp, ts);
        xSyslog(moduleName, XlogInfo, "Cumulative stats have been reset");
    }
    statsLib->statLock.unlock();
}

Status
StatsLib::initStatHandle(StatHandle *handle)
{
    // Use malloc memory here.
    // XXX TODO If we ever start collecting stats in the fast path, does this
    // assumption hold?
    *handle = (StatHandle) memAllocExt(sizeof(Stat), __PRETTY_FUNCTION__);
    if (*handle == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "initStatHandle failed: %s",
                strGetFromStatus(StatusNoMem));
        return StatusNoMem;
    }
    memZero(*handle, sizeof(Stat));

    return StatusOk;
}

Status
StatsLib::readNetworkTraffic(uint64_t *recvBytes, uint64_t *sentBytes)
{
    FILE *fp = NULL;
    Status status = StatusOk;

    // @SymbolCheckIgnore
    fp = fopen("/proc/self/net/dev", "r");
    if (fp == NULL) {
        status = sysErrnoToStatus(errno);
        return status;
    }

    char *line = NULL;
    size_t len = 0;
    ssize_t read;

    char *saveptr;
    char *token;
    unsigned count = 0;
    unsigned lineIdx = 0;

    uint64_t bytesRecv, bytesSend;
    uint64_t bytesRecvTotal = 0;
    uint64_t bytesSendTotal = 0;
    char *endPtr;

    // FIXME Xc-9319: the following logic needs to change
    // if are unable to populate we'll return StatusOk
    while ((read = getline(&line, &len, fp)) != -1) {
        if (lineIdx++ < headerIdx) {
            continue;
        }

        count = 0;
        token = strtok_r(line, " ", &saveptr);

        while (true) {
            if (token == NULL || count > sendIdx) {
                break;
            }

            // skip loopback
            if ((count == 0) &&
                (strncmp(token, loopBackInterface, strlen(token)) == 0)) {
                break;
            }

            if (count == recvIdx) {
                bytesRecv = strtoull(token, &endPtr, 0);
                bytesRecvTotal += bytesRecv;
            }

            if (count == sendIdx) {
                bytesSend = strtoull(token, &endPtr, 0);
                bytesSendTotal += bytesSend;
            }

            token = strtok_r(NULL, " ", &saveptr);
            count++;
        }
    }

    if (line != NULL) {
        // @SymbolCheckIgnore
        free(line);
    }

    if (fp != NULL) {
        // @SymbolCheckIgnore
        fclose(fp);
        fp = NULL;
    }

    *recvBytes = bytesRecvTotal;
    *sentBytes = bytesSendTotal;

    return status;
}

Status
StatsLib::getMyResidentSetSize(uint64_t *rssInBytes)
{
    Status status = StatusOk;
    int ret;
    FILE *fp = NULL;
    char tmp[PATH_MAX];
    long long int myRss = 0;

    // indexes are retrieved from
    // https://www.kernel.org/doc/Documentation/filesystems/proc.txt
    const unsigned tcommIndex = 2;
    const unsigned stateIndex = 3;
    const unsigned rssIndex = 24;

    assert(rssInBytes != NULL);

    // @SymbolCheckIgnore
    fp = fopen("/proc/self/stat", "r");
    if (fp == NULL) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    for (unsigned ii = 1; ii <= rssIndex; ii++) {
        if (ii == rssIndex) {
            ret = fscanf(fp, "%lld ", &myRss);
        } else if (ii == tcommIndex) {
            ret = fscanf(fp, "%s ", tmp);
        } else if (ii == stateIndex) {
            ret = fscanf(fp, "%c ", tmp);
        } else {
            ret = fscanf(fp, "%lld ", (long long int *) tmp);
        }

        if (ret != 1) {
            status = sysErrnoToStatus(errno);
            goto CommonExit;
        }
    }

    // myRss is in num pages of size 4 KB
    *rssInBytes = (uint64_t)(myRss * 4 * KB);

CommonExit:
    if (fp != NULL) {
        // @SymbolCheckIgnore
        fclose(fp);
    }

    if (status != StatusOk) {
        *rssInBytes = 0;
    }

    return status;
}

Status
StatsLib::getMemoryUsage(double *myMemUsageInPercent,
                         uint64_t *totalUsedSysMemoryInBytes,
                         uint64_t *totalSysMemoryInBytes,
                         uint64_t *sysSwapUsedInBytes,
                         uint64_t *sysSwapTotalInBytes)
{
    Status status = StatusOk;
    uint64_t myMemUsage = 0;

    uint64_t totalMemory = 0;
    uint64_t freeMemory = 0;
    uint64_t totalSwap = 0;
    uint64_t freeSwap = 0;

    assert(myMemUsageInPercent != NULL);
    assert(totalUsedSysMemoryInBytes != NULL);
    assert(totalSysMemoryInBytes != NULL);
    assert(sysSwapUsedInBytes != NULL);
    assert(sysSwapTotalInBytes != NULL);
    *myMemUsageInPercent = 0;
    *totalUsedSysMemoryInBytes = 0;
    *totalSysMemoryInBytes = 0;
    *sysSwapUsedInBytes = 0;
    *sysSwapTotalInBytes = 0;

    status = getMyResidentSetSize(&myMemUsage);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = XcSysHelper::get()->getProcFsHelper()->getMemInfo(&totalMemory,
                                                               &freeMemory,
                                                               &totalSwap,
                                                               &freeSwap);

    *myMemUsageInPercent = (myMemUsage / (double) totalMemory) * 100;

    // do not use freeMemory returned from getMemInfo() here, which is
    // under-estimated since it doesn't include OS page cache, etc. Use the
    // other utility which provides free-able memory, to make sure usage isn't
    // inflated and more what a user might expect (the user's notion of used
    // doesn't include OS page cache, buffers, etc.)

    status = XcSysHelper::get()->getProcFsHelper()->getFreeMemSizeInBytes(
        &freeMemory);
    if (status != StatusOk) {
        goto CommonExit;
    }
    *totalUsedSysMemoryInBytes = (totalMemory - freeMemory);
    *totalSysMemoryInBytes = totalMemory;
    *sysSwapUsedInBytes = (totalSwap - freeSwap);
    *sysSwapTotalInBytes = totalSwap;

CommonExit:
    xcAssertIf((status != StatusOk), (*myMemUsageInPercent == 0));
    xcAssertIf((status != StatusOk), (*totalUsedSysMemoryInBytes == 0));
    xcAssertIf((status != StatusOk), (*totalSysMemoryInBytes == 0));
    // swap can be 0 if unconfigured

    return status;
}

StatsLib::SingleStat *
StatsLib::getSingleStatObjectByStatName(StatsGroupInfo *statsGroupInfo,
                                        const char *singleStatName)
{
    SingleStat *singleStat = NULL;
    unsigned ii = 0;
    size_t statNameLength = strlen(singleStatName);

    if (!(statNameLength > 0)) {
        return NULL;
    }

    for (ii = 0; ii < statsGroupInfo->idx; ii++) {
        singleStat = &this->statsArray[statsGroupInfo->startIdx + ii];
        if (strncmp(singleStatName, singleStat->statName, statNameLength) ==
            0) {
            return singleStat;
        }
    }

    assert(0);
    return NULL;
}

Status
StatsLib::fastAllocBytesUsedByBufCacheObj(StatsGroupInfo *statsGroupInfo,
                                          uint64_t *usedBytes)
{
    SingleStat *singleStat = NULL;

    // keep enum values in sync with singleStatNames
    enum { elemSizeBytes, fastAllocs, fastFrees, maxSingleStatName };
    const char *singleStatNames[maxSingleStatName] = {"elemSizeBytes",
                                                      "fastAllocs",
                                                      "fastFrees"};
    uint64_t arrayToGetUsedBytes[maxSingleStatName];
    unsigned ii = 0;

    assert(statsGroupInfo != NULL);
    assert(usedBytes != NULL);
    assertStatic(ArrayLen(singleStatNames) == maxSingleStatName);

    for (ii = elemSizeBytes; ii < maxSingleStatName; ii++) {
        singleStat =
            getSingleStatObjectByStatName(statsGroupInfo, singleStatNames[ii]);
        if (singleStat == NULL) {
            assert(0);
            return StatusInval;
        }
        assert(singleStat->statValType == StatUint64);
        arrayToGetUsedBytes[ii] = singleStat->statAddr->statUint64;
    }

    *usedBytes =
        (arrayToGetUsedBytes[fastAllocs] - arrayToGetUsedBytes[fastFrees]) *
        arrayToGetUsedBytes[elemSizeBytes];

    return StatusOk;
}

Status
StatsLib::numFastAllocUsedAndTotalBytesByBufCacheObj(StatGroupId groupId,
                                                     uint64_t *usedBytes,
                                                     uint64_t *totalBytes)
{
    Status status = StatusOk;

    StatsGroupInfo *statsGroupInfo = NULL;
    SingleStat *singleStat = NULL;

    assert(usedBytes != NULL);
    assert(totalBytes != NULL);
    assert(*usedBytes == 0);
    assert(*totalBytes == 0);

    statsGroupInfo = &this->statsGroupInfoArray[groupId];
    if (statsGroupInfo == NULL) {
        assert(0);  // handling as getStatsCommon handles
        status = StatusInval;
        goto CommonExit;
    }

    status = fastAllocBytesUsedByBufCacheObj(statsGroupInfo, usedBytes);
    if (status != StatusOk) {
        goto CommonExit;
    }

    singleStat = getSingleStatObjectByStatName(statsGroupInfo, "totMemBytes");
    if (singleStat == NULL) {
        assert(0);
        status = StatusInval;
        goto CommonExit;
    }

    assert(singleStat->statValType == StatUint64);
    *totalBytes = singleStat->statAddr->statUint64;

CommonExit:

    return status;
}

Status
StatsLib::retrieveStatsFromXdbGroups(uint64_t *usedBytes, uint64_t *totalBytes)
{
    Status status = StatusOk;
    StatGroupId xdbPageKbGroupId;
    StatGroupId xdbLocalGroupId;

    uint64_t numUsedPageBc = 0;
    uint64_t numTotalPageBc = 0;
    uint64_t numUsedPageKvBc = 0;
    uint64_t numTotalPageKvBc = 0;

    assert(usedBytes != NULL);
    assert(totalBytes != NULL);

    status = getGroupIdFromName("xdb.pagekvbuf.bc", &xdbPageKbGroupId);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = numFastAllocUsedAndTotalBytesByBufCacheObj(xdbPageKbGroupId,
                                                        &numUsedPageKvBc,
                                                        &numTotalPageKvBc);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = getGroupIdFromName("xdb.local.bc", &xdbLocalGroupId);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = numFastAllocUsedAndTotalBytesByBufCacheObj(xdbLocalGroupId,
                                                        &numUsedPageBc,
                                                        &numTotalPageBc);
    if (status != StatusOk) {
        goto CommonExit;
    }

    *usedBytes = numUsedPageBc + numUsedPageKvBc;
    *totalBytes = numTotalPageBc + numTotalPageKvBc;

CommonExit:
    if (status != StatusOk) {
        *usedBytes = 0;
        *totalBytes = 0;
    }

    return status;
}

void
StatsLib::calculateCpuUsage(struct rusage *rUsage, double *cpuUsage)
{
    uint64_t userTimeTotalInUSecs = (uint64_t)(
        (rUsage->ru_utime.tv_sec * USecsPerSec) + (rUsage->ru_utime.tv_usec));

    uint64_t systemTimeTotalInUSecs = (uint64_t)(
        (rUsage->ru_stime.tv_sec * USecsPerSec) + (rUsage->ru_stime.tv_usec));

    *cpuUsage = (double) (userTimeTotalInUSecs + systemTimeTotalInUSecs);
}

Status
StatsLib::getCpuUsageOfThisProcess(pid_t processId, uint64_t *totalJiffies)
{
    Status status = StatusOk;
    int ret;

    char processStatFileName[PATH_MAX] = {'\0'};
    FILE *fp = NULL;
    unsigned ii;
    uint64_t uTimeInJiffies = 0;
    uint64_t sTimeinJiffies = 0;
    char dummy[PATH_MAX];

    // indexes are retrieved from
    // https://www.kernel.org/doc/Documentation/filesystems/proc.txt
    static const unsigned tcommIndex = 2;
    static const unsigned stateIndex = 3;
    static const unsigned utimeIndex = 14;
    static const unsigned stimeIndex = 15;

    *totalJiffies = 0;

    ret = snprintf(processStatFileName,
                   PATH_MAX,
                   "/proc/%d/stat",
                   (int) processId);
    if (ret < 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    fp = fopen(processStatFileName, "r");
    if (fp == NULL) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    for (ii = 1; ii <= stimeIndex; ii++) {
        if (ii == utimeIndex) {
            ret = fscanf(fp, "%lld ", (long long int *) &uTimeInJiffies);
        } else if (ii == stimeIndex) {
            ret = fscanf(fp, "%lld ", (long long int *) &sTimeinJiffies);
        } else if (ii == tcommIndex) {
            ret = fscanf(fp, "%s ", dummy);
        } else if (ii == stateIndex) {
            ret = fscanf(fp, "%c ", dummy);
        } else {
            ret = fscanf(fp, "%lld ", (long long int *) dummy);
        }

        if (ret != 1) {
            status = sysErrnoToStatus(errno);
            goto CommonExit;
        }
    }

    *totalJiffies = (uTimeInJiffies + sTimeinJiffies);

CommonExit:
    if (fp != NULL) {
        fclose(fp);
    }

    return status;
}

Status
StatsLib::getCpuFromAllChildren(pid_t *listOfChildren,
                                size_t numChildren,
                                double *childrenCpuUsage,
                                long numJiffiesPerSec)
{
    Status status = StatusOk;

    size_t currentChildCounter = 0;
    uint64_t perProcessJiffies = 0;
    uint64_t totalJiffies = 0;

    assert(listOfChildren != NULL);
    assert(numChildren > 0);
    assert(childrenCpuUsage != NULL);
    *childrenCpuUsage = 0;

    for (currentChildCounter = 0; currentChildCounter < numChildren;
         currentChildCounter++) {
        status = getCpuUsageOfThisProcess(listOfChildren[currentChildCounter],
                                          &perProcessJiffies);
        if (status != StatusOk) {
            // child probably disappeared
            continue;
        }
        totalJiffies += perProcessJiffies;
    }

    *childrenCpuUsage =
        (double) ((totalJiffies * USecsPerSec) / numJiffiesPerSec);

    return StatusOk;
}

Status
StatsLib::getDatasetUsage(uint64_t *bytesUsed)
{
    Status status = StatusOk;

    *bytesUsed = 0;

    status = Dataset::get()->getDatasetTotalBytesUsed(bytesUsed);

    return status;
}

Status
StatsLib::getUptimeInSeconds(uint64_t *uptimeInSeconds)
{
    Status status = StatusOk;
    int ret = 0;
    struct timespec currentTime;

    assert(uptimeInSeconds != NULL);
    *uptimeInSeconds = 0;

    ret = clock_gettime(CLOCK_REALTIME, &currentTime);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    // this is for XD display, need not worry about nsec part
    *uptimeInSeconds = (uint64_t)(currentTime.tv_sec - bootTime_.tv_sec);

CommonExit:
    return status;
}

Status
StatsLib::getCpuUsageAbsoluteInUSeconds(double *parentCpuUsage,
                                        double *childrenCpuUsage,
                                        double *totalCpuUsage,
                                        uint64_t *numCores)
{
    Status status = StatusOk;
    int ret;
    struct rusage myRUsage;
    long numJiffiesPerSecond = -1;

    pid_t *listOfChildren = NULL;
    size_t numChildren = 0;

    assert(parentCpuUsage != NULL);
    assert(childrenCpuUsage != NULL);
    *parentCpuUsage = *childrenCpuUsage = 0;

    numJiffiesPerSecond = XcSysHelper::get()->getNumJiffiesPerSecond();

    status = Parent::get()->getListOfChildren(&listOfChildren, &numChildren);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (numChildren > 0) {
        if (getCpuFromAllChildren(listOfChildren,
                                  numChildren,
                                  childrenCpuUsage,
                                  numJiffiesPerSecond) != StatusOk) {
            *childrenCpuUsage = 0;
        }
    }

    ret = getrusage(RUSAGE_SELF, &myRUsage);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    calculateCpuUsage(&myRUsage, parentCpuUsage);

    status = getTotalCpuUsageAbsoluteInUSeconds(totalCpuUsage, numCores);

CommonExit:
    xcAssertIf((status != StatusOk), (*parentCpuUsage == 0));
    // *childrenCpuUsage can be zero

    if (listOfChildren != NULL) {
        memFree(listOfChildren);
    }

    return status;
}

Status
StatsLib::getTotalCpuUsageAbsoluteInUSeconds(double *totalCpuUsage,
                                        uint64_t *numCores)
{
    Status status = StatusOk;
    FILE *fp;
    double uptime;
    double idleTime;
    double cpuUsage;
    int count;

    *numCores = (uint64_t) XcSysHelper::get()->getNumOnlineCores();
    fp = fopen("/proc/uptime", "r");
    if (fp == NULL) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
    count = fscanf(fp, "%lf %lf", &uptime, &idleTime);
    fclose(fp);
    if (count == 2) {
        cpuUsage = uptime * *numCores - idleTime;
        *totalCpuUsage = (uint64_t)(cpuUsage * 1000000);
    } else {
        if (!loggedUptimeError) {
            xSyslog(moduleName, XlogErr, "Unexpected format in /proc/uptime");
            loggedUptimeError = true;
        }
        *totalCpuUsage = 0;
    }

CommonExit:
    return status;
}


void
StatsLib::top(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    XcalarApiTopOutputPerNode *topOutputPerNode = NULL;
    XcalarApiTopInput topInput;

    memcpy(&topInput, payload, sizeof(XcalarApiTopInput));
    topOutputPerNode = (XcalarApiTopOutputPerNode *) payload;

    assert(sizeof(*topOutputPerNode) < MsgMgr::getMsgMaxPayloadSize());
    assert((topInput.topStatsRequestType == GetCpuAndNetworkTopStats) ||
           (topInput.topStatsRequestType == GetAllTopStats));

    size_t payloadLength = 0;
    status = getTopStats(&topInput, topOutputPerNode, true);
    if (status != StatusOk) {
        payloadLength = 0;
    } else {
        payloadLength = sizeof(XcalarApiTopOutputPerNode);
    }

    eph->setAckInfo(status, payloadLength);
}

Status
StatsLib::getTopStats(XcalarApiTopInput *input,
                      XcalarApiTopOutputPerNode *topOutputPerNode,
                      bool detailedCpuStats)
{
    Status status = StatusOk;
    XcalarApiTopRequestType topStatsRequestType;

    assert(input != NULL);
    assert(topOutputPerNode != NULL);

    topStatsRequestType = input->topStatsRequestType;

    assert((topStatsRequestType == GetCpuAndNetworkTopStats) ||
           (topStatsRequestType == GetAllTopStats));

    topOutputPerNode->nodeId = Config::get()->getMyNodeId();

    if (topStatsRequestType == GetAllTopStats) {
        status = retrieveStatsFromXdbGroups(&topOutputPerNode->xdbUsedBytes,
                                            &topOutputPerNode->xdbTotalBytes);
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    if (detailedCpuStats) {
        status = getCpuUsageAbsoluteInUSeconds(&topOutputPerNode
                                                    ->parentCpuUsageInPercent,
                                            &topOutputPerNode
                                                    ->childrenCpuUsageInPercent,
                                            &topOutputPerNode->cpuUsageInPercent,
                                            &topOutputPerNode->numCores);
    } else {
        status = getTotalCpuUsageAbsoluteInUSeconds(&topOutputPerNode->cpuUsageInPercent,
                                            &topOutputPerNode->numCores);
    }
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (topStatsRequestType == GetAllTopStats) {
        status = getMemoryUsage(&(topOutputPerNode->memUsageInPercent),
                                &(topOutputPerNode->memUsedInBytes),
                                &(topOutputPerNode->totalAvailableMemInBytes),
                                &(topOutputPerNode->sysSwapUsedInBytes),
                                &(topOutputPerNode->sysSwapTotalInBytes));
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    status = readNetworkTraffic(&(topOutputPerNode->networkRecvInBytesPerSec),
                                &(topOutputPerNode->networkSendInBytesPerSec));
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (topStatsRequestType == GetAllTopStats) {
        status = getUptimeInSeconds(&(topOutputPerNode->uptimeInSeconds));
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    if (topStatsRequestType == GetAllTopStats) {
        status = getDatasetUsage(&(topOutputPerNode->datasetUsedBytes));
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    if (topStatsRequestType == GetAllTopStats) {
        topOutputPerNode->publishedTableUsedBytes =
            HashTreeMgr::get()->getTotalUsedBytes();
    }

    if (topStatsRequestType == GetAllTopStats) {
        // Note: this is using already calculated results.
        if (!XcalarConfig::get()->bufCacheMlock_) {
            // Buf$ with Mlocking OFF.
            topOutputPerNode->sysMemUsedInBytes =
                topOutputPerNode->memUsedInBytes -
                topOutputPerNode->xdbTotalBytes;
        } else if (!XcalarConfig::get()->bufferCacheLazyMemLocking_) {
            // Buf$ with Mlocking ON, Lazy Mlocking OFF.
            topOutputPerNode->sysMemUsedInBytes =
                topOutputPerNode->memUsedInBytes -
                topOutputPerNode->xdbTotalBytes;
        } else {
            // Buf$ with Mlocking ON, Lazy Mlocking ON.
            topOutputPerNode->sysMemUsedInBytes =
                topOutputPerNode->memUsedInBytes -
                topOutputPerNode->xdbUsedBytes;
        }
    }

CommonExit:
    return status;
}

enum StatNames {
    System = 1,
    SystemNumCores,
    SystemCpuMaximumUsage,
    SystemCpuTotalUsage,
    SystemCpuXCEUsage,
    SystemCpuXPUUsage,
    SystemNetworkRecvBytes,
    SystemNetworkSendBytes,
    SystemMemoryUsed,
    SystemMemoryTotalAvailable,
    SystemMemoryCgXPUUsed,
    SystemMemoryCgXCEUsed,
    SystemMemoryXCEMalloced,
    SystemCgXPUPgpgin,
    SystemCgXPUPgpgout,
    SystemCgXCEPgpgin,
    SystemCgXCEPgpgout,
    SystemSwapUsed,
    SystemSwapTotal,
    SystemDatasetUsed,
    Xdb,
    XdbUsedBytes,
    XdbTotalBytes
};

enum StatProtoType { vBool, vUInt, vInt, vDouble, vString, vBytes };

struct StatProtoValue {
    StatProtoValue(bool inValue)
    {
        type = vBool;
        boolValue = inValue;
    }
    StatProtoValue(uint64_t inValue)
    {
        type = vUInt;
        uIntValue = inValue;
    }
    StatProtoValue(int64_t inValue)
    {
        type = vInt;
        intValue = inValue;
    }
    StatProtoValue(double inValue)
    {
        type = vDouble;
        doubleValue = inValue;
    }
    StatProtoValue(const char *inValue)
    {
        type = vString;
        stringValue = inValue;
    }
    StatProtoValue(const void *inValue, size_t len)
    {
        type = vBytes;
        bytesValue = inValue;
        bytesSize = len;
    }
    StatProtoType type;
    bool boolValue;
    uint64_t uIntValue;
    int64_t intValue;
    double doubleValue;
    const char *stringValue;
    const void *bytesValue;
    size_t bytesSize;
};

using namespace xcalar::compute::localtypes::Stats;

static Status
AddStatNode(StatNode *parent,
            const char *name,
            StatNames id,
            StatUnits units,
            const char *maxName,
            const StatProtoValue &value)
{
    try {
        StatNode *childNode = parent->add_children();
        if (childNode == nullptr) {
            return StatusNoMem;
        }
        childNode->set_id(id);
        childNode->mutable_meta()->mutable_name()->set_value(name);
        childNode->mutable_meta()->set_units(units);
        if (maxName != nullptr) {
            childNode->mutable_meta()->mutable_maxname()->set_value(maxName);
        }
        switch (value.type) {
        case vBool:
            childNode->add_values()->set_vbool(value.boolValue);
            break;
        case vUInt:
            childNode->add_values()->set_vuint(value.uIntValue);
            break;
        case vInt:
            childNode->add_values()->set_vint(value.intValue);
            break;
        case vDouble:
            childNode->add_values()->set_vdouble(value.doubleValue);
            break;
        case vString:
            childNode->add_values()->set_vstring(value.stringValue);
            break;
        case vBytes:
            childNode->add_values()->set_vbytes(value.bytesValue,
                                                value.bytesSize);
            break;
        }
    } catch (std::exception &e) {
        return StatusNoMem;
    }
    return StatusOk;
}

static bool
findNameValuePair(FILE *fp, const char *searchName, std::string &resultValue)
{
    char buffer[CGROUP_BUFFER_SIZE];
    char value[CGROUP_BUFFER_SIZE];
    rewind(fp);
    while (fgets(buffer, sizeof(buffer), fp) != nullptr) {
        char name[CGROUP_BUFFER_SIZE];
        if (sscanf(buffer, "%s %s", name, value) != 2) {
            fprintf(stderr, "Unexpected format in file\n");
            return false;
        }
        if (strcmp(name, searchName) == 0) {
            resultValue = value;
            return true;
        }
    }
    return false;
}

static Status
getMemStats(const std::string &statDirRootPath,
            const std::string &statDirName,
            uint64_t &usedBytes,
            uint64_t &pageIns,
            uint64_t &pageOuts)
{
    std::string pathName(statDirRootPath);
    pathName += "/";
    pathName += statDirName;
    pathName += "/";
    std::string memoryStatFileName = pathName + "memory.stat";
    FILE *fp = fopen(memoryStatFileName.c_str(), "r");
    if (fp == nullptr) {
        return sysErrnoToStatus(errno);
    }
    std::string value;
    if (findNameValuePair(fp, "total_pgpgin", value)) {
        pageIns += std::stoul(value);
    }
    if (findNameValuePair(fp, "total_pgpgout", value)) {
        pageOuts += std::stoul(value);
    }
    fclose(fp);
    memoryStatFileName = pathName + "memory.usage_in_bytes";
    fp = fopen(memoryStatFileName.c_str(), "r");
    if (fp == nullptr) {
        return sysErrnoToStatus(errno);
    }
    uint64_t tUsedBytes;
    if (fscanf(fp, "%ld", &tUsedBytes) == 1) {
        usedBytes += tUsedBytes;
    }
    fclose(fp);
    return StatusOk;
}

static Status
getCpuStats(const std::string &statDirRootPath,
            const std::string &statDirName,
            uint64_t &cpuMicroseconds)
{
    std::string pathName(statDirRootPath);
    pathName += "/";
    pathName += statDirName;
    pathName += "/";
    std::string cpuStatFileName = pathName + "cpuacct.usage";
    FILE *fp = fopen(cpuStatFileName.c_str(), "r");
    if (fp == nullptr) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to open %s",
                cpuStatFileName.c_str());
        return sysErrnoToStatus(errno);
    }
    uint64_t cpuNanoseconds;
    if (fscanf(fp, "%ld", &cpuNanoseconds) == 1) {
        cpuMicroseconds += cpuNanoseconds / 1000;
    }
    fclose(fp);
    return StatusOk;
}

struct CGroupUsage {
    uint64_t xpuUsedBytes;
    uint64_t xpuPageIns;
    uint64_t xpuPageOuts;
    uint64_t xceUsedBytes;
    uint64_t xcePageIns;
    uint64_t xcePageOuts;
    uint64_t xpuCpuMicroseconds;
    uint64_t xceCpuMicroseconds;
};

static const char *schedGroups[3] = {"sched0", "sched1", "sched2"};
static const size_t schedGroupCount =
    sizeof(schedGroups) / sizeof(schedGroups[0]);

static Status
getCGroupUsage(CGroupUsage &cGroupUsage)
{
    Status status;
    std::string scopeSuffix;
    StatsLib *statsLib = StatsLib::get();
    std::string memoryPath = std::string(statsLib->cgroupsMemoryPath_);
    std::string cpuPath = std::string(statsLib->cgroupsCpuPath_);

    if (memoryPath.empty()) {
        xSyslog(moduleName, XlogErr, "Failed to find memory");
        status = StatusNoEnt;
        goto CommonExit;
    }
    if (cpuPath.empty()) {
        xSyslog(moduleName, XlogErr, "Failed to find cpuacct");
        status = StatusNoEnt;
        goto CommonExit;
    }

    status = getMemStats(memoryPath,
                         std::string(statsLib->xceCgroupPath_),
                         cGroupUsage.xceUsedBytes,
                         cGroupUsage.xcePageIns,
                         cGroupUsage.xcePageOuts);
    BailIfFailed(status);
    status = getCpuStats(cpuPath,
                        std::string(statsLib->xceCgroupPath_),
                        cGroupUsage.xceCpuMicroseconds);
    BailIfFailed(status);
    scopeSuffix = std::string(".scope");
    for (size_t i = 0; i < schedGroupCount; i++) {
        status = getMemStats(memoryPath,
                             std::string(statsLib->sysXpusCgroupPath_) + schedGroups[i] + scopeSuffix,
                             cGroupUsage.xpuUsedBytes,
                             cGroupUsage.xpuPageIns,
                             cGroupUsage.xpuPageOuts);
        BailIfFailed(status);
        status = getCpuStats(cpuPath,
                         std::string(statsLib->sysXpusCgroupPath_) + schedGroups[i] + scopeSuffix,
                         cGroupUsage.xpuCpuMicroseconds);
        BailIfFailed(status);
    }
    for (size_t i = 0; i < schedGroupCount; i++) {
        status = getMemStats(memoryPath,
                             std::string(statsLib->usrXpusCgroupPath_) + schedGroups[i] + scopeSuffix,
                             cGroupUsage.xpuUsedBytes,
                             cGroupUsage.xpuPageIns,
                             cGroupUsage.xpuPageOuts);
        BailIfFailed(status);
        status = getCpuStats(cpuPath,
                         std::string(statsLib->usrXpusCgroupPath_) + schedGroups[i] + scopeSuffix,
                         cGroupUsage.xpuCpuMicroseconds);
        BailIfFailed(status);
    }

CommonExit:
    return status;
}

static bool
findNameValue(const std::string &str, const char *name, std::string &value)
{
    std::string prefix(name);
    prefix += "=\"";
    size_t namePos = str.find(prefix);
    if (namePos != std::string::npos) {
        size_t valuePos = namePos + prefix.size();
        size_t endQuotePos = str.find('"', valuePos);
        if (endQuotePos != std::string::npos) {
            value = str.substr(valuePos, endQuotePos - valuePos);
            return true;
        }
    }

    return false;
}

Status
StatsLib::getPerfStats(bool getMeta, GetStatsPerHostResponse *response)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    StatNode *systemNode;
    StatNode *xdbNode;
    XcalarApiTopInput topInput;
    topInput.topStatsRequestType = GetAllTopStats;
    XcalarApiTopOutputPerNode topOutput;
    memset(&topOutput, 0, sizeof(topOutput));

    Status status = StatsLib::get()->getTopStats(&topInput, &topOutput, false);
    BailIfFailed(status);

    CGroupUsage cGroupUsage;
    memset(&cGroupUsage, 0, sizeof(cGroupUsage));

    response->mutable_nodeid()->set_value(topOutput.nodeId);
    response->mutable_ts()->set_seconds(ts.tv_sec);
    response->mutable_ts()->set_nanos(ts.tv_nsec);

    //
    // Add System stats.
    //
    systemNode = response->add_statnodes();

    systemNode->set_id(System);
    try {
        systemNode->mutable_meta()->mutable_name()->set_value("System");
    } catch (std::exception &e) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = AddStatNode(systemNode,
                         "NumCores",
                         SystemNumCores,
                         Count,
                         "Constant",
                         StatProtoValue(topOutput.numCores));
    BailIfFailed(status);
    status = AddStatNode(systemNode,
                         "CpuMaximumUsage",
                         SystemCpuMaximumUsage,
                         MicroSeconds,
                         "Constant",
                         StatProtoValue(topOutput.numCores * 1000000));
    BailIfFailed(status);
    status = AddStatNode(systemNode,
                         "CpuTotalUsage",
                         SystemCpuTotalUsage,
                         Percent,
                         "CpuMaximumUsage",
                         StatProtoValue(topOutput.cpuUsageInPercent));
    BailIfFailed(status);
    status = AddStatNode(systemNode,
                         "NetworkRecvBytes",
                         SystemNetworkRecvBytes,
                         Bytes,
                         nullptr,
                         StatProtoValue(topOutput.networkRecvInBytesPerSec));
    BailIfFailed(status);
    status = AddStatNode(systemNode,
                         "NetworkSendBytes",
                         SystemNetworkSendBytes,
                         Bytes,
                         nullptr,
                         StatProtoValue(topOutput.networkSendInBytesPerSec));
    BailIfFailed(status);
    status = AddStatNode(systemNode,
                         "MemoryUsed",
                         SystemMemoryUsed,
                         Bytes,
                         "MemoryTotalAvailable",
                         StatProtoValue(topOutput.memUsedInBytes));
    BailIfFailed(status);
    status = AddStatNode(systemNode,
                         "MemoryTotalAvailable",
                         SystemMemoryTotalAvailable,
                         Bytes,
                         "Constant",
                         StatProtoValue(topOutput.totalAvailableMemInBytes));
    BailIfFailed(status);
    status = AddStatNode(systemNode,
                         "SwapUsed",
                         SystemSwapUsed,
                         Bytes,
                         "SwapTotal",
                         StatProtoValue(topOutput.sysSwapUsedInBytes));
    BailIfFailed(status);
    status = AddStatNode(systemNode,
                         "SwapTotal",
                         SystemSwapTotal,
                         Bytes,
                         nullptr,
                         StatProtoValue(topOutput.sysSwapTotalInBytes));
    BailIfFailed(status);
    status = AddStatNode(systemNode,
                         "DatasetUsed",
                         SystemDatasetUsed,
                         Bytes,
                         nullptr,
                         StatProtoValue(topOutput.datasetUsedBytes));
    BailIfFailed(status);

    if (CgroupMgr::get()->enabled()) {
        status = getCGroupUsage(cGroupUsage);
        BailIfFailed(status);
        status = AddStatNode(systemNode,
                             "MemoryCgXPUUsed",
                             SystemMemoryCgXPUUsed,
                             Bytes,
                             "MemoryTotalAvailable",
                             StatProtoValue(cGroupUsage.xpuUsedBytes));
        BailIfFailed(status);
        status = AddStatNode(systemNode,
                             "MemoryCgXCEUsed",
                             SystemMemoryCgXCEUsed,
                             Bytes,
                             "MemoryTotalAvailable",
                             StatProtoValue(cGroupUsage.xceUsedBytes));
        status = AddStatNode(systemNode,
                             "CgXPUPgpgin",
                             SystemCgXPUPgpgin,
                             Count,
                             nullptr,
                             StatProtoValue(cGroupUsage.xpuPageIns));
        BailIfFailed(status);
        status = AddStatNode(systemNode,
                             "CgXPUPgpgout",
                             SystemCgXPUPgpgout,
                             Count,
                             nullptr,
                             StatProtoValue(cGroupUsage.xpuPageOuts));
        BailIfFailed(status);
        status = AddStatNode(systemNode,
                             "CgXCEPgpgin",
                             SystemCgXCEPgpgin,
                             Count,
                             nullptr,
                             StatProtoValue(cGroupUsage.xcePageIns));
        BailIfFailed(status);
        status = AddStatNode(systemNode,
                             "CgXCEPgpgout",
                             SystemCgXCEPgpgout,
                             Count,
                             nullptr,
                             StatProtoValue(cGroupUsage.xcePageOuts));
        status = AddStatNode(systemNode,
                             "CpuXCEUsage",
                             SystemCpuXCEUsage,
                             Count,
                             nullptr,
                             StatProtoValue(cGroupUsage.xceCpuMicroseconds));
        BailIfFailed(status);
        status = AddStatNode(systemNode,
                             "CpuXPUUsage",
                             SystemCpuXPUUsage,
                             Count,
                             nullptr,
                             StatProtoValue(cGroupUsage.xpuCpuMicroseconds));
        BailIfFailed(status);
    }

    //
    // Add XdbMgr stats.
    //
    xdbNode = response->add_statnodes();
    xdbNode->set_id(Xdb);
    try {
        xdbNode->mutable_meta()->mutable_name()->set_value("Xdb");
    } catch (std::exception &e) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = AddStatNode(xdbNode,
                         "UsedBytes",
                         XdbUsedBytes,
                         Bytes,
                         "TotalBytes",
                         StatProtoValue(topOutput.xdbUsedBytes));
    BailIfFailed(status);
    status = AddStatNode(xdbNode,
                         "TotalBytes",
                         XdbTotalBytes,
                         Bytes,
                         nullptr,
                         StatProtoValue(topOutput.xdbTotalBytes));
    BailIfFailed(status);

CommonExit:
    if (status == StatusOk) {
        response->set_status(true);
    } else {
        response->set_status(false);
        response->set_errmsg(status.message());
    }
    return status;
}

void
TwoPcMsg2pcGetPerfStats1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    size_t size = 0;
    bool getMeta = *((bool *) payload);
    GetStatsPerHostResponse response;
    Status status = StatusOk;

    // XXX Needed because of hack in Message.cpp to free
    // msg->payload and assign msg->payload to eph->payloadToDistribute.
    ephemeral->payloadToDistribute = NULL;

    try {
        status = StatsLib::get()->getPerfStats(getMeta, &response);
        BailIfFailed(status);
    } catch (std::exception &e) {
        status = StatusNoMem;
        goto CommonExit;
    }

    size = response.ByteSizeLong();
    ephemeral->payloadToDistribute = memAlloc(size);
    BailIfNull(ephemeral->payloadToDistribute);

    status =
        pbSerializeToArray(&response, ephemeral->payloadToDistribute, size);
    BailIfFailed(status);

CommonExit:
    ephemeral->payloadLength = (status == StatusOk) ? size : 0;
    ephemeral->status = status;
}

void
StatsLib::addPerfStats(MsgEphemeral *ephemeral, void *payload)
{
    GetStatsOutput *output = (GetStatsOutput *) ephemeral->ephemeral;
    if (ephemeral->status == StatusOk) {
        GetStatsPerHostResponse xresponse;
        output->status =
            pbParseFromArray(&xresponse, payload, ephemeral->payloadLength);
        if (output->status == StatusOk) {
            try {
                // RAII-based lock acquisition
                // Destructor releases mutex when exiting code block
                std::lock_guard<Mutex> perfLG(output->statsMutex);
                GetStatsPerHostResponse *hostResponse =
                    output->response->add_perhoststats();
                if (hostResponse == nullptr) {
                    output->status = StatusNoMem;
                } else {
                    hostResponse->CopyFrom(xresponse);
                }
            } catch (std::exception &e) {
                xSyslog(moduleName,
                        XlogErr,
                        "Allocating per host stats threw exception: %s",
                        e.what());
                output->status = StatusNoMem;
            }
        }
    }
    output->status = ephemeral->status;
}

void
TwoPcMsg2pcGetPerfStats1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                               void *payload)
{
    StatsLib::get()->addPerfStats(ephemeral, payload);
}

void
TwoPcMsg2pcGetPerfStats1::recvDataCompletion(MsgEphemeral *ephemeral,
                                             void *payload)
{
    StatsLib::get()->addPerfStats(ephemeral, payload);
}

Status
StatsLib::getStatsReq(NodeId nodeId,
                      XcalarApiOutput **outputOut,
                      size_t *outputSizeOut)
{
    Status status = StatusOk;
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    MsgMgr *msgMgr = MsgMgr::get();
    MsgStatsResponse response;

    *outputOut = NULL;
    *outputSizeOut = 0;

    msgMgr->twoPcEphemeralInit(&eph,
                               (void *) &nodeId,
                               sizeof(nodeId),
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcRpcGetStat1,
                               (void *) &response,
                               (TwoPcBufLife)(TwoPcMemCopyInput |
                                              TwoPcMemCopyOutput));

    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcRpcGetStat,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                              MsgRecvHdrPlusPayload),
                           TwoPcSyncCmd,
                           TwoPcSingleNode,
                           nodeId,
                           TwoPcClassNonNested);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(!twoPcHandle.twoPcHandle);

    status = response.status;
    if (status != StatusOk) {
        goto CommonExit;
    }

    *outputOut = (XcalarApiOutput *) response.output;
    *outputSizeOut = response.outputSize;

CommonExit:
    return status;
}

Status
StatsLib::getStatsGroupIdMapReq(NodeId nodeId,
                                XcalarApiOutput **outputOut,
                                size_t *outputSizeOut)
{
    Status status = StatusOk;
    MsgEphemeral eph;
    MsgMgr *msgMgr = MsgMgr::get();
    TwoPcHandle twoPcHandle;
    MsgStatsResponse response;

    *outputOut = NULL;
    *outputSizeOut = 0;

    msgMgr->twoPcEphemeralInit(&eph,
                               (void *) &nodeId,
                               sizeof(NodeId),
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcGetStatGroupIdMap1,
                               &response,
                               (TwoPcBufLife)(TwoPcMemCopyInput |
                                              TwoPcMemCopyOutput));

    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcGetStatGroupIdMap,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                              MsgRecvHdrPlusPayload),
                           TwoPcSyncCmd,
                           TwoPcSingleNode,
                           nodeId,
                           TwoPcClassNonNested);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(!twoPcHandle.twoPcHandle);

    status = response.status;
    if (status != StatusOk) {
        goto CommonExit;
    }

    *outputOut = (XcalarApiOutput *) response.output;
    *outputSizeOut = response.outputSize;

CommonExit:
    return status;
}

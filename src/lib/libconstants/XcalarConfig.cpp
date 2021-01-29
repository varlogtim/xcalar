// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "math.h"
#include "constants/XcalarConfig.h"
#include "util/MemTrack.h"
#include "config/Config.h"
#include "parent/Parent.h"
#include "localmsg/LocalMsg.h"
#include "xdb/Xdb.h"
#include "transport/TransportPage.h"
#include "strings/String.h"
#include "parent/ParentChild.h"

static constexpr const char *moduleName = "xcalarConfig";
XcalarConfig *XcalarConfig::instance = NULL;

Status
XcalarConfig::setupConfigMappings()
{
    for (int ii = KeyValueMappings::BcPercentOfMem;
         ii < KeyValueMappings::MappingsCount;
         ii++) {
        KeyValueMappings *curMap = &mappings_[ii];
        switch (ii) {
        // The intent is to have as few "knobs" as possible visible to the
        // customer via XD.  To do this almost all knobs should be hidden.
        // If they're hidden, you ask, how will the customer see them?  If
        // the customer encounters an issue they will work with Xcalar
        // support who will advise changes such as "Change 'config knob foo'
        // to 'value bar'".  When the value of a knob is different from the
        // default value it is displayed by XD (becomes visible to the user
        // even though it is a hidden knob).  The knobs will fit into these
        // catagories:
        // hidden/changable:   the scenario described above
        // hidden/immutable:   not visible and not changable via XD.  These are
        //                     typically developer knobs which are only
        //                     changeable by editing the config file
        // visible/immutable:  something that is displayed by XD but cannot be
        //                     changed.  e.g. XcalarRootCompletePath
        // visible/changeable: is displayed by XD and can be changed
        //
        // XXX Just use automatic enum generator for string to Enum conversion.

        // Percent of memory to use for buffer cache.
        case KeyValueMappings::BcPercentOfMem:
            curMap->keyName = "BufferCachePercentOfTotalMem";
            curMap->variable = &bufferCachePercentOfTotalMem_;
            curMap->bufSize = sizeof(bufferCachePercentOfTotalMem_);
            curMap->defaultInteger = BufferCachePercentOfTotalMemDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::DatasetPercentOfMem:
            curMap->keyName = "DatasetPercentOfMem";
            curMap->variable = &datasetPercentOfMem_;
            curMap->bufSize = sizeof(datasetPercentOfMem_);
            curMap->defaultInteger = DatasetPercentOfMemDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::BufferCacheMlockPct:
            curMap->keyName = "BufferCacheMlockPercent";
            curMap->variable = &bufferCacheMlockPercent_;
            curMap->bufSize = sizeof(bufferCacheMlockPercent_);
            curMap->defaultInteger = BufferCacheMlockPercentDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = 0;
            curMap->maxValue = 100;
            break;

        case KeyValueMappings::RuntimeMixedModeMinCores:
            curMap->keyName = "RuntimeMixedModeMinCores";
            curMap->variable = &runtimeMixedModeMinCores_;
            curMap->bufSize = sizeof(runtimeMixedModeMinCores_);
            curMap->defaultInteger = RuntimeMixedModeMinCoresDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Maximum percent of swap that will be used.  After that point
        // allocations will fail and transaction recovery kicks in.
        case KeyValueMappings::SwapUsagePct:
            curMap->keyName = "SwapUsagePercent";
            curMap->variable = &swapUsagePercent_;
            curMap->bufSize = sizeof(swapUsagePercent_);
            curMap->defaultInteger = SwapUsagePercentDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = SwapUsagePercentMin;
            curMap->maxValue = SwapUsagePercentMax;
            break;

        case KeyValueMappings::BufCacheHdrDumpInCores:
            curMap->keyName = "BufCacheHdrDumpInCores";
            curMap->variable = &bufCacheHdrDumpInCores_;
            curMap->bufSize = sizeof(bufCacheHdrDumpInCores_);
            curMap->defaultBoolean = BufCacheHdrDumpInCoresDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Whether or not to mlock buffer cache.
        case KeyValueMappings::BufferCacheMlock:
            curMap->keyName = "BufferCacheMlock";
            curMap->variable = &bufCacheMlock_;
            curMap->bufSize = sizeof(bufCacheMlock_);
            curMap->defaultBoolean = BufferCacheMlockDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Buffer Cache is not created on TmpFs, but instead uses SHM segments.
        case KeyValueMappings::BufCacheNonTmpFs:
            curMap->keyName = "BufCacheNonTmpFs";
            curMap->variable = &bufCacheNonTmpFs_;
            curMap->bufSize = sizeof(bufCacheNonTmpFs_);
            curMap->defaultBoolean = BufferCacheNonTmpFsDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // When lazily mlocking buffer cache, what percentage is mlocked when a
        // chunk is needed.
        case KeyValueMappings::XdbPageMlockChunkPct:
            curMap->keyName = "XdbPageMlockChunkPct";
            curMap->variable = &xdbPageMlockChunkPct_;
            curMap->bufSize = sizeof(xdbPageMlockChunkPct_);
            curMap->defaultInteger = XdbPageMlockChunkPctDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = XdbPageMlockChunkPctMin;
            curMap->maxValue = XdbPageMlockChunkPctMax;
            break;

        // Maximum size of dataset when modeling.
        case KeyValueMappings::MaxInteractiveDataSize:
            curMap->keyName = MaxInteractiveDataSizeParamName;
            curMap->variable = &maxInteractiveDataSize_;
            curMap->bufSize = sizeof(maxInteractiveDataSize_);
            curMap->defaultLongInteger = MaxInteractiveDataSizeDefault;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Visible;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = MaxInteractiveDataSizeMin;
            curMap->maxValue = MaxInteractiveDataSizeMax;
            break;

        case KeyValueMappings::UdfExportBufferSize:
            curMap->keyName = "UdfExportBufferSize";
            curMap->variable = &udfExportBufferSize_;
            curMap->bufSize = sizeof(udfExportBufferSize_);
            curMap->defaultInteger = UdfExportBufferSizeDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = UdfExportBufferSizeMin;
            curMap->maxValue = UdfExportBufferSizeMax;
            break;

        case KeyValueMappings::PublishTableAccessRetries:
            curMap->keyName = "PublishTableAccessRetries";
            curMap->variable = &pubTableAccessRetries_;
            curMap->bufSize = sizeof(pubTableAccessRetries_);
            curMap->defaultInteger = PubTableRetriesDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = PubTableRetriesMin;
            curMap->maxValue = PubTableRetriesMax;
            break;

        case KeyValueMappings::PublishTableRetriesTimeoutMsec:
            curMap->keyName = "PublishTableRetriesTimeoutMsec";
            curMap->variable = &pubTableRetriesTimeoutMsec_;
            curMap->bufSize = sizeof(pubTableRetriesTimeoutMsec_);
            curMap->defaultInteger = PubTableRetriesTimeoutMsecDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = PubTableRetriesTimeoutMsecMin;
            curMap->maxValue = PubTableRetriesTimeoutMsecMax;
            break;

        case KeyValueMappings::PublishTableRestoreConcurrency:
            curMap->keyName = "PublishTableRestoreConcurrency";
            curMap->variable = &pubTableRestoreConcurrency_;
            curMap->bufSize = sizeof(pubTableRestoreConcurrency_);
            curMap->defaultInteger = PublishTableRestoreConcurrencyDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::DfMaxFields:
            curMap->keyName = "DfMaxFieldsPerRecord";
            curMap->variable = &dfMaxFieldsPerRecord_;
            curMap->bufSize = sizeof(dfMaxFieldsPerRecord_);
            curMap->defaultInteger = DfMaxFieldsPerRecordDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = DfMaxFieldsPerRecordMin;
            curMap->maxValue = DfMaxFieldsPerRecordMax;
            break;

        case KeyValueMappings::EnforceVA:
            // This parameter, if true, leads to Xcalar enforcing memory limits.
            // This is needed so that mallocs can fail and transaction recovery
            // occurs. If this is set to false then OOM will lead to the
            // Operating System kill us without transaction recovery.
            curMap->keyName = "EnforceVALimit";
            curMap->variable = &enforceVALimit_;
            curMap->bufSize = sizeof(enforceVALimit_);
            curMap->defaultBoolean = true;  // Immutable
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Immutable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Whether or not to dump all or memory or just a portion.
        case KeyValueMappings::Minidump:
            curMap->keyName = "Minidump";
            curMap->variable = &minidump_;
            curMap->bufSize = sizeof(minidump_);
            curMap->defaultBoolean = MinidumpDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::Cgroups:
            curMap->keyName = "Cgroups";
            curMap->variable = &cgroups_;
            curMap->bufSize = sizeof(cgroups_);
            curMap->defaultBoolean = CgroupsDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::CgroupsOsDefaultOnBoot:
            curMap->keyName = "CgroupsOsDefaultOnBoot";
            curMap->variable = &cgroupsOsDefaultOnBoot_;
            curMap->bufSize = sizeof(cgroupsOsDefaultOnBoot_);
            curMap->defaultBoolean = CgroupsOsDefaultOnBootDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::CgroupParamsSetOnBoot:
            curMap->keyName = "CgroupParamsSetOnBoot";
            curMap->variable = &cgroupsParamsSetOnBoot_;
            curMap->bufSize = sizeof(cgroupsParamsSetOnBoot_);
            curMap->defaultBoolean = CgroupsParamsSetOnBootDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::AutoCreateIndex:
            curMap->keyName = "AutoCreateIndex";
            curMap->variable = &autoCreateIndex_;
            curMap->bufSize = sizeof(autoCreateIndex_);
            curMap->defaultBoolean = AutoCreateIndexDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Xcalar root
        case KeyValueMappings::XcRootPath:
            curMap->keyName = "XcalarRootCompletePath";
            curMap->variable = xcalarRootCompletePath_;
            curMap->bufSize = sizeof(xcalarRootCompletePath_);
            curMap->defaultString = (void *) XcalarRootCompletePathDefault;
            curMap->type = KeyValueMappings::String;
            curMap->visible = KeyValueMappings::Visible;
            curMap->changeable = KeyValueMappings::Immutable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Directory where log file will be written
        case KeyValueMappings::XcLogDirPath:
            curMap->keyName = "XcalarLogCompletePath";
            curMap->variable = xcalarLogCompletePath_;
            curMap->bufSize = sizeof(xcalarLogCompletePath_);
            curMap->defaultString = (void *) LogDirDefaultPath;
            curMap->type = KeyValueMappings::String;
            curMap->visible = KeyValueMappings::Visible;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Specifies test mode.  Needed for things such as running func tests.
        case KeyValueMappings::TestMode:
            curMap->keyName = "TestMode";
            curMap->variable = &testMode_;
            curMap->bufSize = sizeof(testMode_);
            curMap->defaultBoolean = TestModeDefault;  // Immutable
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Immutable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Whether or not childnode crashes should create a core file.
        case KeyValueMappings::ChildNodeCore:
            curMap->keyName = "ChildNodeCore";
            curMap->variable = &childNodeCore_;
            curMap->bufSize = sizeof(childNodeCore_);
            curMap->defaultBoolean = ChildNodeCoreDefault;  // Immutable
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Immutable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::MaxOutstandApisWork:
            curMap->keyName = "MaximumOutstandingApisWork";
            curMap->variable = &maxOutstandApisWork_;
            curMap->bufSize = sizeof(maxOutstandApisWork_);
            curMap->defaultInteger = MaxOutstandApisWorkDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = MaxOutstandApisWorkMin;
            curMap->maxValue = MaxOutstandApisWorkMax;
            break;

        case KeyValueMappings::LocalMsgTimeoutUSecs:
            curMap->keyName = "XPUTimeoutUSecs";
            curMap->variable = &LocalMsg::timeoutUSecsConfig;
            curMap->bufSize = sizeof(LocalMsg::timeoutUSecsConfig);
            curMap->defaultLongInteger = LocalMsg::TimeoutUSecsDefault;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = LocalMsg::TimeoutUSecsMin;
            curMap->maxValue = LocalMsg::TimeoutUSecsMax;
            break;

        case KeyValueMappings::DurableEnforceClean:
            curMap->keyName = "DurableEnforceClean";
            curMap->variable = &durableEnforceClean_;
            curMap->bufSize = sizeof(durableEnforceClean_);
            curMap->defaultBoolean = DurableEnforceCleanDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Immutable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::DurableEnforceKnown:
            curMap->keyName = "DurableEnforceKnown";
            curMap->variable = &durableEnforceKnown_;
            curMap->bufSize = sizeof(durableEnforceKnown_);
            curMap->defaultBoolean = DurableEnforceKnownDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Immutable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Size of XDB page
        case KeyValueMappings::XdbPageSize:
            curMap->keyName = "XdbPageSize";
            curMap->variable = &xdbPageSize_;
            curMap->bufSize = sizeof(xdbPageSize_);
            curMap->defaultLongInteger = XdbPageSizeDefault;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = XdbPageSizeMin;
            curMap->maxValue = XdbPageSizeMax;
            break;

        // Whether or not to do network compression
        case KeyValueMappings::NetworkCompression:
            curMap->keyName = "NetworkCompression";
            curMap->variable = &TransportPageMgr::doNetworkCompression;
            curMap->bufSize = sizeof(TransportPageMgr::doNetworkCompression);
            curMap->defaultBoolean =
                TransportPageMgr::DoNetworkCompressionDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // If mlock is enabling, whether or not to do so lazily or all at once.
        // The main tradeoffs are boot time performance and better memory usage
        // (only mlock memory as needed).
        case KeyValueMappings::BufferCacheLazyMemLocking:
            curMap->keyName = "BufferCacheLazyMemLocking";
            curMap->variable = &bufferCacheLazyMemLocking_;
            curMap->bufSize = sizeof(bufferCacheLazyMemLocking_);
            curMap->defaultBoolean = BufferCacheLazyMemLockingDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Whether or not XDB page serialization is enabled.
        case KeyValueMappings::XdbSerDesMode:
            curMap->keyName = "XdbSerDesMode";
            curMap->variable = &xdbSerDesMode_;
            curMap->bufSize = sizeof(xdbSerDesMode_);
            curMap->defaultInteger = XdbSerDesModeDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Visible;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            // XXX: Use range checking to to disallow hdfs (value 3)
            curMap->doRangeCheck = true;
            curMap->minValue = 0;
            curMap->maxValue = 4;
            break;

        // Whether or not to compress data when SerDes is in use.
        case KeyValueMappings::XdbSerDesEnableCompression:
            curMap->keyName = "XdbSerDesEnableCompression";
            curMap->variable = &xdbSerdesEnableCompression_;
            curMap->bufSize = sizeof(xdbSerdesEnableCompression_);
            curMap->defaultBoolean = XdbSerDesEnableCompressionDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::XdbSerDesMaxDiskMB:
            curMap->keyName = "XdbSerDesMaxDiskMB";
            curMap->variable = &xdbSerDesMaxDiskMB_;
            curMap->bufSize = sizeof(xdbSerDesMaxDiskMB_);
            curMap->defaultLongInteger = XdbSerDesMaxDiskMBDefault;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Visible;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = 0;
            curMap->maxValue = LONG_MAX;
            break;

        case KeyValueMappings::XdbSerDesParallelism:
            curMap->keyName = "XdbSerDesParallelism";
            curMap->variable = &xdbSerDesParallelism_;
            curMap->bufSize = sizeof(xdbSerDesParallelism_);
            curMap->defaultLongInteger = XdbSerDesParallelismDefault;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = 0;
            curMap->maxValue = XdbSerDesParallelismMax;
            break;

        case KeyValueMappings::XdbHashSlots:
            curMap->keyName = "XdbHashSlots";
            curMap->variable = &XdbMgr::xdbHashSlots;
            curMap->bufSize = sizeof(XdbMgr::xdbHashSlots);
            curMap->defaultInteger = XdbMgr::XdbHashSlotsDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = XdbMgr::XdbHashSlotsMin;
            curMap->maxValue = XdbMgr::XdbHashSlotsMax;
            break;

        case KeyValueMappings::NonBlockingSocket:
            curMap->keyName = "NonBlockingSocket";
            curMap->variable = &nonBlockingSocket_;
            curMap->bufSize = sizeof(nonBlockingSocket_);
            curMap->defaultBoolean = NonBlockingSocketDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::SourcePagesScaleFactor:
            curMap->keyName = "SourcePagesScaleFactor";
            curMap->variable = &sourcePagesScaleFactor_;
            curMap->bufSize = sizeof(sourcePagesScaleFactor_);
            curMap->defaultInteger = PageLimitScaleFactorDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = PageLimitScaleFactorMin;
            curMap->maxValue = PageLimitScaleFactorMax;
            break;

        case KeyValueMappings::DurableUpdateOnLoad:
            curMap->keyName = "DurableUpdateOnLoad";
            curMap->variable = &durableUpdateOnLoad_;
            curMap->bufSize = sizeof(durableUpdateOnLoad_);
            curMap->defaultBoolean = DurableUpdateOnLoadDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Immutable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Path to directory for BufferCache files. It is highly
        // recommended this be on local SSDs.
        case KeyValueMappings::BufferCachePath:
            curMap->keyName = "BufferCachePath";
            curMap->variable = bufferCachePath_;
            curMap->bufSize = sizeof(bufferCachePath_);
            curMap->defaultString = (void *) BufferCachePathDefault;
            curMap->type = KeyValueMappings::String;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Path to directory where SerDes will write files.  It is highly
        // recommended this be on local SSDs.
        case KeyValueMappings::XdbLocalSerDesPath:
            curMap->keyName = "XdbLocalSerDesPath";
            curMap->variable = xdbLocalSerDesPath_;
            curMap->bufSize = sizeof(xdbLocalSerDesPath_);
            curMap->defaultString = (void *) XdbLocalSerDesPathDefault;
            curMap->type = KeyValueMappings::String;
            curMap->visible = KeyValueMappings::Visible;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::XdbMaxPagingFileSize:
            curMap->keyName = "XdbMaxPagingFileSize";
            curMap->variable = &xdbMaxPagingFileSize_;
            curMap->bufSize = sizeof(&xdbMaxPagingFileSize_);
            curMap->defaultLongInteger = 0;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Visible;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Whether to send support bundles back home to Xcalar.
        case KeyValueMappings::SendSupportBundle:
            curMap->keyName = "SendSupportBundle";
            curMap->variable = &sendSupportBundle_;
            curMap->bufSize = sizeof(sendSupportBundle_);
            curMap->defaultBoolean = SendSupportBundleDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Visible;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // XcMonitor election time out.  This is needed by the monitor
        // sanity tests.
        case KeyValueMappings::XcMonElectionTimeout:
            curMap->keyName = "XcMonElectionTimeout";
            curMap->variable = &xcMonElectionTimeout_;
            curMap->bufSize = sizeof(xcMonElectionTimeout_);
            curMap->defaultLongInteger = XcMonElectionTimeoutDefault;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = XcMonElectionTimeoutMin;
            curMap->maxValue = XcMonElectionTimeoutMax;
            break;

#ifdef ENABLE_XCMONITOR_KNOBS
        case KeyValueMappings::XcMonMasterSlaveTimeout:
            curMap->keyName = "XcMonMasterSlaveTimeout";
            curMap->variable = &xcMonMasterSlaveTimeout_;
            curMap->bufSize = sizeof(xcMonMasterSlaveTimeout_);
            curMap->defaultLongInteger = XcMonMasterSlaveTimeoutDefault;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = XcMonMasterSlaveTimeoutMin;
            curMap->maxValue = XcMonMasterSlaveTimeoutMax;
            break;

        case KeyValueMappings::XcMonSlaveMasterTimeout:
            curMap->keyName = "XcMonSlaveMasterTimeout";
            curMap->variable = &xcMonSlaveMasterTimeout_;
            curMap->bufSize = sizeof(xcMonSlaveMasterTimeout_);
            curMap->defaultLongInteger = XcMonSlaveMasterTimeoutDefault;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = XcMonSlaveMasterTimeoutMin;
            curMap->maxValue = XcMonSlaveMasterTimeoutMax;
            break;

        case KeyValueMappings::XcMonUsrNodeReapTimeout:
            curMap->keyName = "XcMonUsrNodeReapTimeout";
            curMap->variable = &xcMonUsrNodeReapTimeout_;
            curMap->bufSize = sizeof(xcMonUsrNodeReapTimeout_);
            curMap->defaultLongInteger = XcMonUsrNodeReapTimeoutDefault;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = XcMonUsrNodeReapTimeoutMin;
            curMap->maxValue = XcMonUsrNodeReapTimeoutMax;
            break;
#endif  // ENABLE_XCMONITOR_KNOBS

        case KeyValueMappings::EnableSparseDag:
            curMap->keyName = "EnableSparseDag";
            curMap->variable = &enableSparseDag_;
            curMap->bufSize = sizeof(enableSparseDag_);
            curMap->defaultBoolean = EnableSparseDagDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Specifies that license enforcement is enabled.
        case KeyValueMappings::EnableLicenseEnforcement:
            curMap->keyName = "EnableLicenseEnforcement";
            curMap->variable = &enableLicenseEnforcement_;
            curMap->bufSize = sizeof(enableLicenseEnforcement_);
            curMap->defaultBoolean = EnableLicenseEnforcementDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Immutable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // The log level for syslogs.
        case KeyValueMappings::ClusterLogLevel:
            curMap->keyName = ClusterLogLevelParamName;
            curMap->variable = clusterLogLevel_;
            curMap->bufSize = sizeof(clusterLogLevel_);
            curMap->defaultString = (void *) "Inval";
            curMap->type = KeyValueMappings::String;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Number of XPUs to create a boot time.
        case KeyValueMappings::XpusBootstrapCachedCount:
            curMap->keyName = "XpusBootstrapCachedCount";
            curMap->variable = &xpuBootstrapCachedCount_;
            curMap->bufSize = sizeof(xpuBootstrapCachedCount_);
            curMap->defaultLongInteger = getXpusBootstrapCachedCountDefault();
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = getXpusBootstrapCachedCountMin();
            curMap->maxValue = getXpusBootstrapCachedCountMax();
            break;

        case KeyValueMappings::XpuMaxRssInKBs:
            curMap->keyName = "XpuMaxRssInKBs";
            curMap->variable = &xpuMaxRssInKBs_;
            curMap->bufSize = sizeof(xpuMaxRssInKBs_);
            curMap->defaultLongInteger = XpuMaxRssInKBsDefault;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;
            curMap->maxValue = 0;
            break;

        case KeyValueMappings::OptimizedExecParallelism:
            curMap->keyName = "OptimizedExecParallelism";
            curMap->variable = &optimizedExecParallelism_;
            curMap->bufSize = sizeof(optimizedExecParallelism_);
            curMap->defaultInteger = defOptimizedExecParallelism();
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;
            curMap->maxValue = 0;
            break;

        case KeyValueMappings::TpShippersCount:
            curMap->keyName = "TpShippersCount";
            curMap->variable = &tpShippersCount_;
            curMap->bufSize = sizeof(tpShippersCount_);
            curMap->defaultInteger = TpShippersDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = TpShippersMin;
            curMap->maxValue = TpShippersMax;
            break;

        case KeyValueMappings::DfStatsShippersCount:
            curMap->keyName = "DfStatsShippersCount";
            curMap->variable = &dfStatsShippersCount_;
            curMap->bufSize = sizeof(dfStatsShippersCount_);
            curMap->defaultInteger = DfStatsShippersDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = DfStatsShippersMin;
            curMap->maxValue = DfStatsShippersMax;
            break;

        case KeyValueMappings::MaxAsyncPagingIOs:
            curMap->keyName = "MaxAsyncPagingIOs";
            curMap->variable = &maxAsyncPagingIOs_;
            curMap->bufSize = sizeof(maxAsyncPagingIOs_);
            curMap->defaultInteger = instance->getMaxAsyncPagingIOsDefault();
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = MaxAsyncPagingIOsMin;
            curMap->maxValue = MaxAsyncPagingIOsMax;
            break;

        // Maximum number of XPUs to keep around.
        case KeyValueMappings::XpusMaxCachedCount:
            curMap->keyName = "XpusMaxCachedCount";
            curMap->variable = &xpuMaxCachedCount_;
            curMap->bufSize = sizeof(xpuMaxCachedCount_);
            curMap->defaultLongInteger = getXpusMaxCachedCountDefault();
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = getXpusMaxCachedCountMin();
            curMap->maxValue = getXpusMaxCachedCountMax();
            break;

        case KeyValueMappings::NoChildLDPreload:
            curMap->keyName = "NoChildLDPreload";
            curMap->variable = &noChildLdPreload_;
            curMap->bufSize = sizeof(noChildLdPreload_);
            curMap->defaultBoolean = NoChildLDPreloadDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Immutable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::IpVersionOptions:
            curMap->keyName = "IpVersionOptions";
            curMap->variable = &ipVersionOptions_;
            curMap->bufSize = sizeof(ipVersionOptions_);
            curMap->defaultInteger = (uint32_t) IpVersionOptionsDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = 1;
            curMap->maxValue = 4;
            break;

        // Stack size for each runtime thread.
        case KeyValueMappings::ThrStackSize:
            curMap->keyName = "ThrStackSize";
            curMap->variable = &thrStackSize_;
            curMap->bufSize = sizeof(thrStackSize_);
            curMap->defaultInteger = ThrStackSizeDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = ThrStackSizeMin;
            curMap->maxValue = ThrStackSizeMax;
            break;

        case KeyValueMappings::AcceptExpiredLicense:
            curMap->keyName = "AcceptExpiredLicense";
            curMap->variable = &acceptExpiredLicense_;
            curMap->bufSize = sizeof(acceptExpiredLicense_);
            curMap->defaultBoolean = AcceptExpiredLicenseDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Immutable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not used
            curMap->maxValue = 0;  // Not used
            break;

        case KeyValueMappings::EnablePredumpHandler:
            curMap->keyName = "EnablePredumpHandler";
            curMap->variable = &enablePredumpHandler_;
            curMap->bufSize = sizeof(enablePredumpHandler_);
            curMap->defaultBoolean = EnablePredumpHandlerDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::MaxKvsSzGlobalMB:
            curMap->keyName = "MaxKvsSzGlobalMB";
            curMap->variable = &maxKvsSzGlobalMB_;
            curMap->bufSize = sizeof(maxKvsSzGlobalMB_);
            curMap->defaultLongInteger = MaxKvsSzGlobalMBDefault;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = MaxKvsSzGlobalMBDefault;         // MB
            curMap->maxValue = MaxKvsSzGlobalMBDefault * 1024;  // MB
            break;

        case KeyValueMappings::MaxKvsSzWorkbookMB:
            curMap->keyName = "MaxKvsSzWorkbookMB";
            curMap->variable = &maxKvsSzWorkbookMB_;
            curMap->bufSize = sizeof(maxKvsSzWorkbookMB_);
            curMap->defaultLongInteger = MaxKvsSzWorkbookMBDefault;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = true;
            curMap->minValue = MaxKvsSzWorkbookMBDefault;         // MB
            curMap->maxValue = MaxKvsSzWorkbookMBDefault * 1024;  // MB
            break;

        case KeyValueMappings::DecimalRescale:
            curMap->keyName = "MoneyRescale";
            curMap->variable = &decimalRescale_;
            curMap->bufSize = sizeof(decimalRescale_);
            curMap->defaultBoolean = DecimalRescaleDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::DecimalScaleDigits:
            curMap->keyName = "MoneyScaleDigits";
            curMap->variable = &decimalScaleDigits_;
            curMap->bufSize = sizeof(decimalScaleDigits_);
            curMap->defaultLongInteger = DecimalScaleDigitsDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;
            curMap->maxValue = 0;
            break;

        case KeyValueMappings::BufCacheDbgMemLimitBytes:
            curMap->keyName = "BufCacheDbgMemLimitBytes";
            curMap->variable = &bufCacheDbgMemLimitBytes_;
            curMap->bufSize = sizeof(bufCacheDbgMemLimitBytes_);
            curMap->defaultLongInteger = 0;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Immutable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::EnablePageCache:
            curMap->keyName = "EnablePageCache";
            curMap->variable = &enablePageCache_;
            curMap->bufSize = sizeof(enablePageCache_);
            curMap->defaultBoolean = EnablePageCacheDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = true;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Whether or not to collect any stats.
        case KeyValueMappings::CollectStats:
            curMap->keyName = "CollectStats";
            curMap->variable = &collectStats_;
            curMap->bufSize = sizeof(collectStats_);
            curMap->defaultBoolean = CollectStatsDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Whether or not to collect dataflow stats.
        case KeyValueMappings::CollectDataflowStats:
            curMap->keyName = "CollectDataflowStats";
            curMap->variable = &collectDataflowStats_;
            curMap->bufSize = sizeof(collectDataflowStats_);
            curMap->defaultBoolean = CollectDataflowStatsDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Whether or not to collect system stats.
        case KeyValueMappings::CollectSystemStats:
            curMap->keyName = "CollectSystemStats";
            curMap->variable = &collectSystemStats_;
            curMap->bufSize = sizeof(collectSystemStats_);
            curMap->defaultBoolean = CollectSystemStatsDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        // Whether or not to collect proc stats.
        case KeyValueMappings::IncludeProcInfoInStats:
            curMap->keyName = "IncludePerProcStats";
            curMap->variable = &includeProcInfoInStats_;
            curMap->bufSize = sizeof(includeProcInfoInStats_);
            curMap->defaultBoolean = IncludeProcInfoInStatsDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::IncludePerCpuStats:
            curMap->keyName = "IncludePerCpuStats";
            curMap->variable = &includePerCpuStats_;
            curMap->bufSize = sizeof(includePerCpuStats_);
            curMap->defaultBoolean = IncludePerCpuStatsDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::IncludeLibstats:
            curMap->keyName = "IncludeLibstats";
            curMap->variable = &includeLibstats_;
            curMap->bufSize = sizeof(includeLibstats_);
            curMap->defaultBoolean = IncludeLibstatsDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::IncludeCgoupStats:
            curMap->keyName = "IncludeCgroupStats";
            curMap->variable = &includeCgroupStats_;
            curMap->bufSize = sizeof(includeCgroupStats_);
            curMap->defaultBoolean = IncludeCgroupStatsDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::IncludeTopStats:
            curMap->keyName = "IncludeTopStats";
            curMap->variable = &includeTopStats_;
            curMap->bufSize = sizeof(includeTopStats_);
            curMap->defaultBoolean = IncludeTopStatsDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::IncludeIOStats:
            curMap->keyName = "IncludeIOStats";
            curMap->variable = &includeIOStats_;
            curMap->bufSize = sizeof(includeIOStats_);
            curMap->defaultBoolean = IncludeIOStatsDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::IncludeTableMetaStats:
            curMap->keyName = "IncludeTableMetaStats";
            curMap->variable = &includeTableMetaStats_;
            curMap->bufSize = sizeof(includeTableMetaStats_);
            curMap->defaultBoolean = IncludeTableMetaStatsDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::CollectConfigStats:
            curMap->keyName = "CollectConfigStats";
            curMap->variable = &collectConfigStats_;
            curMap->bufSize = sizeof(collectConfigStats_);
            curMap->defaultBoolean = CollectConfigStatsDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::RuntimeStats:
            curMap->keyName = "RuntimeStats";
            curMap->variable = &runtimeStats_;
            curMap->bufSize = sizeof(runtimeStats_);
            curMap->defaultBoolean = RuntimeStatsDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::EnableRuntimeStatsApp:
            curMap->keyName = "EnableRuntimeStatsApp";
            curMap->variable = &enableRuntimeStatsApp_;
            curMap->bufSize = sizeof(enableRuntimeStatsApp_);
            curMap->defaultBoolean = RuntimeStatsAppDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::CtxTracesMode:
            curMap->keyName = "CtxTracesMode";
            curMap->variable = &ctxTracesMode_;
            curMap->bufSize = sizeof(ctxTracesMode_);
            curMap->defaultInteger = CtxTracesModeDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;
            curMap->maxValue = 0;
            break;

        case KeyValueMappings::CtxTracesArg:
            curMap->keyName = "CtxTracesArg";
            curMap->variable = &ctxTracesArg_;
            curMap->bufSize = sizeof(ctxTracesArg_);
            curMap->defaultInteger = CtxTracesArgDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;
            curMap->maxValue = 0;
            break;

        // Stats collection interval.
        case KeyValueMappings::StatsCollectionInterval:
            curMap->keyName = "StatsCollectionInterval";
            curMap->variable = &statsCollectionInterval_;
            curMap->bufSize = sizeof(statsCollectionInterval_);
            curMap->defaultInteger = StatsCollectionIntervalDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = 1;
            curMap->maxValue = 120;
            break;

            // Stats write interval.
        case KeyValueMappings::StatsWriteInterval:
            curMap->keyName = "StatsWriteInterval";
            curMap->variable = &statsWriteInterval_;
            curMap->bufSize = sizeof(statsWriteInterval_);
            curMap->defaultInteger = StatsWriteIntervalDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = 1;
            curMap->maxValue = 120;  // Max set to 2 min (to avoid stats files
                                     // and memory payload of stats app getting
                                     // too large)
            break;

        case KeyValueMappings::EnablePageDensityStats:
            curMap->keyName = "EnablePageDensityStats";
            curMap->variable = &enablePageDensityStats_;
            curMap->bufSize = sizeof(enablePageDensityStats_);
            curMap->defaultBoolean = PageDensityStatsDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::ImdPreparePctFailure:
            curMap->keyName = "ImdPreparePercentFailure";
            curMap->variable = &imdPreparePctFailure_;
            curMap->bufSize = sizeof(imdPreparePctFailure_);
            curMap->defaultInteger = ImdPreparePctFailureDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = ImdPreparePctFailureMin;
            curMap->maxValue = ImdPreparePctFailureMax;
            break;

        case KeyValueMappings::ImdCommitPctFailure:
            curMap->keyName = "ImdCommitPercentFailure";
            curMap->variable = &imdCommitPctFailure_;
            curMap->bufSize = sizeof(imdCommitPctFailure_);
            curMap->defaultInteger = ImdCommitPctFailureDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = ImdCommitPctFailureMin;
            curMap->maxValue = ImdCommitPctFailureMax;
            break;

        case KeyValueMappings::ImdPostCommitPctFailure:
            curMap->keyName = "ImdPostCommitPercentFailure";
            curMap->variable = &imdPostCommitPctFailure_;
            curMap->bufSize = sizeof(imdPostCommitPctFailure_);
            curMap->defaultInteger = ImdPostCommitPctFailureDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = ImdPostCommitPctFailureMin;
            curMap->maxValue = ImdPostCommitPctFailureMax;
            break;

        case KeyValueMappings::ImdAbortPctFailure:
            curMap->keyName = "ImdAbortPercentFailure";
            curMap->variable = &imdAbortPctFailure_;
            curMap->bufSize = sizeof(imdAbortPctFailure_);
            curMap->defaultInteger = ImdAbortPctFailureDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = ImdAbortPctFailureMin;
            curMap->maxValue = ImdAbortPctFailureMax;
            break;

        case KeyValueMappings::XcalarPagingThresholdPct:
            curMap->keyName = "XcalarPagingThresholdPct";
            curMap->variable = &xcalarPagingThresholdPct_;
            curMap->bufSize = sizeof(xcalarPagingThresholdPct_);
            curMap->defaultInteger = XcalarPagingThresholdPctDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = XcalarPagingThresholdPctMin;
            curMap->maxValue = XcalarPagingThresholdPctMax;
            break;

#ifdef BUFCACHETRACE
        case KeyValueMappings::AbortOnOom:
            curMap->keyName = "AbortOnOom";
            curMap->variable = &abortOnOom_;
            curMap->bufSize = sizeof(abortOnOom_);
            curMap->defaultInteger = AbortOnOomDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = 0;
            curMap->maxValue = 2;
            break;
#endif

        case KeyValueMappings::RuntimePerfPort:
            curMap->keyName = "RuntimePerfPort";
            curMap->variable = &runtimePerfPort_;
            curMap->bufSize = sizeof(runtimePerfPort_);
            curMap->defaultInteger = RuntimePerfPortDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Visible;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = RuntimePerfPortMin;
            curMap->maxValue = RuntimePerfPortMax;
            break;

        case KeyValueMappings::GrpcServerPort:
            curMap->keyName = "GrpcServerPort";
            curMap->variable = &grpcServerPort_;
            curMap->bufSize = sizeof(grpcServerPort_);
            curMap->defaultInteger = GrpcServerPortDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Visible;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = GrpcServerPortMin;
            curMap->maxValue = GrpcServerPortMax;
            break;

        case KeyValueMappings::DataflowStateDeleteTimeoutInSecs:
            curMap->keyName = "DataflowStateDeleteTimeoutInSecs";
            curMap->variable = &dataflowStateDeleteTimeoutinSecs_;
            curMap->bufSize = sizeof(dataflowStateDeleteTimeoutinSecs_);
            curMap->defaultLongInteger = DataflowStateDelteTimeoutinSecsDefault;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not used
            curMap->maxValue = 0;  // Not used
            break;

        case KeyValueMappings::EnableFailureSummaryReport:
            curMap->keyName = "EnableFailureSummaryReport";
            curMap->variable = &enableFailureSummaryReport_;
            curMap->bufSize = sizeof(enableFailureSummaryReport_);
            curMap->defaultBoolean = FailureSummaryReportDefault;
            curMap->type = KeyValueMappings::Boolean;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not Used
            curMap->maxValue = 0;  // Not Used
            break;

        case KeyValueMappings::BcScanBatchSize:
            curMap->keyName = "BcScanBatchSize";
            curMap->variable = &bcScanBatchSize_;
            curMap->bufSize = sizeof(bcScanBatchSize_);
            curMap->defaultLongInteger = BcScanBatchSizeDefault;
            curMap->type = KeyValueMappings::LongInteger;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;  // Not used
            curMap->maxValue = 0;  // Not used
            break;

        case KeyValueMappings::AccumulatorHashTableSlotCount:
            curMap->keyName = "AccumulatorHashTableSlotCount";
            curMap->variable = &accumulatorHashTableSlotCount_;
            curMap->bufSize = sizeof(accumulatorHashTableSlotCount_);
            curMap->defaultInteger = AccumulatorHashTableSlotCountDefault;
            curMap->type = KeyValueMappings::Integer;
            curMap->visible = KeyValueMappings::Hidden;
            curMap->changeable = KeyValueMappings::Changeable;
            curMap->restartRequired = false;
            curMap->doRangeCheck = true;
            curMap->minValue = AccumulatorHashTableSlotCountMin;
            curMap->maxValue = AccumulatorHashTableSlotCountMax;
            break;

        default:
            assert(0);
            return StatusUnimpl;
        }
    }

    // Set up string defaults.  Unlike scalars, arrays have an additional
    // level of indirection requiring an explicit copy
    for (int ii = KeyValueMappings::BcPercentOfMem;
         ii < KeyValueMappings::MappingsCount;
         ii++) {
        KeyValueMappings *curMap = &mappings_[ii];

        if (curMap->type == KeyValueMappings::String) {
            memZero(curMap->variable, curMap->bufSize);
            if (curMap->defaultString != NULL) {
                Status status = strSnprintf((char *) curMap->variable,
                                            curMap->bufSize,
                                            "%s",
                                            (char *) curMap->defaultString);
                if (status != StatusOk) {
                    return status;
                }
            }
        }
    }

    return StatusOk;
}

XcalarConfig *
XcalarConfig::get()
{
    return instance;
}

Status
XcalarConfig::init()
{
    Status status = StatusOk;

    if (instance != NULL) {
        return status;
    }
    instance = new (std::nothrow) XcalarConfig();
    if (instance == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    instance->coreCount_ = (unsigned) sysconf(_SC_NPROCESSORS_ONLN);
    instance->xpuBootstrapCachedCount_ =
        instance->getXpusBootstrapCachedCountDefault();
    instance->xpuMaxCachedCount_ = instance->getXpusMaxCachedCountDefault();
    instance->optimizedExecParallelism_ =
        instance->defOptimizedExecParallelism();
    instance->maxAsyncPagingIOs_ = instance->getMaxAsyncPagingIOsDefault();

    instance->mappings_ =
        (KeyValueMappings *) memAllocExt(sizeof(KeyValueMappings) *
                                             KeyValueMappings::MappingsCount,
                                         moduleName);
    if (instance == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = instance->setupConfigMappings();
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk && instance != NULL) {
        instance->destroy();
    }
    return status;
}

void
XcalarConfig::destroy()
{
    if (mappings_ != NULL) {
        memFree(mappings_);
        mappings_ = NULL;
    }
    delete instance;
    instance = NULL;
}

uint32_t
XcalarConfig::defOptimizedExecParallelism()
{
    return (uint32_t) coreCount_;
}

uint32_t
XcalarConfig::getMaxAsyncPagingIOsDefault()
{
    return (uint32_t)(coreCount_ * Runtime::TotalSdkScheds);
}

uint64_t
XcalarConfig::getXpusBootstrapCachedCountDefault()
{
    // Even though posix_spawn with vfork of the childnode process is quite
    // cheap, there is no big reason to discard these. Let's keep these
    // cached.
    // Also nothing scientific about the below number except it's a function
    // of core count and number of XPU schedulers for mixed mode.
    return (uint64_t) coreCount_ * ParentChild::MixedModeXpuScheds;
}

MustCheck uint64_t
XcalarConfig::getXpusBootstrapCachedCountMin()
{
    return (uint64_t) 1;
}

MustCheck uint64_t
XcalarConfig::getXpusBootstrapCachedCountMax()
{
    // Pick a large enough max here.
    return getXpusBootstrapCachedCountDefault() * 16;
}

MustCheck uint64_t
XcalarConfig::getXpusMaxCachedCountDefault()
{
    return getXpusBootstrapCachedCountDefault();
}

MustCheck uint64_t
XcalarConfig::getXpusMaxCachedCountMax()
{
    return getXpusBootstrapCachedCountMax();
}

MustCheck uint64_t
XcalarConfig::getXpusMaxCachedCountMin()
{
    return getXpusBootstrapCachedCountMin();
}

// Copyright 2013 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XCALARCONFIG_H_
#define _XCALARCONFIG_H_

#include <sys/time.h>
#include <sys/resource.h>

#include "config/Config.h"
#include "util/System.h"
#include "util/XcalarSocketInfo.h"
#include "newtupbuf/NewTupleTypes.h"
#include "runtime/Mutex.h"

class XcalarConfig final
{
  public:
    static MustCheck XcalarConfig *get();
    static MustCheck Status init();
    void destroy();

    // XXX Make this private.
    KeyValueMappings *mappings_;
    Mutex configLock_;

    // Xcalar Configuration Knobs
    // Exposed in default.cfg file if not hidden, or if hidden, but set to a
    // non-default value.  These are not intended to be changed unless
    // instructed to by Xcalar support.

    // Default/Min/Max values
    static constexpr uint32_t BufferCachePercentOfTotalMemDefault = 70;
    static constexpr uint32_t DatasetPercentOfMemDefault = 90;
    static constexpr uint32_t BufferCacheMlockPercentDefault = 100;
    static constexpr uint32_t BufferCacheMlockPercentMin = 0;
    static constexpr uint32_t BufferCacheMlockPercentMax = 100;
    static constexpr uint32_t SwapUsagePercentDefault = 90;
    static constexpr uint32_t SwapUsagePercentMin = 0;
    static constexpr uint32_t SwapUsagePercentMax = 100;
    static constexpr uint32_t RuntimeMixedModeMinCoresDefault = 32;

    static constexpr uint32_t XdbPageMlockChunkPctDefault = 1;
    static constexpr uint32_t XdbPageMlockChunkPctMin = 1;
    static constexpr uint32_t XdbPageMlockChunkPctMax = 100;
    static constexpr const char *MaxInteractiveDataSizeParamName =
        "MaxInteractiveDataSize";
    static constexpr size_t MaxInteractiveDataSizeDefault = 10 * GB;
    static constexpr size_t MaxInteractiveDataSizeMin = 1 * MB;
    static constexpr size_t MaxInteractiveDataSizeMax = 1 * TB;
    static constexpr unsigned DfMaxFieldsPerRecordDefault = 4 * KB;
    static constexpr int64_t UdfExportBufferSizeMin =
        (TupleMaxNumValuesPerRecord + 1) * DfMaxFieldValueSize;
    static constexpr int64_t UdfExportBufferSizeMax = 1 * GB;
    static constexpr int64_t UdfExportBufferSizeDefault =
        UdfExportBufferSizeMin;
    static constexpr unsigned DfMaxFieldsPerRecordMin =
        DfMaxFieldsPerRecordDefault;
    static constexpr unsigned DfMaxFieldsPerRecordMax = 10000;
    static constexpr bool EnforceVALimitDefault = true;
    static constexpr bool MinidumpDefault = true;
    static constexpr uint64_t QueryJobTimeoutinSecsDefault = SecsPerHour * 24;
    static constexpr bool TestModeDefault = false;
    static constexpr bool BufferCacheMlockDefault = true;
    static constexpr bool BufferCacheNonTmpFsDefault = true;
    static constexpr bool BufferCacheLazyMemLockingDefault = true;
    static constexpr uint32_t MaxOutstandApisWorkDefault = 128;
    static constexpr uint32_t MaxOutstandApisWorkMin = 1;
    static constexpr uint32_t MaxOutstandApisWorkMax = UINT32_MAX;
    static constexpr bool DurableEnforceCleanDefault = false;
    static constexpr bool DurableEnforceKnownDefault = true;
    static constexpr bool DurableUpdateOnLoadDefault = false;
    static constexpr bool NonBlockingSocketDefault = false;
    static constexpr const char *BufferCachePathDefault = "/var/tmp/xcalar";
    static constexpr const char *XdbLocalSerDesPathDefault = "";
    static constexpr uint64_t XdbMaxPagingFileSizeDefault =
        (uint64_t) 10 * 1024 * 1024 * 1024 * 1024;
    static constexpr bool ChildNodeCoreDefault = true;
    static constexpr const char *XcalarRootCompletePathDefault =
        "/var/opt/xcalar";
    static constexpr const char *LogDirDefaultPath = "/var/log/xcalar";

    static constexpr bool SendSupportBundleDefault = false;

    static constexpr const uint64_t PageLimitScaleFactorDefault = 2;
    static constexpr const uint64_t PageLimitScaleFactorMin = 2;
    static constexpr const uint64_t PageLimitScaleFactorMax = 1000;

    // XcMonitor timeouts (in oneTimeUnit units)
    static constexpr const uint64_t XcMonElectionTimeoutDefault = 15 * 60;
    static constexpr const uint64_t XcMonElectionTimeoutMin = 1;
    static constexpr const uint64_t XcMonElectionTimeoutMax = 10 * 15 * 60;
    static constexpr const uint64_t XcMonMasterSlaveTimeoutDefault = 60 * 60;
    static constexpr const uint64_t XcMonMasterSlaveTimeoutMin = 1;
    static constexpr const uint64_t XcMonMasterSlaveTimeoutMax = 10 * 60 * 60;
    static constexpr const uint64_t XcMonSlaveMasterTimeoutDefault = 30 * 60;
    static constexpr const uint64_t XcMonSlaveMasterTimeoutMin = 1;
    static constexpr const uint64_t XcMonSlaveMasterTimeoutMax = 10 * 30 * 60;
    static constexpr const uint64_t XcMonUsrNodeReapTimeoutDefault = 2 * 60;
    static constexpr const uint64_t XcMonUsrNodeReapTimeoutMin = 1;
    static constexpr const uint64_t XcMonUsrNodeReapTimeoutMax = 10 * 2 * 60;

    static constexpr bool EnableLicenseEnforcementDefault = true;
    static constexpr bool EnableSparseDagDefault = true;
    static constexpr const char *ClusterLogLevelParamName = "ClusterLogLevel";
    static constexpr bool NoChildLDPreloadDefault = false;

    // In potentially long running operations, periodically check if a shutdown
    // is in progress or user issued cancel and terminate the request.
    // This number should ALWAYS be a power of two so that the modulo operation
    // can be optimized to a simple and instruction instead of a getting the
    // remainder from divide.
    static constexpr uint64_t GlobalStateCheckInterval = 1 * KB;
    static constexpr uint64_t ThrStackSizeMin = 4 * MB;
    static constexpr uint64_t ThrStackSizeMax = 100 * MB;
    static constexpr uint64_t ThrStackSizeDefault = ThrStackSizeMin;

    enum class SerDesMode : uint32_t {
        Invalid = 0,
        Disabled = 1,
        Local = 2,
        Hdfs = 3,
        PageToLocalFile = 4,
    };
    static constexpr uint32_t XdbSerDesModeDefault =
        (uint32_t) SerDesMode::Disabled;
    static constexpr bool XdbSerDesEnableCompressionDefault = true;
    static constexpr uint64_t XdbSerDesMaxDiskMBDefault = 0;  // 0 is unlimited
    static constexpr uint64_t XdbSerDesParallelismMax = 127;
    static constexpr uint64_t XdbSerDesParallelismDefault = 7;

    enum class IpVersionOptions : uint32_t {
        Invalid = 0,
        UseLibcDefaults = 1,  // To be compliant with RFC-6724
        FavorIpV4 = 2,
        FavorIpV6 = 3,
        IpV4Only = 4,
        IpV6Only = 5,
    };
    static constexpr IpVersionOptions IpVersionOptionsDefault =
        IpVersionOptions::UseLibcDefaults;

    static constexpr const uint64_t XcStatsPushHeartBeatDefault =
        (60 * MSecsPerSec);
    // Max is smaller than Min since higher the value slower the heartbeat
    static constexpr const uint64_t XcMaxStatsPushHeartBeat =
#ifndef DEBUG
        (1 * MSecsPerSec);
#else
        (0 * MSecsPerSec);
#endif  // DEBUG
    static constexpr const uint64_t XcMinStatsPushHeartBeat = UINT64_MAX;
    static constexpr bool IsMultipleNodesPerHostDefault = false;
    static constexpr bool StatsShipmentDefault = false;

    static constexpr bool AcceptExpiredLicenseDefault = false;
    bool acceptExpiredLicense_ = AcceptExpiredLicenseDefault;

    // Enable catching of various signals (eg SIGSEGV) to allow pre-core dump
    // actions like log flushing. Probably makes cores less reliable (Xc-11850)
    static constexpr bool EnablePredumpHandlerDefault = false;
    bool enablePredumpHandler_ = EnablePredumpHandlerDefault;

    // XXX Make all of the below private.

    // Buffer cache memory as a percent of total memory.  The remaining
    // memory can be used for loading datasets.
    uint32_t bufferCachePercentOfTotalMem_ =
        BufferCachePercentOfTotalMemDefault;

    // Dataset memory as a percent of total memory.
    uint32_t datasetPercentOfMem_ = DatasetPercentOfMemDefault;

    uint32_t bufferCacheMlockPercent_ = BufferCacheMlockPercentDefault;

    uint32_t swapUsagePercent_ = SwapUsagePercentDefault;

    uint32_t xdbPageMlockChunkPct_ = XdbPageMlockChunkPctDefault;

    // Sample size used by point during interactive session
    size_t maxInteractiveDataSize_ = MaxInteractiveDataSizeDefault;

    // Fully qualified path for the root of Xcalar install. All logs,
    // sessions and user profiles will be stored here.
    char xcalarRootCompletePath_[XcalarApiMaxPathLen + 1];

    char xcalarLogCompletePath_[XcalarApiMaxPathLen + 1];

    // Not exposed in default.cfg file.  A user, under the direction of Xcalar
    // support, will type in the knob name and specify a value. By not
    // exposing these we don't have to have customer understandable
    // documentation.
    unsigned dfMaxFieldsPerRecord_ = DfMaxFieldsPerRecordDefault;

    int64_t udfExportBufferSize_ = UdfExportBufferSizeDefault;

    static constexpr const unsigned PubTableRetriesMin = 1;
    static constexpr const unsigned PubTableRetriesDefault = 100;
    static constexpr const unsigned PubTableRetriesMax = 1000;
    unsigned pubTableAccessRetries_ = PubTableRetriesDefault;

    static constexpr const unsigned PublishTableRestoreConcurrencyDefault = 1;
    unsigned pubTableRestoreConcurrency_ =
        PublishTableRestoreConcurrencyDefault;

    static constexpr const unsigned PubTableRetriesTimeoutMsecMin = 1;
    static constexpr const unsigned PubTableRetriesTimeoutMsecDefault = 10;
    static constexpr const unsigned PubTableRetriesTimeoutMsecMax = 1000;
    unsigned pubTableRetriesTimeoutMsec_ = PubTableRetriesTimeoutMsecDefault;

    // Set to true to enforce TotalSystemMemory.  In customer builds this needs
    // to be set to false as Xcalar should not enforce system memory limits.
    // It should be done by the operating system.
    bool enforceVALimit_ = EnforceVALimitDefault;

    // If this is set to true we will core dump a subset of memory, excluding
    // XDBs and other large memory consumers
    bool minidump_ = MinidumpDefault;

    bool bufCacheMlock_ = BufferCacheMlockDefault;

    bool bufCacheNonTmpFs_ = BufferCacheNonTmpFsDefault;

    // This is the amount of time before a results set is invalidated.  If
    // it is invalidated XI must do additional work when the user returns
    // to the data...but is not noticeable.
    uint64_t queryJobTimeoutinSecs_ = QueryJobTimeoutinSecsDefault;

    // Set to true to bootstrap in test mode.
    bool testMode_ = TestModeDefault;

    // Set to true for lazy memlocking buffer cache SHM.
    bool bufferCacheLazyMemLocking_ = BufferCacheLazyMemLockingDefault;

    // Maximum outstanding user driver APIs on each node in the cluster.
    uint32_t maxOutstandApisWork_ = MaxOutstandApisWorkDefault;

    // Prevent durable data generated by builds with local modifications from
    // being loaded.  Such data might be incompatible with the production IDL
    bool durableEnforceClean_ = DurableEnforceCleanDefault;

    // Prevent loading durable versions written by writers with unknown SHAs.
    // In most cases this prevents undesired forward compability
    bool durableEnforceKnown_ = DurableEnforceKnownDefault;

    // Rewrite durable data to the latest IDL immediately upon load
    bool durableUpdateOnLoad_ = DurableUpdateOnLoadDefault;

    // Use non blocking sends for twoPcAlt
    bool nonBlockingSocket_ = NonBlockingSocketDefault;

    // Use non blocking sends for twoPcAlt
    int sourcePagesScaleFactor_ = PageLimitScaleFactorDefault;

    // Fully qualified path for Local Xdb serialization
    char xdbLocalSerDesPath_[XcalarApiMaxPathLen + 1];

    // Maximum size of a file that is being used for paging
    uint64_t xdbMaxPagingFileSize_;

    // Fully qualified path for Buffer Cache files
    char bufferCachePath_[XcalarApiMaxPathLen + 1];

    // SerDes Mode
    uint32_t xdbSerDesMode_ = XdbSerDesModeDefault;

    // Compress/decompress serdes data
    bool xdbSerdesEnableCompression_ = XdbSerDesEnableCompressionDefault;

    uint64_t xdbSerDesMaxDiskMB_ = XdbSerDesMaxDiskMBDefault;
    uint64_t xdbSerDesParallelism_ = XdbSerDesParallelismDefault;

    // Should Child Node core?
    bool childNodeCore_ = ChildNodeCoreDefault;

    // Send support bundles back home
    bool sendSupportBundle_ = SendSupportBundleDefault;

    // Allows user to change the default (starting) log level that the
    // cluster uses. syslogs generated before the config file is parsed
    // will use the level hard-coded in the syslog module.
    //
    // The user can then change the level using the loglevelset
    // command/API but the loglevel will reset to this value on reboot.
    // This is only used if it is something other than "Inval".
    char clusterLogLevel_[XcalarApiMaxPathLen];

    // XcMonitor timeout values (in oneTimeUnit units).  These are only here
    // to use the "config" infrastructure.  The "real" values are in the
    // XcMonitor code.
    uint64_t xcMonElectionTimeout_ = XcMonElectionTimeoutDefault;
    uint64_t xcMonMasterSlaveTimeout_ = XcMonMasterSlaveTimeoutDefault;
    uint64_t xcMonSlaveMasterTimeout_ = XcMonSlaveMasterTimeoutDefault;
    uint64_t xcMonUsrNodeReapTimeout_ = XcMonUsrNodeReapTimeoutDefault;
    bool enableLicenseEnforcement_ = EnableLicenseEnforcementDefault;
    bool enableSparseDag_ = EnableSparseDagDefault;

    // Setting this to true will cause the LD_PRELOAD environment variable
    // value not to be inherited by forked processes.
    bool noChildLdPreload_ = NoChildLDPreloadDefault;

    IpVersionOptions ipVersionOptions_ = IpVersionOptionsDefault;

    // Initialized in the init method.
    uint64_t xpuMaxCachedCount_ = 0;
    uint64_t xpuBootstrapCachedCount_ = 0;
    // 128 MB default RSS for XPU
    static constexpr const uint64_t XpuMaxRssInKBsDefault = 128 * 1024;
    uint64_t xpuMaxRssInKBs_ = XpuMaxRssInKBsDefault;

    static constexpr const uint32_t DfStatsShippersMin = 1;
    static constexpr const uint32_t DfStatsShippersMax =
        std::numeric_limits<std::uint32_t>::max();
    static constexpr const uint32_t DfStatsShippersDefault = 64;
    uint32_t dfStatsShippersCount_ = DfStatsShippersDefault;

    static constexpr const uint32_t TpShippersMin = 1;
    static constexpr const uint32_t TpShippersMax =
        std::numeric_limits<std::uint32_t>::max();
    static constexpr const uint32_t TpShippersDefault = 1;
    uint32_t tpShippersCount_ = TpShippersDefault;
    uint32_t optimizedExecParallelism_ = 0;

    uint32_t runtimeMixedModeMinCores_ = RuntimeMixedModeMinCoresDefault;

    MustCheck size_t getMaxInteractiveDataSize()
    {
        return maxInteractiveDataSize_;
    }

    void setMaxInteractiveDataSize(uint64_t newSize)
    {
        maxInteractiveDataSize_ = newSize;
    };

    uint64_t thrStackSize_ = ThrStackSizeDefault;

    static constexpr bool CgroupsDefault = true;
    bool cgroups_ = CgroupsDefault;
    static constexpr bool CgroupsParamsSetOnBootDefault = true;
    bool cgroupsParamsSetOnBoot_ = CgroupsParamsSetOnBootDefault;
    static constexpr bool CgroupsOsDefaultOnBootDefault = false;
    bool cgroupsOsDefaultOnBoot_ = CgroupsOsDefaultOnBootDefault;

    static constexpr bool AutoCreateIndexDefault = false;
    bool autoCreateIndex_ = AutoCreateIndexDefault;

    static constexpr size_t XdbPageSizeMin = 128 * KB;
    static constexpr size_t XdbPageSizeMax = 2 * MB;
    static constexpr size_t XdbPageSizeDefault = XdbPageSizeMin;
    uint64_t xdbPageSize_ = XdbPageSizeDefault;

    static constexpr size_t MaxKvsSzGlobalMBDefault = 1000;   // MB
    static constexpr size_t MaxKvsSzWorkbookMBDefault = 100;  // MB
    size_t maxKvsSzGlobalMB_ = MaxKvsSzGlobalMBDefault;
    size_t maxKvsSzWorkbookMB_ = MaxKvsSzWorkbookMBDefault;

    // Decimal scale knobs
    static constexpr bool DecimalRescaleDefault = true;
    bool decimalRescale_ = DecimalRescaleDefault;
    static constexpr const uint32_t DecimalScaleDigitsDefault = 2;
    uint32_t decimalScaleDigits_ = DecimalScaleDigitsDefault;

    size_t bufCacheDbgMemLimitBytes_ = 0;

    static constexpr const uint32_t MaxAsyncPagingIOsMin = 1;
    static constexpr const uint32_t MaxAsyncPagingIOsMax = 1000;
    // Initialized in the init method
    uint32_t maxAsyncPagingIOs_ = 0;

    static constexpr bool CollectStatsDefault = true;
    bool collectStats_ = CollectStatsDefault;

    static constexpr bool CollectDataflowStatsDefault = true;
    bool collectDataflowStats_ = CollectDataflowStatsDefault;

    static constexpr bool CollectSystemStatsDefault = true;
    bool collectSystemStats_ = CollectSystemStatsDefault;

    static constexpr bool IncludeProcInfoInStatsDefault = false;
    bool includeProcInfoInStats_ = IncludeProcInfoInStatsDefault;

    static constexpr bool IncludePerCpuStatsDefault = true;
    bool includePerCpuStats_ = IncludePerCpuStatsDefault;

    static constexpr bool IncludeCgroupStatsDefault = true;
    bool includeCgroupStats_ = IncludeCgroupStatsDefault;

    static constexpr bool IncludeTopStatsDefault = true;
    bool includeTopStats_ = IncludeTopStatsDefault;

    static constexpr bool IncludeIOStatsDefault = true;
    bool includeIOStats_ = IncludeIOStatsDefault;

    static constexpr bool IncludeLibstatsDefault = true;
    bool includeLibstats_ = IncludeLibstatsDefault;

    static constexpr bool IncludeTableMetaStatsDefault = false;
    bool includeTableMetaStats_ = IncludeTableMetaStatsDefault;

    static constexpr bool CollectConfigStatsDefault = true;
    bool collectConfigStats_ = CollectConfigStatsDefault;

    // XXX TODO Turn on after fixing bugs around unhandled exceptions with
    // memory allocations.
    static constexpr bool RuntimeStatsDefault = false;
    bool runtimeStats_ = RuntimeStatsDefault;
    static constexpr bool RuntimeStatsAppDefault = false;
    bool enableRuntimeStatsApp_ = RuntimeStatsAppDefault;

    enum class CtxTraces : int32_t {
        Disabled = 0,
        AllHistoric = 1,
        BufCacheInFlight = (1 << 1),
        RefsInFlight = (1 << 2),
        DumpOnOom = (1 << 3),
        DumpOnOsPagableOom = (1 << 4),
        DumpOnPagingCounterArg = (1 << 5),
        DumpOnPagingModuloArg = (1 << 6),
    };
    static constexpr int32_t CtxTracesModeDefault =
        (int32_t) CtxTraces::Disabled;
    int32_t ctxTracesMode_ = CtxTracesModeDefault;

    static constexpr const unsigned StatsCollectionIntervalDefault = 5;
    static constexpr int64_t CtxTracesArgDefault = 0;
    int64_t ctxTracesArg_ = CtxTracesArgDefault;

    unsigned statsCollectionInterval_ = StatsCollectionIntervalDefault;

    static constexpr const unsigned StatsWriteIntervalDefault = 30;
    unsigned statsWriteInterval_ = StatsWriteIntervalDefault;

    static constexpr bool PageDensityStatsDefault = false;
    bool enablePageDensityStats_ = PageDensityStatsDefault;

    static constexpr bool EnablePageCacheDefault = true;
    bool enablePageCache_ = EnablePageCacheDefault;

    static constexpr const size_t ImdPreparePctFailureDefault = 0;
    static constexpr const size_t ImdPreparePctFailureMin = 0;
    static constexpr const size_t ImdPreparePctFailureMax = 100;
    size_t imdPreparePctFailure_ = ImdPreparePctFailureDefault;

    static constexpr const size_t ImdCommitPctFailureDefault = 0;
    static constexpr const size_t ImdCommitPctFailureMin = 0;
    static constexpr const size_t ImdCommitPctFailureMax = 100;
    size_t imdCommitPctFailure_ = ImdCommitPctFailureDefault;

    static constexpr const size_t ImdPostCommitPctFailureDefault = 0;
    static constexpr const size_t ImdPostCommitPctFailureMin = 0;
    static constexpr const size_t ImdPostCommitPctFailureMax = 100;
    size_t imdPostCommitPctFailure_ = ImdPostCommitPctFailureDefault;

    static constexpr const size_t ImdAbortPctFailureDefault = 0;
    static constexpr const size_t ImdAbortPctFailureMin = 0;
    static constexpr const size_t ImdAbortPctFailureMax = 100;
    size_t imdAbortPctFailure_ = ImdAbortPctFailureDefault;

    static constexpr const size_t XcalarPagingThresholdPctDefault = 0;
    static constexpr const size_t XcalarPagingThresholdPctMin = 0;
    static constexpr const size_t XcalarPagingThresholdPctMax = 100;
    size_t xcalarPagingThresholdPct_ = XcalarPagingThresholdPctDefault;

    // AccumulatorHashTable slot count
    static constexpr const size_t AccumulatorHashTableSlotCountDefault = 1021;
    static constexpr const size_t AccumulatorHashTableSlotCountMin = 101;
    static constexpr const size_t AccumulatorHashTableSlotCountMax = 100003;
    size_t accumulatorHashTableSlotCount_ =
        AccumulatorHashTableSlotCountDefault;

    static constexpr int32_t AbortOnOomDefault = 0;
    int32_t abortOnOom_ = AbortOnOomDefault;

    static constexpr const uint16_t RuntimePerfPortDefault = 6000;
    static constexpr const uint16_t RuntimePerfPortMin = 1025;
    static constexpr const uint16_t RuntimePerfPortMax = UINT16_MAX;
    uint16_t runtimePerfPort_ = RuntimePerfPortDefault;

    static constexpr const uint16_t GrpcServerPortDefault = 51234;
    static constexpr const uint16_t GrpcServerPortMin = 1025;
    static constexpr const uint16_t GrpcServerPortMax = UINT16_MAX;
    uint32_t grpcServerPort_ = GrpcServerPortDefault;

    static constexpr uint64_t BcScanBatchSizeDefault = 8;
    uint64_t bcScanBatchSize_ = BcScanBatchSizeDefault;

    static constexpr bool BufCacheHdrDumpInCoresDefault = false;
    bool bufCacheHdrDumpInCores_ = BufCacheHdrDumpInCoresDefault;

    // This is the timeout for query state to get deleted once reached.
    static constexpr uint64_t DataflowStateDelteTimeoutinSecsDefault = 5;
    uint64_t dataflowStateDeleteTimeoutinSecs_ =
        DataflowStateDelteTimeoutinSecsDefault;
    // To run dataflow in debug mode to get summary of failures
    static constexpr bool FailureSummaryReportDefault = false;
    bool enableFailureSummaryReport_ = FailureSummaryReportDefault;

  private:
    static XcalarConfig *instance;
    unsigned coreCount_;

    bool licenseCheckDone_ = false;
    // These get set based on the settings in the JSON license file.
    bool hasModelingLicense_;
    bool hasXceSdkLicense_;
    bool hasOperationalLicense_;
    bool hasOperationalMLLicense_;
    bool hasParallelStreamingConnectorsLicense_;

    MustCheck Status setupConfigMappings();

    MustCheck uint32_t defOptimizedExecParallelism();

    MustCheck uint64_t getXpusBootstrapCachedCountDefault();

    MustCheck uint64_t getXpusBootstrapCachedCountMin();

    MustCheck uint64_t getXpusBootstrapCachedCountMax();

    MustCheck uint64_t getXpusMaxCachedCountDefault();

    MustCheck uint64_t getXpusMaxCachedCountMax();

    MustCheck uint64_t getXpusMaxCachedCountMin();

    MustCheck uint32_t getMaxAsyncPagingIOsDefault();

    // Keep private, since this is a Singleton.
    XcalarConfig() = default;

    // Keep private, since this is a Singleton.
    ~XcalarConfig() = default;

    // disallow
    XcalarConfig(const XcalarConfig &) = delete;
    XcalarConfig &operator=(const XcalarConfig &) = delete;
};

#endif  // _XCALARCONFIG_H_

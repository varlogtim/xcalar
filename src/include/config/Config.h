// Copyright 2013-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XLRCONFIG_H_
#define _XLRCONFIG_H_

#include <climits>
#include "bits/posix1_lim.h"

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"
#include "runtime/Mutex.h"

struct KeyValueMappings {
    enum Type {
        Boolean,
        Integer,
        LongInteger,
        String,
    };

    enum {
        // Visible options are displayed by XI.  Hidden options are displayed if
        // they have been changed to a value different than the default.
        Visible = true,
        Hidden = false,
        // A parameter which is marked immutable means it cannot be changed via
        // the API provided to XI.  Internal users will change by editing the
        // default.cfg file directly.
        Changeable = true,
        Immutable = false,
        // Whether or not we should enforce the rules (e.g. such as Immutable)
        // which is done when changing constants via the API (used by XI).
        //
        // For example, TotalSystemMemory is marked "immutable" as we don't want
        // a user to change it via XI.  But a sophisticated user (ala Xcalar
        // engineer) can edit the config file and set TotalSystemMemory.  As
        // the parsing code is the same for both the API and file parsing this
        // specifies whether or not rules such as "immutable" should be
        // enforced.
        LaxEnforcement = false,
        StringentEnforcement = true,
    };

    // Use of some of these configuration parameters is disabled via compile
    // time toggles.  These can be enabled at compile time by defining the
    // toggle.  e.g.
    //      #define ENABLE_XEM 1
    //  The set of toggles include:
    //      ENABLE_XEM : Xcalar Enterprise Manager
    //      ENABLE_HDFS_SERDES : Enable SerDes to hdfs storage
    //      ENABLE_XCMONITOR_KNOBS : Tweak the behavior of xcmonitor
    //      CHANGE_THREAD_COUNT : twean runtime thread count

    enum : int {
        BcPercentOfMem = 0,
        DatasetPercentOfMem,
        SwapUsagePct,
        XdbPageMlockChunkPct,
        MaxInteractiveDataSize,
        UdfExportBufferSize,
        PublishTableAccessRetries,
        PublishTableRetriesTimeoutMsec,
        PublishTableRestoreConcurrency,
        DfMaxFields,
        EnforceVA,
        Minidump,
        XcRootPath,
        TestMode,
        ChildNodeCore,
        MaxOutstandApisWork,
        LocalMsgTimeoutUSecs,
        DurableEnforceClean,
        DurableEnforceKnown,
        XdbPageSize,
        NetworkCompression,
        BufferCacheLazyMemLocking,
        XdbSerDesMode,
        XdbMaxPagingFileSize,
        DurableUpdateOnLoad,
        NonBlockingSocket,
        SourcePagesScaleFactor,
        XcLogDirPath,
        BufferCachePath,
        XdbLocalSerDesPath,
        SendSupportBundle,
        XcMonElectionTimeout,
#ifdef ENABLE_XCMONITOR_KNOBS
        XcMonMasterSlaveTimeout,
        XcMonSlaveMasterTimeout,
        XcMonUsrNodeReapTimeout,
#endif  // ENABLE_XCMONITOR_KNOBS
        EnableSparseDag,
        EnableLicenseEnforcement,
        XdbSerDesEnableCompression,
        XdbHashSlots,
        ClusterLogLevel,
        XpusBootstrapCachedCount,
        XpusMaxCachedCount,
        XpuMaxRssInKBs,
        NoChildLDPreload,
        IpVersionOptions,
        ThrStackSize,
        // Used by test_expiredLicense
        AcceptExpiredLicense,
        EnablePredumpHandler,
        BufCacheHdrDumpInCores,
        BufferCacheMlock,
        BufferCacheMlockPct,
        RuntimeMixedModeMinCores,
        Cgroups,
        CgroupParamsSetOnBoot,
        CgroupsOsDefaultOnBoot,
        AutoCreateIndex,
        BufCacheNonTmpFs,
        MaxKvsSzGlobalMB,
        MaxKvsSzWorkbookMB,
        DecimalScaleDigits,
        DecimalRescale,
        BufCacheDbgMemLimitBytes,
        CollectStats,
        CollectDataflowStats,
        CollectSystemStats,
        StatsCollectionInterval,
        StatsWriteInterval,
        EnablePageDensityStats,
        IncludeProcInfoInStats,
        IncludePerCpuStats,
        IncludeCgoupStats,
        IncludeTopStats,
        IncludeIOStats,
        IncludeLibstats,
        IncludeTableMetaStats,
        CollectConfigStats,
        RuntimeStats,
        CtxTracesMode,
        CtxTracesArg,
        XdbSerDesMaxDiskMB,
        EnablePageCache,
        XdbSerDesParallelism,
        ImdPreparePctFailure,
        ImdCommitPctFailure,
        ImdPostCommitPctFailure,
        ImdAbortPctFailure,
        XcalarPagingThresholdPct,
#ifdef BUFCACHETRACE
        AbortOnOom,
#endif
        RuntimePerfPort,
        DataflowStateDeleteTimeoutInSecs,
        EnableFailureSummaryReport,
        BcScanBatchSize,
        TpShippersCount,
        DfStatsShippersCount,
        EnableRuntimeStatsApp,
        AccumulatorHashTableSlotCount,
        GrpcServerPort,
        OptimizedExecParallelism,
        MaxAsyncPagingIOs,
        MappingsCount  // Must be last
    };

    const char *keyName;
    void *variable;
    size_t bufSize;
    // Default value for the parameter.
    union {
        uint32_t defaultInteger;
        size_t defaultLongInteger;
        bool defaultBoolean;
        void *defaultString;
    };
    Type type;
    bool visible;
    bool changeable;
    bool restartRequired;
    // For numeric values, specifies whether or not to do range checking
    // of the value.
    bool doRangeCheck;
    // If doRangeCheck, specifies the min/max (values are inclusive)
    uint64_t minValue;
    uint64_t maxValue;
};

class Config final
{
  public:
    static constexpr unsigned ConfigUnspecifiedNodeId = 0xbeeffeed;
    static constexpr unsigned ConfigUnspecifiedNumActive = -1;
    static constexpr unsigned ConfigUnspecifiedNumActiveOnPhysical = -1;
    static constexpr int MaxModuleLength = 255;
    static constexpr int MaxKeyLength = 255;
    static constexpr int MaxValueLength = 255;
    // Size of the buffer used to build the config file content.
    static constexpr size_t MaxConfigFileSize = 1 * MB;
    // All set config parameter requests must come through one
    // node as the config infrastructure does not handle concurrent updates
    // from different nodes.
    static constexpr NodeId ConfigApiNode = 0;

    // Extra space in buffer for "extras" such as terminators,
    // or extending path names (e.g. adding ".backup").
    static constexpr int ConfigFileOverhead = 64;

    // Size of buffer used to input/output.
    static constexpr int MaxBufSize =
        MaxModuleLength + MaxKeyLength + MaxValueLength + ConfigFileOverhead;
    static constexpr const char *ContainerName = "container";

    char configFilePath_[XcalarApiMaxPathLen + 1];

    // The following are public only for LibConfigTest.cpp
    struct ConfigNode {
        char *ipAddr;  // IP or hostname from config file.
        int port;
        int apiPort;
        int monitorPort;
        bool local;  // On this physical machine?
    };

    struct Configuration {
        static constexpr unsigned DefaultNumNodes = 0xcafebeef;
        static constexpr int DefaultThriftPort = 0xdeadbeef;
        unsigned numNodes = DefaultNumNodes;
        ConfigNode *nodes = NULL;
        int thriftPort = DefaultThriftPort;
        char *thriftHost = NULL;
    };

    MustCheck NodeId getMyNodeId();
    MustCheck unsigned getActiveNodes();
    MustCheck unsigned getTotalNodes();
    MustCheck NodeId getMyDlmNode(MsgTypeId msgTypeId);

    MustCheck Status loadNodeInfo();
    MustCheck char *getIpAddr(unsigned index);
    MustCheck int getPort(unsigned index);
    MustCheck int getApiPort(unsigned index);
    MustCheck int getMonitorPort(unsigned index);
    MustCheck int getThriftPort();

    MustCheck const char *getThriftHost();
    MustCheck const char *getLogDirPath();
    MustCheck const char *getRootPath();

    MustCheck Status loadLogInfoFromConfig(const char *usrNodeCfgPath);
    MustCheck Status loadConfigFiles(const char *usrNodeCfgPath);
    void unloadConfigFiles();
    MustCheck Status writeConfigFiles(const char *usrNodeCfgPath);

    MustCheck unsigned getLowestNodeIdOnThisHost();
    MustCheck int getActiveNodesOnMyPhysicalNode();
    MustCheck unsigned getMyIndexOnThisMachine();
    MustCheck bool canIgnoreLine(char *line);

    MustCheck Status loadBinaryInfo();
    MustCheck const char *getBinaryName();

    MustCheck Status populateParentBinary();
    const MustCheck char *getParentBinName();

    void setMyNodeId(NodeId myNodeId);
    MustCheck Status setNumActiveNodes(unsigned numActiveNodes);
    void setNumActiveNodesOnMyPhysicalNode(unsigned numActiveNodes);

    MustCheck Status setNumTotalNodes(unsigned numTotalNodes);
    MustCheck Status setConfigParam(char *name, char *value);
    MustCheck Status updateLicense(char *licensePath, char *licenseString);

    static MustCheck Config *get();
    static MustCheck Status init();
    void destroy();

    MustCheck unsigned configIdentifyLocalNodes(ConfigNode *nodes,
                                                unsigned nodeCount);
    MustCheck bool changedFromDefault(KeyValueMappings *kvm);

    class Modules
    {
      public:
        Modules(const char *moduleName) : moduleName_(moduleName) {}
        virtual ~Modules() {}
        const char *moduleName_;
        virtual MustCheck Status parse(Configuration *config,
                                       char *key,
                                       char *value,
                                       bool stringentRules) = 0;
    };
    MustCheck Status parseBoolean(char *input, bool *output);
    MustCheck Status setConfigParameterLocal(char *name, char *value);
    MustCheck bool containerized() { return container_; }

  private:
    enum ModuleTypes : unsigned {
        NodeModule = 0,
        ConstantsModule = 1,
        ThriftModule = 2,
        FuncTestsModule = 3,
        MaxModule,
    };
    static constexpr unsigned DefaultNodeInit = 0xcafec001;
    Modules **modules_ = NULL;

    unsigned myIndexOnThisMachine_ = DefaultNodeInit;
    Configuration config_;

    // Used when reading config file.
    FILE *fp_ = NULL;

    // As originally passed to main.
    char argv0_[PATH_MAX] = "";

    // Name of binary being run.
    char binary_[NAME_MAX] = "";
    char parentBin_[NAME_MAX];

    static Config *instance;
    NodeId myNodeId_ = DefaultNodeInit;
    unsigned activeNodes_ = DefaultNodeInit;
    unsigned activeNodesOnMyPhysicalNode_ = DefaultNodeInit;
    unsigned totalNodes_ = DefaultNodeInit;
    bool container_ = false;

    // Used to serialize XcalarApiSetConfigParam operations as the backend
    // handling cannot handle it.
    Mutex setConfigParamLock_;

    // parse the config file for a specific module/key pair
    struct ParseFilter {
        ModuleTypes module;
        int key;
    };

    bool isLoopbackAddress(struct sockaddr *inAddr);
    MustCheck Status parseConfigFile(const char *path,
                                     Configuration *config,
                                     ParseFilter *pfilter);
    void parseFreeConfig(Configuration *config);
    MustCheck Status jsonifyInput(const char *filePath,
                                  const char *fileContent,
                                  char **outstr);
    MustCheck Status writeNewConfigFile(const char *filePath,
                                        Configuration *config);

    MustCheck Status configPopulateBinaryName(const char *link,
                                              char *binPath,
                                              size_t binPathSize,
                                              char *binName,
                                              size_t binNameSize);

    MustCheck Status setConfigParameter(char *name, char *value);
    MustCheck Status reverseParseNodes(Configuration *config,
                                       char *buff,
                                       size_t buffLen,
                                       size_t *lenUsed);
    MustCheck Status reverseParseThrift(Configuration *config,
                                        char *buff,
                                        size_t buffLen,
                                        size_t *lenUsed);
    MustCheck Status reverseParseConstants(Configuration *config,
                                           char *buff,
                                           size_t buffLen,
                                           size_t *lenUsed);
    MustCheck Status getExistingContent(const char *filePath,
                                        char *buff,
                                        size_t buffLen,
                                        size_t *lenUsed);
    MustCheck bool isReverseParsedLine(char *line);

    // inline func
    MustCheck bool markMatchingNodes(ConfigNode *nodes,
                                     size_t nodeCount,
                                     const char *hostname);
};

class ConfigModuleThrift final : public Config::Modules
{
  public:
    ConfigModuleThrift(const char *moduleName) : Config::Modules(moduleName) {}
    virtual ~ConfigModuleThrift() {}
    virtual MustCheck Status parse(Config::Configuration *config,
                                   char *key,
                                   char *value,
                                   bool stringentRules);

  private:
    ConfigModuleThrift(const ConfigModuleThrift &) = delete;
    ConfigModuleThrift &operator=(const ConfigModuleThrift &) = delete;
};

class ConfigModuleNodes final : public Config::Modules
{
  public:
    ConfigModuleNodes(const char *moduleName) : Config::Modules(moduleName) {}
    virtual ~ConfigModuleNodes() {}
    virtual MustCheck Status parse(Config::Configuration *config,
                                   char *key,
                                   char *value,
                                   bool stringentRules);

  private:
    ConfigModuleNodes(const ConfigModuleNodes &) = delete;
    ConfigModuleNodes &operator=(const ConfigModuleNodes &) = delete;
};

class ConfigModuleConstants final : public Config::Modules
{
  public:
    ConfigModuleConstants(const char *moduleName) : Config::Modules(moduleName)
    {
    }
    virtual ~ConfigModuleConstants() {}
    virtual MustCheck Status parse(Config::Configuration *config,
                                   char *key,
                                   char *value,
                                   bool stringentRules);

  private:
    ConfigModuleConstants(const ConfigModuleConstants &) = delete;
    ConfigModuleConstants &operator=(const ConfigModuleConstants &) = delete;
};

class ConfigModuleFuncTests final : public Config::Modules
{
  public:
    ConfigModuleFuncTests(const char *moduleName) : Config::Modules(moduleName)
    {
    }
    virtual ~ConfigModuleFuncTests() {}
    virtual MustCheck Status parse(Config::Configuration *config,
                                   char *key,
                                   char *value,
                                   bool stringentRules);

  private:
    ConfigModuleFuncTests(const ConfigModuleFuncTests &) = delete;
    ConfigModuleFuncTests &operator=(const ConfigModuleFuncTests &) = delete;
};

#endif  // _XLRCONFIG_H_

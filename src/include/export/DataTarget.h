// Copyright 2017 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATATARGET_H_
#define _DATATARGET_H_

#include "libapis/LibApisCommon.h"
#include "export/DataTargetTypes.h"
#include "log/Log.h"

class ExportTargetMgrIface;

class DataTargetManager final
{
    friend class DataTargetGvm;
    friend MustCheck Status upgradeDataTarget(LogLib::Handle *h);

  public:
    static MustCheck Status init();
    static void destroy();

    static MustCheck DataTargetManager &getRef();

    // Should be called by all nodes at startup
    // Will internally handle determining which should open log
    MustCheck Status addPersistedTargets();
    void removePersistedTargets();
    MustCheck Status listTargets(const char *typePattern,
                                 const char *namePattern,
                                 XcalarApiOutput **output,
                                 size_t *listTargetsOutputSize);

    MustCheck ExportTargetMgrIface *getTargetMgr(ExTargetType type);

    MustCheck Status addTarget(const ExExportTarget *target);
    MustCheck Status removeTarget(const ExExportTargetHdr *hdr);

    // Convert the persisted target structs to the current version
    MustCheck Status migrate(uint8_t *src, uint8_t **tgt);

    struct TargetPersistHeader {
        uint64_t targetSize;
        // This will need to be variable size in the future
        ExExportTarget target;
    };

    struct TargetListPersistedData {
        size_t totalLength;
        uint64_t version;
        struct timespec persistTime;
        ssize_t targetsCount;
        // This only works while they are statically sized
        TargetPersistHeader targets[0];
    };

    static constexpr const uint64_t currentTargetPersistVersion = 1;

    static void setDefaultExportArgs(ExTargetType type, LegacyExportMeta *meta);

  private:
    static DataTargetManager *targetManager;
    static constexpr const NodeId DataTgtLogNodeId = 0;
    static constexpr const char *DataTargetPrefix = "/dataTarget";

    DataTargetManager();
    ~DataTargetManager();
    DataTargetManager(const DataTargetManager &) = delete;
    DataTargetManager &operator=(const DataTargetManager &) = delete;

    MustCheck Status initInternal();
    MustCheck Status openOrCreateLog(bool *existed);
    void closeLog();
    MustCheck Status readTargets(int *numTargets, ExExportTarget **targets);
    MustCheck Status writeTargets();
    MustCheck Status serializeTargets(uint8_t **serializedData,
                                      size_t *size,
                                      size_t blockAlign);
    MustCheck Status deserializeTargets(const uint8_t *serializedData,
                                        size_t size,
                                        int *numTargetsFound,
                                        ExExportTarget **deserializedTargets);

    enum class ListTargetAccess {
        Shared,
        None,
    };
    MustCheck Status listTargetsInternal(const char *typePattern,
                                         const char *namePattern,
                                         XcalarApiOutput **output,
                                         size_t *listTargetsOutputSize,
                                         ListTargetAccess access);
    Status addLocalHandler(void *payload);
    Status removeLocalHandler(void *payload);

    // Calls into target specific persistence
    Status addTargetLocal(const ExExportTarget *target, bool writeThrough);

    static constexpr const char *logFilePrefix = "targetLog";
    // XXX - Determine reasonable value for this
    static constexpr const size_t MaxSerializedTargetListSize = 16 * MB;

    LogLib::Handle targetLog_;
    bool logOpen_;
    ExportTargetMgrIface *exportTargets_[ExTargetTypeLen];
};

struct ExExportGlobalContext {
    char srcTableName[XcalarApiMaxTableNameLen];
    XcalarApiTableInput tempTable;
    TupleValueDesc valueDesc;
    ExExportCreateRule createRule;
    int numNodes;
    bool schemaResolved;
    DagTypes::NodeId dagNodeId;
    DagTypes::DagId dagId;
    XcalarApiUdfContainer udfContainer;

    union {
        struct {
            int startingFileNum;
            char dirName[XcalarApiMaxFileNameLen + 1];
        } sf;
        struct {
            char appName[XcalarApiMaxAppNameLen + 1];
        } udf;
    };
    ExExportMeta exportMeta;  // VARIABLE SIZE
};

struct ExExportLocalContext {
    struct Field {
        DfFieldType type;
        int entryIndex;
    };

    int nodeNum;
    int numChunks;
    // The index into the valueDesc for each field. Check the ExExportMeta
    // for the number of fields and the valuesDesc
    Field *fieldMapping;
};

struct ExExportChunkContext {
    int chunkNum;
};

// Handles all of the general management of targets for a specific
// type. This includes target type specific parts of persistence, as well
// as maintaining the list of export targets for this type.
class ExportTargetMgrIface
{
  public:
    ExportTargetMgrIface() {}
    virtual ~ExportTargetMgrIface() {}

    // Management functions for target metadata
    virtual MustCheck Status getDefaultTargets(int *numTargets,
                                               ExExportTarget **targets) = 0;
    virtual MustCheck Status shouldPersist(const ExExportTarget *target,
                                           bool *persist) = 0;
    virtual MustCheck Status verifyTarget(const ExExportTarget *target) = 0;
    virtual MustCheck Status addTargetLocal(const ExExportTarget *target) = 0;
    virtual MustCheck Status
    removeTargetLocal(const ExExportTargetHdr *hdr) = 0;
    virtual int countTargets(const char *namePattern) = 0;
    virtual MustCheck Status listTargets(ExExportTarget *targets,
                                         int *ii,
                                         int targetsLength,
                                         const char *pattern) = 0;
};

#endif  // _DATATARGET_H_
